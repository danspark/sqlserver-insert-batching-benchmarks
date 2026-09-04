using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;

namespace SqlBench.Core;

internal interface ISqlBatchCommandBuilder
{
    SqlBatchCommand CreateBatchCommand(
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options);
}

internal sealed class SqlBatchCoalescingInsertStrategy : ISqlInsertStrategy
{
    private readonly ISqlInsertStrategy _inner;
    private readonly ISqlBatchCommandBuilder _commandBuilder;
    private readonly int _maximumCommands;
    private readonly TimeSpan _maximumDelay;
    private readonly Channel<Submission> _submissions;
    private readonly SqlRequestMetrics _requestMetrics;
    private readonly Task[] _consumers;
    private readonly object _bindingSync = new();
    private CoordinatorBinding? _binding;
    private int _stopping;

    public SqlBatchCoalescingInsertStrategy(
        ISqlInsertStrategy inner,
        ISqlBatchCommandBuilder commandBuilder,
        int maximumCommands,
        TimeSpan maximumDelay,
        int requestConcurrency)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumCommands, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            maximumCommands,
            SqlLimits.MaximumSqlBatchCommandsPerRequest);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maximumDelay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(requestConcurrency, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            requestConcurrency,
            SqlLimits.MaximumSqlBatchRequestConcurrency);

        _inner = inner;
        _commandBuilder = commandBuilder;
        _maximumCommands = maximumCommands;
        _maximumDelay = maximumDelay;
        _requestMetrics = new SqlRequestMetrics(maximumCommands);
        int channelCapacity = checked(maximumCommands * requestConcurrency * 2);
        _submissions = Channel.CreateBounded<Submission>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = requestConcurrency == 1,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        _consumers = Enumerable.Range(0, requestConcurrency)
            .Select(_ => Task.Run(ConsumeAsync))
            .ToArray();
    }

    public InsertStrategyKind Kind => _inner.Kind;

    public int MaximumBatchSize => _inner.MaximumBatchSize;

    public SqlRequestMetricsSnapshot GetRequestMetrics() => _requestMetrics.Snapshot();

    public async Task<SqlWriteTiming> InsertAsync(
        string connectionString,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(options);
        if (rows.Count == 0 || rows.Count > MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(nameof(rows));
        }

        cancellationToken.ThrowIfCancellationRequested();
        CoordinatorBinding binding = BindOrVerify(connectionString, options);

        if (Volatile.Read(ref _stopping) != 0)
        {
            throw new InvalidOperationException("The SqlBatch coordinator is not accepting work.");
        }

        var submission = new Submission(binding, rows);
        _requestMetrics.CoordinatorSubmissionStarted(Kind);
        bool submitted = false;
        try
        {
            if (!_submissions.Writer.TryWrite(submission))
            {
                _requestMetrics.CoordinatorBackpressured(Kind);
                await _submissions.Writer.WriteAsync(submission, cancellationToken).ConfigureAwait(false);
            }

            submitted = true;
        }
        finally
        {
            if (!submitted)
            {
                _requestMetrics.CoordinatorSubmissionAbandoned(Kind);
            }
        }

        return await submission.Completion.Task.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
        {
            return;
        }

        _submissions.Writer.TryComplete();
        try
        {
            await Task.WhenAll(_consumers).ConfigureAwait(false);
        }
        finally
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task ConsumeAsync()
    {
        var group = new List<Submission>(_maximumCommands);
        while (await _submissions.Reader.WaitToReadAsync().ConfigureAwait(false))
        {
            if (!_submissions.Reader.TryRead(out Submission? first))
            {
                continue;
            }

            _requestMetrics.CoordinatorSubmissionDequeued(first.SubmittedTimestamp, Kind);
            group.Clear();
            group.Add(first);
            long fillStarted = Stopwatch.GetTimestamp();
            await FillAsync(group, fillStarted).ConfigureAwait(false);
            TimeSpan coalescingDelay = Stopwatch.GetElapsedTime(fillStarted);
            await ExecuteAsync(group, coalescingDelay).ConfigureAwait(false);
        }
    }

    private async Task FillAsync(List<Submission> group, long fillStarted)
    {
        while (group.Count < _maximumCommands)
        {
            while (group.Count < _maximumCommands && _submissions.Reader.TryRead(out Submission? submission))
            {
                _requestMetrics.CoordinatorSubmissionDequeued(submission.SubmittedTimestamp, Kind);
                group.Add(submission);
            }

            TimeSpan remaining = _maximumDelay - Stopwatch.GetElapsedTime(fillStarted);
            if (group.Count >= _maximumCommands || remaining <= TimeSpan.Zero)
            {
                return;
            }

            Task<bool> available = _submissions.Reader.WaitToReadAsync().AsTask();
            Task delay = Task.Delay(remaining);
            Task completed = await Task.WhenAny(available, delay).ConfigureAwait(false);
            if (completed == delay || !await available.ConfigureAwait(false))
            {
                return;
            }
        }
    }

    private async Task ExecuteAsync(List<Submission> submissions, TimeSpan coalescingDelay)
    {
        CoordinatorBinding binding = submissions[0].Binding;
        try
        {
            var commands = new SqlBatchCommand[submissions.Count];
            for (int index = 0; index < submissions.Count; index++)
            {
                SqlBatchCommand command = _commandBuilder.CreateBatchCommand(
                    submissions[index].Rows,
                    binding.Options);
                commands[index] = command;
            }

            SqlBatchRequestOutcome outcome = await SqlBatchRequestExecutor.ExecuteAsync(
                binding.ConnectionString,
                binding.Options,
                commands,
                coalescingDelay,
                Kind,
                _requestMetrics).ConfigureAwait(false);
            CompleteSubmissions(submissions, commands, outcome.Timing, outcome.Error);
        }
        catch (Exception error)
        {
            foreach (Submission submission in submissions)
            {
                submission.Completion.TrySetException(error);
            }
        }
    }

    private static void CompleteSubmissions(
        IReadOnlyList<Submission> submissions,
        SqlBatchCommand[] commands,
        SqlWriteTiming timing,
        Exception? requestError)
    {
        for (int index = 0; index < submissions.Count; index++)
        {
            if (SqlInsertStrategyBase.BatchCommandCommitted(commands[index]))
            {
                submissions[index].Completion.TrySetResult(timing);
                continue;
            }

            Exception error = requestError is SqlException sqlError
                && ReferenceEquals(sqlError.BatchCommand, commands[index])
                    ? sqlError
                    : new InvalidOperationException(
                        "The SqlBatch request completed without committing this command.",
                        requestError);
            submissions[index].Completion.TrySetException(error);
        }
    }

    private CoordinatorBinding BindOrVerify(string connectionString, SqlInsertOptions options)
    {
        CoordinatorBinding? binding = Volatile.Read(ref _binding);
        if (binding is null)
        {
            lock (_bindingSync)
            {
                binding = _binding;
                if (binding is null)
                {
                    binding = new CoordinatorBinding(connectionString, options);
                    Volatile.Write(ref _binding, binding);
                }
            }
        }

        if (!string.Equals(binding.ConnectionString, connectionString, StringComparison.Ordinal)
            || binding.Options != options)
        {
            throw new InvalidOperationException(
                "The SqlBatch coordinator is already bound to a different connection or SQL options.");
        }

        return binding;
    }

    private sealed record CoordinatorBinding(string ConnectionString, SqlInsertOptions Options);

    private sealed class Submission(
        CoordinatorBinding binding,
        IReadOnlyList<BenchmarkMessage> rows)
    {
        public CoordinatorBinding Binding { get; } = binding;

        public IReadOnlyList<BenchmarkMessage> Rows { get; } = rows;

        public long SubmittedTimestamp { get; } = Stopwatch.GetTimestamp();

        public TaskCompletionSource<SqlWriteTiming> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}

internal readonly record struct SqlBatchRequestOutcome(
    SqlWriteTiming Timing,
    Exception? Error);

internal static class SqlBatchRequestExecutor
{
    public static async Task<SqlBatchRequestOutcome> ExecuteAsync(
        string connectionString,
        SqlInsertOptions options,
        IReadOnlyList<SqlBatchCommand> commands,
        TimeSpan coalescingDelay,
        InsertStrategyKind strategy,
        SqlRequestMetrics metrics)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(CancellationToken.None).ConfigureAwait(false);
        using var batch = new SqlBatch(connection)
        {
            Timeout = options.CommandTimeoutSeconds
        };
        for (int index = 0; index < commands.Count; index++)
        {
            batch.BatchCommands.Add(commands[index]);
        }

        return await ExecuteBatchAsync(
            batch,
            commands.Count,
            coalescingDelay,
            strategy,
            metrics).ConfigureAwait(false);
    }

    private static async Task<SqlBatchRequestOutcome> ExecuteBatchAsync(
        SqlBatch batch,
        int commandCount,
        TimeSpan coalescingDelay,
        InsertStrategyKind strategy,
        SqlRequestMetrics metrics)
    {
        long requestStarted = Stopwatch.GetTimestamp();
        Exception? requestError = null;
        metrics.RequestStarted(strategy, SqlExecutionKind.SqlBatch);
        try
        {
            await batch.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            requestError = error;
        }
        finally
        {
            metrics.RequestCompleted(strategy, SqlExecutionKind.SqlBatch);
        }

        TimeSpan requestDuration = Stopwatch.GetElapsedTime(requestStarted);
        metrics.Record(
            commandCount,
            requestDuration,
            coalescingDelay,
            strategy,
            SqlExecutionKind.SqlBatch);
        return new SqlBatchRequestOutcome(
            new SqlWriteTiming
            {
                SqlExecution = requestDuration,
                Transaction = requestDuration
            },
            requestError);
    }

}
