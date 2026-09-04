using System.Diagnostics;
using SqlBench.Batching;
using SqlBench.Core;

namespace SqlBench.Worker;

internal sealed class WorkerBatchHandler : IBatchHandler<PendingMessage>, IAsyncDisposable
{
    private readonly string _connectionString;
    private readonly ISqlInsertStrategy? _strategy;
    private readonly SqlInsertOptions _options;
    private readonly FixedConcurrentBuffer<double> _sqlExecutionMilliseconds;
    private readonly FixedConcurrentBuffer<double> _transactionMilliseconds;
    private long _committedRows;

    public WorkerBatchHandler(
        Scenario scenario,
        string connectionString,
        FixedConcurrentBuffer<double> sqlExecutionMilliseconds,
        FixedConcurrentBuffer<double> transactionMilliseconds)
    {
        _connectionString = connectionString;
        _strategy = scenario.Mode == WorkloadMode.NoOpQueue
            ? null
            : SqlInsertStrategyFactory.Create(
                scenario.Strategy,
                scenario.SqlExecution,
                scenario.SqlBatchMaximumCommands,
                scenario.SqlBatchMaximumDelayMilliseconds,
                scenario.SqlBatchRequestConcurrency);
        _options = new SqlInsertOptions
        {
            CommandTimeoutSeconds = scenario.CommandTimeoutSeconds,
            BulkCopyTimeoutSeconds = scenario.BulkCopyTimeoutSeconds,
            BulkCopyTableLock = scenario.BulkCopyTableLock,
            BulkCopyEnableStreaming = scenario.BulkCopyEnableStreaming
        };
        _sqlExecutionMilliseconds = sqlExecutionMilliseconds;
        _transactionMilliseconds = transactionMilliseconds;
    }

    public long CommittedRows => Interlocked.Read(ref _committedRows);

    public SqlRequestMetricsSnapshot GetSqlRequestMetrics() =>
        _strategy?.GetRequestMetrics() ?? new SqlRequestMetricsSnapshot();

    public ValueTask DisposeAsync() => _strategy?.DisposeAsync() ?? ValueTask.CompletedTask;

    public async ValueTask<BatchHandlerResult> HandleAsync(
        IReadOnlyList<PendingMessage> items,
        CancellationToken cancellationToken)
    {
        try
        {
            if (_strategy is not null)
            {
                var messages = new MessageBatch(items);
                SqlWriteTiming timing = await _strategy.InsertAsync(
                    _connectionString,
                    messages,
                    _options,
                    cancellationToken).ConfigureAwait(false);
                _sqlExecutionMilliseconds.Add(timing.SqlExecution.TotalMilliseconds);
                _transactionMilliseconds.Add(timing.Transaction.TotalMilliseconds);
                WorkerTelemetry.SqlExecution.Record(timing.SqlExecution.TotalSeconds);
                WorkerTelemetry.Transaction.Record(timing.Transaction.TotalSeconds);
            }

            long committed = Stopwatch.GetTimestamp();
            for (int index = 0; index < items.Count; index++)
            {
                items[index].CommittedTimestamp = committed;
            }

            Interlocked.Add(ref _committedRows, items.Count);
            if (_strategy is not null)
            {
                WorkerTelemetry.Committed.Add(items.Count);
            }

            return BatchHandlerResult.Success(items.Count);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            return BatchHandlerResult.Failure(items.Count, error);
        }
    }

    private sealed class MessageBatch(IReadOnlyList<PendingMessage> items) : IReadOnlyList<BenchmarkMessage>
    {
        private readonly IReadOnlyList<PendingMessage> _items = items;

        public int Count => _items.Count;

        public BenchmarkMessage this[int index] => _items[index].Message;

        public IEnumerator<BenchmarkMessage> GetEnumerator()
        {
            for (int index = 0; index < _items.Count; index++)
            {
                yield return _items[index].Message;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
