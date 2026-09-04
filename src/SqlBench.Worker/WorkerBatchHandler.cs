using System.Collections.Concurrent;
using System.Diagnostics;
using SqlBench.Batching;
using SqlBench.Core;

namespace SqlBench.Worker;

internal sealed class WorkerBatchHandler : IBatchHandler<PendingMessage>
{
    private readonly string _connectionString;
    private readonly ISqlInsertStrategy? _strategy;
    private readonly SqlInsertOptions _options;
    private readonly ConcurrentBag<double> _sqlExecutionMilliseconds;
    private readonly ConcurrentBag<double> _transactionMilliseconds;
    private long _committedRows;

    public WorkerBatchHandler(
        Scenario scenario,
        string connectionString,
        ConcurrentBag<double> sqlExecutionMilliseconds,
        ConcurrentBag<double> transactionMilliseconds)
    {
        _connectionString = connectionString;
        _strategy = scenario.Mode == WorkloadMode.NoOpQueue
            ? null
            : SqlInsertStrategyFactory.Create(scenario.Strategy);
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

    public async ValueTask<BatchHandlerResult> HandleAsync(
        IReadOnlyList<PendingMessage> items,
        CancellationToken cancellationToken)
    {
        try
        {
            if (_strategy is not null)
            {
                SqlWriteTiming timing = await _strategy.InsertAsync(
                    _connectionString,
                    items.Select(static pending => pending.Message).ToArray(),
                    _options,
                    cancellationToken).ConfigureAwait(false);
                _sqlExecutionMilliseconds.Add(timing.SqlExecution.TotalMilliseconds);
                _transactionMilliseconds.Add(timing.Transaction.TotalMilliseconds);
            }

            long committed = Stopwatch.GetTimestamp();
            foreach (PendingMessage item in items)
            {
                item.CommittedTimestamp = committed;
            }

            Interlocked.Add(ref _committedRows, items.Count);
            return BatchHandlerResult.Success(items.Count);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            return BatchHandlerResult.Failure(items.Count, error);
        }
    }
}
