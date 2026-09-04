using System.Diagnostics;
using SqlBench.Batching;

namespace SqlBench.Worker;

internal sealed class ImmediateBatcher<T>(IBatchHandler<T> handler) : IProcessBatcher<T>
{
    private readonly IBatchHandler<T> _handler = handler;
    private readonly object _sync = new();
    private readonly List<double> _handlerMilliseconds = [];
    private long _completed;
    private long _failed;

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async ValueTask<BatchSubmission> SubmitAsync(T item, CancellationToken cancellationToken = default)
    {
        long start = Stopwatch.GetTimestamp();
        BatchHandlerResult result = await _handler.HandleAsync([item], cancellationToken).ConfigureAwait(false);
        lock (_sync)
        {
            _handlerMilliseconds.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }

        if (result.Count != 1)
        {
            throw new InvalidOperationException("The immediate handler must return one item result.");
        }

        Exception? error = result.GetError(0);
        if (error is null)
        {
            Interlocked.Increment(ref _completed);
            return new BatchSubmission(ValueTask.CompletedTask);
        }

        Interlocked.Increment(ref _failed);
        return new BatchSubmission(ValueTask.FromException(error));
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public BatcherMetricsSnapshot GetMetrics()
    {
        double[] values;
        lock (_sync)
        {
            values = [.. _handlerMilliseconds];
        }

        return new BatcherMetricsSnapshot
        {
            CompletedItems = Interlocked.Read(ref _completed),
            FailedItems = Interlocked.Read(ref _failed),
            ChannelWaitMilliseconds = new DistributionSnapshot(),
            ActualBatchSize = DescribeOnes(values.Length),
            BatchFillMilliseconds = new DistributionSnapshot(),
            HandlerMilliseconds = Describe(values)
        };
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static DistributionSnapshot DescribeOnes(int count) => count == 0
        ? new DistributionSnapshot()
        : new DistributionSnapshot
        {
            Count = count,
            Minimum = 1,
            Mean = 1,
            P50 = 1,
            P95 = 1,
            P99 = 1,
            Maximum = 1
        };

    private static DistributionSnapshot Describe(double[] values)
    {
        if (values.Length == 0)
        {
            return new DistributionSnapshot();
        }

        Array.Sort(values);
        return new DistributionSnapshot
        {
            Count = values.Length,
            Minimum = values[0],
            Mean = values.Average(),
            P50 = At(values, 0.50),
            P95 = At(values, 0.95),
            P99 = At(values, 0.99),
            Maximum = values[^1]
        };
    }

    private static double At(double[] values, double percentile) =>
        values[Math.Clamp((int)Math.Ceiling(percentile * values.Length) - 1, 0, values.Length - 1)];
}
