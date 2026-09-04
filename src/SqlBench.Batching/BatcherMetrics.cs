using System.Diagnostics;

namespace SqlBench.Batching;

public sealed record DistributionSnapshot
{
    public long Count { get; init; }

    public double Minimum { get; init; }

    public double Mean { get; init; }

    public double P50 { get; init; }

    public double P95 { get; init; }

    public double P99 { get; init; }

    public double Maximum { get; init; }
}

public sealed record BatcherMetricsSnapshot
{
    public long CurrentQueueDepth { get; init; }

    public long CompletedItems { get; init; }

    public long FailedItems { get; init; }

    public long BackpressureEvents { get; init; }

    public required DistributionSnapshot ChannelWaitMilliseconds { get; init; }

    public required DistributionSnapshot ActualBatchSize { get; init; }

    public required DistributionSnapshot BatchFillMilliseconds { get; init; }

    public required DistributionSnapshot HandlerMilliseconds { get; init; }
}

internal sealed class BatcherMetrics
{
    private readonly object _sync = new();
    private readonly List<double> _waitMilliseconds = [];
    private readonly List<double> _batchSizes = [];
    private readonly List<double> _fillMilliseconds = [];
    private readonly List<double> _handlerMilliseconds = [];
    private long _queueDepth;
    private long _completed;
    private long _failed;
    private long _backpressure;

    public void Accepted() => Interlocked.Increment(ref _queueDepth);

    public void Dequeued(long acceptedTimestamp)
    {
        Interlocked.Decrement(ref _queueDepth);
        Add(_waitMilliseconds, Stopwatch.GetElapsedTime(acceptedTimestamp).TotalMilliseconds);
    }

    public void Backpressured() => Interlocked.Increment(ref _backpressure);

    public void Batch(int size, TimeSpan fillTime, TimeSpan handlerTime)
    {
        lock (_sync)
        {
            _batchSizes.Add(size);
            _fillMilliseconds.Add(fillTime.TotalMilliseconds);
            _handlerMilliseconds.Add(handlerTime.TotalMilliseconds);
        }
    }

    public void Completed(bool succeeded)
    {
        Interlocked.Increment(ref succeeded ? ref _completed : ref _failed);
    }

    public BatcherMetricsSnapshot Snapshot()
    {
        lock (_sync)
        {
            return new BatcherMetricsSnapshot
            {
                CurrentQueueDepth = Interlocked.Read(ref _queueDepth),
                CompletedItems = Interlocked.Read(ref _completed),
                FailedItems = Interlocked.Read(ref _failed),
                BackpressureEvents = Interlocked.Read(ref _backpressure),
                ChannelWaitMilliseconds = Describe(_waitMilliseconds),
                ActualBatchSize = Describe(_batchSizes),
                BatchFillMilliseconds = Describe(_fillMilliseconds),
                HandlerMilliseconds = Describe(_handlerMilliseconds)
            };
        }
    }

    private void Add(List<double> values, double value)
    {
        lock (_sync)
        {
            values.Add(value);
        }
    }

    private static DistributionSnapshot Describe(List<double> source)
    {
        if (source.Count == 0)
        {
            return new DistributionSnapshot();
        }

        double[] values = [.. source];
        Array.Sort(values);
        return new DistributionSnapshot
        {
            Count = values.Length,
            Minimum = values[0],
            Mean = values.Average(),
            P50 = Percentile(values, 0.50),
            P95 = Percentile(values, 0.95),
            P99 = Percentile(values, 0.99),
            Maximum = values[^1]
        };
    }

    private static double Percentile(double[] sorted, double percentile)
    {
        int index = (int)Math.Ceiling(percentile * sorted.Length) - 1;
        return sorted[Math.Clamp(index, 0, sorted.Length - 1)];
    }
}
