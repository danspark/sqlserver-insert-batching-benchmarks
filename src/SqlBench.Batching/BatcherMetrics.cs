using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Numerics;

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
    private const string MeterName = "SqlBench.Batching";
    private static readonly Meter Meter = new(MeterName);
    private static readonly UpDownCounter<long> QueueDepth = Meter.CreateUpDownCounter<long>(
        "sqlbench.batcher.queue.depth",
        "{item}",
        "Number of accepted items waiting for a batch handler.");
    private static readonly Counter<long> ItemsCompleted = Meter.CreateCounter<long>(
        "sqlbench.batcher.item.completed",
        "{item}");
    private static readonly Counter<long> ItemsFailed = Meter.CreateCounter<long>(
        "sqlbench.batcher.item.failed",
        "{item}");
    private static readonly Counter<long> Backpressure = Meter.CreateCounter<long>(
        "sqlbench.batcher.backpressure",
        "{event}");
    private static readonly Histogram<double> ChannelWait = Meter.CreateHistogram<double>(
        "sqlbench.batcher.channel.wait.duration",
        "s");
    private static readonly Histogram<double> BatchSize = Meter.CreateHistogram<double>(
        "sqlbench.batcher.batch.size",
        "{item}");
    private static readonly Histogram<double> BatchFill = Meter.CreateHistogram<double>(
        "sqlbench.batcher.batch.fill.duration",
        "s");
    private static readonly Histogram<double> Handler = Meter.CreateHistogram<double>(
        "sqlbench.batcher.handler.duration",
        "s");
    private readonly ConcurrentDistribution _waitMilliseconds = new();
    private readonly ConcurrentDistribution _batchSizes = new();
    private readonly ConcurrentDistribution _fillMilliseconds = new();
    private readonly ConcurrentDistribution _handlerMilliseconds = new();
    private long _queueDepth;
    private long _completed;
    private long _failed;
    private long _backpressure;

    public void Accepted()
    {
        Interlocked.Increment(ref _queueDepth);
        QueueDepth.Add(1);
    }

    public void Dequeued(long acceptedTimestamp)
    {
        Interlocked.Decrement(ref _queueDepth);
        QueueDepth.Add(-1);
        TimeSpan elapsed = Stopwatch.GetElapsedTime(acceptedTimestamp);
        _waitMilliseconds.Record(elapsed.TotalMilliseconds);
        ChannelWait.Record(elapsed.TotalSeconds);
    }

    public void Backpressured()
    {
        Interlocked.Increment(ref _backpressure);
        Backpressure.Add(1);
    }

    public void Batch(int size, TimeSpan fillTime, TimeSpan handlerTime)
    {
        _batchSizes.Record(size);
        _fillMilliseconds.Record(fillTime.TotalMilliseconds);
        _handlerMilliseconds.Record(handlerTime.TotalMilliseconds);
        BatchSize.Record(size);
        BatchFill.Record(fillTime.TotalSeconds);
        Handler.Record(handlerTime.TotalSeconds);
    }

    public void Completed(bool succeeded)
    {
        if (succeeded)
        {
            Interlocked.Increment(ref _completed);
            ItemsCompleted.Add(1);
        }
        else
        {
            Interlocked.Increment(ref _failed);
            ItemsFailed.Add(1);
        }
    }

    public BatcherMetricsSnapshot Snapshot()
    {
        return new BatcherMetricsSnapshot
        {
            CurrentQueueDepth = Interlocked.Read(ref _queueDepth),
            CompletedItems = Interlocked.Read(ref _completed),
            FailedItems = Interlocked.Read(ref _failed),
            BackpressureEvents = Interlocked.Read(ref _backpressure),
            ChannelWaitMilliseconds = _waitMilliseconds.Snapshot(),
            ActualBatchSize = _batchSizes.Snapshot(),
            BatchFillMilliseconds = _fillMilliseconds.Snapshot(),
            HandlerMilliseconds = _handlerMilliseconds.Snapshot()
        };
    }

    private sealed class ConcurrentDistribution
    {
        private const int Scale = 1_000;
        private const int DirectBucketCount = 256;
        private const int BucketsPerExponent = 256;
        private const int BucketCount = 8_192;
        private readonly long[] _buckets = new long[BucketCount];
        private long _count;
        private long _maximumScaled;
        private long _minimumScaled = long.MaxValue;
        private long _sumScaled;

        public void Record(double value)
        {
            long scaled = Math.Max(0, checked((long)Math.Round(value * Scale)));
            Interlocked.Increment(ref _buckets[BucketIndex(scaled)]);
            Interlocked.Increment(ref _count);
            Interlocked.Add(ref _sumScaled, scaled);
            UpdateMinimum(ref _minimumScaled, scaled);
            UpdateMaximum(ref _maximumScaled, scaled);
        }

        public DistributionSnapshot Snapshot()
        {
            long count = Interlocked.Read(ref _count);
            if (count == 0)
            {
                return new DistributionSnapshot();
            }

            return new DistributionSnapshot
            {
                Count = count,
                Minimum = Interlocked.Read(ref _minimumScaled) / (double)Scale,
                Mean = Interlocked.Read(ref _sumScaled) / (double)(count * Scale),
                P50 = Percentile(count, 0.50),
                P95 = Percentile(count, 0.95),
                P99 = Percentile(count, 0.99),
                Maximum = Interlocked.Read(ref _maximumScaled) / (double)Scale
            };
        }

        private double Percentile(long count, double percentile)
        {
            long target = Math.Max(1, checked((long)Math.Ceiling(count * percentile)));
            long cumulative = 0;
            for (int index = 0; index < _buckets.Length; index++)
            {
                cumulative += Interlocked.Read(ref _buckets[index]);
                if (cumulative >= target)
                {
                    return BucketMidpoint(index) / (double)Scale;
                }
            }

            return Interlocked.Read(ref _maximumScaled) / (double)Scale;
        }

        private static int BucketIndex(long value)
        {
            if (value < DirectBucketCount)
            {
                return (int)value;
            }

            int exponent = BitOperations.Log2((ulong)value);
            int shift = exponent - 8;
            int significant = (int)(value >> shift);
            int index = DirectBucketCount
                + (shift * BucketsPerExponent)
                + (significant - DirectBucketCount);
            return Math.Min(index, BucketCount - 1);
        }

        private static long BucketMidpoint(int index)
        {
            if (index < DirectBucketCount)
            {
                return index;
            }

            int offset = index - DirectBucketCount;
            int shift = offset / BucketsPerExponent;
            int significant = DirectBucketCount + (offset % BucketsPerExponent);
            long lower = (long)significant << shift;
            long width = 1L << shift;
            return lower + ((width - 1) / 2);
        }

        private static void UpdateMinimum(ref long target, long value)
        {
            long observed = Volatile.Read(ref target);
            while (value < observed)
            {
                long previous = Interlocked.CompareExchange(ref target, value, observed);
                if (previous == observed)
                {
                    return;
                }

                observed = previous;
            }
        }

        private static void UpdateMaximum(ref long target, long value)
        {
            long observed = Volatile.Read(ref target);
            while (value > observed)
            {
                long previous = Interlocked.CompareExchange(ref target, value, observed);
                if (previous == observed)
                {
                    return;
                }

                observed = previous;
            }
        }
    }
}
