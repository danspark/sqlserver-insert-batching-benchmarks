using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Numerics;

namespace SqlBench.Core;

internal sealed class SqlRequestMetrics(int maximumCommandsPerRequest)
{
    private static readonly Meter Meter = new("SqlBench.SqlClient");
    private static readonly Counter<long> Requests = Meter.CreateCounter<long>(
        "sqlbench.sqlclient.dml_execute",
        "{call}");
    private static readonly Counter<long> Commands = Meter.CreateCounter<long>(
        "sqlbench.sqlclient.command",
        "{command}");
    private static readonly Histogram<int> CommandsPerRequest = Meter.CreateHistogram<int>(
        "sqlbench.sqlclient.commands_per_request",
        "{command}");
    private static readonly Histogram<double> RequestDuration = Meter.CreateHistogram<double>(
        "sqlbench.sqlclient.dml_execute.duration",
        "s");
    private static readonly Histogram<double> CoalescingDelay = Meter.CreateHistogram<double>(
        "sqlbench.sqlclient.coalescing.delay",
        "s");
    private static readonly UpDownCounter<long> ActiveRequests = Meter.CreateUpDownCounter<long>(
        "sqlbench.sqlclient.dml_execute.active",
        "{call}");
    private static readonly UpDownCounter<long> CoordinatorQueueDepth = Meter.CreateUpDownCounter<long>(
        "sqlbench.sqlclient.coordinator.queue.depth",
        "{submission}");
    private static readonly Counter<long> CoordinatorBackpressure = Meter.CreateCounter<long>(
        "sqlbench.sqlclient.coordinator.backpressure",
        "{event}");
    private static readonly Histogram<double> CoordinatorQueueWait = Meter.CreateHistogram<double>(
        "sqlbench.sqlclient.coordinator.queue.wait.duration",
        "s");
    private readonly long[] _commandCountBuckets = new long[maximumCommandsPerRequest + 1];
    private readonly ConcurrentDurationDistribution _coordinatorQueueWait = new();
    private long _activeRequests;
    private long _commandCount;
    private long _coordinatorBackpressureEvents;
    private long _coordinatorQueueDepth;
    private long _maximumConcurrentRequests;
    private long _maximumCoordinatorQueueDepth;
    private long _requestCount;
    private long _singleCommandRequestCount;

    public void CoordinatorSubmissionStarted(InsertStrategyKind strategy)
    {
        long depth = Interlocked.Increment(ref _coordinatorQueueDepth);
        UpdateMaximum(ref _maximumCoordinatorQueueDepth, depth);
        TagList tags = Tags(strategy, SqlExecutionKind.SqlBatch);
        CoordinatorQueueDepth.Add(1, tags);
    }

    public void CoordinatorSubmissionAbandoned(InsertStrategyKind strategy)
    {
        Interlocked.Decrement(ref _coordinatorQueueDepth);
        TagList tags = Tags(strategy, SqlExecutionKind.SqlBatch);
        CoordinatorQueueDepth.Add(-1, tags);
    }

    public void CoordinatorSubmissionDequeued(long submissionTimestamp, InsertStrategyKind strategy)
    {
        Interlocked.Decrement(ref _coordinatorQueueDepth);
        TimeSpan wait = Stopwatch.GetElapsedTime(submissionTimestamp);
        _coordinatorQueueWait.Record(wait.TotalMilliseconds);
        TagList tags = Tags(strategy, SqlExecutionKind.SqlBatch);
        CoordinatorQueueDepth.Add(-1, tags);
        CoordinatorQueueWait.Record(wait.TotalSeconds, tags);
    }

    public void CoordinatorBackpressured(InsertStrategyKind strategy)
    {
        Interlocked.Increment(ref _coordinatorBackpressureEvents);
        TagList tags = Tags(strategy, SqlExecutionKind.SqlBatch);
        CoordinatorBackpressure.Add(1, tags);
    }

    public void RequestStarted(InsertStrategyKind strategy, SqlExecutionKind execution)
    {
        long active = Interlocked.Increment(ref _activeRequests);
        UpdateMaximum(ref _maximumConcurrentRequests, active);
        TagList tags = Tags(strategy, execution);
        ActiveRequests.Add(1, tags);
    }

    public void RequestCompleted(InsertStrategyKind strategy, SqlExecutionKind execution)
    {
        Interlocked.Decrement(ref _activeRequests);
        TagList tags = Tags(strategy, execution);
        ActiveRequests.Add(-1, tags);
    }

    public void Record(
        int commandCount,
        TimeSpan duration,
        TimeSpan coalescingDelay,
        InsertStrategyKind strategy,
        SqlExecutionKind execution)
    {
        if ((uint)(commandCount - 1) >= (uint)maximumCommandsPerRequest)
        {
            throw new ArgumentOutOfRangeException(nameof(commandCount));
        }

        Interlocked.Increment(ref _requestCount);
        Interlocked.Add(ref _commandCount, commandCount);
        Interlocked.Increment(ref _commandCountBuckets[commandCount]);
        if (commandCount == 1)
        {
            Interlocked.Increment(ref _singleCommandRequestCount);
        }

        TagList tags = default;
        tags.Add("sqlbench.strategy", strategy.ToString());
        tags.Add("sqlbench.execution", execution.ToString());
        Requests.Add(1, tags);
        Commands.Add(commandCount, tags);
        CommandsPerRequest.Record(commandCount, tags);
        RequestDuration.Record(duration.TotalSeconds, tags);
        CoalescingDelay.Record(coalescingDelay.TotalSeconds, tags);
    }

    public SqlRequestMetricsSnapshot Snapshot()
    {
        long requestCount = Interlocked.Read(ref _requestCount);
        long commandCount = Interlocked.Read(ref _commandCount);
        return new SqlRequestMetricsSnapshot
        {
            RequestCount = requestCount,
            CommandCount = commandCount,
            SingleCommandRequestCount = Interlocked.Read(ref _singleCommandRequestCount),
            ActiveRequests = Interlocked.Read(ref _activeRequests),
            MaximumConcurrentRequests = Interlocked.Read(ref _maximumConcurrentRequests),
            CurrentCoordinatorQueueDepth = Interlocked.Read(ref _coordinatorQueueDepth),
            MaximumCoordinatorQueueDepth = Interlocked.Read(ref _maximumCoordinatorQueueDepth),
            CoordinatorBackpressureEvents = Interlocked.Read(ref _coordinatorBackpressureEvents),
            MeanCommandsPerRequest = requestCount == 0 ? 0 : commandCount / (double)requestCount,
            P50CommandsPerRequest = Percentile(requestCount, 0.50),
            P95CommandsPerRequest = Percentile(requestCount, 0.95),
            P99CommandsPerRequest = Percentile(requestCount, 0.99),
            MaximumCommandsPerRequest = Maximum(),
            CommandsPerRequestCounts = SnapshotBuckets(),
            CoordinatorQueueWaitMilliseconds = _coordinatorQueueWait.Snapshot()
        };
    }

    private static TagList Tags(InsertStrategyKind strategy, SqlExecutionKind execution)
    {
        TagList tags = default;
        tags.Add("sqlbench.strategy", strategy.ToString());
        tags.Add("sqlbench.execution", execution.ToString());
        return tags;
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

    private long[] SnapshotBuckets()
    {
        var buckets = new long[_commandCountBuckets.Length];
        for (int index = 0; index < buckets.Length; index++)
        {
            buckets[index] = Interlocked.Read(ref _commandCountBuckets[index]);
        }

        return buckets;
    }

    private int Percentile(long requestCount, double percentile)
    {
        if (requestCount == 0)
        {
            return 0;
        }

        long target = Math.Max(1, checked((long)Math.Ceiling(requestCount * percentile)));
        long cumulative = 0;
        for (int commandCount = 1; commandCount < _commandCountBuckets.Length; commandCount++)
        {
            cumulative += Interlocked.Read(ref _commandCountBuckets[commandCount]);
            if (cumulative >= target)
            {
                return commandCount;
            }
        }

        return Maximum();
    }

    private int Maximum()
    {
        for (int commandCount = _commandCountBuckets.Length - 1; commandCount > 0; commandCount--)
        {
            if (Interlocked.Read(ref _commandCountBuckets[commandCount]) != 0)
            {
                return commandCount;
            }
        }

        return 0;
    }

    private sealed class ConcurrentDurationDistribution
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

        public void Record(double milliseconds)
        {
            long scaled = Math.Max(0, checked((long)Math.Round(milliseconds * Scale)));
            Interlocked.Increment(ref _buckets[BucketIndex(scaled)]);
            Interlocked.Increment(ref _count);
            Interlocked.Add(ref _sumScaled, scaled);
            UpdateMinimum(ref _minimumScaled, scaled);
            UpdateMaximum(ref _maximumScaled, scaled);
        }

        public PercentileSummary Snapshot()
        {
            long count = Interlocked.Read(ref _count);
            if (count == 0)
            {
                return new PercentileSummary();
            }

            return new PercentileSummary
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
    }
}
