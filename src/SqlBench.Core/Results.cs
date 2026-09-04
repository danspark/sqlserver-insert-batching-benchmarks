namespace SqlBench.Core;

public sealed record PercentileSummary
{
    public long Count { get; init; }
    public double Minimum { get; init; }
    public double Mean { get; init; }
    public double P50 { get; init; }
    public double P95 { get; init; }
    public double P99 { get; init; }
    public double Maximum { get; init; }

    public static PercentileSummary FromMicroseconds(IEnumerable<long> source)
    {
        long[] values = source.Order().ToArray();
        return values.Length == 0 ? new PercentileSummary() : new PercentileSummary
        {
            Count = values.Length,
            Minimum = values[0] / 1_000.0,
            Mean = values.Average() / 1_000.0,
            P50 = At(values, 0.50) / 1_000.0,
            P95 = At(values, 0.95) / 1_000.0,
            P99 = At(values, 0.99) / 1_000.0,
            Maximum = values[^1] / 1_000.0
        };
    }

    public static PercentileSummary FromMilliseconds(IEnumerable<double> source)
    {
        double[] values = source.Order().ToArray();
        return values.Length == 0 ? new PercentileSummary() : new PercentileSummary
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

    private static long At(long[] values, double percentile) =>
        values[Math.Clamp((int)Math.Ceiling(percentile * values.Length) - 1, 0, values.Length - 1)];

    private static double At(double[] values, double percentile) =>
        values[Math.Clamp((int)Math.Ceiling(percentile * values.Length) - 1, 0, values.Length - 1)];
}

public sealed record WorkerConfiguration
{
    public required int WorkerId { get; init; }
    public required RuntimeSettings Runtime { get; init; }
    public required Scenario Scenario { get; init; }
    public required string GateFile { get; init; }
    public required string ReadyFile { get; init; }
    public required string CompletionFile { get; init; }
    public required string ResultFile { get; init; }
}

public sealed record WorkerRunResult
{
    public required int WorkerId { get; init; }
    public long DeliveredMessages { get; init; }
    public long CommittedRows { get; init; }
    public long AcknowledgedMessages { get; init; }
    public long RedeliveredMessages { get; init; }
    public long LastAcknowledgmentTimestamp { get; init; }
    public long StopwatchFrequency { get; init; }
    public required long[] DeliveryToCommitMicroseconds { get; init; }
    public required long[] DeliveryToAcknowledgmentMicroseconds { get; init; }
    public required double[] SqlExecutionMilliseconds { get; init; }
    public required double[] TransactionMilliseconds { get; init; }
    public SqlRequestMetricsSnapshot SqlRequests { get; init; } = new();
    public required SqlBench.Batching.BatcherMetricsSnapshot BatcherMetrics { get; init; }
    public required ProcessMetrics Process { get; init; }
    public required IReadOnlyList<string> Errors { get; init; }
}

public sealed record SqlRequestMetricsSnapshot
{
    public long RequestCount { get; init; }
    public long CommandCount { get; init; }
    public long SingleCommandRequestCount { get; init; }
    public long ActiveRequests { get; init; }
    public long MaximumConcurrentRequests { get; init; }
    public long CurrentCoordinatorQueueDepth { get; init; }
    public long MaximumCoordinatorQueueDepth { get; init; }
    public long CoordinatorBackpressureEvents { get; init; }
    public double MeanCommandsPerRequest { get; init; }
    public int P50CommandsPerRequest { get; init; }
    public int P95CommandsPerRequest { get; init; }
    public int P99CommandsPerRequest { get; init; }
    public int MaximumCommandsPerRequest { get; init; }
    public long[] CommandsPerRequestCounts { get; init; } = [];
    public PercentileSummary CoordinatorQueueWaitMilliseconds { get; init; } = new();
}

public sealed record ProcessMetrics
{
    public double CpuSeconds { get; init; }
    public long PeakWorkingSetBytes { get; init; }
    public long AllocatedBytes { get; init; }
    public int Generation0Collections { get; init; }
    public int Generation1Collections { get; init; }
    public int Generation2Collections { get; init; }
}

public sealed record CorrectnessResult
{
    public bool Passed { get; init; }
    public required DatabaseVerification Database { get; init; }
    public required QueueState Queue { get; init; }
    public long HiddenWorkerFailureCount { get; init; }
}

public sealed record ScenarioResult
{
    public required string ScenarioName { get; init; }
    public required string GitCommit { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required Scenario Configuration { get; init; }
    public int SerializedMessageBytes { get; init; }
    public int ApproximateDatabaseRowBytes { get; init; }
    public double DurationSeconds { get; init; }
    public long DeliveredMessages { get; init; }
    public long CommittedRows { get; init; }
    public long AcknowledgedMessages { get; init; }
    public double CommittedRowsPerSecond { get; init; }
    public double AcknowledgedMessagesPerSecond { get; init; }
    public required PercentileSummary DeliveryToCommitMilliseconds { get; init; }
    public required PercentileSummary DeliveryToAcknowledgmentMilliseconds { get; init; }
    public required PercentileSummary SqlExecutionMilliseconds { get; init; }
    public required PercentileSummary TransactionMilliseconds { get; init; }
    public required IReadOnlyList<WorkerRunResult> Workers { get; init; }
    public required DatabaseMetrics SqlServerBefore { get; init; }
    public required DatabaseMetrics SqlServerAfter { get; init; }
    public required RabbitMqMetrics RabbitMqBefore { get; init; }
    public required RabbitMqMetrics RabbitMqAfter { get; init; }
    public required QueueState QueueBefore { get; init; }
    public required QueueState QueueAfter { get; init; }
    public required CorrectnessResult Correctness { get; init; }
    public required IReadOnlyList<string> Errors { get; init; }
    public bool Valid => Correctness.Passed && Errors.Count == 0;
}
