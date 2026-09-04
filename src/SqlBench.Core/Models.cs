namespace SqlBench.Core;

public enum InsertStrategyKind
{
    Individual,
    TableValuedParameter,
    MultipleInsertStatements,
    MultiRowValues,
    BulkCopy
}

public enum BatchingKind
{
    None,
    Channel,
    LockSwap
}

public enum WorkloadMode
{
    Queue,
    NoOpQueue,
    DirectDatabase
}

public enum ParentDistribution
{
    Uniform,
    ModerateSkew,
    HotParent
}

public sealed record BenchmarkMessage
{
    public required Guid MessageId { get; init; }

    public required int ParentId { get; init; }

    public required Guid CorrelationId { get; init; }

    public required DateTime OccurredAt { get; init; }

    public required int SequenceNo { get; init; }

    public required long CounterValue { get; init; }

    public required short Priority { get; init; }

    public required decimal Amount { get; init; }

    public required bool IsActive { get; init; }

    public required string Code { get; init; }

    public required string Description { get; init; }

    public required byte[] PayloadHash { get; init; }

    public string? OptionalNote { get; init; }
}

public sealed record Scenario
{
    public required string Name { get; init; }

    public string Stage { get; init; } = "broad";

    public WorkloadMode Mode { get; init; } = WorkloadMode.Queue;

    public InsertStrategyKind Strategy { get; init; }

    public BatchingKind Batching { get; init; } = BatchingKind.Channel;

    public ParentDistribution Distribution { get; init; } = ParentDistribution.Uniform;

    public int Seed { get; init; } = 0x5EED_2026;

    public int RowCount { get; init; } = 10_000;

    public int WorkerInstances { get; init; } = 1;

    public int WritersPerInstance { get; init; } = 1;

    public int BatchSize { get; init; } = 100;

    public int MaximumBatchingDelayMilliseconds { get; init; } = 5;

    public int ChannelCapacity { get; init; } = 2_000;

    public ushort RabbitMqPrefetch { get; init; } = 400;

    public int CommandTimeoutSeconds { get; init; } = 120;

    public int BulkCopyTimeoutSeconds { get; init; } = 120;

    public bool BulkCopyTableLock { get; init; } = true;

    public bool BulkCopyEnableStreaming { get; init; } = true;

    public int Repetition { get; init; } = 1;

    public bool Warmup { get; init; }

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(Name);
        ArgumentOutOfRangeException.ThrowIfLessThan(RowCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(WorkerInstances, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(WritersPerInstance, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaximumBatchingDelayMilliseconds, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(ChannelCapacity, BatchSize);

        int maximum = SqlLimits.MaximumRowsPerParameterizedCommand;
        if (Strategy is InsertStrategyKind.MultipleInsertStatements or InsertStrategyKind.MultiRowValues
            && BatchSize > maximum)
        {
            throw new ArgumentOutOfRangeException(
                nameof(BatchSize),
                $"The {Strategy} strategy supports at most {maximum} rows with this 13-parameter schema.");
        }

        if (Strategy == InsertStrategyKind.Individual && BatchSize != 1)
        {
            throw new ArgumentException("Individual insert per message requires a batch size of 1.");
        }

        if (Batching == BatchingKind.None && BatchSize != 1)
        {
            throw new ArgumentException("The no-batching path requires a batch size of 1.");
        }
    }
}

public sealed record BenchmarkProfile
{
    public required string Name { get; init; }

    public int WarmupRows { get; init; } = 1_000;

    public required IReadOnlyList<Scenario> Scenarios { get; init; }
}

public static class SqlLimits
{
    public const int ParametersPerRow = 13;
    public const int SqlServerParameterLimit = 2_100;
    public const int MaximumRowsPerParameterizedCommand = SqlServerParameterLimit / ParametersPerRow;
}

public sealed record RuntimeSettings
{
    public required string SqlConnectionString { get; init; }

    public required string RabbitMqConnectionString { get; init; }

    public required string RabbitMqManagementUri { get; init; }

    public required string RabbitMqUser { get; init; }

    public required string RabbitMqPassword { get; init; }

    public string QueueName { get; init; } = "sqlbench.messages";
}

public static class RuntimeSettingsLoader
{
    public static RuntimeSettings FromEnvironment() => new()
    {
        SqlConnectionString = Required("SQLBENCH_SQL_CONNECTION", "ConnectionStrings__sqldb"),
        RabbitMqConnectionString = Required("SQLBENCH_RABBIT_CONNECTION", "ConnectionStrings__rabbitmq"),
        RabbitMqManagementUri = Required("SQLBENCH_RABBIT_MANAGEMENT"),
        RabbitMqUser = Required("SQLBENCH_RABBIT_USER"),
        RabbitMqPassword = Required("SQLBENCH_RABBIT_PASSWORD"),
        QueueName = Optional("SQLBENCH_QUEUE", "sqlbench.messages")
    };

    private static string Required(params string[] names)
    {
        foreach (string name in names)
        {
            string? value = Environment.GetEnvironmentVariable(name);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        throw new InvalidOperationException($"Required environment variable missing. Set one of: {string.Join(", ", names)}.");
    }

    private static string Optional(string name, string fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? fallback : value;
    }
}
