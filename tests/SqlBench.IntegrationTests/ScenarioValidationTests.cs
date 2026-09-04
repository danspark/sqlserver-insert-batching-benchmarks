using System.Text.Json;
using System.Text.Json.Serialization;
using SqlBench.Core;

namespace SqlBench.IntegrationTests;

public sealed class ScenarioValidationTests
{
    private static readonly JsonSerializerOptions ProfileJsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    [Theory]
    [InlineData("Smoke.json")]
    [InlineData("Full.json")]
    [InlineData("Confirmation.json")]
    [InlineData("SqlBatch.json")]
    [InlineData("SqlBatchConfirmation.json")]
    public void EveryCheckedInScenarioIsValid(string fileName)
    {
        string path = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory,
            "../../../../../config",
            fileName));
        BenchmarkProfile profile = JsonSerializer.Deserialize<BenchmarkProfile>(
            File.ReadAllText(path),
            ProfileJsonOptions)!;

        Assert.NotEmpty(profile.Scenarios);
        foreach (Scenario scenario in profile.Scenarios)
        {
            scenario.Validate();
        }
    }

    [Fact]
    public void SqlBatchCommandLimitCannotExceedProcessWriterCount()
    {
        Scenario scenario = ValidScenario() with
        {
            SqlExecution = SqlExecutionKind.SqlBatch,
            WritersPerInstance = 2,
            SqlBatchMaximumCommands = 3
        };

        ArgumentException error = Assert.Throws<ArgumentException>(scenario.Validate);

        Assert.Contains("writers", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BulkCopyCannotExecuteThroughSqlBatch()
    {
        Scenario scenario = ValidScenario() with
        {
            Strategy = InsertStrategyKind.BulkCopy,
            SqlExecution = SqlExecutionKind.SqlBatch
        };

        ArgumentException error = Assert.Throws<ArgumentException>(scenario.Validate);

        Assert.Contains("SqlBulkCopy", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OneCommandSqlBatchControlIsValid()
    {
        Scenario scenario = ValidScenario() with
        {
            SqlExecution = SqlExecutionKind.SqlBatch,
            WritersPerInstance = 1,
            SqlBatchMaximumCommands = 1
        };

        scenario.Validate();
    }

    [Fact]
    public void DirectControlCannotPretendToBeMultipleProcesses()
    {
        Scenario scenario = ValidScenario() with
        {
            Mode = WorkloadMode.DirectDatabase,
            WorkerInstances = 2
        };

        ArgumentException error = Assert.Throws<ArgumentException>(scenario.Validate);

        Assert.Contains("one worker instance", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SqlBatchRequestConcurrencyCannotExceedWriterCount()
    {
        Scenario scenario = ValidScenario() with
        {
            SqlExecution = SqlExecutionKind.SqlBatch,
            WritersPerInstance = 2,
            SqlBatchRequestConcurrency = 3
        };

        ArgumentException error = Assert.Throws<ArgumentException>(scenario.Validate);

        Assert.Contains("request concurrency", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlBatchCommandLimitCannotExceedSafetyBound()
    {
        Scenario scenario = ValidScenario() with
        {
            SqlExecution = SqlExecutionKind.SqlBatch,
            WritersPerInstance = SqlLimits.MaximumSqlBatchCommandsPerRequest + 1,
            SqlBatchMaximumCommands = SqlLimits.MaximumSqlBatchCommandsPerRequest + 1
        };

        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(scenario.Validate);

        Assert.Equal(nameof(Scenario.SqlBatchMaximumCommands), error.ParamName);
    }

    [Fact]
    public void SqlBatchRequestConcurrencyCannotExceedSafetyBound()
    {
        Scenario scenario = ValidScenario() with
        {
            SqlExecution = SqlExecutionKind.SqlBatch,
            WritersPerInstance = SqlLimits.MaximumSqlBatchRequestConcurrency + 1,
            SqlBatchRequestConcurrency = SqlLimits.MaximumSqlBatchRequestConcurrency + 1
        };

        ArgumentOutOfRangeException error = Assert.Throws<ArgumentOutOfRangeException>(scenario.Validate);

        Assert.Equal(nameof(Scenario.SqlBatchRequestConcurrency), error.ParamName);
    }

    [Fact]
    public void SqlBatchFactoryEnforcesCoordinatorSafetyBounds()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: SqlLimits.MaximumSqlBatchCommandsPerRequest + 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch,
            sqlBatchRequestConcurrency: SqlLimits.MaximumSqlBatchRequestConcurrency + 1));
    }

    private static Scenario ValidScenario() => new()
    {
        Name = "validation",
        Strategy = InsertStrategyKind.Individual,
        Batching = BatchingKind.None,
        RowCount = 1,
        WorkerInstances = 1,
        WritersPerInstance = 1,
        BatchSize = 1,
        MaximumBatchingDelayMilliseconds = 1,
        ChannelCapacity = 1,
        RabbitMqPrefetch = 1
    };
}
