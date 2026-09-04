using Microsoft.Data.SqlClient;
using SqlBench.Core;
using SqlBench.Worker;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace SqlBench.IntegrationTests;

[Collection("SQL Server integration")]
public sealed class SqlStrategyIntegrationTests
{
    public static TheoryData<InsertStrategyKind, int> StrategyBatchSizes => new()
    {
        { InsertStrategyKind.Individual, 1 },
        { InsertStrategyKind.Individual, 10 },
        { InsertStrategyKind.TableValuedParameter, 1 },
        { InsertStrategyKind.TableValuedParameter, 10 },
        { InsertStrategyKind.TableValuedParameter, 100 },
        { InsertStrategyKind.MultipleInsertStatements, 1 },
        { InsertStrategyKind.MultipleInsertStatements, 10 },
        { InsertStrategyKind.MultipleInsertStatements, 100 },
        { InsertStrategyKind.MultiRowValues, 1 },
        { InsertStrategyKind.MultiRowValues, 10 },
        { InsertStrategyKind.MultiRowValues, 100 },
        { InsertStrategyKind.BulkCopy, 1 },
        { InsertStrategyKind.BulkCopy, 10 },
        { InsertStrategyKind.BulkCopy, 100 }
    };

    [Theory]
    [MemberData(nameof(StrategyBatchSizes))]
    public async Task InsertsEveryLogicalRow(InsertStrategyKind kind, int configuredBatchSize)
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        IReadOnlyList<BenchmarkMessage> rows = WorkloadGenerator.Generate(137, 12345, ParentDistribution.ModerateSkew);
        ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(kind);
        int batchSize = kind == InsertStrategyKind.Individual ? 1 : configuredBatchSize;
        foreach (BenchmarkMessage[] batch in rows.Chunk(batchSize))
        {
            await strategy.InsertAsync(
                database.GetTargetConnectionString(),
                batch,
                new SqlInsertOptions(),
                CancellationToken.None);
        }

        DatabaseVerification verification = await database.VerifyAsync(
            rows.Select(static row => row.MessageId).ToHashSet(),
            CancellationToken.None);
        Assert.True(verification.Passed(rows.Count));
    }

    [Theory]
    [InlineData(InsertStrategyKind.TableValuedParameter)]
    [InlineData(InsertStrategyKind.MultipleInsertStatements)]
    [InlineData(InsertStrategyKind.MultiRowValues)]
    [InlineData(InsertStrategyKind.BulkCopy)]
    public async Task RollsBackAnEntireFailedBatch(InsertStrategyKind kind)
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        BenchmarkMessage[] rows = WorkloadGenerator.Generate(2, 9001, ParentDistribution.Uniform).ToArray();
        rows[1] = rows[1] with { MessageId = rows[0].MessageId };
        ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(kind);

        await Assert.ThrowsAsync<SqlException>(() => strategy.InsertAsync(
            database.GetTargetConnectionString(),
            rows,
            new SqlInsertOptions(),
            CancellationToken.None));

        DatabaseVerification verification = await database.VerifyAsync([], CancellationToken.None);
        Assert.True(verification.Passed(0));
    }

    [Fact]
    public async Task IndividualFailureDoesNotUndoAnEarlierCommittedMessage()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        BenchmarkMessage row = WorkloadGenerator.Generate(1, 42, ParentDistribution.Uniform)[0];
        ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(InsertStrategyKind.Individual);
        await strategy.InsertAsync(database.GetTargetConnectionString(), [row], new SqlInsertOptions(), CancellationToken.None);

        await Assert.ThrowsAsync<SqlException>(() => strategy.InsertAsync(
            database.GetTargetConnectionString(), [row], new SqlInsertOptions(), CancellationToken.None));

        DatabaseVerification verification = await database.VerifyAsync([row.MessageId], CancellationToken.None);
        Assert.True(verification.Passed(1));
    }

    [Fact]
    public async Task FailedTransactionalBatchIsRequeuedWithoutAcknowledgments()
    {
        RuntimeSettings? configuredRuntime = TryLoadRuntime();
        if (configuredRuntime is null)
        {
            return;
        }

        RuntimeSettings runtime = configuredRuntime with
        {
            QueueName = $"sqlbench.integration.{Guid.NewGuid():N}"
        };
        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        await using var queue = new RabbitQueueClient(runtime);
        await queue.DeclareAsync(CancellationToken.None);

        string workingDirectory = Path.Combine(Path.GetTempPath(), $"sqlbench-integration-{Guid.NewGuid():N}");
        Directory.CreateDirectory(workingDirectory);
        try
        {
            BenchmarkMessage[] messages = WorkloadGenerator.Generate(2, 8128, ParentDistribution.Uniform).ToArray();
            messages[1] = messages[1] with { MessageId = messages[0].MessageId };
            await queue.PublishAsync(messages, CancellationToken.None);

            string gateFile = Path.Combine(workingDirectory, "start.gate");
            string readyFile = Path.Combine(workingDirectory, "worker.ready");
            string completionFile = Path.Combine(workingDirectory, "worker.complete");
            string resultFile = Path.Combine(workingDirectory, "worker.result.json");
            string configFile = Path.Combine(workingDirectory, "worker.config.json");
            var configuration = new WorkerConfiguration
            {
                WorkerId = 1,
                Runtime = runtime,
                Scenario = new Scenario
                {
                    Name = "integration-failed-batch",
                    Strategy = InsertStrategyKind.TableValuedParameter,
                    Batching = BatchingKind.Channel,
                    RowCount = 2,
                    BatchSize = 2,
                    ChannelCapacity = 2,
                    MaximumBatchingDelayMilliseconds = 20,
                    WritersPerInstance = 1,
                    WorkerInstances = 1,
                    RabbitMqPrefetch = 2
                },
                GateFile = gateFile,
                ReadyFile = readyFile,
                CompletionFile = completionFile,
                ResultFile = resultFile
            };
            var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
            {
                WriteIndented = true,
                Converters = { new JsonStringEnumConverter() }
            };
            await File.WriteAllTextAsync(configFile, JsonSerializer.Serialize(configuration, options));
            await File.WriteAllTextAsync(gateFile, "run");

            int exitCode = await WorkerRunner.RunAsync(["--config", configFile]);
            Assert.Equal(1, exitCode);

            WorkerRunResult result = JsonSerializer.Deserialize<WorkerRunResult>(
                await File.ReadAllTextAsync(resultFile), options)!;
            Assert.Equal(0, result.AcknowledgedMessages);
            Assert.NotEmpty(result.Errors);

            QueueState state = await queue.ReadStateAsync(CancellationToken.None);
            Assert.Equal(2, state.Ready);
            Assert.Equal(0, state.Unacknowledged);
            DatabaseVerification verification = await database.VerifyAsync([], CancellationToken.None);
            Assert.True(verification.Passed(0));
        }
        finally
        {
            await queue.PurgeAsync(CancellationToken.None);
            Directory.Delete(workingDirectory, recursive: true);
        }
    }

    private static RuntimeSettings? TryLoadRuntime()
    {
        try
        {
            return RuntimeSettingsLoader.FromEnvironment();
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }
}

[CollectionDefinition("SQL Server integration", DisableParallelization = true)]
public sealed class SqlServerIntegrationGroup;
