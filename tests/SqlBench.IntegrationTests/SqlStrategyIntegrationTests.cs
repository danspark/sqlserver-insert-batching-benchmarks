using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
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

    [Fact]
    public async Task SqlBatchPublishesSqlClientOpenTelemetryMetrics()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var observed = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "SqlBench.SqlClient")
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, _, _, _) =>
            observed.TryAdd(instrument.Name, 0));
        listener.SetMeasurementEventCallback<int>((instrument, _, _, _) =>
            observed.TryAdd(instrument.Name, 0));
        listener.SetMeasurementEventCallback<double>((instrument, _, _, _) =>
            observed.TryAdd(instrument.Name, 0));
        listener.Start();

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        BenchmarkMessage row = WorkloadGenerator.Generate(1, 11221, ParentDistribution.Uniform)[0];
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch);

        await strategy.InsertAsync(
            database.GetTargetConnectionString(),
            [row],
            new SqlInsertOptions(),
            CancellationToken.None);

        Assert.Contains("sqlbench.sqlclient.dml_execute", observed.Keys);
        Assert.Contains("sqlbench.sqlclient.command", observed.Keys);
        Assert.Contains("sqlbench.sqlclient.commands_per_request", observed.Keys);
        Assert.Contains("sqlbench.sqlclient.dml_execute.duration", observed.Keys);
        Assert.Contains("sqlbench.sqlclient.dml_execute.active", observed.Keys);
        Assert.Contains("sqlbench.sqlclient.coordinator.queue.depth", observed.Keys);
        Assert.Contains("sqlbench.sqlclient.coordinator.queue.wait.duration", observed.Keys);
    }

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
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(kind);
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

    public static TheoryData<InsertStrategyKind, int> SqlBatchStrategyBatchSizes => new()
    {
        { InsertStrategyKind.Individual, 1 },
        { InsertStrategyKind.TableValuedParameter, 10 },
        { InsertStrategyKind.MultipleInsertStatements, 10 },
        { InsertStrategyKind.MultiRowValues, 10 }
    };

    [Fact]
    public async Task SqlBatchRejectsCancellationBeforeAdmission()
    {
        IReadOnlyList<BenchmarkMessage> rows = WorkloadGenerator.Generate(
            1,
            21223,
            ParentDistribution.Uniform);
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => strategy.InsertAsync(
            "Server=invalid;Integrated Security=True",
            rows,
            new SqlInsertOptions(),
            cancellation.Token));

        SqlRequestMetricsSnapshot metrics = strategy.GetRequestMetrics();
        Assert.Equal(0, metrics.CoordinatorQueueWaitMilliseconds.Count);
        Assert.Equal(0, metrics.CurrentCoordinatorQueueDepth);
        Assert.Equal(0, metrics.CommandCount);
    }

    [Theory]
    [MemberData(nameof(SqlBatchStrategyBatchSizes))]
    public async Task SqlBatchExecutesOneWriterCommandPerRequest(
        InsertStrategyKind kind,
        int batchSize)
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        IReadOnlyList<BenchmarkMessage> rows = WorkloadGenerator.Generate(
            20,
            22334,
            ParentDistribution.Uniform);
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            kind,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: 1,
            sqlBatchMaximumDelayMilliseconds: 1);
        foreach (BenchmarkMessage[] batch in rows.Chunk(batchSize))
        {
            await strategy.InsertAsync(
                database.GetTargetConnectionString(),
                batch,
                new SqlInsertOptions(),
                CancellationToken.None);
        }

        SqlRequestMetricsSnapshot metrics = strategy.GetRequestMetrics();
        int expectedRequests = (rows.Count + batchSize - 1) / batchSize;
        Assert.Equal(expectedRequests, metrics.RequestCount);
        Assert.Equal(expectedRequests, metrics.CommandCount);
        Assert.Equal(expectedRequests, metrics.SingleCommandRequestCount);
        DatabaseVerification verification = await database.VerifyAsync(
            rows.Select(static row => row.MessageId).ToHashSet(),
            CancellationToken.None);
        Assert.True(verification.Passed(rows.Count));
    }

    [Fact]
    public async Task SqlBatchCoalescesConcurrentIndividualWritersIntoFewerRequests()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        IReadOnlyList<BenchmarkMessage> rows = WorkloadGenerator.Generate(
            8,
            33445,
            ParentDistribution.Uniform);
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: 4,
            sqlBatchMaximumDelayMilliseconds: 20);
        Task<SqlWriteTiming>[] inserts = rows
            .Select(row => strategy.InsertAsync(
                database.GetTargetConnectionString(),
                [row],
                new SqlInsertOptions(),
                CancellationToken.None))
            .ToArray();

        await Task.WhenAll(inserts);

        SqlRequestMetricsSnapshot metrics = strategy.GetRequestMetrics();
        Assert.Equal(rows.Count, metrics.CommandCount);
        Assert.Equal(2, metrics.RequestCount);
        Assert.Equal(4, metrics.MaximumCommandsPerRequest);
        Assert.Equal(inserts.Length, metrics.CoordinatorQueueWaitMilliseconds.Count);
        Assert.Equal(0, metrics.CurrentCoordinatorQueueDepth);
        Assert.Equal(0, metrics.ActiveRequests);
        DatabaseVerification verification = await database.VerifyAsync(
            rows.Select(static row => row.MessageId).ToHashSet(),
            CancellationToken.None);
        Assert.True(verification.Passed(rows.Count));
    }

    [Fact]
    public async Task SqlBatchCanExecuteThroughConcurrentRequestLanes()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        IReadOnlyList<BenchmarkMessage> rows = WorkloadGenerator.Generate(
            800,
            34556,
            ParentDistribution.Uniform);
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.MultipleInsertStatements,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: 2,
            sqlBatchMaximumDelayMilliseconds: 20,
            sqlBatchRequestConcurrency: 2);
        Task<SqlWriteTiming>[] inserts = rows
            .Chunk(100)
            .Select(batch => strategy.InsertAsync(
                database.GetTargetConnectionString(),
                batch,
                new SqlInsertOptions(),
                CancellationToken.None))
            .ToArray();

        await Task.WhenAll(inserts);

        SqlRequestMetricsSnapshot metrics = strategy.GetRequestMetrics();
        Assert.Equal(inserts.Length, metrics.CommandCount);
        Assert.Equal(4, metrics.RequestCount);
        Assert.Equal(2, metrics.MaximumCommandsPerRequest);
        Assert.True(metrics.MaximumConcurrentRequests > 1);
        Assert.Equal(inserts.Length, metrics.CoordinatorQueueWaitMilliseconds.Count);
        Assert.Equal(0, metrics.CurrentCoordinatorQueueDepth);
        Assert.Equal(0, metrics.ActiveRequests);
        DatabaseVerification verification = await database.VerifyAsync(
            rows.Select(static row => row.MessageId).ToHashSet(),
            CancellationToken.None);
        Assert.True(verification.Passed(rows.Count));
    }

    [Fact]
    public async Task SqlBatchCoordinatorReportsBackpressure()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        IReadOnlyList<BenchmarkMessage> rows = WorkloadGenerator.Generate(
            32,
            35667,
            ParentDistribution.Uniform);
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: 1,
            sqlBatchMaximumDelayMilliseconds: 1);
        Task<SqlWriteTiming>[] inserts = rows
            .Select(row => strategy.InsertAsync(
                database.GetTargetConnectionString(),
                [row],
                new SqlInsertOptions(),
                CancellationToken.None))
            .ToArray();

        await Task.WhenAll(inserts);

        SqlRequestMetricsSnapshot metrics = strategy.GetRequestMetrics();
        Assert.True(metrics.CoordinatorBackpressureEvents > 0);
        Assert.True(metrics.MaximumCoordinatorQueueDepth > 0);
        Assert.Equal(rows.Count, metrics.CoordinatorQueueWaitMilliseconds.Count);
        Assert.Equal(0, metrics.CurrentCoordinatorQueueDepth);
    }

    [Fact]
    public async Task SqlBatchCoordinatorBindsConnectionAndOptionsOnFirstUse()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        BenchmarkMessage row = WorkloadGenerator.Generate(1, 36778, ParentDistribution.Uniform)[0];
        string connectionString = database.GetTargetConnectionString();
        var options = new SqlInsertOptions();
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch);

        await strategy.InsertAsync(connectionString, [row], options, CancellationToken.None);

        InvalidOperationException connectionError = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            strategy.InsertAsync(
                $"{connectionString};Application Name=SqlBenchDifferent",
                [row],
                options,
                CancellationToken.None));
        InvalidOperationException optionsError = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            strategy.InsertAsync(
                connectionString,
                [row],
                options with { CommandTimeoutSeconds = options.CommandTimeoutSeconds + 1 },
                CancellationToken.None));

        Assert.Contains("already bound", connectionError.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("already bound", optionsError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SqlBatchReportsCommittedAndFailedWriterCommandsSeparately()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        BenchmarkMessage row = WorkloadGenerator.Generate(1, 44556, ParentDistribution.Uniform)[0];
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: 2,
            sqlBatchMaximumDelayMilliseconds: 20);
        Task<SqlWriteTiming> first = strategy.InsertAsync(
            database.GetTargetConnectionString(),
            [row],
            new SqlInsertOptions(),
            CancellationToken.None);
        Task<SqlWriteTiming> duplicate = strategy.InsertAsync(
            database.GetTargetConnectionString(),
            [row],
            new SqlInsertOptions(),
            CancellationToken.None);

        await Assert.ThrowsAnyAsync<Exception>(() => Task.WhenAll(first, duplicate));

        Assert.True(first.IsCompletedSuccessfully);
        Assert.True(duplicate.IsFaulted);
        SqlRequestMetricsSnapshot metrics = strategy.GetRequestMetrics();
        Assert.Equal(1, metrics.RequestCount);
        Assert.Equal(2, metrics.CommandCount);
        DatabaseVerification verification = await database.VerifyAsync(
            [row.MessageId],
            CancellationToken.None);
        Assert.True(verification.Passed(1));
    }

    [Fact]
    public async Task SqlBatchLeavesCommandsAfterOneWriterFailureUnexecuted()
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        IReadOnlyList<BenchmarkMessage> rows = WorkloadGenerator.Generate(
            2,
            55667,
            ParentDistribution.Uniform);
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            InsertStrategyKind.Individual,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: 3,
            sqlBatchMaximumDelayMilliseconds: 20);
        Task<SqlWriteTiming> first = strategy.InsertAsync(
            database.GetTargetConnectionString(),
            [rows[0]],
            new SqlInsertOptions(),
            CancellationToken.None);
        Task<SqlWriteTiming> duplicate = strategy.InsertAsync(
            database.GetTargetConnectionString(),
            [rows[0]],
            new SqlInsertOptions(),
            CancellationToken.None);
        Task<SqlWriteTiming> third = strategy.InsertAsync(
            database.GetTargetConnectionString(),
            [rows[1]],
            new SqlInsertOptions(),
            CancellationToken.None);

        await Assert.ThrowsAnyAsync<Exception>(() => Task.WhenAll(first, duplicate, third));

        Assert.True(first.IsCompletedSuccessfully);
        Assert.True(duplicate.IsFaulted);
        Assert.True(third.IsFaulted);
        DatabaseVerification verification = await database.VerifyAsync(
            [rows[0].MessageId],
            CancellationToken.None);
        Assert.True(verification.Passed(1));
    }

    [Theory]
    [InlineData(InsertStrategyKind.TableValuedParameter)]
    [InlineData(InsertStrategyKind.MultipleInsertStatements)]
    [InlineData(InsertStrategyKind.MultiRowValues)]
    public async Task SqlBatchRollsBackAnEntireFailedWriterBatch(InsertStrategyKind kind)
    {
        RuntimeSettings? runtime = TryLoadRuntime();
        if (runtime is null)
        {
            return;
        }

        var database = new DatabaseManager(runtime.SqlConnectionString);
        await database.InitializeAsync(CancellationToken.None);
        await database.ResetTargetAsync(CancellationToken.None);
        BenchmarkMessage[] rows = WorkloadGenerator.Generate(2, 66778, ParentDistribution.Uniform).ToArray();
        rows[1] = rows[1] with { MessageId = rows[0].MessageId };
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(
            kind,
            SqlExecutionKind.SqlBatch,
            sqlBatchMaximumCommands: 1,
            sqlBatchMaximumDelayMilliseconds: 1);

        await Assert.ThrowsAnyAsync<Exception>(() => strategy.InsertAsync(
            database.GetTargetConnectionString(),
            rows,
            new SqlInsertOptions(),
            CancellationToken.None));

        DatabaseVerification verification = await database.VerifyAsync([], CancellationToken.None);
        Assert.True(verification.Passed(0));
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
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(kind);

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
        await using ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(InsertStrategyKind.Individual);
        await strategy.InsertAsync(database.GetTargetConnectionString(), [row], new SqlInsertOptions(), CancellationToken.None);

        await Assert.ThrowsAsync<SqlException>(() => strategy.InsertAsync(
            database.GetTargetConnectionString(), [row], new SqlInsertOptions(), CancellationToken.None));

        DatabaseVerification verification = await database.VerifyAsync([row.MessageId], CancellationToken.None);
        Assert.True(verification.Passed(1));
    }

    [Theory]
    [InlineData(SqlExecutionKind.Native)]
    [InlineData(SqlExecutionKind.SqlBatch)]
    public async Task FailedTransactionalBatchIsRequeuedWithoutAcknowledgments(SqlExecutionKind sqlExecution)
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
                    Name = $"integration-failed-batch-{sqlExecution}",
                    Strategy = InsertStrategyKind.TableValuedParameter,
                    SqlExecution = sqlExecution,
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
