using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Hosting;
using SqlBench.Core;
using SqlBench.ServiceDefaults;

namespace SqlBench.Controller;

internal static class ControllerApplication
{
    private const int ApproximateDatabaseRowBytes = 390;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public static async Task<int> RunAsync(string[] args)
    {
        if (args.Contains("--idle", StringComparer.OrdinalIgnoreCase))
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
            builder.AddSqlBenchServiceDefaults();
            using IHost host = builder.Build();
            await host.RunAsync().ConfigureAwait(false);
            return 0;
        }

        string profilePath = RequireOption(args, "--profile");
        string outputDirectory = ReadOption(args, "--output")
            ?? Path.Combine("results", "local", DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture));
        string workerAssembly = ReadOption(args, "--worker")
            ?? Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../../SqlBench.Worker/bin/Release/net10.0/SqlBench.Worker.dll"));
        string gitCommit = ReadOption(args, "--git-commit") ?? ReadGitCommit();
        string? scenarioFilter = ReadOption(args, "--scenario");
        int? rowOverride = int.TryParse(ReadOption(args, "--rows"), out int rows) ? rows : null;

        await using FileStream profileStream = File.OpenRead(profilePath);
        BenchmarkProfile profile = await JsonSerializer.DeserializeAsync<BenchmarkProfile>(profileStream, JsonOptions)
            .ConfigureAwait(false)
            ?? throw new InvalidOperationException("The benchmark profile was empty.");
        Scenario[] scenarios = profile.Scenarios
            .Where(scenario => scenarioFilter is null || scenario.Name.Contains(scenarioFilter, StringComparison.OrdinalIgnoreCase))
            .Select(scenario => rowOverride is null ? scenario : scenario with { RowCount = rowOverride.Value })
            .ToArray();
        if (scenarios.Length == 0)
        {
            throw new InvalidOperationException("No scenarios matched the selected profile and filter.");
        }

        Directory.CreateDirectory(outputDirectory);
        RuntimeSettings runtime = RuntimeSettingsLoader.FromEnvironment();
        var database = new DatabaseManager(runtime.SqlConnectionString);
        Console.WriteLine("Initializing SQL Server schema and RabbitMQ queue...");
        await database.InitializeAsync(CancellationToken.None).ConfigureAwait(false);
        await using var queue = new RabbitQueueClient(runtime);
        await queue.DeclareAsync(CancellationToken.None).ConfigureAwait(false);

        var warmed = new HashSet<string>(StringComparer.Ordinal);
        var results = new List<string>();
        int exitCode = 0;
        foreach (Scenario source in RotateScenarios(scenarios))
        {
            source.Validate();
            string implementation = $"{source.Mode}/{source.Strategy}/{source.Batching}";
            if (warmed.Add(implementation))
            {
                Scenario warmup = source with
                {
                    Name = $"warmup-{source.Name}",
                    Stage = "warmup",
                    Warmup = true,
                    Repetition = 0,
                    RowCount = Math.Min(profile.WarmupRows, source.RowCount)
                };
                Console.WriteLine($"Warming {implementation} with {warmup.RowCount:N0} rows...");
                _ = await RunScenarioAsync(warmup, runtime, database, queue, workerAssembly, gitCommit, outputDirectory, save: false)
                    .ConfigureAwait(false);
            }

            Console.WriteLine($"Running {source.Name} ({source.RowCount:N0} rows)...");
            ScenarioResult result = await RunScenarioAsync(
                source, runtime, database, queue, workerAssembly, gitCommit, outputDirectory, save: true)
                .ConfigureAwait(false);
            string fileName = SafeFileName(source.Name) + $"-r{source.Repetition}.json";
            results.Add(fileName);
            Console.WriteLine(
                $"  {result.CommittedRowsPerSecond:N0} committed rows/s, " +
                $"{result.AcknowledgedMessagesPerSecond:N0} ack/s, " +
                $"p99 ack {result.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms, " +
                $"correct={result.Correctness.Passed}");
            if (!result.Valid)
            {
                exitCode = 1;
            }
        }

        var manifest = new { profile = profile.Name, gitCommit, generatedAt = DateTimeOffset.UtcNow, files = results };
        await File.WriteAllTextAsync(
            Path.Combine(outputDirectory, "manifest.json"), JsonSerializer.Serialize(manifest, JsonOptions))
            .ConfigureAwait(false);
        return exitCode;
    }

    private static async Task<ScenarioResult> RunScenarioAsync(
        Scenario scenario,
        RuntimeSettings runtime,
        DatabaseManager database,
        RabbitQueueClient queue,
        string workerAssembly,
        string gitCommit,
        string outputDirectory,
        bool save)
    {
        IReadOnlyList<BenchmarkMessage> workload = WorkloadGenerator.Generate(
            scenario.RowCount, scenario.Seed, scenario.Distribution);
        await database.ResetTargetAsync(CancellationToken.None).ConfigureAwait(false);
        await queue.PurgeAsync(CancellationToken.None).ConfigureAwait(false);

        ScenarioResult result = scenario.Mode == WorkloadMode.DirectDatabase
            ? await RunDirectAsync(scenario, workload, database, queue, gitCommit).ConfigureAwait(false)
            : await RunQueueAsync(
                scenario, workload, runtime, database, queue, workerAssembly, gitCommit, outputDirectory)
                .ConfigureAwait(false);

        if (save)
        {
            string resultPath = Path.Combine(outputDirectory, SafeFileName(scenario.Name) + $"-r{scenario.Repetition}.json");
            await File.WriteAllTextAsync(resultPath, JsonSerializer.Serialize(result, JsonOptions)).ConfigureAwait(false);
        }

        return result;
    }

    private static async Task<ScenarioResult> RunQueueAsync(
        Scenario scenario,
        IReadOnlyList<BenchmarkMessage> workload,
        RuntimeSettings runtime,
        DatabaseManager database,
        RabbitQueueClient queue,
        string workerAssembly,
        string gitCommit,
        string outputDirectory)
    {
        await queue.PublishAsync(workload, CancellationToken.None).ConfigureAwait(false);
        QueueState preloaded = await queue.ReadStateAsync(CancellationToken.None).ConfigureAwait(false);
        if (preloaded.Ready != scenario.RowCount || preloaded.Unacknowledged != 0)
        {
            throw new InvalidOperationException(
                $"Preload verification failed. Expected {scenario.RowCount} ready and 0 unacknowledged, " +
                $"observed {preloaded.Ready} ready and {preloaded.Unacknowledged} unacknowledged.");
        }

        DatabaseMetrics sqlBefore = await database.ReadMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        RabbitMqMetrics rabbitBefore = await queue.ReadBrokerMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        string workDirectory = Path.Combine(outputDirectory, ".work", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(workDirectory);
        string gateFile = Path.Combine(workDirectory, "start.gate");
        var processes = new List<WorkerProcess>();
        for (int workerId = 1; workerId <= scenario.WorkerInstances; workerId++)
        {
            string configFile = Path.Combine(workDirectory, $"worker-{workerId}.config.json");
            string readyFile = Path.Combine(workDirectory, $"worker-{workerId}.ready");
            string completionFile = Path.Combine(workDirectory, $"worker-{workerId}.complete");
            string resultFile = Path.Combine(workDirectory, $"worker-{workerId}.result.json");
            var config = new WorkerConfiguration
            {
                WorkerId = workerId,
                Runtime = runtime,
                Scenario = scenario,
                GateFile = gateFile,
                ReadyFile = readyFile,
                CompletionFile = completionFile,
                ResultFile = resultFile
            };
            await File.WriteAllTextAsync(configFile, JsonSerializer.Serialize(config, JsonOptions)).ConfigureAwait(false);
            processes.Add(StartWorker(
                workerAssembly, configFile, resultFile, readyFile, completionFile, workDirectory, workerId));
        }

        await WaitForWorkerReadinessAsync(processes, TimeSpan.FromMinutes(2)).ConfigureAwait(false);
        var timer = Stopwatch.StartNew();
        await File.WriteAllTextAsync(gateFile, "run").ConfigureAwait(false);
        await WaitForWorkerCompletionAsync(processes, TimeSpan.FromMinutes(30)).ConfigureAwait(false);
        timer.Stop();
        await WaitForWorkersAsync(processes, TimeSpan.FromMinutes(2)).ConfigureAwait(false);

        var workerResults = new List<WorkerRunResult>();
        var errors = new List<string>();
        foreach (WorkerProcess worker in processes)
        {
            if (worker.Process.ExitCode != 0)
            {
                errors.Add($"Worker {worker.WorkerId} exited with code {worker.Process.ExitCode}: {await worker.StandardError.ConfigureAwait(false)}");
            }

            if (File.Exists(worker.ResultFile))
            {
                await using FileStream stream = File.OpenRead(worker.ResultFile);
                WorkerRunResult? workerResult = await JsonSerializer.DeserializeAsync<WorkerRunResult>(stream, JsonOptions)
                    .ConfigureAwait(false);
                if (workerResult is not null)
                {
                    workerResults.Add(workerResult);
                    errors.AddRange(workerResult.Errors.Select(error => $"Worker {worker.WorkerId}: {error}"));
                }
            }
            else
            {
                errors.Add($"Worker {worker.WorkerId} did not write a result file.");
            }
        }

        QueueState finalQueue = await queue.ReadStateAsync(CancellationToken.None).ConfigureAwait(false);
        int expectedDatabaseRows = scenario.Mode == WorkloadMode.NoOpQueue ? 0 : scenario.RowCount;
        IReadOnlyCollection<Guid> expectedIds = scenario.Mode == WorkloadMode.NoOpQueue
            ? Array.Empty<Guid>()
            : workload.Select(static row => row.MessageId).ToHashSet();
        DatabaseVerification databaseVerification = await database.VerifyAsync(expectedIds, CancellationToken.None)
            .ConfigureAwait(false);
        DatabaseMetrics sqlAfter = await database.ReadMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        RabbitMqMetrics rabbitAfter = await queue.ReadBrokerMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        long delivered = workerResults.Sum(static worker => worker.DeliveredMessages);
        long committed = workerResults.Sum(static worker => worker.CommittedRows);
        long acknowledged = workerResults.Sum(static worker => worker.AcknowledgedMessages);
        bool correct = databaseVerification.Passed(expectedDatabaseRows)
            && finalQueue.Ready == 0
            && finalQueue.Unacknowledged == 0
            && acknowledged == scenario.RowCount
            && errors.Count == 0;
        return CreateResult(
            scenario, gitCommit, workload, timer.Elapsed, delivered, committed, acknowledged, workerResults,
            sqlBefore, sqlAfter, rabbitBefore, rabbitAfter, preloaded, finalQueue,
            databaseVerification, correct, errors);
    }

    private static async Task<ScenarioResult> RunDirectAsync(
        Scenario scenario,
        IReadOnlyList<BenchmarkMessage> workload,
        DatabaseManager database,
        RabbitQueueClient queue,
        string gitCommit)
    {
        DatabaseMetrics sqlBefore = await database.ReadMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        RabbitMqMetrics rabbitBefore = await queue.ReadBrokerMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        ISqlInsertStrategy strategy = SqlInsertStrategyFactory.Create(scenario.Strategy);
        var options = new SqlInsertOptions
        {
            CommandTimeoutSeconds = scenario.CommandTimeoutSeconds,
            BulkCopyTimeoutSeconds = scenario.BulkCopyTimeoutSeconds,
            BulkCopyEnableStreaming = scenario.BulkCopyEnableStreaming,
            BulkCopyTableLock = scenario.BulkCopyTableLock
        };
        var commitLatencies = new System.Collections.Concurrent.ConcurrentBag<long>();
        var sqlDurations = new System.Collections.Concurrent.ConcurrentBag<double>();
        var transactionDurations = new System.Collections.Concurrent.ConcurrentBag<double>();
        int nextBatch = -1;
        int batchCount = (workload.Count + scenario.BatchSize - 1) / scenario.BatchSize;
        int concurrency = scenario.WorkerInstances * scenario.WritersPerInstance;
        var timer = Stopwatch.StartNew();
        Task[] writers = Enumerable.Range(0, concurrency).Select(async writerIndex =>
        {
            _ = writerIndex;
            while (true)
            {
                int batchIndex = Interlocked.Increment(ref nextBatch);
                if (batchIndex >= batchCount)
                {
                    return;
                }

                BenchmarkMessage[] batch = workload.Skip(batchIndex * scenario.BatchSize).Take(scenario.BatchSize).ToArray();
                long start = Stopwatch.GetTimestamp();
                SqlWriteTiming timing = await strategy.InsertAsync(
                    database.GetTargetConnectionString(), batch, options, CancellationToken.None)
                    .ConfigureAwait(false);
                long microseconds = (long)Math.Round(Stopwatch.GetElapsedTime(start).TotalMilliseconds * 1_000.0);
                foreach (BenchmarkMessage row in batch)
                {
                    _ = row;
                    commitLatencies.Add(microseconds);
                }

                sqlDurations.Add(timing.SqlExecution.TotalMilliseconds);
                transactionDurations.Add(timing.Transaction.TotalMilliseconds);
            }
        }).ToArray();
        await Task.WhenAll(writers).ConfigureAwait(false);
        timer.Stop();

        QueueState finalQueue = await queue.ReadStateAsync(CancellationToken.None).ConfigureAwait(false);
        DatabaseVerification verification = await database.VerifyAsync(
            workload.Select(static row => row.MessageId).ToHashSet(), CancellationToken.None).ConfigureAwait(false);
        DatabaseMetrics sqlAfter = await database.ReadMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        RabbitMqMetrics rabbitAfter = await queue.ReadBrokerMetricsAsync(CancellationToken.None).ConfigureAwait(false);
        var workerResult = new WorkerRunResult
        {
            WorkerId = 1,
            DeliveredMessages = workload.Count,
            CommittedRows = workload.Count,
            AcknowledgedMessages = 0,
            DeliveryToCommitMicroseconds = [.. commitLatencies],
            DeliveryToAcknowledgmentMicroseconds = [],
            SqlExecutionMilliseconds = [.. sqlDurations],
            TransactionMilliseconds = [.. transactionDurations],
            BatcherMetrics = EmptyBatcherMetrics(),
            Process = new ProcessMetrics(),
            Errors = []
        };
        bool correct = verification.Passed(scenario.RowCount) && finalQueue.Ready == 0 && finalQueue.Unacknowledged == 0;
        return CreateResult(
            scenario, gitCommit, workload, timer.Elapsed, workload.Count, workload.Count, 0, [workerResult],
            sqlBefore, sqlAfter, rabbitBefore, rabbitAfter, finalQueue, finalQueue, verification, correct, []);
    }

    private static ScenarioResult CreateResult(
        Scenario scenario,
        string gitCommit,
        IReadOnlyList<BenchmarkMessage> workload,
        TimeSpan duration,
        long delivered,
        long committed,
        long acknowledged,
        IReadOnlyList<WorkerRunResult> workers,
        DatabaseMetrics sqlBefore,
        DatabaseMetrics sqlAfter,
        RabbitMqMetrics rabbitBefore,
        RabbitMqMetrics rabbitAfter,
        QueueState queueBefore,
        QueueState queueAfter,
        DatabaseVerification verification,
        bool correct,
        List<string> errors)
    {
        double seconds = Math.Max(duration.TotalSeconds, double.Epsilon);
        return new ScenarioResult
        {
            ScenarioName = scenario.Name,
            GitCommit = gitCommit,
            Timestamp = DateTimeOffset.UtcNow,
            Configuration = scenario,
            SerializedMessageBytes = WorkloadGenerator.GetSerializedSize(workload.Take(1_000).ToArray()),
            ApproximateDatabaseRowBytes = ApproximateDatabaseRowBytes,
            DurationSeconds = seconds,
            DeliveredMessages = delivered,
            CommittedRows = committed,
            AcknowledgedMessages = acknowledged,
            CommittedRowsPerSecond = committed / seconds,
            AcknowledgedMessagesPerSecond = acknowledged / seconds,
            DeliveryToCommitMilliseconds = PercentileSummary.FromMicroseconds(workers.SelectMany(static worker => worker.DeliveryToCommitMicroseconds)),
            DeliveryToAcknowledgmentMilliseconds = PercentileSummary.FromMicroseconds(workers.SelectMany(static worker => worker.DeliveryToAcknowledgmentMicroseconds)),
            SqlExecutionMilliseconds = PercentileSummary.FromMilliseconds(workers.SelectMany(static worker => worker.SqlExecutionMilliseconds)),
            TransactionMilliseconds = PercentileSummary.FromMilliseconds(workers.SelectMany(static worker => worker.TransactionMilliseconds)),
            Workers = workers,
            SqlServerBefore = sqlBefore,
            SqlServerAfter = sqlAfter,
            RabbitMqBefore = rabbitBefore,
            RabbitMqAfter = rabbitAfter,
            QueueBefore = queueBefore,
            QueueAfter = queueAfter,
            Correctness = new CorrectnessResult
            {
                Passed = correct,
                Database = verification,
                Queue = queueAfter,
                HiddenWorkerFailureCount = errors.Count
            },
            Errors = errors
        };
    }

    private static WorkerProcess StartWorker(
        string assembly,
        string configFile,
        string resultFile,
        string readyFile,
        string completionFile,
        string workDirectory,
        int workerId)
    {
        var startInfo = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = workDirectory
        };
        startInfo.ArgumentList.Add(Path.GetFullPath(assembly));
        startInfo.ArgumentList.Add("--config");
        startInfo.ArgumentList.Add(configFile);
        Process process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Could not start worker {workerId}.");
        return new WorkerProcess(
            workerId, process, resultFile, readyFile, completionFile,
            process.StandardOutput.ReadToEndAsync(), process.StandardError.ReadToEndAsync());
    }

    private static async Task WaitForWorkerReadinessAsync(IReadOnlyList<WorkerProcess> workers, TimeSpan timeout)
    {
        var timer = Stopwatch.StartNew();
        while (workers.Any(worker => !File.Exists(worker.ReadyFile)))
        {
            foreach (WorkerProcess worker in workers.Where(worker => worker.Process.HasExited))
            {
                throw new InvalidOperationException(
                    $"Worker {worker.WorkerId} exited before becoming ready: {await worker.StandardError.ConfigureAwait(false)}");
            }

            if (timer.Elapsed > timeout)
            {
                throw new TimeoutException("Workers did not reach the start gate before the timeout.");
            }

            await Task.Delay(10).ConfigureAwait(false);
        }
    }

    private static async Task WaitForWorkersAsync(IReadOnlyList<WorkerProcess> workers, TimeSpan timeout)
    {
        using var cancellation = new CancellationTokenSource(timeout);
        try
        {
            await Task.WhenAll(workers.Select(worker => worker.Process.WaitForExitAsync(cancellation.Token))).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            foreach (WorkerProcess worker in workers.Where(worker => !worker.Process.HasExited))
            {
                worker.Process.Kill(entireProcessTree: true);
            }

            throw new TimeoutException($"Workers exceeded the {timeout} scenario timeout.");
        }
    }

    private static async Task WaitForWorkerCompletionAsync(
        IReadOnlyList<WorkerProcess> workers,
        TimeSpan timeout)
    {
        var timer = Stopwatch.StartNew();
        while (workers.Any(worker => !File.Exists(worker.CompletionFile)))
        {
            if (workers.Any(worker => worker.Process.HasExited && !File.Exists(worker.CompletionFile)))
            {
                return;
            }

            if (timer.Elapsed > timeout)
            {
                foreach (WorkerProcess worker in workers.Where(worker => !worker.Process.HasExited))
                {
                    worker.Process.Kill(entireProcessTree: true);
                }

                throw new TimeoutException($"Workers exceeded the {timeout} measured scenario timeout.");
            }

            await Task.Delay(5).ConfigureAwait(false);
        }
    }

    private static IEnumerable<Scenario> RotateScenarios(IReadOnlyList<Scenario> source)
    {
        return source
            .GroupBy(static scenario => StageRank(scenario.Stage))
            .OrderBy(static group => group.Key)
            .SelectMany(group =>
            {
                var random = new Random(unchecked(0x5EED_2026 + group.Key));
                return group.OrderBy(_ => random.Next());
            });
    }

    private static int StageRank(string stage) => stage switch
    {
        "control" => 0,
        "broad" => 1,
        "refine-batch" or "refine-delay" or "refine-capacity" or "refine-prefetch" or
            "refine-batcher" or "refine-concurrency" => 2,
        "scaling" => 3,
        "direct-control" => 4,
        "finalist" => 5,
        _ => 6
    };

    private static string RequireOption(string[] args, string name) =>
        ReadOption(args, name) ?? throw new ArgumentException($"Missing required option {name}.");

    private static string? ReadOption(string[] args, string name)
    {
        int index = Array.FindIndex(args, value => string.Equals(value, name, StringComparison.OrdinalIgnoreCase));
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }

    private static string ReadGitCommit()
    {
        try
        {
            var info = new ProcessStartInfo("git", "rev-parse HEAD")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            using Process process = Process.Start(info)!;
            string commit = process.StandardOutput.ReadToEnd().Trim();
            process.WaitForExit();
            return process.ExitCode == 0 ? commit : "uncommitted";
        }
        catch
        {
            return "uncommitted";
        }
    }

    private static string SafeFileName(string name)
    {
        var value = new StringBuilder(name.Length);
        foreach (char character in name.ToLowerInvariant())
        {
            value.Append(char.IsAsciiLetterOrDigit(character) ? character : '-');
        }

        return value.ToString().Trim('-');
    }

    private static SqlBench.Batching.BatcherMetricsSnapshot EmptyBatcherMetrics() => new()
    {
        ChannelWaitMilliseconds = new SqlBench.Batching.DistributionSnapshot(),
        ActualBatchSize = new SqlBench.Batching.DistributionSnapshot(),
        BatchFillMilliseconds = new SqlBench.Batching.DistributionSnapshot(),
        HandlerMilliseconds = new SqlBench.Batching.DistributionSnapshot()
    };

    private sealed record WorkerProcess(
        int WorkerId,
        Process Process,
        string ResultFile,
        string ReadyFile,
        string CompletionFile,
        Task<string> StandardOutput,
        Task<string> StandardError);
}
