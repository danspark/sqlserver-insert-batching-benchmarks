using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SqlBench.Core;

namespace SqlBench.Report;

internal static class ReportApplication
{
    private const string StartMarker = "<!-- RESULTS:START -->";
    private const string EndMarker = "<!-- RESULTS:END -->";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public static async Task<int> RunAsync(string[] args)
    {
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        CultureInfo.CurrentUICulture = CultureInfo.InvariantCulture;
        string resultDirectory = RequireOption(args, "--results");
        string readmePath = ReadOption(args, "--readme") ?? "README.md";
        string chartsDirectory = ReadOption(args, "--charts") ?? Path.Combine("docs", "charts");
        List<(string Path, ScenarioResult Result)> results = await LoadAsync(resultDirectory).ConfigureAwait(false);
        if (results.Count == 0)
        {
            throw new InvalidOperationException($"No scenario results were found under {resultDirectory}.");
        }

        Directory.CreateDirectory(chartsDirectory);
        string repositoryRoot = Path.GetDirectoryName(Path.GetFullPath(readmePath))!;
        await WriteCsvAsync(Path.Combine(resultDirectory, "summary.csv"), results).ConfigureAwait(false);
        List<ScenarioResult> valid = results.Select(static item => item.Result).Where(static result => result.Valid).ToList();
        List<ScenarioResult> bestQueue = BestByStrategy(valid.Where(static result =>
            result.Configuration.Mode == WorkloadMode.Queue));
        List<ScenarioResult> bestDirect = BestByStrategy(valid.Where(static result =>
            result.Configuration.Mode == WorkloadMode.DirectDatabase));
        ScenarioResult? noOp = valid
            .Where(static result => result.Configuration.Mode == WorkloadMode.NoOpQueue)
            .OrderByDescending(static result => result.AcknowledgedMessagesPerSecond)
            .FirstOrDefault();
        await File.WriteAllTextAsync(
            Path.Combine(chartsDirectory, "queue-throughput.svg"),
            RenderBarChart(bestQueue, "Best observed queue throughput", static result => result.CommittedRowsPerSecond, "committed rows/s"))
            .ConfigureAwait(false);
        await File.WriteAllTextAsync(
            Path.Combine(chartsDirectory, "queue-p99-latency.svg"),
            RenderBarChart(bestQueue, "p99 delivery-to-acknowledgment latency", static result => result.DeliveryToAcknowledgmentMilliseconds.P99, "milliseconds"))
            .ConfigureAwait(false);

        string generated = BuildMarkdown(results, valid, bestQueue, bestDirect, noOp, repositoryRoot, chartsDirectory);
        string readme = await File.ReadAllTextAsync(readmePath).ConfigureAwait(false);
        int start = readme.IndexOf(StartMarker, StringComparison.Ordinal);
        int end = readme.IndexOf(EndMarker, StringComparison.Ordinal);
        if (start < 0 || end <= start)
        {
            throw new InvalidOperationException("README result markers were not found or were out of order.");
        }

        string updated = readme[..(start + StartMarker.Length)]
            + Environment.NewLine
            + generated
            + Environment.NewLine
            + readme[end..];
        await File.WriteAllTextAsync(readmePath, updated).ConfigureAwait(false);
        Console.WriteLine($"Generated report from {results.Count} raw scenario files.");
        return results.Any(static item => !item.Result.Valid) ? 1 : 0;
    }

    private static async Task<List<(string Path, ScenarioResult Result)>> LoadAsync(string directory)
    {
        var results = new List<(string, ScenarioResult)>();
        foreach (string path in Directory.EnumerateFiles(directory, "*.json", SearchOption.AllDirectories))
        {
            if (Path.GetFileName(path) is "manifest.json" or "environment.json")
            {
                continue;
            }

            try
            {
                await using FileStream stream = File.OpenRead(path);
                ScenarioResult? result = await JsonSerializer.DeserializeAsync<ScenarioResult>(stream, JsonOptions)
                    .ConfigureAwait(false);
                if (result is not null)
                {
                    results.Add((path, result));
                }
            }
            catch (JsonException)
            {
                // Non-result JSON files are allowed beside raw result files.
            }
        }

        return results;
    }

    private static List<ScenarioResult> BestByStrategy(IEnumerable<ScenarioResult> source) => source
        .GroupBy(static result => result.Configuration.Strategy)
        .Select(static group => group.OrderByDescending(static result => result.CommittedRowsPerSecond).First())
        .OrderBy(static result => result.Configuration.Strategy)
        .ToList();

    private static string BuildMarkdown(
        List<(string Path, ScenarioResult Result)> all,
        List<ScenarioResult> valid,
        List<ScenarioResult> bestQueue,
        List<ScenarioResult> bestDirect,
        ScenarioResult? noOp,
        string repositoryRoot,
        string chartsDirectory)
    {
        var text = new StringBuilder();
        text.AppendLine();
        text.AppendLine("## Measured results");
        text.AppendLine();
        text.AppendLine($"Generated from {all.Count} raw scenario files. {valid.Count} passed every correctness check.");
        string measuredCommits = string.Join(", ", valid
            .GroupBy(static result => result.GitCommit)
            .OrderBy(static group => group.Key)
            .Select(static group => $"`{group.Key[..Math.Min(7, group.Key.Length)]}` ({group.Count()} runs)"));
        text.AppendLine($"Measured binaries: {measuredCommits}.");
        text.AppendLine();
        if (noOp is not null)
        {
            text.AppendLine($"The no-op queue ceiling was **{noOp.AcknowledgedMessagesPerSecond:N0} messages/s** " +
                $"with p99 delivery-to-acknowledgment latency of {noOp.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms.");
            text.AppendLine();
            ScenarioResult? fastestSql = bestQueue.OrderByDescending(static result => result.CommittedRowsPerSecond)
                .FirstOrDefault();
            if (fastestSql is not null && noOp.AcknowledgedMessagesPerSecond > fastestSql.CommittedRowsPerSecond)
            {
                text.AppendLine($"RabbitMQ did not set the observed ceiling: the no-op path was " +
                    $"{noOp.AcknowledgedMessagesPerSecond / fastestSql.CommittedRowsPerSecond:N2}x faster than the " +
                    $"fastest SQL-backed run ({fastestSql.ScenarioName}).");
            }
            else if (fastestSql is not null)
            {
                text.AppendLine($"RabbitMQ may limit {fastestSql.ScenarioName}: its SQL-backed rate met or exceeded " +
                    "the observed no-op queue ceiling, so that result is not attributed solely to SQL Server.");
            }

            text.AppendLine();
        }

        text.AppendLine("![Queue throughput](docs/charts/queue-throughput.svg)");
        text.AppendLine();
        text.AppendLine("![Queue p99 acknowledgment latency](docs/charts/queue-p99-latency.svg)");
        text.AppendLine();
        text.AppendLine("### Best observed queue configuration by strategy");
        text.AppendLine();
        text.AppendLine("| Strategy | Batcher | Workers x writers | Batch | Delay | Capacity | Prefetch | Distribution | Rows/s | p50 commit | p99 ack | Speedup | Correct |");
        text.AppendLine("|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---|");
        double baseline = bestQueue.FirstOrDefault(static result => result.Configuration.Strategy == InsertStrategyKind.Individual)
            ?.CommittedRowsPerSecond ?? 0;
        foreach (ScenarioResult result in bestQueue)
        {
            Scenario config = result.Configuration;
            double speedup = baseline > 0 ? result.CommittedRowsPerSecond / baseline : 0;
            text.AppendLine(
                $"| {StrategyName(config.Strategy)} | {config.Batching} | {config.WorkerInstances} x {config.WritersPerInstance} | " +
                $"{config.BatchSize:N0} | {config.MaximumBatchingDelayMilliseconds} ms | {config.ChannelCapacity:N0} | " +
                $"{config.RabbitMqPrefetch:N0} | {config.Distribution} | {result.CommittedRowsPerSecond:N0} | " +
                $"{result.DeliveryToCommitMilliseconds.P50:N2} ms | {result.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms | " +
                $"{speedup:N2}x | yes |");
        }

        IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult>[] confirmations = valid
            .Where(static result => result.Configuration.Stage == "confirmation")
            .GroupBy(static result => (result.Configuration.Strategy, result.GitCommit))
            .OrderBy(static group => group.Key.Strategy)
            .ThenBy(static group => group.Min(static result => result.Timestamp))
            .ToArray();
        if (confirmations.Length > 0)
        {
            text.AppendLine();
            text.AppendLine("### Long-run finalist confirmation");
            text.AppendLine();
            text.AppendLine("Results from different binaries are kept separate so a code change cannot silently alter a finalist's aggregate.");
            text.AppendLine();
            text.AppendLine("| Strategy | Binary | Repetitions | Rows/run | Configuration | Median rows/s | Range | Median p99 ack | Median duration |");
            text.AppendLine("|---|---|---:|---:|---|---:|---:|---:|---:|");
            foreach (IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult> group in confirmations)
            {
                Scenario config = group.First().Configuration;
                double[] rates = group.Select(static result => result.CommittedRowsPerSecond).Order().ToArray();
                double[] latencies = group.Select(static result => result.DeliveryToAcknowledgmentMilliseconds.P99).Order().ToArray();
                double[] durations = group.Select(static result => result.DurationSeconds).Order().ToArray();
                text.AppendLine(
                    $"| {StrategyName(group.Key.Strategy)} | `{ShortCommit(group.Key.Commit)}` | {group.Count()} | {config.RowCount:N0} | " +
                    $"{config.WorkerInstances} worker{(config.WorkerInstances == 1 ? string.Empty : "s")} x " +
                    $"{config.WritersPerInstance} writer{(config.WritersPerInstance == 1 ? string.Empty : "s")}, batch {config.BatchSize:N0} | " +
                    $"{Median(rates):N0} | {rates[0]:N0}–{rates[^1]:N0} | {Median(latencies):N2} ms | " +
                    $"{Median(durations):N2} s |");
            }

            AppendCrossBinaryComparison(text, confirmations);

            text.AppendLine();
            text.AppendLine("### Confirmation resource use");
            text.AppendLine();
            text.AppendLine("Each row is the repetition nearest that finalist's median throughput. Worker peak RSS is the sum of per-process peaks; the SQL values are DMV deltas over the run. Full before/after metrics, waits, GC counts, and one-second container samples remain in the raw artifacts.");
            text.AppendLine();
            text.AppendLine("| Strategy | Binary | App CPU | Worker peak RSS | Allocated/row | GC0 | GC1 | GC2 | SQL CPU | SQL writes | SQL write stall | WRITELOG wait | Rabbit memory |");
            text.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
            foreach (IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult> group in confirmations)
            {
                double[] rates = group.Select(static result => result.CommittedRowsPerSecond).Order().ToArray();
                double medianRate = Median(rates);
                ScenarioResult representative = group.MinBy(result =>
                    Math.Abs(result.CommittedRowsPerSecond - medianRate))!;
                double appCpuSeconds = representative.Workers.Sum(static worker => worker.Process.CpuSeconds);
                long peakWorkingSetBytes = representative.Workers.Sum(static worker => worker.Process.PeakWorkingSetBytes);
                long allocatedBytes = representative.Workers.Sum(static worker => worker.Process.AllocatedBytes);
                int generation0Collections = representative.Workers.Sum(static worker => worker.Process.Generation0Collections);
                int generation1Collections = representative.Workers.Sum(static worker => worker.Process.Generation1Collections);
                int generation2Collections = representative.Workers.Sum(static worker => worker.Process.Generation2Collections);
                long sqlCpuMilliseconds = Math.Max(0,
                    representative.SqlServerAfter.ProcessKernelTimeMilliseconds
                    + representative.SqlServerAfter.ProcessUserTimeMilliseconds
                    - representative.SqlServerBefore.ProcessKernelTimeMilliseconds
                    - representative.SqlServerBefore.ProcessUserTimeMilliseconds);
                long sqlWrites = Math.Max(0,
                    representative.SqlServerAfter.BytesWritten - representative.SqlServerBefore.BytesWritten);
                long sqlWriteStall = Math.Max(0,
                    representative.SqlServerAfter.WriteStallMilliseconds
                    - representative.SqlServerBefore.WriteStallMilliseconds);
                long writeLogWait = WaitDelta(representative, "WRITELOG");
                text.AppendLine(
                    $"| {StrategyName(group.Key.Strategy)} | `{ShortCommit(group.Key.Commit)}` | {appCpuSeconds:N1} s | " +
                    $"{ToMebibytes(peakWorkingSetBytes):N0} MiB | {allocatedBytes / (double)representative.Configuration.RowCount:N0} B | " +
                    $"{generation0Collections:N0} | {generation1Collections:N0} | {generation2Collections:N0} | " +
                    $"{sqlCpuMilliseconds / 1_000.0:N1} s | " +
                    $"{ToMebibytes(sqlWrites):N0} MiB | {sqlWriteStall:N0} ms | {writeLogWait:N0} ms | " +
                    $"{ToMebibytes(representative.RabbitMqAfter.MemoryBytes):N0} MiB |");
            }
        }

        text.AppendLine();
        text.AppendLine("### Direct-to-database controls");
        text.AppendLine();
        text.AppendLine("| Strategy | Writers | Batch | Direct rows/s | Comparable queue rows/s | Queue/direct | p50 commit delta | SQL p50 | Transaction p50 |");
        text.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (ScenarioResult result in bestDirect)
        {
            Scenario config = result.Configuration;
            ScenarioResult? queueResult = valid
                .Where(queue => queue.Configuration.Mode == WorkloadMode.Queue
                    && queue.Configuration.Strategy == config.Strategy
                    && queue.Configuration.Batching == config.Batching
                    && queue.Configuration.WorkerInstances == config.WorkerInstances
                    && queue.Configuration.WritersPerInstance == config.WritersPerInstance
                    && queue.Configuration.BatchSize == config.BatchSize
                    && queue.Configuration.Distribution == config.Distribution)
                .OrderByDescending(static queue => queue.Configuration.RowCount)
                .ThenByDescending(static queue => queue.CommittedRowsPerSecond)
                .FirstOrDefault();
            double queueRate = queueResult?.CommittedRowsPerSecond ?? 0;
            double throughputRatio = result.CommittedRowsPerSecond == 0 ? 0 : queueRate / result.CommittedRowsPerSecond;
            double latencyDelta = (queueResult?.DeliveryToCommitMilliseconds.P50 ?? 0)
                - result.DeliveryToCommitMilliseconds.P50;
            text.AppendLine(
                $"| {StrategyName(config.Strategy)} | {config.WorkerInstances * config.WritersPerInstance} | {config.BatchSize:N0} | " +
                $"{result.CommittedRowsPerSecond:N0} | {queueRate:N0} | {throughputRatio:P1} | {latencyDelta:N2} ms | " +
                $"{result.SqlExecutionMilliseconds.P50:N2} ms | {result.TransactionMilliseconds.P50:N2} ms |");
        }

        text.AppendLine();
        text.AppendLine("These direct controls are single 10,000-row runs, so the ratios estimate pipeline overhead rather than isolate it. A ratio above 100% reflects observed run-order and concurrency variance; it is not negative RabbitMQ overhead.");

        text.AppendLine();
        text.AppendLine("### Worker scaling");
        text.AppendLine();
        text.AppendLine("| Strategy | Workers | Best rows/s | p99 ack | Delivered share range | Mean batch size |");
        text.AppendLine("|---|---:|---:|---:|---:|---:|");
        IEnumerable<ScenarioResult> scaling = valid
            .Where(static result => result.Configuration.Mode == WorkloadMode.Queue && result.Configuration.Stage == "scaling")
            .GroupBy(static result => (result.Configuration.Strategy, result.Configuration.WorkerInstances))
            .Select(static group => group.OrderByDescending(static result => result.CommittedRowsPerSecond).First())
            .OrderBy(static result => result.Configuration.Strategy)
            .ThenBy(static result => result.Configuration.WorkerInstances);
        foreach (ScenarioResult result in scaling)
        {
            double[] shares = result.DeliveredMessages == 0
                ? [0]
                : result.Workers.Select(worker => 100.0 * worker.DeliveredMessages / result.DeliveredMessages).ToArray();
            double batch = result.Workers.Count == 0 ? 0 : result.Workers.Average(static worker => worker.BatcherMetrics.ActualBatchSize.Mean);
            text.AppendLine(
                $"| {StrategyName(result.Configuration.Strategy)} | {result.Configuration.WorkerInstances} | " +
                $"{result.CommittedRowsPerSecond:N0} | {result.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms | " +
                $"{shares.Min():N1}–{shares.Max():N1}% | {batch:N1} |");
        }

        text.AppendLine();
        text.AppendLine("### Throughput-latency Pareto frontier");
        text.AppendLine();
        text.AppendLine("| Strategy | Scenario | Rows/s | p99 ack | Batch | Writers | Workers |");
        text.AppendLine("|---|---|---:|---:|---:|---:|---:|");
        foreach (IGrouping<InsertStrategyKind, ScenarioResult> group in valid
            .Where(static result => result.Configuration.Mode == WorkloadMode.Queue)
            .GroupBy(static result => result.Configuration.Strategy))
        {
            foreach (ScenarioResult result in Pareto(group))
            {
                text.AppendLine(
                    $"| {StrategyName(group.Key)} | {result.ScenarioName} | {result.CommittedRowsPerSecond:N0} | " +
                    $"{result.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms | {result.Configuration.BatchSize:N0} | " +
                    $"{result.Configuration.WritersPerInstance} | {result.Configuration.WorkerInstances} |");
            }
        }

        ScenarioResult[] failed = all.Select(static item => item.Result).Where(static result => !result.Valid).ToArray();
        text.AppendLine();
        text.AppendLine("### Correctness");
        text.AppendLine();
        text.AppendLine(failed.Length == 0
            ? "Every published run passed row-count, distinct-ID, missing-ID, foreign-key, queue-ready, queue-unacknowledged, acknowledgment, and worker-error checks."
            : $"{failed.Length} run(s) failed correctness and were excluded: {string.Join(", ", failed.Select(static result => result.ScenarioName))}.");
        text.AppendLine();
        text.AppendLine("### Raw data");
        text.AppendLine();
        foreach ((string path, ScenarioResult result) in all.OrderBy(static item => item.Result.ScenarioName))
        {
            string relative = Path.GetRelativePath(repositoryRoot, Path.GetFullPath(path)).Replace('\\', '/');
            text.AppendLine($"- [{result.ScenarioName}, repetition {result.Configuration.Repetition}]({relative})");
        }

        return text.ToString().TrimEnd();
    }

    private static IEnumerable<ScenarioResult> Pareto(IEnumerable<ScenarioResult> source)
    {
        ScenarioResult[] values = source.ToArray();
        return values.Where(candidate => !values.Any(other =>
                other != candidate
                && other.CommittedRowsPerSecond >= candidate.CommittedRowsPerSecond
                && other.DeliveryToAcknowledgmentMilliseconds.P99 <= candidate.DeliveryToAcknowledgmentMilliseconds.P99
                && (other.CommittedRowsPerSecond > candidate.CommittedRowsPerSecond
                    || other.DeliveryToAcknowledgmentMilliseconds.P99 < candidate.DeliveryToAcknowledgmentMilliseconds.P99)))
            .OrderBy(static result => result.DeliveryToAcknowledgmentMilliseconds.P99);
    }

    private static double Median(double[] sorted) => sorted.Length % 2 == 0
        ? (sorted[(sorted.Length / 2) - 1] + sorted[sorted.Length / 2]) / 2
        : sorted[sorted.Length / 2];

    private static void AppendCrossBinaryComparison(
        StringBuilder text,
        IEnumerable<IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult>> confirmations)
    {
        foreach (IGrouping<InsertStrategyKind, IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult>> strategyGroup
            in confirmations.GroupBy(static group => group.Key.Strategy))
        {
            IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult>[] versions = strategyGroup
                .OrderBy(static group => group.Min(static result => result.Timestamp))
                .ToArray();
            if (versions.Length < 2)
            {
                continue;
            }

            IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult> baseline = versions[^2];
            IGrouping<(InsertStrategyKind Strategy, string Commit), ScenarioResult> current = versions[^1];
            if (!EquivalentConfiguration(baseline.First().Configuration, current.First().Configuration))
            {
                continue;
            }

            double baselineAllocation = Median(baseline
                .Select(static result => AllocatedBytesPerRow(result))
                .Order()
                .ToArray());
            double currentAllocation = Median(current
                .Select(static result => AllocatedBytesPerRow(result))
                .Order()
                .ToArray());
            double baselineThroughput = Median(baseline
                .Select(static result => result.CommittedRowsPerSecond)
                .Order()
                .ToArray());
            double currentThroughput = Median(current
                .Select(static result => result.CommittedRowsPerSecond)
                .Order()
                .ToArray());

            text.AppendLine();
            text.AppendLine("#### Cross-binary optimization check");
            text.AppendLine();
            text.AppendLine(
                $"For the same {StrategyName(strategyGroup.Key)} configuration and {current.First().Configuration.RowCount:N0}-row workload, " +
                $"`{ShortCommit(current.Key.Commit)}` used {currentAllocation:N0} worker-allocated bytes/row versus " +
                $"{baselineAllocation:N0} at `{ShortCommit(baseline.Key.Commit)}` ({PercentChange(baselineAllocation, currentAllocation)}). " +
                $"Median Gen0/Gen1/Gen2 collections changed from {MedianCollectionCount(baseline, 0):N0}/" +
                $"{MedianCollectionCount(baseline, 1):N0}/{MedianCollectionCount(baseline, 2):N0} to " +
                $"{MedianCollectionCount(current, 0):N0}/{MedianCollectionCount(current, 1):N0}/" +
                $"{MedianCollectionCount(current, 2):N0}. Median throughput changed from {baselineThroughput:N0} to " +
                $"{currentThroughput:N0} committed rows/s ({PercentChange(baselineThroughput, currentThroughput)}). " +
                "All repetitions passed correctness; the throughput spread shows why allocation and rate are reported independently.");
        }
    }

    private static double AllocatedBytesPerRow(ScenarioResult result) => result.Configuration.RowCount == 0
        ? 0
        : result.Workers.Sum(static worker => worker.Process.AllocatedBytes) / (double)result.Configuration.RowCount;

    private static double MedianCollectionCount(IEnumerable<ScenarioResult> results, int generation) => Median(results
        .Select(result => generation switch
        {
            0 => result.Workers.Sum(static worker => worker.Process.Generation0Collections),
            1 => result.Workers.Sum(static worker => worker.Process.Generation1Collections),
            _ => result.Workers.Sum(static worker => worker.Process.Generation2Collections)
        })
        .Select(static count => (double)count)
        .Order()
        .ToArray());

    private static bool EquivalentConfiguration(Scenario left, Scenario right) =>
        left.Mode == right.Mode
        && left.Strategy == right.Strategy
        && left.Batching == right.Batching
        && left.Distribution == right.Distribution
        && left.Seed == right.Seed
        && left.RowCount == right.RowCount
        && left.WorkerInstances == right.WorkerInstances
        && left.WritersPerInstance == right.WritersPerInstance
        && left.BatchSize == right.BatchSize
        && left.MaximumBatchingDelayMilliseconds == right.MaximumBatchingDelayMilliseconds
        && left.ChannelCapacity == right.ChannelCapacity
        && left.RabbitMqPrefetch == right.RabbitMqPrefetch;

    private static string PercentChange(double baseline, double current) => baseline == 0
        ? "n/a"
        : $"{((current / baseline) - 1) * 100:N1}%";

    private static string ShortCommit(string commit) => commit[..Math.Min(7, commit.Length)];

    private static long WaitDelta(ScenarioResult result, string waitType)
    {
        long before = result.SqlServerBefore.Waits
            .FirstOrDefault(wait => string.Equals(wait.WaitType, waitType, StringComparison.Ordinal))
            ?.WaitTimeMilliseconds ?? 0;
        long after = result.SqlServerAfter.Waits
            .FirstOrDefault(wait => string.Equals(wait.WaitType, waitType, StringComparison.Ordinal))
            ?.WaitTimeMilliseconds ?? 0;
        return Math.Max(0, after - before);
    }

    private static double ToMebibytes(long bytes) => bytes / 1_048_576.0;

    private static async Task WriteCsvAsync(
        string path,
        List<(string Path, ScenarioResult Result)> results)
    {
        var csv = new StringBuilder();
        csv.AppendLine("scenario,commit,timestamp,mode,strategy,batcher,workers,writers,batch,delay_ms,capacity,prefetch,distribution,rows,duration_s,committed_rows_s,ack_s,p50_commit_ms,p95_commit_ms,p99_commit_ms,p50_ack_ms,p95_ack_ms,p99_ack_ms,errors,correct");
        foreach (ScenarioResult result in results.Select(static item => item.Result))
        {
            Scenario config = result.Configuration;
            csv.AppendLine(string.Join(',',
                Csv(result.ScenarioName), Csv(result.GitCommit), Csv(result.Timestamp.ToString("O", CultureInfo.InvariantCulture)),
                config.Mode, config.Strategy, config.Batching, config.WorkerInstances, config.WritersPerInstance,
                config.BatchSize, config.MaximumBatchingDelayMilliseconds, config.ChannelCapacity, config.RabbitMqPrefetch,
                config.Distribution, config.RowCount,
                result.DurationSeconds.ToString("F6", CultureInfo.InvariantCulture),
                result.CommittedRowsPerSecond.ToString("F3", CultureInfo.InvariantCulture),
                result.AcknowledgedMessagesPerSecond.ToString("F3", CultureInfo.InvariantCulture),
                result.DeliveryToCommitMilliseconds.P50.ToString("F3", CultureInfo.InvariantCulture),
                result.DeliveryToCommitMilliseconds.P95.ToString("F3", CultureInfo.InvariantCulture),
                result.DeliveryToCommitMilliseconds.P99.ToString("F3", CultureInfo.InvariantCulture),
                result.DeliveryToAcknowledgmentMilliseconds.P50.ToString("F3", CultureInfo.InvariantCulture),
                result.DeliveryToAcknowledgmentMilliseconds.P95.ToString("F3", CultureInfo.InvariantCulture),
                result.DeliveryToAcknowledgmentMilliseconds.P99.ToString("F3", CultureInfo.InvariantCulture),
                result.Errors.Count, result.Correctness.Passed));
        }

        await File.WriteAllTextAsync(path, csv.ToString()).ConfigureAwait(false);
    }

    private static string RenderBarChart(
        List<ScenarioResult> results,
        string title,
        Func<ScenarioResult, double> selector,
        string unit)
    {
        const int width = 900;
        const int left = 205;
        const int barHeight = 42;
        int height = 90 + results.Count * 62;
        double maximum = Math.Max(1, results.Select(selector).DefaultIfEmpty(1).Max());
        var svg = new StringBuilder();
        svg.AppendLine($"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"{Escape(title)}\">");
        svg.AppendLine("<style>text{font-family:ui-sans-serif,system-ui,sans-serif;fill:#17202a}.title{font-size:22px;font-weight:700}.label{font-size:14px}.value{font-size:14px;font-weight:650}.bar{fill:#2878b5}</style>");
        svg.AppendLine($"<rect width=\"{width}\" height=\"{height}\" fill=\"#fff\"/><text class=\"title\" x=\"20\" y=\"34\">{Escape(title)}</text>");
        for (int index = 0; index < results.Count; index++)
        {
            ScenarioResult result = results[index];
            double value = selector(result);
            double barWidth = (width - left - 130) * value / maximum;
            int y = 62 + index * 62;
            svg.AppendLine($"<text class=\"label\" x=\"20\" y=\"{y + 27}\">{Escape(StrategyName(result.Configuration.Strategy))}</text>");
            svg.AppendLine($"<rect class=\"bar\" x=\"{left}\" y=\"{y}\" width=\"{barWidth.ToString("F1", CultureInfo.InvariantCulture)}\" height=\"{barHeight}\" rx=\"4\"/>");
            svg.AppendLine($"<text class=\"value\" x=\"{(left + barWidth + 8).ToString("F1", CultureInfo.InvariantCulture)}\" y=\"{y + 27}\">{value:N1} {Escape(unit)}</text>");
        }

        svg.AppendLine("</svg>");
        return svg.ToString();
    }

    private static string StrategyName(InsertStrategyKind kind) => kind switch
    {
        InsertStrategyKind.Individual => "Individual insert per message",
        InsertStrategyKind.TableValuedParameter => "Table-valued parameter",
        InsertStrategyKind.MultipleInsertStatements => "Multiple INSERT statements",
        InsertStrategyKind.MultiRowValues => "Multi-row VALUES",
        InsertStrategyKind.BulkCopy => "SqlBulkCopy",
        _ => kind.ToString()
    };

    private static string Csv(string value) => $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    private static string Escape(string value) => System.Net.WebUtility.HtmlEncode(value);

    private static string RequireOption(string[] args, string name) =>
        ReadOption(args, name) ?? throw new ArgumentException($"Missing required option {name}.");

    private static string? ReadOption(string[] args, string name)
    {
        int index = Array.FindIndex(args, value => string.Equals(value, name, StringComparison.OrdinalIgnoreCase));
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }
}
