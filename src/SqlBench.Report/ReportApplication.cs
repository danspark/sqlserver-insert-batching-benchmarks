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
        string readmeDirectory = Path.GetDirectoryName(Path.GetFullPath(readmePath))!;
        await WriteCsvAsync(Path.Combine(resultDirectory, "summary.csv"), results).ConfigureAwait(false);
        List<ScenarioResult> valid = results.Select(static item => item.Result).Where(static result => result.Valid).ToList();
        List<ScenarioResult> bestQueue = BestByVariant(valid.Where(static result =>
            result.Configuration.Mode == WorkloadMode.Queue));
        List<ScenarioResult> bestDirect = BestByVariant(valid.Where(static result =>
            result.Configuration.Mode == WorkloadMode.DirectDatabase));
        await File.WriteAllTextAsync(
            Path.Combine(chartsDirectory, "queue-throughput.svg"),
            RenderBarChart(bestQueue, "Best observed queue throughput", static result => result.CommittedRowsPerSecond, "committed rows/s"))
            .ConfigureAwait(false);
        await File.WriteAllTextAsync(
            Path.Combine(chartsDirectory, "queue-p99-latency.svg"),
            RenderBarChart(bestQueue, "p99 delivery-to-acknowledgment latency", static result => result.DeliveryToAcknowledgmentMilliseconds.P99, "milliseconds"))
            .ConfigureAwait(false);

        string generated = BuildMarkdown(results, valid, bestQueue, bestDirect, readmeDirectory, chartsDirectory);
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
        foreach (string path in Directory.EnumerateFiles(directory, "*.json", SearchOption.AllDirectories)
            .Order(StringComparer.Ordinal))
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

    private static List<ScenarioResult> BestByVariant(IEnumerable<ScenarioResult> source) => source
        .GroupBy(static result => (
            result.Configuration.Strategy,
            result.Configuration.SqlExecution))
        .Select(static group => group.OrderByDescending(static result => result.CommittedRowsPerSecond).First())
        .OrderBy(static result => result.Configuration.Strategy)
        .ThenBy(static result => result.Configuration.SqlExecution)
        .ToList();

    private static string BuildMarkdown(
        List<(string Path, ScenarioResult Result)> all,
        List<ScenarioResult> valid,
        List<ScenarioResult> bestQueue,
        List<ScenarioResult> bestDirect,
        string readmeDirectory,
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
            .Select(static group => $"`{ShortCommit(group.Key)}` ({group.Count()} runs)"));
        text.AppendLine($"Measured binaries: {measuredCommits}.");
        text.AppendLine();
        AppendSqlBatchConclusion(text, valid);
        AppendQueueCeilings(text, valid);

        string throughputChart = RelativeMarkdownPath(
            readmeDirectory,
            Path.Combine(chartsDirectory, "queue-throughput.svg"));
        string latencyChart = RelativeMarkdownPath(
            readmeDirectory,
            Path.Combine(chartsDirectory, "queue-p99-latency.svg"));
        text.AppendLine($"![Queue throughput]({throughputChart})");
        text.AppendLine();
        text.AppendLine($"![Queue p99 acknowledgment latency]({latencyChart})");
        text.AppendLine();
        text.AppendLine("### Best observed queue configuration by strategy and SQL execution API");
        text.AppendLine();
        text.AppendLine("| Strategy | SQL API | SqlBatch cap/lanes/delay | Batcher | Workers x writers | Batch | Delay | Capacity | Prefetch | Distribution | Rows | Rows/s | DML execute calls | Rows/execute | Commands/execute mean/p95/max | p50 commit | p99 ack | Speedup | Correct |");
        text.AppendLine("|---|---|---:|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|");
        double baseline = bestQueue.FirstOrDefault(static result =>
                result.Configuration.Strategy == InsertStrategyKind.Individual
                && result.Configuration.SqlExecution == SqlExecutionKind.Native)
            ?.CommittedRowsPerSecond ?? 0;
        foreach (ScenarioResult result in bestQueue)
        {
            Scenario config = result.Configuration;
            double speedup = baseline > 0 ? result.CommittedRowsPerSecond / baseline : 0;
            SqlRequestAggregate requests = RequestSummary(result);
            text.AppendLine(
                $"| {StrategyName(config.Strategy)} | {SqlApiName(config)} | {SqlBatchConfiguration(config)} | {config.Batching} | " +
                $"{config.WorkerInstances} x {config.WritersPerInstance} | " +
                $"{config.BatchSize:N0} | {config.MaximumBatchingDelayMilliseconds} ms | {config.ChannelCapacity:N0} | " +
                $"{config.RabbitMqPrefetch:N0} | {config.Distribution} | {config.RowCount:N0} | {result.CommittedRowsPerSecond:N0} | " +
                $"{RequestCount(requests.RequestCount)} | {RowsPerRequest(result, requests)} | {CommandDistribution(requests)} | " +
                $"{result.DeliveryToCommitMilliseconds.P50:N2} ms | {result.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms | " +
                $"{speedup:N2}x | yes |");
        }

        AppendSqlBatchComparisons(text, valid);

        IGrouping<ConfirmationKey, ScenarioResult>[] confirmations = valid
            .Where(static result => result.Configuration.Stage == "confirmation")
            .GroupBy(static result => new ConfirmationKey(
                result.Configuration.Strategy,
                result.Configuration.SqlExecution,
                result.GitCommit,
                result.ScenarioName))
            .OrderBy(static group => group.Key.Strategy)
            .ThenBy(static group => group.Key.SqlExecution)
            .ThenBy(static group => group.Min(static result => result.Timestamp))
            .ToArray();
        if (confirmations.Length > 0)
        {
            text.AppendLine();
            text.AppendLine("### Long-run finalist confirmation");
            text.AppendLine();
            text.AppendLine("Results from different binaries are kept separate so a code change cannot silently alter a finalist's aggregate.");
            text.AppendLine();
            text.AppendLine("DML execute counts and actual commands per execute come from the repetition nearest the median throughput.");
            text.AppendLine();
            text.AppendLine("| Strategy | SQL API | SqlBatch cap/lanes/delay | Binary | Repetitions | Rows/run | Configuration | DML execute calls | Commands/execute mean/p95/max | Median rows/s | Range | Median p99 ack | Median duration |");
            text.AppendLine("|---|---|---:|---|---:|---:|---|---:|---:|---:|---:|---:|---:|");
            foreach (IGrouping<ConfirmationKey, ScenarioResult> group in confirmations)
            {
                Scenario config = group.First().Configuration;
                double[] rates = group.Select(static result => result.CommittedRowsPerSecond).Order().ToArray();
                double[] latencies = group.Select(static result => result.DeliveryToAcknowledgmentMilliseconds.P99).Order().ToArray();
                double[] durations = group.Select(static result => result.DurationSeconds).Order().ToArray();
                double medianRate = Median(rates);
                ScenarioResult representative = group.MinBy(result =>
                    Math.Abs(result.CommittedRowsPerSecond - medianRate))!;
                SqlRequestAggregate requests = RequestSummary(representative);
                text.AppendLine(
                    $"| {StrategyName(group.Key.Strategy)} | {SqlApiName(config)} | {SqlBatchConfiguration(config)} | " +
                    $"`{ShortCommit(group.Key.Commit)}` | " +
                    $"{group.Count()} | {config.RowCount:N0} | " +
                    $"{config.WorkerInstances} worker{(config.WorkerInstances == 1 ? string.Empty : "s")} x " +
                    $"{config.WritersPerInstance} writer{(config.WritersPerInstance == 1 ? string.Empty : "s")}, batch {config.BatchSize:N0} | " +
                    $"{RequestCount(requests.RequestCount)} | {CommandDistribution(requests)} | " +
                    $"{medianRate:N0} | {rates[0]:N0}–{rates[^1]:N0} | {Median(latencies):N2} ms | " +
                    $"{Median(durations):N2} s |");
            }

            AppendCrossBinaryComparison(text, confirmations);

            text.AppendLine();
            text.AppendLine("### Confirmation resource use");
            text.AppendLine();
            text.AppendLine("Each row is the repetition nearest that finalist's median throughput. Worker peak RSS is the sum of per-process peaks; the SQL values are DMV deltas over the run. Full before/after metrics, waits, GC counts, and one-second container samples remain in the raw artifacts.");
            text.AppendLine();
            text.AppendLine("| Strategy | SQL API | Binary | App CPU | Worker peak RSS | Allocated/row | GC0 | GC1 | GC2 | SQL CPU | SQL writes | SQL write stall | WRITELOG wait | Rabbit memory |");
            text.AppendLine("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
            foreach (IGrouping<ConfirmationKey, ScenarioResult> group in confirmations)
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
                    $"| {StrategyName(group.Key.Strategy)} | {SqlApiName(representative.Configuration)} | " +
                    $"`{ShortCommit(group.Key.Commit)}` | {appCpuSeconds:N1} s | " +
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
        text.AppendLine("| Strategy | SQL API | SqlBatch cap/lanes/delay | Writers | Batch | DML execute calls | Commands/execute mean/p95/max | Direct rows/s | Comparable queue rows/s | Queue/direct | p50 commit delta | SQL p50 | Transaction p50 |");
        text.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach (ScenarioResult result in bestDirect)
        {
            Scenario config = result.Configuration;
            ScenarioResult? queueResult = valid
                .Where(queue => queue.Configuration.Mode == WorkloadMode.Queue
                    && queue.GitCommit == result.GitCommit
                    && ComparablePipeline(queue.Configuration, config))
                .OrderByDescending(static queue => queue.CommittedRowsPerSecond)
                .FirstOrDefault();
            double queueRate = queueResult?.CommittedRowsPerSecond ?? 0;
            double throughputRatio = result.CommittedRowsPerSecond == 0 ? 0 : queueRate / result.CommittedRowsPerSecond;
            double latencyDelta = (queueResult?.DeliveryToCommitMilliseconds.P50 ?? 0)
                - result.DeliveryToCommitMilliseconds.P50;
            SqlRequestAggregate requests = RequestSummary(result);
            text.AppendLine(
                $"| {StrategyName(config.Strategy)} | {SqlApiName(config)} | {SqlBatchConfiguration(config)} | " +
                $"{config.WorkerInstances * config.WritersPerInstance} | {config.BatchSize:N0} | " +
                $"{RequestCount(requests.RequestCount)} | {CommandDistribution(requests)} | " +
                $"{result.CommittedRowsPerSecond:N0} | {queueRate:N0} | {throughputRatio:P1} | {latencyDelta:N2} ms | " +
                $"{result.SqlExecutionMilliseconds.P50:N2} ms | {result.TransactionMilliseconds.P50:N2} ms |");
        }

        text.AppendLine();
        text.AppendLine("These direct controls use the row count recorded in each raw result, so the ratios estimate pipeline overhead rather than isolate it. A ratio above 100% reflects observed run-order and concurrency variance; it is not negative RabbitMQ overhead.");

        text.AppendLine();
        text.AppendLine("### Worker scaling");
        text.AppendLine();
        text.AppendLine("| Strategy | SQL API | SqlBatch cap/lanes/delay | Workers | Best rows/s | DML execute calls | Commands/execute mean/p95/max | p99 ack | Delivered share range | Mean batch size |");
        text.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        IEnumerable<ScenarioResult> scaling = valid
            .Where(static result => result.Configuration.Mode == WorkloadMode.Queue
                && result.Configuration.Stage is "scaling" or "instance-scaling")
            .GroupBy(static result => (
                result.Configuration.Strategy,
                result.Configuration.SqlExecution,
                result.Configuration.WorkerInstances))
            .Select(static group => group.OrderByDescending(static result => result.CommittedRowsPerSecond).First())
            .OrderBy(static result => result.Configuration.Strategy)
            .ThenBy(static result => result.Configuration.SqlExecution)
            .ThenBy(static result => result.Configuration.WorkerInstances);
        foreach (ScenarioResult result in scaling)
        {
            double[] shares = result.DeliveredMessages == 0
                ? [0]
                : result.Workers.Select(worker => 100.0 * worker.DeliveredMessages / result.DeliveredMessages).ToArray();
            double batch = result.Workers.Count == 0 ? 0 : result.Workers.Average(static worker => worker.BatcherMetrics.ActualBatchSize.Mean);
            SqlRequestAggregate requests = RequestSummary(result);
            text.AppendLine(
                $"| {StrategyName(result.Configuration.Strategy)} | {SqlApiName(result.Configuration)} | " +
                $"{SqlBatchConfiguration(result.Configuration)} | " +
                $"{result.Configuration.WorkerInstances} | {result.CommittedRowsPerSecond:N0} | " +
                $"{RequestCount(requests.RequestCount)} | {CommandDistribution(requests)} | " +
                $"{result.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms | " +
                $"{shares.Min():N1}–{shares.Max():N1}% | {batch:N1} |");
        }

        text.AppendLine();
        text.AppendLine("### Throughput-latency Pareto frontier");
        text.AppendLine();
        text.AppendLine("Frontiers compare only runs from the same binary, row count, and data distribution.");
        text.AppendLine();
        text.AppendLine("| Strategy | Binary | Rows | Distribution | SQL API | Scenario | Rows/s | p99 ack | Batch | Writers | Workers |");
        text.AppendLine("|---|---|---:|---|---|---|---:|---:|---:|---:|---:|");
        foreach (IGrouping<(
            InsertStrategyKind Strategy,
            string Commit,
            int RowCount,
            ParentDistribution Distribution), ScenarioResult> group in valid
            .Where(static result => result.Configuration.Mode == WorkloadMode.Queue)
            .GroupBy(static result => (
                Strategy: result.Configuration.Strategy,
                Commit: result.GitCommit,
                RowCount: result.Configuration.RowCount,
                Distribution: result.Configuration.Distribution))
            .OrderBy(static group => group.Key.Strategy)
            .ThenBy(static group => group.Key.Commit)
            .ThenBy(static group => group.Key.RowCount)
            .ThenBy(static group => group.Key.Distribution))
        {
            foreach (ScenarioResult result in Pareto(group))
            {
                text.AppendLine(
                    $"| {StrategyName(group.Key.Strategy)} | `{ShortCommit(group.Key.Commit)}` | " +
                    $"{group.Key.RowCount:N0} | {group.Key.Distribution} | {SqlApiName(result.Configuration)} | " +
                    $"{result.ScenarioName} | {result.CommittedRowsPerSecond:N0} | " +
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
        foreach ((string path, ScenarioResult result) in all
            .OrderBy(static item => item.Result.ScenarioName)
            .ThenBy(static item => item.Path, StringComparer.Ordinal))
        {
            string relative = RelativeMarkdownPath(readmeDirectory, path);
            text.AppendLine($"- [{result.ScenarioName}, repetition {result.Configuration.Repetition}]({relative})");
        }

        return text.ToString().TrimEnd();
    }

    private static void AppendSqlBatchConclusion(StringBuilder text, IReadOnlyList<ScenarioResult> valid)
    {
        IGrouping<(string Commit, InsertStrategyKind Strategy, SqlExecutionKind Execution), ScenarioResult>[] finalists = valid
            .Where(static result => result.Configuration is
            {
                Mode: WorkloadMode.Queue,
                Stage: "confirmation"
            })
            .GroupBy(static result => (
                Commit: result.GitCommit,
                Strategy: result.Configuration.Strategy,
                Execution: result.Configuration.SqlExecution))
            .ToArray();
        var comparisons = new List<(string Commit, InsertStrategyKind Strategy, double Native, double SqlBatch)>();
        foreach (IGrouping<(string Commit, InsertStrategyKind Strategy),
            IGrouping<(string Commit, InsertStrategyKind Strategy, SqlExecutionKind Execution), ScenarioResult>> group
            in finalists.GroupBy(static group => (group.Key.Commit, group.Key.Strategy)))
        {
            IGrouping<(string Commit, InsertStrategyKind Strategy, SqlExecutionKind Execution), ScenarioResult>? native =
                group.FirstOrDefault(static candidate => candidate.Key.Execution == SqlExecutionKind.Native);
            IGrouping<(string Commit, InsertStrategyKind Strategy, SqlExecutionKind Execution), ScenarioResult>? sqlBatch =
                group.FirstOrDefault(static candidate => candidate.Key.Execution == SqlExecutionKind.SqlBatch);
            if (native is null || sqlBatch is null)
            {
                continue;
            }

            comparisons.Add((
                group.Key.Commit,
                group.Key.Strategy,
                Median(native.Select(static result => result.CommittedRowsPerSecond).Order().ToArray()),
                Median(sqlBatch.Select(static result => result.CommittedRowsPerSecond).Order().ToArray())));
        }

        if (comparisons.Count == 0)
        {
            return;
        }

        text.AppendLine("### SqlBatch conclusion");
        text.AppendLine();
        text.AppendLine("These are observed medians from the repeated finalist matrix, not global optima. Throughput remains the selection metric even when the faster path allocates more.");
        text.AppendLine();
        text.AppendLine("| Binary | Strategy | Native median rows/s | SqlBatch median rows/s | Change | Faster tested API |");
        text.AppendLine("|---|---|---:|---:|---:|---|");
        foreach ((string commit, InsertStrategyKind strategy, double native, double sqlBatch) in comparisons
            .OrderBy(static comparison => comparison.Commit)
            .ThenBy(static comparison => comparison.Strategy))
        {
            text.AppendLine(
                $"| `{ShortCommit(commit)}` | {StrategyName(strategy)} | {native:N0} | {sqlBatch:N0} | " +
                $"{PercentChange(native, sqlBatch)} | {(sqlBatch > native ? "SqlBatch" : "Native")} |");
        }

        text.AppendLine();
    }

    private static void AppendQueueCeilings(StringBuilder text, IReadOnlyList<ScenarioResult> valid)
    {
        IGrouping<string, ScenarioResult>[] byCommit = valid
            .GroupBy(static result => result.GitCommit)
            .OrderBy(static group => group.Key)
            .ToArray();
        if (byCommit.Length == 0)
        {
            return;
        }

        text.AppendLine("### Same-binary no-op queue ceilings");
        text.AppendLine();
        text.AppendLine("A ceiling is compared only when its binary, worker topology, process batcher, batch settings, prefetch, seed, and distribution match that binary's fastest SQL-backed run.");
        text.AppendLine();
        text.AppendLine("| Binary | Fastest SQL-backed scenario | SQL rows/s | Matching no-op ack/s | No-op p99 ack | Headroom | Attribution |");
        text.AppendLine("|---|---|---:|---:|---:|---:|---|");
        foreach (IGrouping<string, ScenarioResult> commitResults in byCommit)
        {
            ScenarioResult? fastestSql = commitResults
                .Where(static result => result.Configuration.Mode == WorkloadMode.Queue)
                .OrderByDescending(static result => result.CommittedRowsPerSecond)
                .FirstOrDefault();
            if (fastestSql is null)
            {
                continue;
            }

            ScenarioResult? noOp = commitResults
                .Where(result => result.Configuration.Mode == WorkloadMode.NoOpQueue
                    && ComparableQueueControl(fastestSql.Configuration, result.Configuration))
                .OrderByDescending(static result => result.AcknowledgedMessagesPerSecond)
                .FirstOrDefault();
            if (noOp is null)
            {
                text.AppendLine(
                    $"| `{ShortCommit(commitResults.Key)}` | {fastestSql.ScenarioName} | " +
                    $"{fastestSql.CommittedRowsPerSecond:N0} | n/a | n/a | n/a | unavailable: no matching control |");
                continue;
            }

            double headroom = noOp.AcknowledgedMessagesPerSecond / fastestSql.CommittedRowsPerSecond;
            string attribution = headroom > 1
                ? "RabbitMQ ceiling not reached"
                : "queue may limit this scenario";
            text.AppendLine(
                $"| `{ShortCommit(commitResults.Key)}` | {fastestSql.ScenarioName} | " +
                $"{fastestSql.CommittedRowsPerSecond:N0} | {noOp.AcknowledgedMessagesPerSecond:N0} | " +
                $"{noOp.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms | {headroom:N2}x | {attribution} |");
        }

        text.AppendLine();
    }

    private static void AppendSqlBatchComparisons(StringBuilder text, IReadOnlyList<ScenarioResult> valid)
    {
        var pairs = new List<(ScenarioResult Native, ScenarioResult SqlBatch)>();
        foreach (ScenarioResult sqlBatch in valid.Where(static result =>
            result.Configuration.Mode == WorkloadMode.Queue
            && result.Configuration.SqlExecution == SqlExecutionKind.SqlBatch))
        {
            ScenarioResult? native = valid
                .Where(candidate => candidate.Configuration.Mode == WorkloadMode.Queue
                    && candidate.Configuration.SqlExecution == SqlExecutionKind.Native
                    && candidate.GitCommit == sqlBatch.GitCommit
                    && candidate.Configuration.Repetition == sqlBatch.Configuration.Repetition
                    && ComparableAcrossSqlApis(candidate.Configuration, sqlBatch.Configuration))
                .MinBy(candidate => Math.Abs((candidate.Timestamp - sqlBatch.Timestamp).Ticks));
            if (native is not null)
            {
                pairs.Add((native, sqlBatch));
            }
        }

        if (pairs.Count == 0)
        {
            return;
        }

        text.AppendLine();
        text.AppendLine("### Paired SqlCommand and SqlBatch executions");
        text.AppendLine();
        text.AppendLine("Each pair uses the same measured binary, rows, seed, distribution, worker topology, process batcher, logical batch size, capacity, and prefetch. Each SqlBatch request lane has one outstanding execution and its commands execute serially on that connection. Counts are DML execute API invocations. Native transaction begin and commit operations are outside this counter, so it is not a total network or TDS round-trip count. A SqlBatch execute is sent as one TDS request containing one RPC record per command.");
        text.AppendLine();
        text.AppendLine("| Stage | Repetition | Strategy | Distribution | Workers x writers | Logical batch | SqlBatch cap/lanes/delay | Actual commands/execute mean/p95/max | Native rows/s | SqlBatch rows/s | Rate change | Native DML executes | SqlBatch DML executes | Execute reduction | SqlBatch p99 ack |");
        text.AppendLine("|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        foreach ((ScenarioResult native, ScenarioResult sqlBatch) in pairs
            .OrderBy(static pair => StageRank(pair.SqlBatch.Configuration.Stage))
            .ThenBy(static pair => pair.SqlBatch.Configuration.Strategy)
            .ThenBy(static pair => pair.SqlBatch.Configuration.Distribution)
            .ThenBy(static pair => pair.SqlBatch.Configuration.WorkerInstances)
            .ThenBy(static pair => pair.SqlBatch.Configuration.WritersPerInstance)
            .ThenBy(static pair => pair.SqlBatch.Configuration.SqlBatchMaximumCommands)
            .ThenBy(static pair => pair.SqlBatch.Configuration.SqlBatchRequestConcurrency)
            .ThenBy(static pair => pair.SqlBatch.Configuration.SqlBatchMaximumDelayMilliseconds))
        {
            SqlRequestAggregate nativeRequests = RequestSummary(native);
            SqlRequestAggregate batchRequests = RequestSummary(sqlBatch);
            double rateChange = native.CommittedRowsPerSecond == 0
                ? 0
                : (sqlBatch.CommittedRowsPerSecond / native.CommittedRowsPerSecond) - 1;
            double requestReduction = nativeRequests.RequestCount == 0
                ? 0
                : 1 - (batchRequests.RequestCount / (double)nativeRequests.RequestCount);
            Scenario config = sqlBatch.Configuration;
            text.AppendLine(
                $"| {config.Stage} | {config.Repetition} | {StrategyName(config.Strategy)} | {config.Distribution} | " +
                $"{config.WorkerInstances} x {config.WritersPerInstance} | {config.BatchSize:N0} | " +
                $"{config.SqlBatchMaximumCommands}/{config.SqlBatchRequestConcurrency}/{config.SqlBatchMaximumDelayMilliseconds} ms | " +
                $"{CommandDistribution(batchRequests)} | {native.CommittedRowsPerSecond:N0} | " +
                $"{sqlBatch.CommittedRowsPerSecond:N0} | {rateChange:P1} | " +
                $"{RequestCount(nativeRequests.RequestCount)} | {RequestCount(batchRequests.RequestCount)} | " +
                $"{requestReduction:P1} | {sqlBatch.DeliveryToAcknowledgmentMilliseconds.P99:N2} ms |");
        }
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
        IEnumerable<IGrouping<ConfirmationKey, ScenarioResult>> confirmations)
    {
        foreach (IGrouping<(
            InsertStrategyKind Strategy,
            SqlExecutionKind SqlExecution,
            string ScenarioName),
            IGrouping<ConfirmationKey, ScenarioResult>> strategyGroup
            in confirmations.GroupBy(static group => (
                group.Key.Strategy,
                group.Key.SqlExecution,
                group.Key.ScenarioName)))
        {
            IGrouping<ConfirmationKey, ScenarioResult>[] versions = strategyGroup
                .OrderBy(static group => group.Min(static result => result.Timestamp))
                .ToArray();
            if (versions.Length < 2)
            {
                continue;
            }

            IGrouping<ConfirmationKey, ScenarioResult> baseline = versions[^2];
            IGrouping<ConfirmationKey, ScenarioResult> current = versions[^1];
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
                $"For the same {StrategyName(strategyGroup.Key.Strategy)} configuration using " +
                $"{SqlApiName(current.First().Configuration)} and a {current.First().Configuration.RowCount:N0}-row workload, " +
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
        && left.SqlExecution == right.SqlExecution
        && left.Distribution == right.Distribution
        && left.Seed == right.Seed
        && left.RowCount == right.RowCount
        && left.WorkerInstances == right.WorkerInstances
        && left.WritersPerInstance == right.WritersPerInstance
        && left.BatchSize == right.BatchSize
        && left.MaximumBatchingDelayMilliseconds == right.MaximumBatchingDelayMilliseconds
        && left.ChannelCapacity == right.ChannelCapacity
        && left.RabbitMqPrefetch == right.RabbitMqPrefetch
        && left.SqlBatchMaximumCommands == right.SqlBatchMaximumCommands
        && left.SqlBatchMaximumDelayMilliseconds == right.SqlBatchMaximumDelayMilliseconds
        && left.SqlBatchRequestConcurrency == right.SqlBatchRequestConcurrency;

    private static bool ComparablePipeline(Scenario queue, Scenario direct) =>
        queue.Strategy == direct.Strategy
        && queue.Batching == direct.Batching
        && queue.SqlExecution == direct.SqlExecution
        && queue.Distribution == direct.Distribution
        && queue.Seed == direct.Seed
        && queue.RowCount == direct.RowCount
        && queue.WorkerInstances == direct.WorkerInstances
        && queue.WritersPerInstance == direct.WritersPerInstance
        && queue.BatchSize == direct.BatchSize
        && queue.MaximumBatchingDelayMilliseconds == direct.MaximumBatchingDelayMilliseconds
        && queue.ChannelCapacity == direct.ChannelCapacity
        && queue.RabbitMqPrefetch == direct.RabbitMqPrefetch
        && queue.SqlBatchMaximumCommands == direct.SqlBatchMaximumCommands
        && queue.SqlBatchMaximumDelayMilliseconds == direct.SqlBatchMaximumDelayMilliseconds
        && queue.SqlBatchRequestConcurrency == direct.SqlBatchRequestConcurrency
        && queue.CommandTimeoutSeconds == direct.CommandTimeoutSeconds
        && queue.BulkCopyTimeoutSeconds == direct.BulkCopyTimeoutSeconds
        && queue.BulkCopyTableLock == direct.BulkCopyTableLock
        && queue.BulkCopyEnableStreaming == direct.BulkCopyEnableStreaming;

    private static bool ComparableQueueControl(Scenario queue, Scenario noOp) =>
        queue.Batching == noOp.Batching
        && queue.Distribution == noOp.Distribution
        && queue.Seed == noOp.Seed
        && queue.WorkerInstances == noOp.WorkerInstances
        && queue.WritersPerInstance == noOp.WritersPerInstance
        && queue.BatchSize == noOp.BatchSize
        && queue.MaximumBatchingDelayMilliseconds == noOp.MaximumBatchingDelayMilliseconds
        && queue.ChannelCapacity == noOp.ChannelCapacity
        && queue.RabbitMqPrefetch == noOp.RabbitMqPrefetch;

    private static bool ComparableAcrossSqlApis(Scenario native, Scenario sqlBatch) =>
        native.Strategy == sqlBatch.Strategy
        && native.Batching == sqlBatch.Batching
        && native.Distribution == sqlBatch.Distribution
        && native.Seed == sqlBatch.Seed
        && native.RowCount == sqlBatch.RowCount
        && native.WorkerInstances == sqlBatch.WorkerInstances
        && native.WritersPerInstance == sqlBatch.WritersPerInstance
        && native.BatchSize == sqlBatch.BatchSize
        && native.MaximumBatchingDelayMilliseconds == sqlBatch.MaximumBatchingDelayMilliseconds
        && native.ChannelCapacity == sqlBatch.ChannelCapacity
        && native.RabbitMqPrefetch == sqlBatch.RabbitMqPrefetch
        && native.CommandTimeoutSeconds == sqlBatch.CommandTimeoutSeconds
        && native.BulkCopyTimeoutSeconds == sqlBatch.BulkCopyTimeoutSeconds
        && native.BulkCopyTableLock == sqlBatch.BulkCopyTableLock
        && native.BulkCopyEnableStreaming == sqlBatch.BulkCopyEnableStreaming;

    private static int StageRank(string stage) => stage switch
    {
        "single-command-control" => 0,
        "writer-scaling" or "sqlbatch-writers" => 1,
        "command-cap" or "sqlbatch-commands" => 2,
        "request-concurrency" => 3,
        "delay" or "sqlbatch-delay" => 4,
        "distribution" => 5,
        "instance-scaling" => 6,
        "confirmation" => 7,
        _ => 8
    };

    private static string PercentChange(double baseline, double current) => baseline == 0
        ? "n/a"
        : $"{((current / baseline) - 1) * 100:N1}%";

    private static string ShortCommit(string commit)
    {
        const string dirtySuffix = "-dirty";
        bool isDirty = commit.EndsWith(dirtySuffix, StringComparison.Ordinal);
        int cleanLength = isDirty ? commit.Length - dirtySuffix.Length : commit.Length;
        string shortened = commit[..Math.Min(7, cleanLength)];
        return isDirty ? shortened + dirtySuffix : shortened;
    }

    private static string RelativeMarkdownPath(string baseDirectory, string path) =>
        Path.GetRelativePath(baseDirectory, Path.GetFullPath(path)).Replace('\\', '/');

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
        csv.AppendLine("scenario,commit,timestamp,mode,strategy,sql_execution,batcher,workers,writers,batch,delay_ms,capacity,prefetch,sqlbatch_max_commands,sqlbatch_delay_ms,sqlbatch_request_concurrency,dml_execute_calls,sql_commands,rows_per_execute,mean_commands_per_execute,p95_commands_per_execute,max_commands_per_execute,distribution,rows,duration_s,committed_rows_s,ack_s,p50_commit_ms,p95_commit_ms,p99_commit_ms,p50_ack_ms,p95_ack_ms,p99_ack_ms,errors,correct");
        foreach (ScenarioResult result in results.Select(static item => item.Result))
        {
            Scenario config = result.Configuration;
            SqlRequestAggregate requests = RequestSummary(result);
            csv.AppendLine(string.Join(',',
                Csv(result.ScenarioName), Csv(result.GitCommit), Csv(result.Timestamp.ToString("O", CultureInfo.InvariantCulture)),
                config.Mode, config.Strategy, config.SqlExecution, config.Batching,
                config.WorkerInstances, config.WritersPerInstance,
                config.BatchSize, config.MaximumBatchingDelayMilliseconds, config.ChannelCapacity, config.RabbitMqPrefetch,
                config.SqlBatchMaximumCommands, config.SqlBatchMaximumDelayMilliseconds,
                config.SqlBatchRequestConcurrency,
                requests.RequestCount, requests.CommandCount,
                requests.RequestCount == 0
                    ? "0"
                    : (result.CommittedRows / (double)requests.RequestCount).ToString("F3", CultureInfo.InvariantCulture),
                requests.MeanCommandsPerRequest.ToString("F3", CultureInfo.InvariantCulture),
                requests.P95CommandsPerRequest, requests.MaximumCommandsPerRequest,
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
        const int width = 1_050;
        const int left = 330;
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
            svg.AppendLine($"<text class=\"label\" x=\"20\" y=\"{y + 27}\">{Escape(VariantName(result.Configuration))}</text>");
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

    private static string SqlApiName(Scenario scenario) => scenario.SqlExecution switch
    {
        SqlExecutionKind.SqlBatch => "SqlBatch",
        _ when scenario.Strategy == InsertStrategyKind.BulkCopy => "SqlBulkCopy",
        _ => "SqlCommand"
    };

    private static string SqlBatchConfiguration(Scenario scenario) => scenario.SqlExecution switch
    {
        SqlExecutionKind.SqlBatch =>
            $"{scenario.SqlBatchMaximumCommands}/{scenario.SqlBatchRequestConcurrency}/{scenario.SqlBatchMaximumDelayMilliseconds} ms",
        _ => "n/a"
    };

    private static string VariantName(Scenario scenario) =>
        $"{StrategyName(scenario.Strategy)} ({SqlApiName(scenario)})";

    private static SqlRequestAggregate RequestSummary(ScenarioResult result)
    {
        long requests = result.Workers.Sum(static worker => worker.SqlRequests.RequestCount);
        long commands = result.Workers.Sum(static worker => worker.SqlRequests.CommandCount);
        int bucketCount = result.Workers
            .Select(static worker => worker.SqlRequests.CommandsPerRequestCounts.Length)
            .DefaultIfEmpty(0)
            .Max();
        if (bucketCount == 0)
        {
            return new SqlRequestAggregate(
                requests,
                commands,
                requests == 0 ? 0 : commands / (double)requests,
                result.Workers.Select(static worker => worker.SqlRequests.P50CommandsPerRequest).DefaultIfEmpty(0).Max(),
                result.Workers.Select(static worker => worker.SqlRequests.P95CommandsPerRequest).DefaultIfEmpty(0).Max(),
                result.Workers.Select(static worker => worker.SqlRequests.P99CommandsPerRequest).DefaultIfEmpty(0).Max(),
                result.Workers.Select(static worker => worker.SqlRequests.MaximumCommandsPerRequest).DefaultIfEmpty(0).Max());
        }

        var buckets = new long[bucketCount];
        foreach (WorkerRunResult worker in result.Workers)
        {
            long[] source = worker.SqlRequests.CommandsPerRequestCounts;
            for (int index = 0; index < source.Length; index++)
            {
                buckets[index] += source[index];
            }
        }

        return new SqlRequestAggregate(
            requests,
            commands,
            requests == 0 ? 0 : commands / (double)requests,
            BucketPercentile(buckets, requests, 0.50),
            BucketPercentile(buckets, requests, 0.95),
            BucketPercentile(buckets, requests, 0.99),
            Array.FindLastIndex(buckets, static count => count != 0));
    }

    private static int BucketPercentile(long[] buckets, long count, double percentile)
    {
        if (count == 0)
        {
            return 0;
        }

        long target = Math.Max(1, checked((long)Math.Ceiling(count * percentile)));
        long cumulative = 0;
        for (int index = 1; index < buckets.Length; index++)
        {
            cumulative += buckets[index];
            if (cumulative >= target)
            {
                return index;
            }
        }

        return 0;
    }

    private static string RequestCount(long requestCount) => requestCount == 0
        ? "n/a"
        : requestCount.ToString("N0", CultureInfo.InvariantCulture);

    private static string RowsPerRequest(ScenarioResult result, SqlRequestAggregate requests) =>
        requests.RequestCount == 0
            ? "n/a"
            : (result.CommittedRows / (double)requests.RequestCount).ToString("N1", CultureInfo.InvariantCulture);

    private static string CommandDistribution(SqlRequestAggregate requests) => requests.RequestCount == 0
        ? "n/a"
        : $"{requests.MeanCommandsPerRequest:N2}/{requests.P95CommandsPerRequest}/{requests.MaximumCommandsPerRequest}";

    private static string Csv(string value) => $"\"{value.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    private static string Escape(string value) => System.Net.WebUtility.HtmlEncode(value);

    private static string RequireOption(string[] args, string name) =>
        ReadOption(args, name) ?? throw new ArgumentException($"Missing required option {name}.");

    private static string? ReadOption(string[] args, string name)
    {
        int index = Array.FindIndex(args, value => string.Equals(value, name, StringComparison.OrdinalIgnoreCase));
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }

    private sealed record ConfirmationKey(
        InsertStrategyKind Strategy,
        SqlExecutionKind SqlExecution,
        string Commit,
        string ScenarioName);

    private sealed record SqlRequestAggregate(
        long RequestCount,
        long CommandCount,
        double MeanCommandsPerRequest,
        int P50CommandsPerRequest,
        int P95CommandsPerRequest,
        int P99CommandsPerRequest,
        int MaximumCommandsPerRequest);
}
