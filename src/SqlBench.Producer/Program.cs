using SqlBench.Core;

static string? ReadOption(string[] values, string name)
{
    int index = Array.FindIndex(values, value => string.Equals(value, name, StringComparison.OrdinalIgnoreCase));
    return index >= 0 && index + 1 < values.Length ? values[index + 1] : null;
}

int count = int.Parse(ReadOption(args, "--count") ?? "10000", System.Globalization.CultureInfo.InvariantCulture);
int seed = int.Parse(ReadOption(args, "--seed") ?? "1592606758", System.Globalization.CultureInfo.InvariantCulture);
ParentDistribution distribution = Enum.Parse<ParentDistribution>(
    ReadOption(args, "--distribution") ?? nameof(ParentDistribution.Uniform),
    ignoreCase: true);
RuntimeSettings runtime = RuntimeSettingsLoader.FromEnvironment();
IReadOnlyList<BenchmarkMessage> workload = WorkloadGenerator.Generate(count, seed, distribution);
await using var queue = new RabbitQueueClient(runtime);
await queue.DeclareAsync(CancellationToken.None).ConfigureAwait(false);
if (args.Contains("--purge", StringComparer.OrdinalIgnoreCase))
{
    await queue.PurgeAsync(CancellationToken.None).ConfigureAwait(false);
}

await queue.PublishAsync(workload, CancellationToken.None).ConfigureAwait(false);
QueueState state = await queue.ReadStateAsync(CancellationToken.None).ConfigureAwait(false);
Console.WriteLine(
    $"Published {count:N0} persistent messages with confirms. " +
    $"Queue now has {state.Ready:N0} ready and {state.Unacknowledged:N0} unacknowledged messages.");
