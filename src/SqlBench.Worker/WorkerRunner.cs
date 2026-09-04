using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Resources;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using SqlBench.Batching;
using SqlBench.Core;
using SqlBench.ServiceDefaults;

namespace SqlBench.Worker;

public static class WorkerRunner
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
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

        string? configPath = ReadOption(args, "--config");
        if (string.IsNullOrWhiteSpace(configPath))
        {
            Console.Error.WriteLine("Usage: SqlBench.Worker --config <worker-config.json> or --idle");
            return 2;
        }

        await using FileStream stream = File.OpenRead(configPath);
        WorkerConfiguration config = await JsonSerializer.DeserializeAsync<WorkerConfiguration>(stream, JsonOptions)
            .ConfigureAwait(false)
            ?? throw new InvalidOperationException("Worker configuration was empty.");

        IHost? telemetryHost = null;
        if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT")))
        {
            HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
            builder.AddSqlBenchServiceDefaults();
            builder.Services.AddOpenTelemetry().ConfigureResource(resource => resource.AddService(
                serviceName: "sqlbench-worker",
                serviceInstanceId: $"worker-{config.WorkerId}"));
            telemetryHost = builder.Build();
            await telemetryHost.StartAsync().ConfigureAwait(false);
        }

        try
        {
            return await ExecuteAsync(config).ConfigureAwait(false);
        }
        finally
        {
            if (telemetryHost is not null)
            {
                await telemetryHost.StopAsync().ConfigureAwait(false);
                telemetryHost.Dispose();
            }
        }
    }

    private static async Task<int> ExecuteAsync(WorkerConfiguration config)
    {
        config.Scenario.Validate();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(logging => logging
            .SetMinimumLevel(LogLevel.Warning)
            .AddSimpleConsole(options => options.SingleLine = true));
        var sqlExecution = new FixedConcurrentBuffer<double>(config.Scenario.RowCount);
        var transaction = new FixedConcurrentBuffer<double>(config.Scenario.RowCount);
        var errors = new ConcurrentBag<string>();
        var deliveryToCommit = new FixedConcurrentBuffer<long>(config.Scenario.RowCount);
        var deliveryToAck = new FixedConcurrentBuffer<long>(config.Scenario.RowCount);
        var handler = new WorkerBatchHandler(
            config.Scenario,
            new DatabaseManager(config.Runtime.SqlConnectionString).GetTargetConnectionString(),
            sqlExecution,
            transaction);
        await using IProcessBatcher<PendingMessage> batcher = CreateBatcher(
            config.Scenario,
            handler,
            loggerFactory);
        await batcher.StartAsync(CancellationToken.None).ConfigureAwait(false);

        var counters = new WorkerCounters();
        using var failure = new CancellationTokenSource();

        var factory = new ConnectionFactory
        {
            Uri = new Uri(config.Runtime.RabbitMqConnectionString),
            AutomaticRecoveryEnabled = false,
            TopologyRecoveryEnabled = false,
            ConsumerDispatchConcurrency = (ushort)config.Scenario.WritersPerInstance,
            ClientProvidedName = $"sql-insert-benchmark-worker-{config.WorkerId}"
        };

        await using IConnection connection = await factory.CreateConnectionAsync().ConfigureAwait(false);
        var consumers = new List<ConsumerRegistration>();
        using var process = Process.GetCurrentProcess();
        TimeSpan cpuStart = process.TotalProcessorTime;
        long allocationsStart = GC.GetTotalAllocatedBytes(precise: false);
        int generation0Start = GC.CollectionCount(0);
        int generation1Start = GC.CollectionCount(1);
        int generation2Start = GC.CollectionCount(2);
        try
        {
            for (int writer = 0; writer < config.Scenario.WritersPerInstance; writer++)
            {
                IChannel channel = await connection.CreateChannelAsync().ConfigureAwait(false);
                await channel.BasicQosAsync(
                    prefetchSize: 0,
                    prefetchCount: config.Scenario.RabbitMqPrefetch,
                    global: false).ConfigureAwait(false);
                var acknowledgments = new AcknowledgmentPump(
                    channel,
                    config.Scenario.RabbitMqPrefetch == 0
                        ? config.Scenario.RowCount
                        : config.Scenario.RabbitMqPrefetch,
                    deliveryToCommit,
                    deliveryToAck,
                    errors,
                    failure,
                    counters);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (sender, delivery) =>
                {
                    _ = sender;
                    Interlocked.Increment(ref counters.InFlight);
                    WorkerTelemetry.InFlight.Add(1);
                    bool completionOwnsInFlight = false;
                    PendingMessage? pending = null;
                    try
                    {
                        BenchmarkMessage message = MessageSerializer.Deserialize(delivery.Body);
                        long deliveredAt = Stopwatch.GetTimestamp();
                        Interlocked.Increment(ref counters.Delivered);
                        WorkerTelemetry.Delivered.Add(1);
                        if (delivery.Redelivered)
                        {
                            Interlocked.Increment(ref counters.Redelivered);
                            WorkerTelemetry.Redelivered.Add(1);
                        }

                        pending = PendingMessage.Rent(
                            message,
                            delivery.DeliveryTag,
                            deliveredAt);
                        BatchSubmission submission = await batcher.SubmitAsync(pending, failure.Token)
                            .ConfigureAwait(false);
                        acknowledgments.Track(pending, submission.Completion);
                        completionOwnsInFlight = true;
                    }
                    catch (Exception error)
                    {
                        if (!completionOwnsInFlight)
                        {
                            pending?.Return();
                            Interlocked.Decrement(ref counters.InFlight);
                            WorkerTelemetry.InFlight.Add(-1);
                        }

                        errors.Add(error.ToString());
                        WorkerTelemetry.Errors.Add(1);
                        failure.Cancel();
                    }
                };
                consumers.Add(new ConsumerRegistration(channel, acknowledgments, consumer));
            }

            Directory.CreateDirectory(Path.GetDirectoryName(config.ReadyFile)!);
            await File.WriteAllTextAsync(config.ReadyFile, "ready").ConfigureAwait(false);
            while (!File.Exists(config.GateFile))
            {
                await Task.Delay(2).ConfigureAwait(false);
            }

            process.Refresh();
            cpuStart = process.TotalProcessorTime;
            allocationsStart = GC.GetTotalAllocatedBytes(precise: false);
            generation0Start = GC.CollectionCount(0);
            generation1Start = GC.CollectionCount(1);
            generation2Start = GC.CollectionCount(2);
            foreach (ConsumerRegistration registration in consumers)
            {
                registration.ConsumerTag = await registration.Channel.BasicConsumeAsync(
                    queue: config.Runtime.QueueName,
                    autoAck: false,
                    consumer: registration.Consumer).ConfigureAwait(false);
            }

            await WaitForDrainAsync(config.Runtime, counters, failure.Token)
                .ConfigureAwait(false);
            await File.WriteAllTextAsync(config.CompletionFile, "complete").ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (failure.IsCancellationRequested)
        {
            errors.Add("The worker stopped after a batch, consumer, or acknowledgment failure.");
            WorkerTelemetry.Errors.Add(1);
        }
        finally
        {
            foreach (ConsumerRegistration registration in consumers)
            {
                if (registration.ConsumerTag is null)
                {
                    continue;
                }

                try
                {
                    await registration.Channel.BasicCancelAsync(registration.ConsumerTag).ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    errors.Add($"Consumer cancellation failed: {error.Message}");
                    WorkerTelemetry.Errors.Add(1);
                }
            }

            await batcher.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await WaitForCommitContinuationsAsync(counters).ConfigureAwait(false);
            foreach (ConsumerRegistration registration in consumers)
            {
                await registration.Acknowledgments.CompleteAsync().ConfigureAwait(false);
            }

            await WaitForCompletionsAsync(counters).ConfigureAwait(false);

            foreach (ConsumerRegistration registration in consumers)
            {
                await registration.Channel.DisposeAsync().ConfigureAwait(false);
            }
        }

        process.Refresh();
        var result = new WorkerRunResult
        {
            WorkerId = config.WorkerId,
            DeliveredMessages = Interlocked.Read(ref counters.Delivered),
            CommittedRows = config.Scenario.Mode == WorkloadMode.NoOpQueue ? 0 : handler.CommittedRows,
            AcknowledgedMessages = Interlocked.Read(ref counters.Acknowledged),
            RedeliveredMessages = Interlocked.Read(ref counters.Redelivered),
            LastAcknowledgmentTimestamp = Interlocked.Read(ref counters.LastAcknowledgmentTimestamp),
            StopwatchFrequency = Stopwatch.Frequency,
            DeliveryToCommitMicroseconds = deliveryToCommit.ToArray(),
            DeliveryToAcknowledgmentMicroseconds = deliveryToAck.ToArray(),
            SqlExecutionMilliseconds = sqlExecution.ToArray(),
            TransactionMilliseconds = transaction.ToArray(),
            BatcherMetrics = batcher.GetMetrics(),
            Process = new ProcessMetrics
            {
                CpuSeconds = (process.TotalProcessorTime - cpuStart).TotalSeconds,
                PeakWorkingSetBytes = process.PeakWorkingSet64,
                AllocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocationsStart,
                Generation0Collections = GC.CollectionCount(0) - generation0Start,
                Generation1Collections = GC.CollectionCount(1) - generation1Start,
                Generation2Collections = GC.CollectionCount(2) - generation2Start
            },
            Errors = [.. errors]
        };

        Directory.CreateDirectory(Path.GetDirectoryName(config.ResultFile)!);
        await File.WriteAllTextAsync(
            config.ResultFile,
            JsonSerializer.Serialize(result, JsonOptions)).ConfigureAwait(false);
        return result.Errors.Count == 0 ? 0 : 1;
    }

    private static IProcessBatcher<PendingMessage> CreateBatcher(
        Scenario scenario,
        IBatchHandler<PendingMessage> handler,
        ILoggerFactory loggerFactory)
    {
        var options = new BatcherOptions
        {
            Capacity = scenario.ChannelCapacity,
            MaximumBatchSize = scenario.BatchSize,
            MaximumDelay = TimeSpan.FromMilliseconds(scenario.MaximumBatchingDelayMilliseconds),
            HandlerConcurrency = scenario.WritersPerInstance
        };
        return scenario.Batching switch
        {
            BatchingKind.None => new ImmediateBatcher<PendingMessage>(handler),
            BatchingKind.Channel => new ChannelBatcher<PendingMessage>(
                handler,
                options,
                loggerFactory.CreateLogger<ChannelBatcher<PendingMessage>>()),
            BatchingKind.LockSwap => new LockSwapBatcher<PendingMessage>(
                handler,
                options,
                loggerFactory.CreateLogger<LockSwapBatcher<PendingMessage>>()),
            _ => throw new ArgumentOutOfRangeException(nameof(scenario))
        };
    }

    private static async Task WaitForDrainAsync(
        RuntimeSettings settings,
        WorkerCounters counters,
        CancellationToken cancellationToken)
    {
        await using var queue = new RabbitQueueClient(settings);
        int emptySamples = 0;
        while (emptySamples < 3)
        {
            cancellationToken.ThrowIfCancellationRequested();
            QueueState state = await queue.ReadStateAsync(cancellationToken).ConfigureAwait(false);
            if (state.Ready == 0 && state.Unacknowledged == 0 && Interlocked.Read(ref counters.InFlight) == 0)
            {
                emptySamples++;
            }
            else
            {
                emptySamples = 0;
            }

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task WaitForCompletionsAsync(WorkerCounters counters)
    {
        while (Interlocked.Read(ref counters.InFlight) != 0)
        {
            await Task.Delay(1).ConfigureAwait(false);
        }
    }

    private static async Task WaitForCommitContinuationsAsync(WorkerCounters counters)
    {
        while (Interlocked.Read(ref counters.CommitContinuations) != 0)
        {
            await Task.Delay(1).ConfigureAwait(false);
        }
    }

    private static string? ReadOption(string[] args, string name)
    {
        int index = Array.FindIndex(args, value => string.Equals(value, name, StringComparison.OrdinalIgnoreCase));
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }

    private sealed class ConsumerRegistration(
        IChannel channel,
        AcknowledgmentPump acknowledgments,
        AsyncEventingBasicConsumer consumer)
    {
        public IChannel Channel { get; } = channel;

        public AcknowledgmentPump Acknowledgments { get; } = acknowledgments;

        public AsyncEventingBasicConsumer Consumer { get; } = consumer;

        public string? ConsumerTag { get; set; }
    }

}
