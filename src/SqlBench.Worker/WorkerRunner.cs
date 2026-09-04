using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
        return await ExecuteAsync(config).ConfigureAwait(false);
    }

    private static async Task<int> ExecuteAsync(WorkerConfiguration config)
    {
        config.Scenario.Validate();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(logging => logging
            .SetMinimumLevel(LogLevel.Warning)
            .AddSimpleConsole(options => options.SingleLine = true));
        var sqlExecution = new ConcurrentBag<double>();
        var transaction = new ConcurrentBag<double>();
        var errors = new ConcurrentBag<string>();
        var deliveryToCommit = new ConcurrentBag<long>();
        var deliveryToAck = new ConcurrentBag<long>();
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

        long delivered = 0;
        long acknowledged = 0;
        long redelivered = 0;
        long inFlight = 0;
        long trackedId = 0;
        var tracked = new ConcurrentDictionary<long, Task>();
        using var failure = new CancellationTokenSource();

        var factory = new ConnectionFactory
        {
            Uri = new Uri(config.Runtime.RabbitMqConnectionString),
            AutomaticRecoveryEnabled = false,
            TopologyRecoveryEnabled = false,
            ConsumerDispatchConcurrency = 1,
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
                var acknowledgmentLock = new SemaphoreSlim(1, 1);
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (sender, delivery) =>
                {
                    _ = sender;
                    Interlocked.Increment(ref inFlight);
                    bool completionOwnsInFlight = false;
                    try
                    {
                        BenchmarkMessage message = MessageSerializer.Deserialize(delivery.Body);
                        long deliveredAt = Stopwatch.GetTimestamp();
                        Interlocked.Increment(ref delivered);
                        if (delivery.Redelivered)
                        {
                            Interlocked.Increment(ref redelivered);
                        }

                        var pending = new PendingMessage
                        {
                            Message = message,
                            Channel = channel,
                            DeliveryTag = delivery.DeliveryTag,
                            DeliveredTimestamp = deliveredAt
                        };
                        BatchSubmission submission = await batcher.SubmitAsync(pending, failure.Token)
                            .ConfigureAwait(false);
                        long id = Interlocked.Increment(ref trackedId);
                        Task completion = CompleteAndAcknowledgeAsync(
                            pending,
                            submission,
                            acknowledgmentLock,
                            deliveryToCommit,
                            deliveryToAck,
                            errors,
                            failure,
                            () => Interlocked.Increment(ref acknowledged),
                            () => Interlocked.Decrement(ref inFlight));
                        completionOwnsInFlight = true;
                        tracked[id] = completion;
                        _ = completion.ContinueWith(
                            (completedTask, state) =>
                            {
                                var item = ((ConcurrentDictionary<long, Task> Tasks, long Id))state!;
                                item.Tasks.TryRemove(item.Id, out _);
                                _ = completedTask.Exception;
                            },
                            (tracked, id),
                            CancellationToken.None,
                            TaskContinuationOptions.ExecuteSynchronously,
                            TaskScheduler.Default);
                    }
                    catch (Exception error)
                    {
                        if (!completionOwnsInFlight)
                        {
                            Interlocked.Decrement(ref inFlight);
                        }

                        errors.Add(error.ToString());
                        failure.Cancel();
                    }
                };
                consumers.Add(new ConsumerRegistration(channel, acknowledgmentLock, consumer));
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

            await WaitForDrainAsync(config.Runtime, () => Interlocked.Read(ref inFlight), failure.Token)
                .ConfigureAwait(false);
            await File.WriteAllTextAsync(config.CompletionFile, "complete").ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (failure.IsCancellationRequested)
        {
            errors.Add("The worker stopped after a batch, consumer, or acknowledgment failure.");
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
                }
            }

            await batcher.StopAsync(CancellationToken.None).ConfigureAwait(false);
            if (!tracked.IsEmpty)
            {
                await Task.WhenAll(tracked.Values).ConfigureAwait(false);
            }

            foreach (ConsumerRegistration registration in consumers)
            {
                registration.AcknowledgmentLock.Dispose();
                await registration.Channel.DisposeAsync().ConfigureAwait(false);
            }
        }

        process.Refresh();
        var result = new WorkerRunResult
        {
            WorkerId = config.WorkerId,
            DeliveredMessages = Interlocked.Read(ref delivered),
            CommittedRows = config.Scenario.Mode == WorkloadMode.NoOpQueue ? 0 : handler.CommittedRows,
            AcknowledgedMessages = Interlocked.Read(ref acknowledged),
            RedeliveredMessages = Interlocked.Read(ref redelivered),
            DeliveryToCommitMicroseconds = [.. deliveryToCommit],
            DeliveryToAcknowledgmentMicroseconds = [.. deliveryToAck],
            SqlExecutionMilliseconds = [.. sqlExecution],
            TransactionMilliseconds = [.. transaction],
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

    private static async Task CompleteAndAcknowledgeAsync(
        PendingMessage pending,
        BatchSubmission submission,
        SemaphoreSlim acknowledgmentLock,
        ConcurrentBag<long> deliveryToCommit,
        ConcurrentBag<long> deliveryToAck,
        ConcurrentBag<string> errors,
        CancellationTokenSource failure,
        Action acknowledged,
        Action completed)
    {
        try
        {
            await submission.Completion.ConfigureAwait(false);
            deliveryToCommit.Add(ToMicroseconds(pending.DeliveredTimestamp, pending.CommittedTimestamp));
            await acknowledgmentLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                await pending.Channel.BasicAckAsync(pending.DeliveryTag, multiple: false).ConfigureAwait(false);
            }
            finally
            {
                acknowledgmentLock.Release();
            }

            deliveryToAck.Add(ToMicroseconds(pending.DeliveredTimestamp, Stopwatch.GetTimestamp()));
            acknowledged();
        }
        catch (Exception error)
        {
            errors.Add(error.ToString());
            failure.Cancel();
        }
        finally
        {
            completed();
        }
    }

    private static async Task WaitForDrainAsync(
        RuntimeSettings settings,
        Func<long> inFlight,
        CancellationToken cancellationToken)
    {
        await using var queue = new RabbitQueueClient(settings);
        int emptySamples = 0;
        while (emptySamples < 3)
        {
            cancellationToken.ThrowIfCancellationRequested();
            QueueState state = await queue.ReadStateAsync(cancellationToken).ConfigureAwait(false);
            if (state.Ready == 0 && state.Unacknowledged == 0 && inFlight() == 0)
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

    private static long ToMicroseconds(long start, long end) =>
        (long)Math.Round(Stopwatch.GetElapsedTime(start, end).TotalMilliseconds * 1_000.0);

    private static string? ReadOption(string[] args, string name)
    {
        int index = Array.FindIndex(args, value => string.Equals(value, name, StringComparison.OrdinalIgnoreCase));
        return index >= 0 && index + 1 < args.Length ? args[index + 1] : null;
    }

    private sealed class ConsumerRegistration(
        IChannel channel,
        SemaphoreSlim acknowledgmentLock,
        AsyncEventingBasicConsumer consumer)
    {
        public IChannel Channel { get; } = channel;

        public SemaphoreSlim AcknowledgmentLock { get; } = acknowledgmentLock;

        public AsyncEventingBasicConsumer Consumer { get; } = consumer;

        public string? ConsumerTag { get; set; }
    }
}
