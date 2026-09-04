using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace SqlBench.Core;

public sealed class RabbitQueueClient(RuntimeSettings settings) : IAsyncDisposable
{
    private readonly RuntimeSettings _settings = settings;
    private IConnection? _connection;

    public async Task DeclareAsync(CancellationToken cancellationToken)
    {
        IConnection connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await channel.QueueDeclareAsync(
            queue: _settings.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task PurgeAsync(CancellationToken cancellationToken)
    {
        IConnection connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await channel.QueueDeclareAsync(
            _settings.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await channel.QueuePurgeAsync(_settings.QueueName, cancellationToken).ConfigureAwait(false);
    }

    public async Task PublishAsync(
        IReadOnlyList<BenchmarkMessage> messages,
        CancellationToken cancellationToken)
    {
        IConnection connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);
        await using IChannel channel = await connection.CreateChannelAsync(options, cancellationToken)
            .ConfigureAwait(false);
        await channel.QueueDeclareAsync(
            _settings.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var pendingConfirms = new List<Task>(512);
        foreach (BenchmarkMessage message in messages)
        {
            var properties = new BasicProperties
            {
                Persistent = true,
                ContentType = "application/json",
                ContentEncoding = "utf-8",
                MessageId = message.MessageId.ToString("D"),
                Type = nameof(BenchmarkMessage)
            };
            pendingConfirms.Add(channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: _settings.QueueName,
                mandatory: true,
                basicProperties: properties,
                body: MessageSerializer.Serialize(message),
                cancellationToken: cancellationToken).AsTask());
            if (pendingConfirms.Count == 512)
            {
                await Task.WhenAll(pendingConfirms).ConfigureAwait(false);
                pendingConfirms.Clear();
            }
        }

        if (pendingConfirms.Count > 0)
        {
            await Task.WhenAll(pendingConfirms).ConfigureAwait(false);
        }
    }

    public async Task<QueueState> ReadStateAsync(CancellationToken cancellationToken)
    {
        IConnection connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using IChannel channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        QueueDeclareOk declared = await channel.QueueDeclarePassiveAsync(_settings.QueueName, cancellationToken)
            .ConfigureAwait(false);

        using HttpClient client = CreateManagementClient();
        string queue = Uri.EscapeDataString(_settings.QueueName);
        using HttpResponseMessage response = await client.GetAsync(
            $"api/queues/%2F/{queue}",
            cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        await using Stream body = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using JsonDocument document = await JsonDocument.ParseAsync(body, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        JsonElement root = document.RootElement;
        long unacknowledged = declared.ConsumerCount == 0
            ? 0
            : GetInt64(root, "messages_unacknowledged");
        return new QueueState
        {
            // The AMQP queue.declare-ok count is immediate. Management statistics are
            // sampled and can otherwise report the state from before a confirmed preload.
            Ready = declared.MessageCount,
            // In this benchmark, unacknowledged deliveries only exist on consumer
            // channels. Zero consumers is therefore an authoritative zero even when
            // the management plugin has not refreshed its sampled counter yet.
            Unacknowledged = unacknowledged,
            Consumers = declared.ConsumerCount,
            Redeliveries = ReadRateTotal(root, "redeliver_details"),
            Deliveries = ReadRateTotal(root, "deliver_get_details"),
            Acknowledgments = ReadRateTotal(root, "ack_details")
        };
    }

    public async Task<RabbitMqMetrics> ReadBrokerMetricsAsync(CancellationToken cancellationToken)
    {
        using HttpClient client = CreateManagementClient();
        using HttpResponseMessage response = await client.GetAsync("api/nodes", cancellationToken)
            .ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        await using Stream body = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using JsonDocument document = await JsonDocument.ParseAsync(body, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        JsonElement node = document.RootElement.EnumerateArray().First();
        return new RabbitMqMetrics
        {
            MemoryBytes = GetInt64(node, "mem_used"),
            DiskFreeBytes = GetInt64(node, "disk_free"),
            FileDescriptorsUsed = GetInt64(node, "fd_used"),
            SocketsUsed = GetInt64(node, "sockets_used"),
            ErlangProcessesUsed = GetInt64(node, "proc_used"),
            GarbageCollectionCount = TryGetNestedInt64(node, "gc_num")
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection is not null)
        {
            return _connection;
        }

        var factory = new ConnectionFactory
        {
            Uri = new Uri(_settings.RabbitMqConnectionString),
            AutomaticRecoveryEnabled = false,
            TopologyRecoveryEnabled = false,
            ConsumerDispatchConcurrency = 1,
            ClientProvidedName = "sql-insert-benchmark-controller"
        };
        _connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        return _connection;
    }

    private HttpClient CreateManagementClient()
    {
        var client = new HttpClient
        {
            BaseAddress = new Uri(_settings.RabbitMqManagementUri.EndsWith('/')
                ? _settings.RabbitMqManagementUri
                : _settings.RabbitMqManagementUri + "/"),
            Timeout = TimeSpan.FromSeconds(30)
        };
        string credentials = Convert.ToBase64String(
            Encoding.UTF8.GetBytes($"{_settings.RabbitMqUser}:{_settings.RabbitMqPassword}"));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        return client;
    }

    private static long GetInt64(JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out JsonElement value) && value.TryGetInt64(out long number)
            ? number
            : 0;

    private static long TryGetNestedInt64(JsonElement element, string propertyName)
    {
        if (element.TryGetProperty("gc", out JsonElement gc))
        {
            return GetInt64(gc, propertyName);
        }

        return GetInt64(element, propertyName);
    }

    private static double ReadRateTotal(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out JsonElement details))
        {
            return 0;
        }

        return details.TryGetProperty("rate", out JsonElement rate) && rate.TryGetDouble(out double value)
            ? value
            : 0;
    }
}

public sealed record QueueState
{
    public long Ready { get; init; }

    public long Unacknowledged { get; init; }

    public long Consumers { get; init; }

    public double Redeliveries { get; init; }

    public double Deliveries { get; init; }

    public double Acknowledgments { get; init; }
}

public sealed record RabbitMqMetrics
{
    public long MemoryBytes { get; init; }

    public long DiskFreeBytes { get; init; }

    public long FileDescriptorsUsed { get; init; }

    public long SocketsUsed { get; init; }

    public long ErlangProcessesUsed { get; init; }

    public long GarbageCollectionCount { get; init; }
}
