using RabbitMQ.Client;
using SqlBench.Core;

namespace SqlBench.Worker;

internal sealed class PendingMessage
{
    public required BenchmarkMessage Message { get; init; }
    public required IChannel Channel { get; init; }
    public required ulong DeliveryTag { get; init; }
    public required long DeliveredTimestamp { get; init; }
    public long CommittedTimestamp { get; set; }
}
