using System.Collections.Concurrent;
using SqlBench.Core;

namespace SqlBench.Worker;

internal sealed class PendingMessage
{
    private static readonly ConcurrentBag<PendingMessage> Pool = [];

    private PendingMessage()
    {
    }

    public BenchmarkMessage Message { get; private set; } = null!;

    public ulong DeliveryTag { get; private set; }

    public long DeliveredTimestamp { get; private set; }

    public long CommittedTimestamp { get; set; }

    public static PendingMessage Rent(
        BenchmarkMessage message,
        ulong deliveryTag,
        long deliveredTimestamp)
    {
        if (!Pool.TryTake(out PendingMessage? pending))
        {
            pending = new PendingMessage();
        }

        pending.Message = message;
        pending.DeliveryTag = deliveryTag;
        pending.DeliveredTimestamp = deliveredTimestamp;
        pending.CommittedTimestamp = 0;
        return pending;
    }

    public void Return()
    {
        Message = null!;
        DeliveryTag = 0;
        DeliveredTimestamp = 0;
        CommittedTimestamp = 0;
        Pool.Add(this);
    }
}
