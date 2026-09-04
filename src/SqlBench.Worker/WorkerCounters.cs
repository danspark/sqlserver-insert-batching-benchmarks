namespace SqlBench.Worker;

internal sealed class WorkerCounters
{
    public long Delivered;

    public long Acknowledged;

    public long Redelivered;

    public long LastAcknowledgmentTimestamp;

    public long InFlight;

    public long CommitContinuations;

    public static void UpdateMaximum(ref long target, long value)
    {
        long observed = Volatile.Read(ref target);
        while (value > observed)
        {
            long previous = Interlocked.CompareExchange(ref target, value, observed);
            if (previous == observed)
            {
                return;
            }

            observed = previous;
        }
    }
}
