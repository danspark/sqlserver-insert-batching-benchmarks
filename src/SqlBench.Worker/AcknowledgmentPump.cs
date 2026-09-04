using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using RabbitMQ.Client;

namespace SqlBench.Worker;

internal sealed class AcknowledgmentPump
{
    private readonly IChannel _rabbitChannel;
    private readonly FixedConcurrentBuffer<long> _deliveryToCommit;
    private readonly FixedConcurrentBuffer<long> _deliveryToAcknowledgment;
    private readonly ConcurrentBag<string> _errors;
    private readonly CancellationTokenSource _failure;
    private readonly WorkerCounters _counters;
    private readonly Channel<PendingMessage> _committed;
    private readonly Task _consumer;

    public AcknowledgmentPump(
        IChannel rabbitChannel,
        int capacity,
        FixedConcurrentBuffer<long> deliveryToCommit,
        FixedConcurrentBuffer<long> deliveryToAcknowledgment,
        ConcurrentBag<string> errors,
        CancellationTokenSource failure,
        WorkerCounters counters)
    {
        _rabbitChannel = rabbitChannel;
        _deliveryToCommit = deliveryToCommit;
        _deliveryToAcknowledgment = deliveryToAcknowledgment;
        _errors = errors;
        _failure = failure;
        _counters = counters;
        _committed = Channel.CreateBounded<PendingMessage>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        _consumer = ConsumeAsync();
    }

    public void Track(PendingMessage pending, ValueTask completion)
    {
        Interlocked.Increment(ref _counters.CommitContinuations);
        CommitContinuation.Rent(this, pending).Start(completion);
    }

    public async Task CompleteAsync()
    {
        _committed.Writer.TryComplete();
        await _consumer.ConfigureAwait(false);
    }

    private void CommitSucceeded(PendingMessage pending)
    {
        TimeSpan latency = Stopwatch.GetElapsedTime(
            pending.DeliveredTimestamp,
            pending.CommittedTimestamp);
        _deliveryToCommit.Add(ToMicroseconds(latency));
        WorkerTelemetry.DeliveryToCommit.Record(latency.TotalSeconds);
        if (!_committed.Writer.TryWrite(pending))
        {
            throw new InvalidOperationException("The acknowledgment queue could not accept a committed message.");
        }
    }

    private void CommitFailed(PendingMessage pending, Exception error)
    {
        RecordFailure(error);
        CompletePending(pending);
    }

    private async Task ConsumeAsync()
    {
        await foreach (PendingMessage pending in _committed.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                await _rabbitChannel.BasicAckAsync(pending.DeliveryTag, multiple: false).ConfigureAwait(false);
                long acknowledgedAt = Stopwatch.GetTimestamp();
                TimeSpan latency = Stopwatch.GetElapsedTime(pending.DeliveredTimestamp, acknowledgedAt);
                _deliveryToAcknowledgment.Add(ToMicroseconds(latency));
                WorkerTelemetry.DeliveryToAcknowledgment.Record(latency.TotalSeconds);
                WorkerTelemetry.Acknowledged.Add(1);
                Interlocked.Increment(ref _counters.Acknowledged);
                WorkerCounters.UpdateMaximum(ref _counters.LastAcknowledgmentTimestamp, acknowledgedAt);
            }
            catch (Exception error)
            {
                RecordFailure(error);
            }
            finally
            {
                CompletePending(pending);
            }
        }
    }

    private void RecordFailure(Exception error)
    {
        _errors.Add(error.ToString());
        WorkerTelemetry.Errors.Add(1);
        _failure.Cancel();
    }

    private void CompletePending(PendingMessage pending)
    {
        pending.Return();
        Interlocked.Decrement(ref _counters.InFlight);
        WorkerTelemetry.InFlight.Add(-1);
    }

    private static long ToMicroseconds(TimeSpan duration) => duration.Ticks / 10;

    private sealed class CommitContinuation
    {
        private static readonly ConcurrentBag<CommitContinuation> Pool = [];
        private readonly Action _continuation;
        private ValueTaskAwaiter _awaiter;
        private AcknowledgmentPump? _owner;
        private PendingMessage? _pending;

        private CommitContinuation() => _continuation = Continue;

        public static CommitContinuation Rent(AcknowledgmentPump owner, PendingMessage pending)
        {
            if (!Pool.TryTake(out CommitContinuation? continuation))
            {
                continuation = new CommitContinuation();
            }

            continuation._owner = owner;
            continuation._pending = pending;
            return continuation;
        }

        public void Start(ValueTask completion)
        {
            _awaiter = completion.GetAwaiter();
            if (_awaiter.IsCompleted)
            {
                Continue();
            }
            else
            {
                _awaiter.UnsafeOnCompleted(_continuation);
            }
        }

        private void Continue()
        {
            AcknowledgmentPump owner = _owner!;
            PendingMessage pending = _pending!;
            try
            {
                _awaiter.GetResult();
                owner.CommitSucceeded(pending);
            }
            catch (Exception error)
            {
                owner.CommitFailed(pending, error);
            }
            finally
            {
                _awaiter = default;
                _owner = null;
                _pending = null;
                Interlocked.Decrement(ref owner._counters.CommitContinuations);
                Pool.Add(this);
            }
        }
    }
}
