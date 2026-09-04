using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace SqlBench.Batching;

public sealed class LockSwapBatcher<T> : IProcessBatcher<T>
{
    private static readonly Action<ILogger, Exception?> TimerStopped = LoggerMessage.Define(
        LogLevel.Debug,
        new EventId(1, nameof(TimerStopped)),
        "Lock-swap batcher timer stopped.");
    private readonly IBatchHandler<T> _handler;
    private readonly BatcherOptions _options;
    private readonly ILogger _logger;
    private readonly object _sync = new();
    private readonly BatcherMetrics _metrics = new();
    private readonly SemaphoreSlim _capacity;
    private readonly SemaphoreSlim _handlers;
    private readonly CancellationTokenSource _timerCancellation = new();
    private readonly ConcurrentDictionary<long, Task> _inFlight = new();
    private PooledBatch<T> _buffer;
    private Task? _timer;
    private long _taskId;
    private int _started;
    private int _stopping;

    public LockSwapBatcher(
        IBatchHandler<T> handler,
        BatcherOptions options,
        ILogger<LockSwapBatcher<T>> logger)
    {
        options.Validate();
        _handler = handler;
        _options = options;
        _logger = logger;
        _capacity = new SemaphoreSlim(options.Capacity, options.Capacity);
        _handlers = new SemaphoreSlim(options.HandlerConcurrency, options.HandlerConcurrency);
        _buffer = PooledBatch<T>.Rent(options.MaximumBatchSize);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException("The batcher has already started.");
        }

        _timer = RunTimerAsync(_timerCancellation.Token);
        return Task.CompletedTask;
    }

    public async ValueTask<BatchSubmission> SubmitAsync(
        T item,
        CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _started) == 0 || Volatile.Read(ref _stopping) != 0)
        {
            throw new InvalidOperationException("The batcher is not accepting work.");
        }

        if (!await _capacity.WaitAsync(0, cancellationToken).ConfigureAwait(false))
        {
            _metrics.Backpressured();
            await _capacity.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        PooledWorkItem<T> work = PooledWorkItem<T>.Rent(item, Stopwatch.GetTimestamp());
        _metrics.Accepted();
        PooledBatch<T>? ready = null;
        lock (_sync)
        {
            _buffer.Add(work);
            if (_buffer.Count >= _options.MaximumBatchSize)
            {
                ready = SwapBuffer();
            }
        }

        if (ready is not null)
        {
            Track(ProcessBatchAsync(ready, CancellationToken.None));
        }

        return new BatchSubmission(work.Completion);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
        {
            return;
        }

        await _timerCancellation.CancelAsync().ConfigureAwait(false);
        if (_timer is not null)
        {
            await _timer.ConfigureAwait(false);
        }

        Flush();
        while (!_inFlight.IsEmpty)
        {
            await Task.WhenAll(_inFlight.Values).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public BatcherMetricsSnapshot GetMetrics() => _metrics.Snapshot();

    public async ValueTask DisposeAsync()
    {
        await _timerCancellation.CancelAsync().ConfigureAwait(false);
        _timerCancellation.Dispose();
        _capacity.Dispose();
        _handlers.Dispose();
    }

    private async Task RunTimerAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var timer = new PeriodicTimer(_options.MaximumDelay);
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                Flush();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            TimerStopped(_logger, null);
        }
    }

    private void Flush()
    {
        PooledBatch<T>? ready = null;
        lock (_sync)
        {
            if (_buffer.Count > 0)
            {
                ready = SwapBuffer();
            }
        }

        if (ready is not null)
        {
            Track(ProcessBatchAsync(ready, CancellationToken.None));
        }
    }

    private PooledBatch<T> SwapBuffer()
    {
        PooledBatch<T> ready = _buffer;
        _buffer = PooledBatch<T>.Rent(_options.MaximumBatchSize);
        return ready;
    }

    private void Track(Task task)
    {
        long id = Interlocked.Increment(ref _taskId);
        _inFlight[id] = task;
        _ = task.ContinueWith(
            (completedTask, state) =>
            {
                var tuple = ((ConcurrentDictionary<long, Task> Tasks, long Id))state!;
                tuple.Tasks.TryRemove(tuple.Id, out _);
                _ = completedTask.Exception;
            },
            (_inFlight, id),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private async Task ProcessBatchAsync(PooledBatch<T> batch, CancellationToken cancellationToken)
    {
        await _handlers.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            long handlerStart = Stopwatch.GetTimestamp();
            long oldest = batch.GetWorkItem(0).AcceptedTimestamp;
            for (int index = 0; index < batch.Count; index++)
            {
                PooledWorkItem<T> item = batch.GetWorkItem(index);
                oldest = Math.Min(oldest, item.AcceptedTimestamp);
                _metrics.Dequeued(item.AcceptedTimestamp);
            }

            BatchHandlerResult result;
            try
            {
                result = await _handler.HandleAsync(batch, cancellationToken).ConfigureAwait(false);
                if (result.Count != batch.Count)
                {
                    throw new InvalidOperationException("The batch handler returned a result count that did not match the batch.");
                }
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                result = BatchHandlerResult.Failure(batch.Count, error);
            }

            _metrics.Batch(
                batch.Count,
                Stopwatch.GetElapsedTime(oldest, handlerStart),
                Stopwatch.GetElapsedTime(handlerStart));

            for (int index = 0; index < batch.Count; index++)
            {
                Exception? error = result.GetError(index);
                _metrics.Completed(error is null);
                _capacity.Release();
                batch.GetWorkItem(index).Complete(error);
            }

            batch.Return();
        }
        finally
        {
            _handlers.Release();
        }
    }

}
