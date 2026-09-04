using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace SqlBench.Batching;

public sealed class ChannelBatcher<T> : IProcessBatcher<T>
{
    private static readonly Action<ILogger, int, Exception?> WorkerAborted = LoggerMessage.Define<int>(
        LogLevel.Debug,
        new EventId(1, nameof(WorkerAborted)),
        "Channel batcher worker {WorkerIndex} was aborted.");
    private readonly IBatchHandler<T> _handler;
    private readonly BatcherOptions _options;
    private readonly ILogger _logger;
    private readonly Channel<WorkItem> _channel;
    private readonly BatcherMetrics _metrics = new();
    private readonly CancellationTokenSource _abort = new();
    private Task[]? _workers;
    private int _started;
    private int _stopping;

    public ChannelBatcher(
        IBatchHandler<T> handler,
        BatcherOptions options,
        ILogger<ChannelBatcher<T>> logger)
    {
        options.Validate();
        _handler = handler;
        _options = options;
        _logger = logger;
        _channel = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(options.Capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = options.HandlerConcurrency == 1,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException("The batcher has already started.");
        }

        _workers = Enumerable.Range(0, _options.HandlerConcurrency)
            .Select(index => Task.Run(() => RunWorkerAsync(index, _abort.Token), CancellationToken.None))
            .ToArray();
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

        var work = new WorkItem(item, Stopwatch.GetTimestamp());
        if (!_channel.Writer.TryWrite(work))
        {
            _metrics.Backpressured();
            await _channel.Writer.WriteAsync(work, cancellationToken).ConfigureAwait(false);
        }

        _metrics.Accepted();
        return new BatchSubmission(work.Completion.Task);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
        {
            return;
        }

        _channel.Writer.TryComplete();
        if (_workers is not null)
        {
            await Task.WhenAll(_workers).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public BatcherMetricsSnapshot GetMetrics() => _metrics.Snapshot();

    public async ValueTask DisposeAsync()
    {
        await _abort.CancelAsync().ConfigureAwait(false);
        _abort.Dispose();
    }

    private async Task RunWorkerAsync(int workerIndex, CancellationToken cancellationToken)
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!_channel.Reader.TryRead(out WorkItem? first))
                {
                    continue;
                }

                var batch = new List<WorkItem>(_options.MaximumBatchSize) { first };
                _metrics.Dequeued(first.AcceptedTimestamp);
                long fillStart = Stopwatch.GetTimestamp();
                await FillBatchAsync(batch, fillStart, cancellationToken).ConfigureAwait(false);
                TimeSpan fillTime = Stopwatch.GetElapsedTime(fillStart);
                await ProcessBatchAsync(batch, fillTime, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            WorkerAborted(_logger, workerIndex, null);
        }
    }

    private async Task FillBatchAsync(
        List<WorkItem> batch,
        long fillStart,
        CancellationToken cancellationToken)
    {
        while (batch.Count < _options.MaximumBatchSize)
        {
            while (batch.Count < _options.MaximumBatchSize && _channel.Reader.TryRead(out WorkItem? item))
            {
                batch.Add(item);
                _metrics.Dequeued(item.AcceptedTimestamp);
            }

            TimeSpan remaining = _options.MaximumDelay - Stopwatch.GetElapsedTime(fillStart);
            if (batch.Count >= _options.MaximumBatchSize || remaining <= TimeSpan.Zero)
            {
                return;
            }

            Task<bool> available = _channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
            Task delay = Task.Delay(remaining, cancellationToken);
            Task completed = await Task.WhenAny(available, delay).ConfigureAwait(false);
            if (completed == delay || !await available.ConfigureAwait(false))
            {
                return;
            }
        }
    }

    private async Task ProcessBatchAsync(
        List<WorkItem> batch,
        TimeSpan fillTime,
        CancellationToken cancellationToken)
    {
        long handlerStart = Stopwatch.GetTimestamp();
        BatchHandlerResult result;
        try
        {
            result = await _handler.HandleAsync(
                batch.Select(static item => item.Value).ToArray(),
                cancellationToken).ConfigureAwait(false);

            if (result.ItemErrors.Count != batch.Count)
            {
                throw new InvalidOperationException("The batch handler returned a result count that did not match the batch.");
            }
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            result = BatchHandlerResult.Failure(batch.Count, error);
        }

        TimeSpan handlerTime = Stopwatch.GetElapsedTime(handlerStart);
        _metrics.Batch(batch.Count, fillTime, handlerTime);

        for (int index = 0; index < batch.Count; index++)
        {
            Exception? error = result.ItemErrors[index];
            _metrics.Completed(error is null);
            if (error is null)
            {
                batch[index].Completion.TrySetResult();
            }
            else
            {
                batch[index].Completion.TrySetException(error);
            }
        }
    }

    private sealed class WorkItem(T value, long acceptedTimestamp)
    {
        public T Value { get; } = value;

        public long AcceptedTimestamp { get; } = acceptedTimestamp;

        public TaskCompletionSource Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
