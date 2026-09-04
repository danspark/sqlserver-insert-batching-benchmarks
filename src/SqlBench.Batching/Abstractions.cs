using Microsoft.Extensions.Hosting;

namespace SqlBench.Batching;

public sealed record BatcherOptions
{
    public int Capacity { get; init; } = 1_000;

    public int MaximumBatchSize { get; init; } = 100;

    public TimeSpan MaximumDelay { get; init; } = TimeSpan.FromMilliseconds(5);

    public int HandlerConcurrency { get; init; } = 1;

    public void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(Capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaximumBatchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(HandlerConcurrency, 1);

        if (MaximumDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MaximumDelay));
        }
    }
}

public interface IBatchHandler<T>
{
    ValueTask<BatchHandlerResult> HandleAsync(
        IReadOnlyList<T> items,
        CancellationToken cancellationToken);
}

public readonly record struct BatchHandlerResult
{
    private readonly IReadOnlyList<Exception?>? _itemErrors;

    private BatchHandlerResult(int count, Exception? commonError, IReadOnlyList<Exception?>? itemErrors)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (itemErrors is not null && itemErrors.Count != count)
        {
            throw new ArgumentException("The item error count must match the batch count.", nameof(itemErrors));
        }

        Count = count;
        CommonError = commonError;
        _itemErrors = itemErrors;
    }

    public int Count { get; }

    public Exception? CommonError { get; }

    public static BatchHandlerResult Success(int count) => new(count, null, null);

    public static BatchHandlerResult Failure(int count, Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new BatchHandlerResult(count, error, null);
    }

    public static BatchHandlerResult FromItemErrors(IReadOnlyList<Exception?> itemErrors)
    {
        ArgumentNullException.ThrowIfNull(itemErrors);
        return new BatchHandlerResult(itemErrors.Count, null, itemErrors);
    }

    public Exception? GetError(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, Count);
        return _itemErrors is null ? CommonError : _itemErrors[index];
    }
}

public readonly record struct BatchSubmission
{
    public BatchSubmission(ValueTask completion) => Completion = completion;

    public ValueTask Completion { get; }
}

public interface IProcessBatcher<T> : IHostedService, IAsyncDisposable
{
    ValueTask<BatchSubmission> SubmitAsync(T item, CancellationToken cancellationToken = default);

    BatcherMetricsSnapshot GetMetrics();
}
