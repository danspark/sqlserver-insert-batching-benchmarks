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

public sealed record BatchHandlerResult
{
    public required IReadOnlyList<Exception?> ItemErrors { get; init; }

    public static BatchHandlerResult Success(int count) => new()
    {
        ItemErrors = new Exception?[count]
    };

    public static BatchHandlerResult Failure(int count, Exception error) => new()
    {
        ItemErrors = Enumerable.Repeat<Exception?>(error, count).ToArray()
    };
}

public sealed class BatchSubmission
{
    public BatchSubmission(Task completion) => Completion = completion;

    public Task Completion { get; }
}

public interface IProcessBatcher<T> : IHostedService, IAsyncDisposable
{
    ValueTask<BatchSubmission> SubmitAsync(T item, CancellationToken cancellationToken = default);

    BatcherMetricsSnapshot GetMetrics();
}
