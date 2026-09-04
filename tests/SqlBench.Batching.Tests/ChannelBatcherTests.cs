using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using SqlBench.Batching;

namespace SqlBench.Batching.Tests;

public sealed class ChannelBatcherTests
{
    [Fact]
    public async Task FlushesAtMaximumBatchSize()
    {
        var sizes = new ConcurrentQueue<int>();
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>((items, _) =>
            {
                sizes.Enqueue(items.Count);
                return ValueTask.FromResult(BatchHandlerResult.Success(items.Count));
            }),
            batchSize: 3,
            delay: TimeSpan.FromMilliseconds(30));

        BatchSubmission[] submissions = await SubmitAsync(batcher, 1, 2, 3, 4, 5);
        await Task.WhenAll(submissions.Select(static item => item.Completion.AsTask()));
        await batcher.StopAsync(CancellationToken.None);

        Assert.Contains(3, sizes);
        Assert.Equal(5, sizes.Sum());
        Assert.All(sizes, size => Assert.InRange(size, 1, 3));
    }

    [Fact]
    public async Task FlushesWhenMaximumDelayExpires()
    {
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>((items, _) =>
            {
                handled.TrySetResult();
                return ValueTask.FromResult(BatchHandlerResult.Success(items.Count));
            }),
            batchSize: 100,
            delay: TimeSpan.FromMilliseconds(40));

        BatchSubmission submission = await batcher.SubmitAsync(1);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await submission.Completion;
        await batcher.StopAsync(CancellationToken.None);

        Assert.Equal(1, batcher.GetMetrics().CompletedItems);
        Assert.InRange(batcher.GetMetrics().BatchFillMilliseconds.Minimum, 20, 500);
    }

    [Fact]
    public async Task RecordsAndAppliesBackpressure()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>(async (items, _) =>
            {
                await release.Task;
                return BatchHandlerResult.Success(items.Count);
            }),
            batchSize: 1,
            delay: TimeSpan.FromSeconds(1),
            capacity: 1);

        BatchSubmission first = await batcher.SubmitAsync(1);
        await Task.Delay(20);
        BatchSubmission second = await batcher.SubmitAsync(2);
        Task<BatchSubmission> third = batcher.SubmitAsync(3).AsTask();
        await Task.Delay(20);
        Assert.False(third.IsCompleted);

        release.TrySetResult();
        BatchSubmission thirdSubmission = await third;
        await Task.WhenAll(
            first.Completion.AsTask(),
            second.Completion.AsTask(),
            thirdSubmission.Completion.AsTask());
        await batcher.StopAsync(CancellationToken.None);
        Assert.True(batcher.GetMetrics().BackpressureEvents >= 1);
    }

    [Fact]
    public async Task CancelsAWriterWaitingForCapacity()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>(async (items, _) =>
            {
                await release.Task;
                return BatchHandlerResult.Success(items.Count);
            }),
            batchSize: 1,
            delay: TimeSpan.FromSeconds(1),
            capacity: 1);

        BatchSubmission first = await batcher.SubmitAsync(1);
        await Task.Delay(20);
        BatchSubmission second = await batcher.SubmitAsync(2);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => batcher.SubmitAsync(3, cancellation.Token).AsTask());
        release.TrySetResult();
        await Task.WhenAll(first.Completion.AsTask(), second.Completion.AsTask());

        BatchSubmission afterCancellation = await batcher.SubmitAsync(4);
        await afterCancellation.Completion;
        await batcher.StopAsync(CancellationToken.None);

        Assert.Equal(3, batcher.GetMetrics().CompletedItems);
    }

    [Fact]
    public async Task DrainsAcceptedPartialBatchDuringShutdown()
    {
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>((items, _) =>
                ValueTask.FromResult(BatchHandlerResult.Success(items.Count))),
            batchSize: 100,
            delay: TimeSpan.FromMinutes(1));
        BatchSubmission[] submissions = await SubmitAsync(batcher, 1, 2, 3);

        await batcher.StopAsync(CancellationToken.None);
        await Task.WhenAll(submissions.Select(static item => item.Completion.AsTask()));

        Assert.Equal(3, batcher.GetMetrics().CompletedItems);
        Assert.Equal(3, batcher.GetMetrics().ActualBatchSize.Maximum);
    }

    [Fact]
    public async Task ReportsHandlerFailureToEveryItem()
    {
        var expected = new InvalidOperationException("batch failed");
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>((_, _) => throw expected),
            batchSize: 2,
            delay: TimeSpan.FromMilliseconds(10));
        BatchSubmission[] submissions = await SubmitAsync(batcher, 1, 2);

        foreach (BatchSubmission submission in submissions)
        {
            InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
                () => submission.Completion.AsTask());
            Assert.Same(expected, actual);
        }

        await batcher.StopAsync(CancellationToken.None);
        Assert.Equal(2, batcher.GetMetrics().FailedItems);
    }

    [Fact]
    public async Task PreservesPerItemCompletionAndFailure()
    {
        var expected = new InvalidOperationException("second item failed");
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>((items, _) => ValueTask.FromResult(
                BatchHandlerResult.FromItemErrors([null, expected]))),
            batchSize: 2,
            delay: TimeSpan.FromMilliseconds(10));
        BatchSubmission[] submissions = await SubmitAsync(batcher, 1, 2);

        await submissions[0].Completion;
        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => submissions[1].Completion.AsTask());
        await batcher.StopAsync(CancellationToken.None);

        Assert.Same(expected, actual);
        Assert.Equal(1, batcher.GetMetrics().CompletedItems);
        Assert.Equal(1, batcher.GetMetrics().FailedItems);
    }

    [Fact]
    public async Task RunsHandlersConcurrently()
    {
        int current = 0;
        int maximum = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>(async (items, _) =>
            {
                int now = Interlocked.Increment(ref current);
                InterlockedExtensions.Max(ref maximum, now);
                await release.Task;
                Interlocked.Decrement(ref current);
                return BatchHandlerResult.Success(items.Count);
            }),
            batchSize: 1,
            delay: TimeSpan.FromSeconds(1),
            concurrency: 2);
        BatchSubmission[] submissions = await SubmitAsync(batcher, 1, 2, 3, 4);

        await WaitUntilAsync(() => Volatile.Read(ref maximum) == 2, TimeSpan.FromSeconds(2));
        release.TrySetResult();
        await Task.WhenAll(submissions.Select(static item => item.Completion.AsTask()));
        await batcher.StopAsync(CancellationToken.None);

        Assert.Equal(2, maximum);
    }

    [Fact]
    public async Task EmptyBatcherStopsWithoutInvokingHandler()
    {
        int calls = 0;
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>((items, _) =>
            {
                Interlocked.Increment(ref calls);
                return ValueTask.FromResult(BatchHandlerResult.Success(items.Count));
            }));

        await batcher.StopAsync(CancellationToken.None);

        Assert.Equal(0, calls);
        Assert.Equal(0, batcher.GetMetrics().CompletedItems);
    }

    [Fact]
    public async Task ReusesPooledCompletionsAcrossBatches()
    {
        await using ChannelBatcher<int> batcher = Create(
            new DelegateHandler<int>((items, _) =>
                ValueTask.FromResult(BatchHandlerResult.Success(items.Count))),
            batchSize: 1,
            delay: TimeSpan.FromSeconds(1));

        for (int index = 0; index < 2_000; index++)
        {
            BatchSubmission submission = await batcher.SubmitAsync(index);
            await submission.Completion;
        }

        await batcher.StopAsync(CancellationToken.None);
        Assert.Equal(2_000, batcher.GetMetrics().CompletedItems);
    }

    [Fact]
    public void BatchHandlerResultValidatesIndexes()
    {
        BatchHandlerResult result = BatchHandlerResult.Success(1);

        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetError(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => result.GetError(1));
    }

    private static ChannelBatcher<int> Create(
        IBatchHandler<int> handler,
        int batchSize = 10,
        TimeSpan? delay = null,
        int capacity = 100,
        int concurrency = 1)
    {
        var batcher = new ChannelBatcher<int>(
            handler,
            new BatcherOptions
            {
                Capacity = capacity,
                MaximumBatchSize = batchSize,
                MaximumDelay = delay ?? TimeSpan.FromMilliseconds(10),
                HandlerConcurrency = concurrency
            },
            NullLogger<ChannelBatcher<int>>.Instance);
        batcher.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return batcher;
    }

    private static async Task<BatchSubmission[]> SubmitAsync(ChannelBatcher<int> batcher, params int[] values)
    {
        var submissions = new BatchSubmission[values.Length];
        for (int index = 0; index < values.Length; index++)
        {
            submissions[index] = await batcher.SubmitAsync(values[index]);
        }

        return submissions;
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout)
    {
        using var cancellation = new CancellationTokenSource(timeout);
        while (!predicate())
        {
            await Task.Delay(5, cancellation.Token);
        }
    }

    private sealed class DelegateHandler<T>(
        Func<IReadOnlyList<T>, CancellationToken, ValueTask<BatchHandlerResult>> callback) : IBatchHandler<T>
    {
        public ValueTask<BatchHandlerResult> HandleAsync(
            IReadOnlyList<T> items,
            CancellationToken cancellationToken) => callback(items, cancellationToken);
    }

    private static class InterlockedExtensions
    {
        public static void Max(ref int target, int candidate)
        {
            int current;
            do
            {
                current = Volatile.Read(ref target);
                if (current >= candidate)
                {
                    return;
                }
            }
            while (Interlocked.CompareExchange(ref target, candidate, current) != current);
        }
    }
}
