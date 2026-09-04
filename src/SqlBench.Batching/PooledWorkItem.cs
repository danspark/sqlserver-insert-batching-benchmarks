using System.Collections.Concurrent;
using System.Threading.Tasks.Sources;

namespace SqlBench.Batching;

internal sealed class PooledWorkItem<T> : IValueTaskSource
{
    private static readonly ConcurrentBag<PooledWorkItem<T>> Pool = [];
    private ManualResetValueTaskSourceCore<bool> _completion = new()
    {
        RunContinuationsAsynchronously = true
    };
    private T _value = default!;

    private PooledWorkItem()
    {
    }

    public T Value => _value;

    public long AcceptedTimestamp { get; private set; }

    public ValueTask Completion => new(this, _completion.Version);

    public static PooledWorkItem<T> Rent(T value, long acceptedTimestamp)
    {
        if (!Pool.TryTake(out PooledWorkItem<T>? item))
        {
            item = new PooledWorkItem<T>();
        }

        item._completion.Reset();
        item._value = value;
        item.AcceptedTimestamp = acceptedTimestamp;
        return item;
    }

    public void Complete(Exception? error)
    {
        if (error is null)
        {
            _completion.SetResult(true);
        }
        else
        {
            _completion.SetException(error);
        }
    }

    public void ReturnUnsubmitted()
    {
        _value = default!;
        AcceptedTimestamp = 0;
        Pool.Add(this);
    }

    void IValueTaskSource.GetResult(short token)
    {
        try
        {
            _completion.GetResult(token);
        }
        finally
        {
            _value = default!;
            AcceptedTimestamp = 0;
            Pool.Add(this);
        }
    }

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _completion.GetStatus(token);

    void IValueTaskSource.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags) =>
        _completion.OnCompleted(continuation, state, token, flags);
}

internal sealed class PooledBatch<T> : IReadOnlyList<T>
{
    private static readonly ConcurrentBag<PooledBatch<T>> Pool = [];
    private readonly List<PooledWorkItem<T>> _items;

    private PooledBatch(int capacity) => _items = new List<PooledWorkItem<T>>(capacity);

    public int Count => _items.Count;

    public T this[int index] => _items[index].Value;

    public static PooledBatch<T> Rent(int capacity)
    {
        if (!Pool.TryTake(out PooledBatch<T>? batch))
        {
            return new PooledBatch<T>(capacity);
        }

        batch._items.EnsureCapacity(capacity);
        return batch;
    }

    public void Add(PooledWorkItem<T> item) => _items.Add(item);

    public PooledWorkItem<T> GetWorkItem(int index) => _items[index];

    public void Return()
    {
        _items.Clear();
        Pool.Add(this);
    }

    public IEnumerator<T> GetEnumerator()
    {
        for (int index = 0; index < _items.Count; index++)
        {
            yield return _items[index].Value;
        }
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
}
