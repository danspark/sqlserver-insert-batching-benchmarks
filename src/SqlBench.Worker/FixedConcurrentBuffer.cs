namespace SqlBench.Worker;

internal sealed class FixedConcurrentBuffer<T>(int capacity)
    where T : struct
{
    private readonly T[] _values = new T[capacity];
    private int _count;

    public void Add(T value)
    {
        int index = Interlocked.Increment(ref _count) - 1;
        if ((uint)index >= (uint)_values.Length)
        {
            throw new InvalidOperationException($"The fixed metric buffer capacity of {_values.Length:N0} was exceeded.");
        }

        _values[index] = value;
    }

    public T[] ToArray()
    {
        int count = Math.Min(Volatile.Read(ref _count), _values.Length);
        return _values.AsSpan(0, count).ToArray();
    }
}
