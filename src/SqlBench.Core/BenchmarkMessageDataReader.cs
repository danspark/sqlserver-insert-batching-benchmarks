using System.Data;
using System.Globalization;

namespace SqlBench.Core;

internal sealed class BenchmarkMessageDataReader(IReadOnlyList<BenchmarkMessage> rows) : IDataReader
{
    private static readonly string[] Names =
    [
        "MessageId", "ParentId", "CorrelationId", "OccurredAt", "SequenceNo",
        "CounterValue", "Priority", "Amount", "IsActive", "Code", "Description",
        "PayloadHash", "OptionalNote"
    ];

    private static readonly Type[] Types =
    [
        typeof(Guid), typeof(int), typeof(Guid), typeof(DateTime), typeof(int),
        typeof(long), typeof(short), typeof(decimal), typeof(bool), typeof(string),
        typeof(string), typeof(byte[]), typeof(string)
    ];

    private readonly IReadOnlyList<BenchmarkMessage> _rows = rows;
    private int _index = -1;

    public int FieldCount => Names.Length;

    public int Depth => 0;

    public bool IsClosed { get; private set; }

    public int RecordsAffected => -1;

    public object this[int index] => GetValue(index);

    public object this[string name] => GetValue(GetOrdinal(name));

    public bool Read()
    {
        ObjectDisposedException.ThrowIf(IsClosed, this);
        if (_index + 1 >= _rows.Count)
        {
            return false;
        }

        _index++;
        return true;
    }

    public bool NextResult() => false;

    public void Close() => IsClosed = true;

    public DataTable? GetSchemaTable() => null;

    public string GetName(int index) => Names[index];

    public string GetDataTypeName(int index) => GetFieldType(index).Name;

    public Type GetFieldType(int index) => Types[index];

    public int GetOrdinal(string name)
    {
        int index = Array.FindIndex(Names, candidate =>
            string.Equals(candidate, name, StringComparison.OrdinalIgnoreCase));
        return index >= 0
            ? index
            : throw new ArgumentException($"Unknown column '{name}'.", nameof(name));
    }

    public object GetValue(int index)
    {
        BenchmarkMessage row = Current;
        return index switch
        {
            0 => row.MessageId,
            1 => row.ParentId,
            2 => row.CorrelationId,
            3 => row.OccurredAt,
            4 => row.SequenceNo,
            5 => row.CounterValue,
            6 => row.Priority,
            7 => row.Amount,
            8 => row.IsActive,
            9 => row.Code,
            10 => row.Description,
            11 => row.PayloadHash,
            12 => (object?)row.OptionalNote ?? DBNull.Value,
            _ => throw new ArgumentOutOfRangeException(nameof(index), index, "Unknown column ordinal.")
        };
    }

    public int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int index = 0; index < count; index++)
        {
            values[index] = GetValue(index);
        }

        return count;
    }

    public bool IsDBNull(int index) => GetValue(index) is DBNull;

    public Guid GetGuid(int index) => (Guid)GetValue(index);

    public DateTime GetDateTime(int index) => (DateTime)GetValue(index);

    public string GetString(int index) => (string)GetValue(index);

    public bool GetBoolean(int index) => (bool)GetValue(index);

    public byte GetByte(int index) => Convert.ToByte(GetValue(index), CultureInfo.InvariantCulture);

    public char GetChar(int index) => Convert.ToChar(GetValue(index), CultureInfo.InvariantCulture);

    public short GetInt16(int index) => Convert.ToInt16(GetValue(index), CultureInfo.InvariantCulture);

    public int GetInt32(int index) => Convert.ToInt32(GetValue(index), CultureInfo.InvariantCulture);

    public long GetInt64(int index) => Convert.ToInt64(GetValue(index), CultureInfo.InvariantCulture);

    public float GetFloat(int index) => Convert.ToSingle(GetValue(index), CultureInfo.InvariantCulture);

    public double GetDouble(int index) => Convert.ToDouble(GetValue(index), CultureInfo.InvariantCulture);

    public decimal GetDecimal(int index) => Convert.ToDecimal(GetValue(index), CultureInfo.InvariantCulture);

    public IDataReader GetData(int index) => throw new InvalidCastException(GetName(index));

    public long GetBytes(int index, long fieldOffset, byte[]? buffer, int bufferOffset, int length)
    {
        byte[] source = (byte[])GetValue(index);
        if (buffer is null)
        {
            return source.Length;
        }

        int available = Math.Max(0, source.Length - checked((int)fieldOffset));
        int count = Math.Min(length, available);
        Array.Copy(source, fieldOffset, buffer, bufferOffset, count);
        return count;
    }

    public long GetChars(int index, long fieldOffset, char[]? buffer, int bufferOffset, int length)
    {
        string source = GetString(index);
        if (buffer is null)
        {
            return source.Length;
        }

        int available = Math.Max(0, source.Length - checked((int)fieldOffset));
        int count = Math.Min(length, available);
        source.CopyTo(checked((int)fieldOffset), buffer, bufferOffset, count);
        return count;
    }

    public void Dispose() => Close();

    private BenchmarkMessage Current => _index >= 0 && _index < _rows.Count
        ? _rows[_index]
        : throw new InvalidOperationException("Read must position the data reader on a row.");
}
