using System.Data;
using System.Diagnostics;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;

namespace SqlBench.Core;

public sealed record SqlInsertOptions
{
    public int CommandTimeoutSeconds { get; init; } = 120;

    public int BulkCopyTimeoutSeconds { get; init; } = 120;

    public bool BulkCopyTableLock { get; init; } = true;

    public bool BulkCopyEnableStreaming { get; init; } = true;
}

public sealed record SqlWriteTiming
{
    public required TimeSpan SqlExecution { get; init; }

    public required TimeSpan Transaction { get; init; }
}

public interface ISqlInsertStrategy
{
    InsertStrategyKind Kind { get; }

    int MaximumBatchSize { get; }

    Task<SqlWriteTiming> InsertAsync(
        string connectionString,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken);
}

public static class SqlInsertStrategyFactory
{
    public static ISqlInsertStrategy Create(InsertStrategyKind kind) => kind switch
    {
        InsertStrategyKind.Individual => new IndividualInsertStrategy(),
        InsertStrategyKind.TableValuedParameter => new TableValuedParameterInsertStrategy(),
        InsertStrategyKind.MultipleInsertStatements => new MultipleStatementsInsertStrategy(),
        InsertStrategyKind.MultiRowValues => new MultiRowValuesInsertStrategy(),
        InsertStrategyKind.BulkCopy => new BulkCopyInsertStrategy(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind))
    };
}

internal abstract class SqlInsertStrategyBase : ISqlInsertStrategy
{
    public abstract InsertStrategyKind Kind { get; }

    public virtual int MaximumBatchSize => 5_000;

    public async Task<SqlWriteTiming> InsertAsync(
        string connectionString,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(rows);
        if (rows.Count == 0 || rows.Count > MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(nameof(rows));
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        long transactionStart = Stopwatch.GetTimestamp();
        await using SqlTransaction transaction = (SqlTransaction)await connection
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            long executionStart = Stopwatch.GetTimestamp();
            await ExecuteAsync(connection, transaction, rows, options, cancellationToken).ConfigureAwait(false);
            TimeSpan execution = Stopwatch.GetElapsedTime(executionStart);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return new SqlWriteTiming
            {
                SqlExecution = execution,
                Transaction = Stopwatch.GetElapsedTime(transactionStart)
            };
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    protected abstract Task ExecuteAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken);

    protected static void AddRowParameters(SqlCommand command, BenchmarkMessage row, int index)
    {
        Add(command, $"@messageId{index}", SqlDbType.UniqueIdentifier, row.MessageId);
        Add(command, $"@parentId{index}", SqlDbType.Int, row.ParentId);
        Add(command, $"@correlationId{index}", SqlDbType.UniqueIdentifier, row.CorrelationId);
        Add(command, $"@occurredAt{index}", SqlDbType.DateTime2, row.OccurredAt).Scale = 3;
        Add(command, $"@sequenceNo{index}", SqlDbType.Int, row.SequenceNo);
        Add(command, $"@counterValue{index}", SqlDbType.BigInt, row.CounterValue);
        Add(command, $"@priority{index}", SqlDbType.SmallInt, row.Priority);
        SqlParameter amount = Add(command, $"@amount{index}", SqlDbType.Decimal, row.Amount);
        amount.Precision = 18;
        amount.Scale = 4;
        Add(command, $"@isActive{index}", SqlDbType.Bit, row.IsActive);
        Add(command, $"@code{index}", SqlDbType.VarChar, row.Code).Size = 32;
        Add(command, $"@description{index}", SqlDbType.NVarChar, row.Description).Size = 128;
        Add(command, $"@payloadHash{index}", SqlDbType.Binary, row.PayloadHash).Size = 16;
        Add(command, $"@optionalNote{index}", SqlDbType.NVarChar, row.OptionalNote).Size = 64;
    }

    protected static string RowParameterList(int index) =>
        $"(@messageId{index},@parentId{index},@correlationId{index},@occurredAt{index}," +
        $"@sequenceNo{index},@counterValue{index},@priority{index},@amount{index}," +
        $"@isActive{index},@code{index},@description{index},@payloadHash{index},@optionalNote{index})";

    protected static string InsertPrefix => """
        INSERT dbo.BenchmarkTarget
        (MessageId,ParentId,CorrelationId,OccurredAt,SequenceNo,CounterValue,Priority,Amount,IsActive,Code,Description,PayloadHash,OptionalNote)
        VALUES
        """;

    private static SqlParameter Add(SqlCommand command, string name, SqlDbType type, object? value)
    {
        var parameter = new SqlParameter(name, type)
        {
            Value = value ?? DBNull.Value
        };
        command.Parameters.Add(parameter);
        return parameter;
    }
}

internal sealed class IndividualInsertStrategy : SqlInsertStrategyBase
{
    public override InsertStrategyKind Kind => InsertStrategyKind.Individual;

    public override int MaximumBatchSize => 1;

    protected override async Task ExecuteAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken)
    {
        await using SqlCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = options.CommandTimeoutSeconds;
        command.CommandText = InsertPrefix + RowParameterList(0) + ";";
        AddRowParameters(command, rows[0], 0);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

internal sealed class MultipleStatementsInsertStrategy : SqlInsertStrategyBase
{
    public override InsertStrategyKind Kind => InsertStrategyKind.MultipleInsertStatements;

    public override int MaximumBatchSize => SqlLimits.MaximumRowsPerParameterizedCommand;

    protected override async Task ExecuteAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken)
    {
        await using SqlCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = options.CommandTimeoutSeconds;
        var sql = new StringBuilder(rows.Count * 360);
        for (int index = 0; index < rows.Count; index++)
        {
            sql.Append(InsertPrefix).Append(RowParameterList(index)).AppendLine(";");
            AddRowParameters(command, rows[index], index);
        }

        command.CommandText = sql.ToString();
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

internal sealed class MultiRowValuesInsertStrategy : SqlInsertStrategyBase
{
    public override InsertStrategyKind Kind => InsertStrategyKind.MultiRowValues;

    public override int MaximumBatchSize => SqlLimits.MaximumRowsPerParameterizedCommand;

    protected override async Task ExecuteAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken)
    {
        await using SqlCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = options.CommandTimeoutSeconds;
        var sql = new StringBuilder(rows.Count * 210).Append(InsertPrefix);
        for (int index = 0; index < rows.Count; index++)
        {
            if (index > 0)
            {
                sql.Append(',');
            }

            sql.AppendLine().Append(RowParameterList(index));
            AddRowParameters(command, rows[index], index);
        }

        command.CommandText = sql.Append(';').ToString();
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

internal sealed class TableValuedParameterInsertStrategy : SqlInsertStrategyBase
{
    private static readonly SqlMetaData[] Metadata =
    [
        new("MessageId", SqlDbType.UniqueIdentifier),
        new("ParentId", SqlDbType.Int),
        new("CorrelationId", SqlDbType.UniqueIdentifier),
        new("OccurredAt", SqlDbType.DateTime2, precision: 0, scale: 3),
        new("SequenceNo", SqlDbType.Int),
        new("CounterValue", SqlDbType.BigInt),
        new("Priority", SqlDbType.SmallInt),
        new("Amount", SqlDbType.Decimal, 18, 4),
        new("IsActive", SqlDbType.Bit),
        new("Code", SqlDbType.VarChar, 32),
        new("Description", SqlDbType.NVarChar, 128),
        new("PayloadHash", SqlDbType.Binary, 16),
        new("OptionalNote", SqlDbType.NVarChar, 64)
    ];

    public override InsertStrategyKind Kind => InsertStrategyKind.TableValuedParameter;

    protected override async Task ExecuteAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken)
    {
        await using SqlCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandTimeout = options.CommandTimeoutSeconds;
        command.CommandType = CommandType.StoredProcedure;
        command.CommandText = "dbo.InsertBenchmarkRows";
        var parameter = new SqlParameter("@Rows", SqlDbType.Structured)
        {
            TypeName = "dbo.BenchmarkRowType",
            Value = StreamRecords(rows)
        };
        command.Parameters.Add(parameter);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static IEnumerable<SqlDataRecord> StreamRecords(IReadOnlyList<BenchmarkMessage> rows)
    {
        var record = new SqlDataRecord(Metadata);
        foreach (BenchmarkMessage row in rows)
        {
            record.SetGuid(0, row.MessageId);
            record.SetInt32(1, row.ParentId);
            record.SetGuid(2, row.CorrelationId);
            record.SetDateTime(3, row.OccurredAt);
            record.SetInt32(4, row.SequenceNo);
            record.SetInt64(5, row.CounterValue);
            record.SetInt16(6, row.Priority);
            record.SetDecimal(7, row.Amount);
            record.SetBoolean(8, row.IsActive);
            record.SetString(9, row.Code);
            record.SetString(10, row.Description);
            record.SetBytes(11, 0, row.PayloadHash, 0, row.PayloadHash.Length);
            if (row.OptionalNote is null)
            {
                record.SetDBNull(12);
            }
            else
            {
                record.SetString(12, row.OptionalNote);
            }

            yield return record;
        }
    }
}

internal sealed class BulkCopyInsertStrategy : SqlInsertStrategyBase
{
    private static readonly string[] Columns =
    [
        "MessageId", "ParentId", "CorrelationId", "OccurredAt", "SequenceNo",
        "CounterValue", "Priority", "Amount", "IsActive", "Code", "Description",
        "PayloadHash", "OptionalNote"
    ];

    public override InsertStrategyKind Kind => InsertStrategyKind.BulkCopy;

    protected override async Task ExecuteAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken)
    {
        SqlBulkCopyOptions bulkOptions = SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.KeepNulls;
        if (options.BulkCopyTableLock)
        {
            bulkOptions |= SqlBulkCopyOptions.TableLock;
        }

        using var bulk = new SqlBulkCopy(connection, bulkOptions, transaction)
        {
            DestinationTableName = "dbo.BenchmarkTarget",
            BatchSize = rows.Count,
            BulkCopyTimeout = options.BulkCopyTimeoutSeconds,
            EnableStreaming = options.BulkCopyEnableStreaming
        };
        foreach (string column in Columns)
        {
            bulk.ColumnMappings.Add(column, column);
        }

        using var reader = new BenchmarkMessageDataReader(rows);
        await bulk.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);
    }
}
