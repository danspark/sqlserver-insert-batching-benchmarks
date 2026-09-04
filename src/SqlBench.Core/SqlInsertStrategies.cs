using System.Data;
using System.Collections.Concurrent;
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

public readonly record struct SqlWriteTiming
{
    public required TimeSpan SqlExecution { get; init; }

    public required TimeSpan Transaction { get; init; }
}

public interface ISqlInsertStrategy : IAsyncDisposable
{
    InsertStrategyKind Kind { get; }

    int MaximumBatchSize { get; }

    SqlRequestMetricsSnapshot GetRequestMetrics();

    Task<SqlWriteTiming> InsertAsync(
        string connectionString,
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options,
        CancellationToken cancellationToken);
}

public static class SqlInsertStrategyFactory
{
    public static ISqlInsertStrategy Create(
        InsertStrategyKind kind,
        SqlExecutionKind execution = SqlExecutionKind.Native,
        int sqlBatchMaximumCommands = 1,
        int sqlBatchMaximumDelayMilliseconds = 1,
        int sqlBatchRequestConcurrency = 1)
    {
        ISqlInsertStrategy strategy = kind switch
        {
            InsertStrategyKind.Individual => new IndividualInsertStrategy(),
            InsertStrategyKind.TableValuedParameter => new TableValuedParameterInsertStrategy(),
            InsertStrategyKind.MultipleInsertStatements => new MultipleStatementsInsertStrategy(),
            InsertStrategyKind.MultiRowValues => new MultiRowValuesInsertStrategy(),
            InsertStrategyKind.BulkCopy => new BulkCopyInsertStrategy(),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };

        return execution switch
        {
            SqlExecutionKind.Native => strategy,
            SqlExecutionKind.SqlBatch when strategy is ISqlBatchCommandBuilder builder =>
                new SqlBatchCoalescingInsertStrategy(
                    strategy,
                    builder,
                    sqlBatchMaximumCommands,
                    TimeSpan.FromMilliseconds(sqlBatchMaximumDelayMilliseconds),
                    sqlBatchRequestConcurrency),
            SqlExecutionKind.SqlBatch => throw new ArgumentException(
                $"The {kind} strategy cannot execute through SqlBatch.",
                nameof(execution)),
            _ => throw new ArgumentOutOfRangeException(nameof(execution))
        };
    }
}

internal abstract class SqlInsertStrategyBase : ISqlInsertStrategy
{
    private const string SqlBatchCommittedParameter = "@sqlbenchCommitted";
    private static readonly string[][] ParameterNames = Enumerable.Range(0, SqlLimits.MaximumRowsPerParameterizedCommand)
        .Select(static index => new[]
        {
            $"@messageId{index}",
            $"@parentId{index}",
            $"@correlationId{index}",
            $"@occurredAt{index}",
            $"@sequenceNo{index}",
            $"@counterValue{index}",
            $"@priority{index}",
            $"@amount{index}",
            $"@isActive{index}",
            $"@code{index}",
            $"@description{index}",
            $"@payloadHash{index}",
            $"@optionalNote{index}"
        })
        .ToArray();
    private static readonly string[] RowParameterLists = ParameterNames
        .Select(static names => $"({string.Join(',', names)})")
        .ToArray();
    private readonly SqlRequestMetrics _requestMetrics = new(1);

    public abstract InsertStrategyKind Kind { get; }

    public virtual int MaximumBatchSize => 5_000;

    public SqlRequestMetricsSnapshot GetRequestMetrics() => _requestMetrics.Snapshot();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

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
            TimeSpan execution;
            _requestMetrics.RequestStarted(Kind, SqlExecutionKind.Native);
            try
            {
                await ExecuteAsync(connection, transaction, rows, options, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                execution = Stopwatch.GetElapsedTime(executionStart);
                _requestMetrics.RequestCompleted(Kind, SqlExecutionKind.Native);
                _requestMetrics.Record(1, execution, TimeSpan.Zero, Kind, SqlExecutionKind.Native);
            }

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

    protected static void AddRowParameters(SqlCommand command, BenchmarkMessage row, int index) =>
        AddRowParameters(command.Parameters, row, index);

    protected static void AddRowParameters(SqlParameterCollection parameters, BenchmarkMessage row, int index)
    {
        string[] names = ParameterNames[index];
        Add(parameters, names[0], SqlDbType.UniqueIdentifier, row.MessageId);
        Add(parameters, names[1], SqlDbType.Int, row.ParentId);
        Add(parameters, names[2], SqlDbType.UniqueIdentifier, row.CorrelationId);
        Add(parameters, names[3], SqlDbType.DateTime2, row.OccurredAt).Scale = 3;
        Add(parameters, names[4], SqlDbType.Int, row.SequenceNo);
        Add(parameters, names[5], SqlDbType.BigInt, row.CounterValue);
        Add(parameters, names[6], SqlDbType.SmallInt, row.Priority);
        SqlParameter amount = Add(parameters, names[7], SqlDbType.Decimal, row.Amount);
        amount.Precision = 18;
        amount.Scale = 4;
        Add(parameters, names[8], SqlDbType.Bit, row.IsActive);
        Add(parameters, names[9], SqlDbType.VarChar, row.Code).Size = 32;
        Add(parameters, names[10], SqlDbType.NVarChar, row.Description).Size = 128;
        Add(parameters, names[11], SqlDbType.Binary, row.PayloadHash).Size = 16;
        Add(parameters, names[12], SqlDbType.NVarChar, row.OptionalNote).Size = 64;
    }

    protected static string RowParameterList(int index) => RowParameterLists[index];

    protected static SqlBatchCommand CreateTransactionalBatchCommand(string commandText)
    {
        var command = new SqlBatchCommand($$"""
            SET XACT_ABORT ON;
            SET {{SqlBatchCommittedParameter}} = 0;
            BEGIN TRY
                BEGIN TRANSACTION;
                {{commandText}}
                COMMIT TRANSACTION;
                SET {{SqlBatchCommittedParameter}} = 1;
            END TRY
            BEGIN CATCH
                IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
                THROW;
            END CATCH;
            """);
        command.Parameters.Add(new SqlParameter(SqlBatchCommittedParameter, SqlDbType.Bit)
        {
            Direction = ParameterDirection.InputOutput,
            Value = false
        });
        return command;
    }

    internal static bool BatchCommandCommitted(SqlBatchCommand command) =>
        command.Parameters[SqlBatchCommittedParameter].Value is true;

    protected const string InsertPrefix = """
        INSERT dbo.BenchmarkTarget
        (MessageId,ParentId,CorrelationId,OccurredAt,SequenceNo,CounterValue,Priority,Amount,IsActive,Code,Description,PayloadHash,OptionalNote)
        VALUES
        """;

    private static SqlParameter Add(SqlParameterCollection parameters, string name, SqlDbType type, object? value)
    {
        var parameter = new SqlParameter(name, type)
        {
            Value = value ?? DBNull.Value
        };
        parameters.Add(parameter);
        return parameter;
    }
}

internal sealed class IndividualInsertStrategy : SqlInsertStrategyBase, ISqlBatchCommandBuilder
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

    public SqlBatchCommand CreateBatchCommand(
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options)
    {
        _ = options;
        SqlBatchCommand command = CreateTransactionalBatchCommand(InsertPrefix + RowParameterList(0) + ";");
        AddRowParameters(command.Parameters, rows[0], 0);
        return command;
    }
}

internal sealed class MultipleStatementsInsertStrategy : SqlInsertStrategyBase, ISqlBatchCommandBuilder
{
    private static readonly ConcurrentDictionary<int, string> CommandTexts = new();

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
        command.CommandText = GetCommandText(rows.Count);
        for (int index = 0; index < rows.Count; index++)
        {
            AddRowParameters(command, rows[index], index);
        }

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public SqlBatchCommand CreateBatchCommand(
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options)
    {
        _ = options;
        SqlBatchCommand command = CreateTransactionalBatchCommand(GetCommandText(rows.Count));
        for (int index = 0; index < rows.Count; index++)
        {
            AddRowParameters(command.Parameters, rows[index], index);
        }

        return command;
    }

    private static string GetCommandText(int rowCount) => CommandTexts.GetOrAdd(rowCount, static count =>
    {
        var sql = new StringBuilder(count * 360);
        for (int index = 0; index < count; index++)
        {
            sql.Append(InsertPrefix).Append(RowParameterList(index)).AppendLine(";");
        }

        return sql.ToString();
    });
}

internal sealed class MultiRowValuesInsertStrategy : SqlInsertStrategyBase, ISqlBatchCommandBuilder
{
    private static readonly ConcurrentDictionary<int, string> CommandTexts = new();

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
        command.CommandText = GetCommandText(rows.Count);
        for (int index = 0; index < rows.Count; index++)
        {
            AddRowParameters(command, rows[index], index);
        }

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public SqlBatchCommand CreateBatchCommand(
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options)
    {
        _ = options;
        SqlBatchCommand command = CreateTransactionalBatchCommand(GetCommandText(rows.Count));
        for (int index = 0; index < rows.Count; index++)
        {
            AddRowParameters(command.Parameters, rows[index], index);
        }

        return command;
    }

    private static string GetCommandText(int rowCount) => CommandTexts.GetOrAdd(rowCount, static count =>
    {
        var sql = new StringBuilder(count * 210).Append(InsertPrefix);
        for (int index = 0; index < count; index++)
        {
            if (index > 0)
            {
                sql.Append(',');
            }

            sql.AppendLine().Append(RowParameterList(index));
        }

        return sql.Append(';').ToString();
    });
}

internal sealed class TableValuedParameterInsertStrategy : SqlInsertStrategyBase, ISqlBatchCommandBuilder
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

    public SqlBatchCommand CreateBatchCommand(
        IReadOnlyList<BenchmarkMessage> rows,
        SqlInsertOptions options)
    {
        _ = options;
        SqlBatchCommand command = CreateTransactionalBatchCommand("EXEC dbo.InsertBenchmarkRows @Rows;");
        command.Parameters.Add(new SqlParameter("@Rows", SqlDbType.Structured)
        {
            TypeName = "dbo.BenchmarkRowType",
            Value = StreamRecords(rows)
        });
        return command;
    }

    private static IEnumerable<SqlDataRecord> StreamRecords(IReadOnlyList<BenchmarkMessage> rows)
    {
        var record = new SqlDataRecord(Metadata);
        for (int index = 0; index < rows.Count; index++)
        {
            BenchmarkMessage row = rows[index];
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
