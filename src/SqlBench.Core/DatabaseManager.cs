using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;

namespace SqlBench.Core;

public sealed partial class DatabaseManager(string connectionString)
{
    public const string DatabaseName = "SqlInsertBenchmarks";
    private readonly string _connectionString = connectionString;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var targetBuilder = new SqlConnectionStringBuilder(_connectionString);
        var masterBuilder = new SqlConnectionStringBuilder(_connectionString)
        {
            InitialCatalog = "master"
        };

        await using (var connection = new SqlConnection(masterBuilder.ConnectionString))
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandTimeout = 180;
            command.CommandText = $$"""
                IF DB_ID(N'{{DatabaseName}}') IS NULL
                BEGIN
                    CREATE DATABASE [{{DatabaseName}}]
                    ON PRIMARY
                    (
                        NAME = N'{{DatabaseName}}_Data',
                        FILENAME = N'/var/opt/mssql/data/{{DatabaseName}}.mdf',
                        SIZE = 512MB,
                        FILEGROWTH = 256MB
                    )
                    LOG ON
                    (
                        NAME = N'{{DatabaseName}}_Log',
                        FILENAME = N'/var/opt/mssql/data/{{DatabaseName}}.ldf',
                        SIZE = 512MB,
                        FILEGROWTH = 256MB
                    );
                END;
                ALTER DATABASE [{{DatabaseName}}] SET RECOVERY SIMPLE;
                ALTER DATABASE [{{DatabaseName}}] SET DELAYED_DURABILITY = DISABLED;
                ALTER DATABASE [{{DatabaseName}}] SET TARGET_RECOVERY_TIME = 60 SECONDS;
                """;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        targetBuilder.InitialCatalog = DatabaseName;
        await using var target = new SqlConnection(targetBuilder.ConnectionString);
        await target.OpenAsync(cancellationToken).ConfigureAwait(false);
        string schema = await ReadSchemaAsync(cancellationToken).ConfigureAwait(false);
        foreach (string batch in GoBatchRegex().Split(schema).Where(static text => !string.IsNullOrWhiteSpace(text)))
        {
            await using var command = target.CreateCommand();
            command.CommandTimeout = 180;
            command.CommandText = batch;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task ResetTargetAsync(CancellationToken cancellationToken)
    {
        await using SqlConnection connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using SqlCommand command = connection.CreateCommand();
        command.CommandText = "TRUNCATE TABLE dbo.BenchmarkTarget;";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<DatabaseVerification> VerifyAsync(
        IReadOnlyCollection<Guid> expectedMessageIds,
        CancellationToken cancellationToken)
    {
        await using SqlConnection connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using SqlCommand counts = connection.CreateCommand();
        counts.CommandText = """
            SELECT
                COUNT_BIG(*) AS [RowCount],
                COUNT_BIG(DISTINCT MessageId) AS [DistinctMessageIds],
                SUM(CASE WHEN parent.ParentId IS NULL THEN CONVERT(bigint, 1) ELSE CONVERT(bigint, 0) END) AS [InvalidForeignKeys]
            FROM dbo.BenchmarkTarget AS target
            LEFT JOIN dbo.ParentEntity AS parent ON parent.ParentId = target.ParentId;
            """;
        await using SqlDataReader reader = await counts.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        long rowCount = reader.GetInt64(0);
        long distinct = reader.GetInt64(1);
        long invalidForeignKeys = reader.IsDBNull(2) ? 0 : reader.GetInt64(2);
        await reader.CloseAsync().ConfigureAwait(false);

        var actual = new HashSet<Guid>();
        await using SqlCommand ids = connection.CreateCommand();
        ids.CommandText = "SELECT MessageId FROM dbo.BenchmarkTarget;";
        await using SqlDataReader idReader = await ids.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await idReader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            actual.Add(idReader.GetGuid(0));
        }

        int missing = expectedMessageIds.Count(id => !actual.Contains(id));
        int unexpected = actual.Count(id => !expectedMessageIds.Contains(id));
        return new DatabaseVerification
        {
            RowCount = rowCount,
            DistinctMessageIdCount = distinct,
            DuplicateMessageIdCount = rowCount - distinct,
            MissingMessageIdCount = missing,
            UnexpectedMessageIdCount = unexpected,
            InvalidForeignKeyCount = invalidForeignKeys
        };
    }

    public async Task<DatabaseMetrics> ReadMetricsAsync(CancellationToken cancellationToken)
    {
        await using SqlConnection connection = await OpenAsync(cancellationToken).ConfigureAwait(false);
        await using SqlCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                memory.physical_memory_in_use_kb,
                memory.process_physical_memory_low,
                memory.process_virtual_memory_low,
                system.process_kernel_time_ms,
                system.process_user_time_ms
            FROM sys.dm_os_process_memory AS memory
            CROSS JOIN sys.dm_os_sys_info AS system;

            SELECT
                SUM(num_of_bytes_read),
                SUM(num_of_bytes_written),
                SUM(io_stall_read_ms),
                SUM(io_stall_write_ms)
            FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL);

            SELECT
                CONVERT(bigint, total_log_size_mb * 1048576.0),
                CONVERT(bigint, active_log_size_mb * 1048576.0),
                CONVERT(decimal(19, 3), log_since_last_log_backup_mb),
                log_truncation_holdup_reason
            FROM sys.dm_db_log_stats(DB_ID());

            SELECT TOP (20)
                wait_type,
                waiting_tasks_count,
                wait_time_ms,
                signal_wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN
            (
                'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP',
                'CHECKPOINT_QUEUE', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT',
                'DBMIRROR_EVENTS_QUEUE', 'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE',
                'FT_IFTS_SCHEDULER_IDLE_WAIT', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE', 'ONDEMAND_TASK_QUEUE',
                'QDS_ASYNC_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'SLEEP_TASK',
                'SP_SERVER_DIAGNOSTICS_SLEEP', 'SQLTRACE_BUFFER_FLUSH',
                'WAITFOR', 'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT'
            )
            ORDER BY wait_time_ms DESC;
            """;
        await using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        long memoryKb = reader.GetInt64(0);
        bool physicalLow = reader.GetBoolean(1);
        bool virtualLow = reader.GetBoolean(2);
        long kernelTime = reader.GetInt64(3);
        long userTime = reader.GetInt64(4);
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        long bytesRead = reader.IsDBNull(0) ? 0 : Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture);
        long bytesWritten = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
        long readStall = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture);
        long writeStall = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture);
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        long totalLogBytes = reader.GetInt64(0);
        long usedLogBytes = reader.GetInt64(1);
        decimal logSinceBackup = reader.IsDBNull(2) ? 0 : reader.GetDecimal(2);
        string? logTruncation = reader.IsDBNull(3) ? null : reader.GetString(3);
        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        var waits = new List<SqlWaitMetric>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            waits.Add(new SqlWaitMetric
            {
                WaitType = reader.GetString(0),
                WaitingTasks = reader.GetInt64(1),
                WaitTimeMilliseconds = reader.GetInt64(2),
                SignalWaitTimeMilliseconds = reader.GetInt64(3)
            });
        }

        return new DatabaseMetrics
        {
            PhysicalMemoryKilobytes = memoryKb,
            PhysicalMemoryLow = physicalLow,
            VirtualMemoryLow = virtualLow,
            ProcessKernelTimeMilliseconds = kernelTime,
            ProcessUserTimeMilliseconds = userTime,
            BytesRead = bytesRead,
            BytesWritten = bytesWritten,
            ReadStallMilliseconds = readStall,
            WriteStallMilliseconds = writeStall,
            TotalLogBytes = totalLogBytes,
            UsedLogBytes = usedLogBytes,
            LogSinceBackupMegabytes = logSinceBackup,
            LogTruncationHoldupReason = logTruncation,
            Waits = waits
        };
    }

    public string GetTargetConnectionString()
    {
        var builder = new SqlConnectionStringBuilder(_connectionString)
        {
            InitialCatalog = DatabaseName
        };
        return builder.ConnectionString;
    }

    private async Task<SqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(GetTargetConnectionString());
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    private static async Task<string> ReadSchemaAsync(CancellationToken cancellationToken)
    {
        await using Stream stream = Assembly.GetExecutingAssembly()
            .GetManifestResourceStream("SqlBench.Core.schema.sql")
            ?? throw new InvalidOperationException("The embedded database schema was not found.");
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
    }

    [GeneratedRegex(@"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline)]
    private static partial Regex GoBatchRegex();
}

public sealed record DatabaseVerification
{
    public long RowCount { get; init; }

    public long DistinctMessageIdCount { get; init; }

    public long DuplicateMessageIdCount { get; init; }

    public int MissingMessageIdCount { get; init; }

    public int UnexpectedMessageIdCount { get; init; }

    public long InvalidForeignKeyCount { get; init; }

    public bool Passed(int expected) =>
        RowCount == expected
        && DistinctMessageIdCount == expected
        && DuplicateMessageIdCount == 0
        && MissingMessageIdCount == 0
        && UnexpectedMessageIdCount == 0
        && InvalidForeignKeyCount == 0;
}

public sealed record DatabaseMetrics
{
    public long PhysicalMemoryKilobytes { get; init; }

    public bool PhysicalMemoryLow { get; init; }

    public bool VirtualMemoryLow { get; init; }

    public long ProcessKernelTimeMilliseconds { get; init; }

    public long ProcessUserTimeMilliseconds { get; init; }

    public long BytesRead { get; init; }

    public long BytesWritten { get; init; }

    public long ReadStallMilliseconds { get; init; }

    public long WriteStallMilliseconds { get; init; }

    public long TotalLogBytes { get; init; }

    public long UsedLogBytes { get; init; }

    public decimal LogSinceBackupMegabytes { get; init; }

    public string? LogTruncationHoldupReason { get; init; }

    public IReadOnlyList<SqlWaitMetric> Waits { get; init; } = [];
}

public sealed record SqlWaitMetric
{
    public required string WaitType { get; init; }

    public long WaitingTasks { get; init; }

    public long WaitTimeMilliseconds { get; init; }

    public long SignalWaitTimeMilliseconds { get; init; }
}
