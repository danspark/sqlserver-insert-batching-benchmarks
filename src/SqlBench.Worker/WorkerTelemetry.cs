using System.Diagnostics.Metrics;

namespace SqlBench.Worker;

internal static class WorkerTelemetry
{
    private static readonly Meter Meter = new("SqlBench.Worker");

    public static readonly Counter<long> Delivered = Meter.CreateCounter<long>(
        "sqlbench.worker.message.delivered",
        "{message}");

    public static readonly Counter<long> Committed = Meter.CreateCounter<long>(
        "sqlbench.worker.row.committed",
        "{row}");

    public static readonly Counter<long> Acknowledged = Meter.CreateCounter<long>(
        "sqlbench.worker.message.acknowledged",
        "{message}");

    public static readonly Counter<long> Redelivered = Meter.CreateCounter<long>(
        "sqlbench.worker.message.redelivered",
        "{message}");

    public static readonly Counter<long> Errors = Meter.CreateCounter<long>(
        "sqlbench.worker.error",
        "{error}");

    public static readonly UpDownCounter<long> InFlight = Meter.CreateUpDownCounter<long>(
        "sqlbench.worker.message.in_flight",
        "{message}");

    public static readonly Histogram<double> DeliveryToCommit = Meter.CreateHistogram<double>(
        "sqlbench.worker.delivery_to_commit.duration",
        "s");

    public static readonly Histogram<double> DeliveryToAcknowledgment = Meter.CreateHistogram<double>(
        "sqlbench.worker.delivery_to_acknowledgment.duration",
        "s");

    public static readonly Histogram<double> SqlExecution = Meter.CreateHistogram<double>(
        "sqlbench.worker.sql.execution.duration",
        "s");

    public static readonly Histogram<double> Transaction = Meter.CreateHistogram<double>(
        "sqlbench.worker.sql.transaction.duration",
        "s");
}
