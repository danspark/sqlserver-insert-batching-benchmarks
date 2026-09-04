# Setup and troubleshooting

## Script behavior

`scripts/run-benchmarks.ps1` verifies `docker`, Docker Compose, `dotnet`, and `pwsh`; builds Release; starts only the `sqlserver` and `rabbitmq` Compose services; waits for their health checks; runs the selected controller profile; samples container statistics; validates results; generates a CSV, report, and SVG charts inside the result directory; and removes only Compose resources it created. `-UpdateReadme` writes a complete profile under `results/published`, then rebuilds the repository's canonical tables and charts from all published raw results. It rejects `-Scenario`, `-Rows`, and output directories outside `results/published`, which keeps a filtered diagnostic run from replacing the published aggregate. `-LeaveRunning` keeps created resources. `-UseRunningEnvironment` leaves lifecycle management to the caller and requires connection environment variables.

Use `-Scenario <text>` to select names containing text and `-Rows <count>` for a diagnostic row-count override. An override changes comparability and should not be mixed with published matrix results.

## Aspire

`aspire run` starts SQL Server, RabbitMQ, an idle controller, and one idle worker. Set `Parameters:workerReplicas` through Aspire configuration to show several worker processes. Aspire injects SQL Server and RabbitMQ references, orders startup through readiness, and exports worker/controller OpenTelemetry to the dashboard.

The benchmark script manages measured worker processes itself so it can place every process behind the same gate and collect one result file per instance. A measured worker starts its telemetry provider when the calling environment includes `OTEL_EXPORTER_OTLP_ENDPOINT`; it appears in the dashboard as `sqlbench-worker`, with each process identified by `worker-N`. Copy the controller resource's `OTEL_*` environment into the shell together with the required `SQLBENCH_*` connection settings before using `-UseRunningEnvironment`. `OTEL_METRIC_EXPORT_INTERVAL=1000` gives one-second samples for short smoke runs.

The Metrics page groups benchmark instruments under `SqlBench.Batching`, `SqlBench.Worker`, and `SqlBench.SqlClient`. The SQL client meter shows DML execute calls, submitted commands, commands per execute, execute time, coalescing delay, active calls, coordinator depth, coordinator wait, and coordinator backpressure. The DML execute counter excludes native transaction begin and commit operations. Runtime allocation and GC instruments are under `System.Runtime`; useful starting points are `dotnet.gc.heap.total_allocated`, `dotnet.gc.collections`, `dotnet.gc.pause.time`, `dotnet.gc.last_collection.heap.size`, `dotnet.process.cpu.time`, and `dotnet.process.memory.working_set`.

Stop Aspire before running the script-managed Compose environment on the same ports.

## Compose worker scaling

The optional `workers` profile builds the worker image. To inspect four idle worker containers:

```powershell
docker compose --profile workers up -d --scale worker=4
```

The measured script uses local Release worker processes because the shared gate and raw result directory are host-scoped. The worker image accepts the same `--config` JSON contract for custom container orchestration.

## SQL Server startup

First startup pulls a large image and creates pre-sized 512 MiB data and log files. The Compose health check uses `sqlcmd` inside the container. Inspect failures with:

```powershell
docker compose logs sqlserver
```

The script creates fresh credentials for each environment it creates. Existing project containers may contain credentials from the process that created them. Use that environment's variables with `-UseRunningEnvironment`, or remove the project explicitly with `docker compose down --volumes` when its data is disposable.

## RabbitMQ startup

RabbitMQ readiness uses `rabbitmq-diagnostics -q ping`. Queue count and broker metrics require the management plugin included by the pinned image. Inspect failures with:

```powershell
docker compose logs rabbitmq
```

## Failed runs

A worker error intentionally fails the scenario. SQL-backed deliveries stay unacknowledged until the connection closes, when RabbitMQ makes them ready for redelivery. The controller records the error and queue state, and the report excludes the scenario from valid performance comparisons.

Result work files live below `.work` in the timestamped directory. Worker stderr, configuration, and partial results remain there for diagnosis. Configuration files contain generated credentials while a run is active, so `results/local` is ignored by Git. Published raw files contain measurements only.
