# Setup and troubleshooting

## Script behavior

`scripts/run-benchmarks.ps1` verifies `docker`, Docker Compose, `dotnet`, and `pwsh`; builds Release; starts only the `sqlserver` and `rabbitmq` Compose services; waits for their health checks; runs the selected controller profile; samples container statistics; validates results; regenerates the CSV, README tables, and SVG charts; and removes only Compose resources it created. `-LeaveRunning` keeps those resources. `-UseRunningEnvironment` leaves lifecycle management to the caller and requires connection environment variables.

Use `-Scenario <text>` to select names containing text and `-Rows <count>` for a diagnostic row-count override. An override changes comparability and should not be mixed with published matrix results.

## Aspire

`aspire run` starts SQL Server, RabbitMQ, an idle controller, and one idle worker. Set `Parameters:workerReplicas` through Aspire configuration to show several worker processes. Aspire injects SQL Server and RabbitMQ references, orders startup through readiness, and exports worker/controller OpenTelemetry to the dashboard.

The benchmark script manages measured worker processes itself so it can place every process behind the same gate and collect one result file per instance. Stop Aspire before running the script-managed Compose environment on the same ports.

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
