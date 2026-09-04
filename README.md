# SQL Server insert batching benchmarks

This repository asks a narrow question: for a preloaded RabbitMQ workload, which tested combination of SQL insert strategy, process-level batcher, writer concurrency, batch size, batching delay, channel capacity, prefetch, parent-key distribution, and worker count commits rows fastest without weakening transaction or acknowledgment semantics?

The answer is an observed result, not a universal recommendation. It applies only to the recorded machine, container limits, schema, workload, durability settings, and configuration matrix.

## Architecture

```mermaid
flowchart LR
    P[Deterministic producer] -->|persistent messages and confirms| Q[(Preloaded durable RabbitMQ queue)]
    G[Start gate] --> W1[Worker instance 1]
    G --> WN[Worker instance N]
    Q -->|manual delivery| W1
    Q -->|competing consumer| WN
    W1 --> B1[Process-level batcher]
    WN --> BN[Independent process-level batcher]
    B1 --> S[Selected SQL strategy]
    BN --> S
    S -->|one transaction per logical batch| D[(SQL Server)]
    D -->|commit succeeds| A[RabbitMQ acknowledgment]
    C[Benchmark controller] --> P
    C --> G
    C --> V[Database, queue, worker, and telemetry verification]
```

Each worker process owns its batching state. Worker processes consume from the same queue as competing consumers. The generic batching library has no SQL Server, RabbitMQ, ASP.NET Core, benchmark entity, or benchmark message dependency.

```mermaid
sequenceDiagram
    participant C as Controller
    participant Q as RabbitMQ
    participant W as Worker processes
    participant B as Process batcher
    participant S as SQL Server
    C->>S: reset schema state and target rows
    C->>Q: purge benchmark queue
    C->>Q: publish complete deterministic workload
    Q-->>C: publisher confirms
    C->>Q: verify expected ready count and zero unacknowledged
    C->>W: start processes behind gate and await readiness
    C->>C: start monotonic timer
    C->>W: release gate
    Q->>W: deliver messages with manual acknowledgment
    W->>B: submit deserialized pending work
    B->>S: execute selected strategy in one transaction
    S-->>B: transaction committed
    B-->>W: complete each item
    W->>Q: acknowledge each committed message
    W-->>C: post-ack drain marker
    C->>C: stop timer and accept or reject run
    W-->>C: final metrics and errors
    C->>Q: verify zero ready and zero unacknowledged
    C->>S: verify rows, IDs, uniqueness, and foreign keys
```

## Persistence strategies

- **Individual insert per message.** One parameterized `INSERT`, one transaction commit, then one acknowledgment. Its legal batch size is one.
- **Table-valued parameter.** The client streams `SqlDataRecord` values into `dbo.BenchmarkRowType`; `dbo.InsertBenchmarkRows` performs one set-based insert and one commit.
- **Multiple INSERT statements.** One parameterized command contains one complete `INSERT` statement per item and commits once. Data values never enter command text.
- **Multi-row VALUES.** One parameterized `INSERT ... VALUES (...), (...)` command commits once. Thirteen parameters per row and SQL Server's 2,100-parameter ceiling limit a command to 161 rows.
- **SqlBulkCopy.** Client-side `Microsoft.Data.SqlClient.SqlBulkCopy` writes through the same explicit transaction with check constraints, null preservation, configurable table locking, timeout, batch size, and streaming mode. This is not T-SQL `BULK INSERT`.

Every strategy uses `READ COMMITTED`, full transaction-log commit semantics, the same target table, constraints, indexes, connection pool, command timeout, and database durability settings. SQL Server uses `SIMPLE` recovery with `DELAYED_DURABILITY = DISABLED`. Simple recovery controls log reuse; it does not turn a transaction commit into a delayed commit.

## Process batching

The `ChannelBatcher<T>` implementation uses a bounded `Channel<T>`, configurable handler concurrency, a maximum batch size, and a monotonic maximum-delay deadline. Bounded writes apply backpressure. Shutdown closes the writer and drains every accepted item before stopping. Each submission receives its own completion task. Metrics cover queue depth, channel wait, batch size, fill time, handler time, completions, failures, and backpressure.

The `LockSwapBatcher<T>` implementation has no intermediate channel. Producers append under a short lock; a full buffer or periodic timer swaps ownership of the list with a new buffer. A capacity semaphore applies equivalent backpressure and a handler semaphore limits concurrency. It drains the final partial buffer on shutdown.

The no-batching baseline invokes the handler immediately. It is used for individual insert per message and does not count as the second batching design.

## Queue and completion semantics

The producer creates deterministic message IDs and payloads, publishes persistent messages to a durable queue, and waits for publisher confirms. Workers stay behind a start gate until the controller verifies the full ready-message count. Publication and process startup are outside the measured interval.

Workers deserialize on delivery. A SQL-backed item becomes committed only after its transaction commits. The worker then acknowledges that delivery on its RabbitMQ channel. A message counts as completed only after both events succeed. The primary runs disable automatic connection recovery and application retry. A failed batch stops the worker, leaves its deliveries unacknowledged, and invalidates the run.

Delivery-to-commit and delivery-to-acknowledgment use `Stopwatch` timestamps inside the worker. Deliberate queue residence before the gate is not reported as latency. Delivered, committed, and acknowledged counts remain separate in every raw result.

The no-op queue control uses the same client, payload, JSON deserialization, competing-consumer topology, prefetch, process batcher, and manual acknowledgment path. Its handler performs no database write. The direct database control sends the pre-generated in-memory rows through the same SQL strategy classes without RabbitMQ, deserialization, process batching, or acknowledgments.

## Schema and data

`dbo.ParentEntity` contains 10,000 seeded rows. `dbo.BenchmarkTarget` has a `bigint IDENTITY` clustered primary key; a unique constraint on deterministic `MessageId`; a foreign key and supporting index on `ParentId`; and columns using `uniqueidentifier`, `datetime2(3)`, `int`, `bigint`, `smallint`, `decimal(18,4)`, `bit`, `varchar`, `nvarchar`, `binary(16)`, and nullable `nvarchar`. The schema stays unchanged across strategies.

The checked-in schema estimates a 390-byte logical row before page and index overhead. The runner records the average serialized JSON message size from each workload in raw output.

The default seed is `1592606758` (`0x5EED2026`). `System.Random` selects parent IDs; SHA-256 of the seed and sequence creates message and correlation IDs plus fixed binary data. Other values derive from the sequence or seeded generator. The three profiles are:

- **Uniform:** every parent ID has equal selection probability.
- **Moderate skew:** 80% of rows select uniformly from parent IDs 1 through 2,000; 20% select from 2,001 through 10,000.
- **Hot parent:** 95% reference parent ID 1; 5% select uniformly from the other parents.

Every strategy in a distribution receives rows generated by the same algorithm and seed.

## Search method

`config/Full.json` is generated by `scripts/generate-full-profile.ps1` and checked in. Its 90 scenarios bound the search rather than forming a wasteful Cartesian product. The broad stage covers every strategy and distribution. Refinement stages vary legal batch sizes, 1/5/20/50 ms delays, capacities, prefetch values, channel versus lock-swap batching, 1/2/4/8 writers, and 1/2/4 worker processes. Direct controls follow, then each selected finalist runs three times. Dynamic SQL tests stop at the calculated 161-row legal maximum. TVP and bulk copy test 10, 50, 100, 500, 1,000, and 5,000 rows. After that sweep, `config/Confirmation.json` repeats the best observed configuration for each strategy three times with 750,000 identical rows per run.

Stages run in search order; scenario order is rotated deterministically inside each stage. Each implementation receives an unreported warmup, followed by a database and queue reset. Raw configuration remains attached to every result.

<!-- RESULTS:START -->

## Measured results

Generated from 105 raw scenario files. 105 passed every correctness check.
Measured binaries: `c6d3469` (15 runs), `e0d9785` (90 runs).

The no-op queue ceiling was **34,825 messages/s** with p99 delivery-to-acknowledgment latency of 6.22 ms.

RabbitMQ did not set the observed ceiling: the no-op path was 1.20x faster than the fastest SQL-backed run (confirmed-table-valued-parameter).

![Queue throughput](docs/charts/queue-throughput.svg)

![Queue p99 acknowledgment latency](docs/charts/queue-p99-latency.svg)

### Best observed queue configuration by strategy

| Strategy | Batcher | Workers x writers | Batch | Delay | Capacity | Prefetch | Distribution | Rows/s | p50 commit | p99 ack | Speedup | Correct |
|---|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---|
| Individual insert per message | None | 2 x 4 | 1 | 5 ms | 1,000 | 250 | Uniform | 3,004 | 7.74 ms | 20.90 ms | 1.00x | yes |
| Table-valued parameter | Channel | 1 x 2 | 5,000 | 5 ms | 10,000 | 20,000 | Uniform | 29,082 | 167.23 ms | 417.83 ms | 9.68x | yes |
| Multiple INSERT statements | Channel | 4 x 2 | 50 | 5 ms | 2,000 | 200 | Uniform | 15,132 | 94.89 ms | 158.06 ms | 5.04x | yes |
| Multi-row VALUES | Channel | 2 x 2 | 100 | 5 ms | 2,000 | 400 | Uniform | 13,505 | 79.11 ms | 249.30 ms | 4.50x | yes |
| SqlBulkCopy | Channel | 1 x 2 | 1,000 | 5 ms | 8,000 | 2,000 | Uniform | 23,051 | 118.56 ms | 412.96 ms | 7.67x | yes |

### Long-run finalist confirmation

| Strategy | Repetitions | Rows/run | Configuration | Median rows/s | Range | Median p99 ack | Median duration |
|---|---:|---:|---|---:|---:|---:|---:|
| Individual insert per message | 3 | 750,000 | 2 workers x 4 writers, batch 1 | 1,934 | 1,842–2,053 | 165.98 ms | 387.76 s |
| Table-valued parameter | 3 | 750,000 | 1 worker x 2 writers, batch 5,000 | 23,957 | 20,739–29,082 | 417.83 ms | 31.31 s |
| Multiple INSERT statements | 3 | 750,000 | 4 workers x 2 writers, batch 50 | 13,928 | 8,474–15,132 | 265.98 ms | 53.85 s |
| Multi-row VALUES | 3 | 750,000 | 2 workers x 2 writers, batch 100 | 8,809 | 8,749–8,964 | 1,246.02 ms | 85.14 s |
| SqlBulkCopy | 3 | 750,000 | 1 worker x 2 writers, batch 1,000 | 20,543 | 20,081–21,789 | 339.33 ms | 36.51 s |

### Confirmation resource use

Each row is the repetition nearest that finalist's median throughput. Worker peak RSS is the sum of per-process peaks; the SQL values are DMV deltas over the run. Full before/after metrics, waits, GC counts, and one-second container samples remain in the raw artifacts.

| Strategy | App CPU | Worker peak RSS | Allocated | SQL CPU | SQL writes | SQL write stall | WRITELOG wait | Rabbit memory |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Individual insert per message | 914.0 s | 377 MiB | 19.13 GiB | 908.5 s | 2,806 MiB | 198,650 ms | 838,325 ms | 255 MiB |
| Table-valued parameter | 104.6 s | 202 MiB | 2.02 GiB | 20.9 s | 647 MiB | 6,961 ms | 424 ms | 240 MiB |
| Multiple INSERT statements | 143.9 s | 590 MiB | 8.82 GiB | 128.8 s | 832 MiB | 7,762 ms | 30,161 ms | 246 MiB |
| Multi-row VALUES | 121.3 s | 330 MiB | 7.27 GiB | 123.7 s | 967 MiB | 34,434 ms | 9,074 ms | 240 MiB |
| SqlBulkCopy | 167.8 s | 199 MiB | 2.07 GiB | 32.4 s | 887 MiB | 3,856 ms | 546 ms | 235 MiB |

### Direct-to-database controls

| Strategy | Writers | Batch | Direct rows/s | Comparable queue rows/s | Queue/direct | p50 commit delta | SQL p50 | Transaction p50 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Individual insert per message | 4 | 1 | 477 | 1,057 | 221.6 % | 1.49 ms | 0.68 ms | 7.52 ms |
| Table-valued parameter | 2 | 500 | 19,708 | 11,109 | 56.4 % | 73.79 ms | 38.62 ms | 43.69 ms |
| Multiple INSERT statements | 2 | 50 | 7,650 | 7,810 | 102.1 % | 33.79 ms | 6.16 ms | 11.99 ms |
| Multi-row VALUES | 2 | 100 | 12,929 | 10,097 | 78.1 % | 52.34 ms | 8.09 ms | 14.12 ms |
| SqlBulkCopy | 2 | 1,000 | 28,133 | 21,789 | 77.5 % | 79.75 ms | 47.00 ms | 53.24 ms |

These direct controls are single 10,000-row runs, so the ratios estimate pipeline overhead rather than isolate it. A ratio above 100% reflects observed run-order and concurrency variance; it is not negative RabbitMQ overhead.

### Worker scaling

| Strategy | Workers | Best rows/s | p99 ack | Delivered share range | Mean batch size |
|---|---:|---:|---:|---:|---:|
| Individual insert per message | 1 | 1,757 | 18.89 ms | 100.0–100.0% | 1.0 |
| Individual insert per message | 2 | 3,004 | 20.90 ms | 49.3–50.7% | 1.0 |
| Individual insert per message | 4 | 2,565 | 269.36 ms | 24.8–25.2% | 1.0 |
| Table-valued parameter | 1 | 11,910 | 275.14 ms | 100.0–100.0% | 476.2 |
| Table-valued parameter | 2 | 1,864 | 3,079.30 ms | 44.2–55.8% | 435.6 |
| Table-valued parameter | 4 | 8,217 | 1,104.76 ms | 20.0–34.5% | 380.7 |
| Multiple INSERT statements | 1 | 748 | 785.65 ms | 100.0–100.0% | 50.0 |
| Multiple INSERT statements | 2 | 9,511 | 258.15 ms | 48.5–51.5% | 50.0 |
| Multiple INSERT statements | 4 | 10,308 | 321.68 ms | 24.0–27.0% | 49.8 |
| Multi-row VALUES | 1 | 8,362 | 223.18 ms | 100.0–100.0% | 100.0 |
| Multi-row VALUES | 2 | 13,505 | 249.30 ms | 50.0–50.0% | 100.0 |
| Multi-row VALUES | 4 | 11,744 | 431.38 ms | 23.4–28.6% | 96.2 |
| SqlBulkCopy | 1 | 3,343 | 1,563.07 ms | 100.0–100.0% | 833.3 |
| SqlBulkCopy | 2 | 14,942 | 447.92 ms | 46.2–53.8% | 769.3 |
| SqlBulkCopy | 4 | 15,813 | 565.23 ms | 14.5–36.0% | 479.2 |

### Throughput-latency Pareto frontier

| Strategy | Scenario | Rows/s | p99 ack | Batch | Writers | Workers |
|---|---|---:|---:|---:|---:|---:|
| Table-valued parameter | refine-tablevaluedparameter-batch-10 | 1,128 | 126.54 ms | 10 | 2 | 1 |
| Table-valued parameter | refine-tablevaluedparameter-batch-100 | 4,065 | 265.83 ms | 100 | 2 | 1 |
| Table-valued parameter | scaling-tablevaluedparameter-workers-1 | 11,910 | 275.14 ms | 500 | 2 | 1 |
| Table-valued parameter | broad-tablevaluedparameter-hotparent | 12,181 | 349.56 ms | 500 | 2 | 1 |
| Table-valued parameter | refine-tablevaluedparameter-batch-5000 | 19,911 | 387.38 ms | 5,000 | 2 | 1 |
| Table-valued parameter | confirmed-table-valued-parameter | 23,957 | 396.10 ms | 5,000 | 2 | 1 |
| Table-valued parameter | confirmed-table-valued-parameter | 29,082 | 417.83 ms | 5,000 | 2 | 1 |
| Individual insert per message | broad-individual-uniform | 1,784 | 14.08 ms | 1 | 4 | 1 |
| Individual insert per message | scaling-individual-workers-2 | 3,004 | 20.90 ms | 1 | 4 | 2 |
| SqlBulkCopy | refine-bulkcopy-batch-10 | 834 | 177.80 ms | 10 | 2 | 1 |
| SqlBulkCopy | refine-bulk-prefetch-500 | 10,547 | 210.45 ms | 500 | 2 | 1 |
| SqlBulkCopy | refine-bulk-capacity-500 | 11,074 | 250.89 ms | 500 | 2 | 1 |
| SqlBulkCopy | refine-bulk-capacity-8000 | 12,832 | 265.28 ms | 500 | 2 | 1 |
| SqlBulkCopy | confirmed-bulk-copy | 21,789 | 321.37 ms | 1,000 | 2 | 1 |
| SqlBulkCopy | finalist-bulkcopy | 23,051 | 412.96 ms | 1,000 | 2 | 1 |
| Multiple INSERT statements | finalist-multipleinsertstatements | 7,810 | 90.77 ms | 50 | 2 | 1 |
| Multiple INSERT statements | confirmed-multiple-statements | 15,132 | 158.06 ms | 50 | 2 | 4 |
| Multi-row VALUES | refine-multirowvalues-batch-10 | 2,212 | 92.79 ms | 10 | 2 | 1 |
| Multi-row VALUES | broad-multirowvalues-uniform | 8,412 | 214.97 ms | 100 | 2 | 1 |
| Multi-row VALUES | scaling-multirowvalues-workers-2 | 13,505 | 249.30 ms | 100 | 2 | 2 |

### Correctness

Every published run passed row-count, distinct-ID, missing-ID, foreign-key, queue-ready, queue-unacknowledged, acknowledgment, and worker-error checks.

### Raw data

- [broad-bulkcopy-hotparent, repetition 1](results/published/20260904-full-e0d9785/broad-bulkcopy-hotparent-r1.json)
- [broad-bulkcopy-moderateskew, repetition 1](results/published/20260904-full-e0d9785/broad-bulkcopy-moderateskew-r1.json)
- [broad-bulkcopy-uniform, repetition 1](results/published/20260904-full-e0d9785/broad-bulkcopy-uniform-r1.json)
- [broad-individual-hotparent, repetition 1](results/published/20260904-full-e0d9785/broad-individual-hotparent-r1.json)
- [broad-individual-moderateskew, repetition 1](results/published/20260904-full-e0d9785/broad-individual-moderateskew-r1.json)
- [broad-individual-uniform, repetition 1](results/published/20260904-full-e0d9785/broad-individual-uniform-r1.json)
- [broad-multipleinsertstatements-hotparent, repetition 1](results/published/20260904-full-e0d9785/broad-multipleinsertstatements-hotparent-r1.json)
- [broad-multipleinsertstatements-moderateskew, repetition 1](results/published/20260904-full-e0d9785/broad-multipleinsertstatements-moderateskew-r1.json)
- [broad-multipleinsertstatements-uniform, repetition 1](results/published/20260904-full-e0d9785/broad-multipleinsertstatements-uniform-r1.json)
- [broad-multirowvalues-hotparent, repetition 1](results/published/20260904-full-e0d9785/broad-multirowvalues-hotparent-r1.json)
- [broad-multirowvalues-moderateskew, repetition 1](results/published/20260904-full-e0d9785/broad-multirowvalues-moderateskew-r1.json)
- [broad-multirowvalues-uniform, repetition 1](results/published/20260904-full-e0d9785/broad-multirowvalues-uniform-r1.json)
- [broad-tablevaluedparameter-hotparent, repetition 1](results/published/20260904-full-e0d9785/broad-tablevaluedparameter-hotparent-r1.json)
- [broad-tablevaluedparameter-moderateskew, repetition 1](results/published/20260904-full-e0d9785/broad-tablevaluedparameter-moderateskew-r1.json)
- [broad-tablevaluedparameter-uniform, repetition 1](results/published/20260904-full-e0d9785/broad-tablevaluedparameter-uniform-r1.json)
- [confirmed-bulk-copy, repetition 1](results/published/20260904-confirmation-c6d3469/confirmed-bulk-copy-r1.json)
- [confirmed-bulk-copy, repetition 2](results/published/20260904-confirmation-c6d3469/confirmed-bulk-copy-r2.json)
- [confirmed-bulk-copy, repetition 3](results/published/20260904-confirmation-c6d3469/confirmed-bulk-copy-r3.json)
- [confirmed-individual, repetition 1](results/published/20260904-confirmation-c6d3469/confirmed-individual-r1.json)
- [confirmed-individual, repetition 2](results/published/20260904-confirmation-c6d3469/confirmed-individual-r2.json)
- [confirmed-individual, repetition 3](results/published/20260904-confirmation-c6d3469/confirmed-individual-r3.json)
- [confirmed-multi-row-values, repetition 3](results/published/20260904-confirmation-c6d3469/confirmed-multi-row-values-r3.json)
- [confirmed-multi-row-values, repetition 2](results/published/20260904-confirmation-c6d3469/confirmed-multi-row-values-r2.json)
- [confirmed-multi-row-values, repetition 1](results/published/20260904-confirmation-c6d3469/confirmed-multi-row-values-r1.json)
- [confirmed-multiple-statements, repetition 3](results/published/20260904-confirmation-c6d3469/confirmed-multiple-statements-r3.json)
- [confirmed-multiple-statements, repetition 1](results/published/20260904-confirmation-c6d3469/confirmed-multiple-statements-r1.json)
- [confirmed-multiple-statements, repetition 2](results/published/20260904-confirmation-c6d3469/confirmed-multiple-statements-r2.json)
- [confirmed-table-valued-parameter, repetition 1](results/published/20260904-confirmation-c6d3469/confirmed-table-valued-parameter-r1.json)
- [confirmed-table-valued-parameter, repetition 3](results/published/20260904-confirmation-c6d3469/confirmed-table-valued-parameter-r3.json)
- [confirmed-table-valued-parameter, repetition 2](results/published/20260904-confirmation-c6d3469/confirmed-table-valued-parameter-r2.json)
- [control-noop-queue, repetition 1](results/published/20260904-full-e0d9785/control-noop-queue-r1.json)
- [direct-bulkcopy, repetition 1](results/published/20260904-full-e0d9785/direct-bulkcopy-r1.json)
- [direct-individual, repetition 1](results/published/20260904-full-e0d9785/direct-individual-r1.json)
- [direct-multipleinsertstatements, repetition 1](results/published/20260904-full-e0d9785/direct-multipleinsertstatements-r1.json)
- [direct-multirowvalues, repetition 1](results/published/20260904-full-e0d9785/direct-multirowvalues-r1.json)
- [direct-tablevaluedparameter, repetition 1](results/published/20260904-full-e0d9785/direct-tablevaluedparameter-r1.json)
- [finalist-bulkcopy, repetition 3](results/published/20260904-full-e0d9785/finalist-bulkcopy-r3.json)
- [finalist-bulkcopy, repetition 1](results/published/20260904-full-e0d9785/finalist-bulkcopy-r1.json)
- [finalist-bulkcopy, repetition 2](results/published/20260904-full-e0d9785/finalist-bulkcopy-r2.json)
- [finalist-individual, repetition 3](results/published/20260904-full-e0d9785/finalist-individual-r3.json)
- [finalist-individual, repetition 1](results/published/20260904-full-e0d9785/finalist-individual-r1.json)
- [finalist-individual, repetition 2](results/published/20260904-full-e0d9785/finalist-individual-r2.json)
- [finalist-multipleinsertstatements, repetition 1](results/published/20260904-full-e0d9785/finalist-multipleinsertstatements-r1.json)
- [finalist-multipleinsertstatements, repetition 3](results/published/20260904-full-e0d9785/finalist-multipleinsertstatements-r3.json)
- [finalist-multipleinsertstatements, repetition 2](results/published/20260904-full-e0d9785/finalist-multipleinsertstatements-r2.json)
- [finalist-multirowvalues, repetition 3](results/published/20260904-full-e0d9785/finalist-multirowvalues-r3.json)
- [finalist-multirowvalues, repetition 2](results/published/20260904-full-e0d9785/finalist-multirowvalues-r2.json)
- [finalist-multirowvalues, repetition 1](results/published/20260904-full-e0d9785/finalist-multirowvalues-r1.json)
- [finalist-tablevaluedparameter, repetition 2](results/published/20260904-full-e0d9785/finalist-tablevaluedparameter-r2.json)
- [finalist-tablevaluedparameter, repetition 3](results/published/20260904-full-e0d9785/finalist-tablevaluedparameter-r3.json)
- [finalist-tablevaluedparameter, repetition 1](results/published/20260904-full-e0d9785/finalist-tablevaluedparameter-r1.json)
- [refine-bulk-capacity-2000, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-capacity-2000-r1.json)
- [refine-bulk-capacity-500, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-capacity-500-r1.json)
- [refine-bulk-capacity-8000, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-capacity-8000-r1.json)
- [refine-bulk-delay-1, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-delay-1-r1.json)
- [refine-bulk-delay-20, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-delay-20-r1.json)
- [refine-bulk-delay-5, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-delay-5-r1.json)
- [refine-bulk-delay-50, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-delay-50-r1.json)
- [refine-bulk-prefetch-1000, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-prefetch-1000-r1.json)
- [refine-bulk-prefetch-2000, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-prefetch-2000-r1.json)
- [refine-bulk-prefetch-4000, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-prefetch-4000-r1.json)
- [refine-bulk-prefetch-500, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-prefetch-500-r1.json)
- [refine-bulk-writers-1, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-writers-1-r1.json)
- [refine-bulk-writers-2, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-writers-2-r1.json)
- [refine-bulk-writers-4, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-writers-4-r1.json)
- [refine-bulk-writers-8, repetition 1](results/published/20260904-full-e0d9785/refine-bulk-writers-8-r1.json)
- [refine-bulkcopy-batch-10, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-batch-10-r1.json)
- [refine-bulkcopy-batch-100, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-batch-100-r1.json)
- [refine-bulkcopy-batch-1000, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-batch-1000-r1.json)
- [refine-bulkcopy-batch-50, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-batch-50-r1.json)
- [refine-bulkcopy-batch-500, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-batch-500-r1.json)
- [refine-bulkcopy-batch-5000, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-batch-5000-r1.json)
- [refine-bulkcopy-channel, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-channel-r1.json)
- [refine-bulkcopy-lockswap, repetition 1](results/published/20260904-full-e0d9785/refine-bulkcopy-lockswap-r1.json)
- [refine-multipleinsertstatements-batch-10, repetition 1](results/published/20260904-full-e0d9785/refine-multipleinsertstatements-batch-10-r1.json)
- [refine-multipleinsertstatements-batch-100, repetition 1](results/published/20260904-full-e0d9785/refine-multipleinsertstatements-batch-100-r1.json)
- [refine-multipleinsertstatements-batch-161, repetition 1](results/published/20260904-full-e0d9785/refine-multipleinsertstatements-batch-161-r1.json)
- [refine-multipleinsertstatements-batch-50, repetition 1](results/published/20260904-full-e0d9785/refine-multipleinsertstatements-batch-50-r1.json)
- [refine-multirowvalues-batch-10, repetition 1](results/published/20260904-full-e0d9785/refine-multirowvalues-batch-10-r1.json)
- [refine-multirowvalues-batch-100, repetition 1](results/published/20260904-full-e0d9785/refine-multirowvalues-batch-100-r1.json)
- [refine-multirowvalues-batch-161, repetition 1](results/published/20260904-full-e0d9785/refine-multirowvalues-batch-161-r1.json)
- [refine-multirowvalues-batch-50, repetition 1](results/published/20260904-full-e0d9785/refine-multirowvalues-batch-50-r1.json)
- [refine-tablevaluedparameter-batch-10, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-batch-10-r1.json)
- [refine-tablevaluedparameter-batch-100, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-batch-100-r1.json)
- [refine-tablevaluedparameter-batch-1000, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-batch-1000-r1.json)
- [refine-tablevaluedparameter-batch-50, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-batch-50-r1.json)
- [refine-tablevaluedparameter-batch-500, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-batch-500-r1.json)
- [refine-tablevaluedparameter-batch-5000, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-batch-5000-r1.json)
- [refine-tablevaluedparameter-channel, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-channel-r1.json)
- [refine-tablevaluedparameter-lockswap, repetition 1](results/published/20260904-full-e0d9785/refine-tablevaluedparameter-lockswap-r1.json)
- [scaling-bulkcopy-workers-1, repetition 1](results/published/20260904-full-e0d9785/scaling-bulkcopy-workers-1-r1.json)
- [scaling-bulkcopy-workers-2, repetition 1](results/published/20260904-full-e0d9785/scaling-bulkcopy-workers-2-r1.json)
- [scaling-bulkcopy-workers-4, repetition 1](results/published/20260904-full-e0d9785/scaling-bulkcopy-workers-4-r1.json)
- [scaling-individual-workers-1, repetition 1](results/published/20260904-full-e0d9785/scaling-individual-workers-1-r1.json)
- [scaling-individual-workers-2, repetition 1](results/published/20260904-full-e0d9785/scaling-individual-workers-2-r1.json)
- [scaling-individual-workers-4, repetition 1](results/published/20260904-full-e0d9785/scaling-individual-workers-4-r1.json)
- [scaling-multipleinsertstatements-workers-1, repetition 1](results/published/20260904-full-e0d9785/scaling-multipleinsertstatements-workers-1-r1.json)
- [scaling-multipleinsertstatements-workers-2, repetition 1](results/published/20260904-full-e0d9785/scaling-multipleinsertstatements-workers-2-r1.json)
- [scaling-multipleinsertstatements-workers-4, repetition 1](results/published/20260904-full-e0d9785/scaling-multipleinsertstatements-workers-4-r1.json)
- [scaling-multirowvalues-workers-1, repetition 1](results/published/20260904-full-e0d9785/scaling-multirowvalues-workers-1-r1.json)
- [scaling-multirowvalues-workers-2, repetition 1](results/published/20260904-full-e0d9785/scaling-multirowvalues-workers-2-r1.json)
- [scaling-multirowvalues-workers-4, repetition 1](results/published/20260904-full-e0d9785/scaling-multirowvalues-workers-4-r1.json)
- [scaling-tablevaluedparameter-workers-1, repetition 1](results/published/20260904-full-e0d9785/scaling-tablevaluedparameter-workers-1-r1.json)
- [scaling-tablevaluedparameter-workers-2, repetition 1](results/published/20260904-full-e0d9785/scaling-tablevaluedparameter-workers-2-r1.json)
- [scaling-tablevaluedparameter-workers-4, repetition 1](results/published/20260904-full-e0d9785/scaling-tablevaluedparameter-workers-4-r1.json)
<!-- RESULTS:END -->

## Recorded environment

The published runs used CachyOS Linux 7.2.2 on an x64 host with an Intel Core i9-12900K, 24 logical processors, and NVMe-backed Docker Desktop storage. The benchmark constrains SQL Server to four CPUs and 4 GiB, RabbitMQ to two CPUs and 1 GiB, and keeps those limits fixed. Machine-readable environment snapshots are stored beside the raw results. Exact images are pinned by tag and multi-platform digest:

- SQL Server Developer 2025 CU8 on Ubuntu 22.04, `sha256:2f9da673779dc5556d385164f6b1541d169ff1eeed97b9833ca0308e8628e683`
- RabbitMQ 4.3.5 management, `sha256:ffd1b50c522ad20172ffd6716a2f41db375c7269560c8f3fb9a694e210ef0852`
- .NET SDK 10.0.400 and runtime 10.0.11
- Aspire 13.5.3
- `Microsoft.Data.SqlClient` 7.0.2 and `RabbitMQ.Client` 7.2.2

`Directory.Packages.props`, `global.json`, `compose.yaml`, and the worker Dockerfile are the version authorities.

## Reproduce

Prerequisites are Docker with Compose, .NET SDK 10.0.400 or a compatible patch selected by `global.json`, and PowerShell Core 7. The scripts create random credentials in process memory and do not write them to the repository.

Run the quick correctness and performance check:

```powershell
pwsh ./scripts/run-benchmarks.ps1 -Profile Smoke
```

Run the staged matrix:

```powershell
pwsh ./scripts/run-benchmarks.ps1 -Profile Full
```

Repeat the selected finalists with the longer confirmation workload:

```powershell
pwsh ./scripts/run-benchmarks.ps1 -Profile Confirmation
```

Keep containers for inspection:

```powershell
pwsh ./scripts/run-benchmarks.ps1 -Profile Smoke -LeaveRunning
```

Start the development graph and dashboard:

```text
aspire run
```

In another terminal, run the smoke profile against the Aspire-provided resources using the environment values shown for the controller resource in the dashboard:

```powershell
pwsh ./scripts/run-benchmarks.ps1 -Profile Smoke -UseRunningEnvironment
```

Run one selected full-profile scenario the same way:

```powershell
pwsh ./scripts/run-benchmarks.ps1 -Profile Full -Scenario refine-bulkcopy-batch-1000 -UseRunningEnvironment
```

See [setup and troubleshooting](docs/setup.md) for profile filters, overrides, Aspire worker replicas, Compose scaling, readiness failures, and cleanup behavior.

## Correctness suite

The unit suite checks maximum batch size and delay, bounded-channel backpressure, canceled producers, graceful drain, handler failure, per-item outcomes, handler concurrency, an empty stop, and the final partial batch. Integration tests exercise every SQL strategy at several logical workload sizes, verify all inserted IDs, test transaction rollback for every batched strategy, preserve an earlier committed individual insert when a later message fails, and prove that a failed transactional batch is requeued without acknowledgments.

CI builds with nullable references and recommended analyzers enabled, runs unit tests, starts the pinned dependencies, and runs integration correctness tests. Primary performance data comes only from explicit local benchmark runs, never CI timing.

## Limitations

- Results describe one local containerized machine and are sensitive to storage, CPU scheduling, thermal state, Docker virtualization, SQL Server cache state, and other activity.
- Application metrics include process CPU, peak working set, allocations, garbage collections, batch behavior, SQL timing, and latency. SQL Server DMV snapshots and RabbitMQ management snapshots are before/after values, so they are less precise than dedicated host telemetry.
- `SqlBulkCopy` streams each logical batch through an `IDataReader`; the process batcher still owns the batch's message objects until commit.
- Direct database controls use the same adapters and transactions but do not reproduce every scheduling cost in the worker process.
- Published per-run JSON retains percentile and distribution summaries rather than every per-message latency sample, keeping the repository practical to clone. Live worker samples are reduced only after those summaries are calculated.
- The checked-in matrix is bounded. It cannot establish a global optimum beyond its listed values.
