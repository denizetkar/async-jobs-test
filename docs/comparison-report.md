# Async Job Workflow Engine Comparison Report

## Executive Summary

This report compares workflow engines for a Python FastAPI + Postgres 17 +
SQLAlchemy 2 stack with two hard requirements:

1. **Transactional task scheduling** — task dispatch participates in the same
   SQL transaction as application DB writes (if the tx rolls back, no task is
   enqueued).
2. **Runtime task DAG** — tasks can be added and connected at runtime. Each
   connected component of the task DAG is a workflow (by reachability). The
   engine must support forming dependency edges between tasks whose IDs and
   existence are not known at code-time.

### The Core Tension

These two requirements are in tension. Only **procrastinate** and **DBOS** can
do transactional enqueue (they insert the job row on the caller's SQLAlchemy
connection). Every other engine dispatches via a separate RPC/broker to its
own datastore, making in-transaction enqueue architecturally impossible
without the Transactional Outbox pattern.

Meanwhile, procrastinate has **no native dependency edges** (flat queue only),
and DBOS is the only engine that satisfies both requirements simultaneously.

### The Scenario

A user uploads input files (excel, csv, etc.) which are processed asynchronously
(slow work). Each processing task gets a persistent ID. The user then references
those task IDs to start simulations with parameters. The engine must:

1. Accept the dependency edge `simulation → processed dataset` at runtime
2. Block the simulation until the dataset processing completes
3. Handle multiple uploads and multiple simulations per dataset, each
   forming a connected component of the task DAG

---

## Decision Matrix

| Engine | Transactional Scheduling | Runtime Task DAG | Postgres-Native | Infra Complexity | Python/FastAPI | License | Maturity |
|---|---|---|---|---|---|---|---|
| **procrastinate** 3.9 | YES — `configure(connection=conn).defer()` | NO — polling | YES (only PG) | Low (PG only) | Excellent (async-first) | MIT | Production-ready |
| **DBOS** 2.28 | YES — `enqueue_in_transaction(session, ...)` | YES — `set_event`/`recv` | YES (PG only) | Low (PG only) | Excellent (FastAPI-native) | MIT | Production-ready |
| **Temporal** 1.31 | NO — RPC to Temporal Server | YES — signals, retry-bounded activities | NO (separate DB) | High (Server + DB + UI) | Good (mature SDK) | Apache-2.0 | Production-ready |
| **Prefect** 3.8 | NO — Prefect Server API | YES — `.map()` + retry task | NO (separate DB) | Medium-High (Server + DB) | Good (async, `serve()`) | Apache-2.0 | Production-ready |
| **Outbox + CDC** | YES — outbox row in-tx → Debezium → Kafka | NO — polling | YES (outbox in app DB) | High (Kafka + Debezium + Connect) | N/A (architectural pattern) | OSS | Production-ready |
| Celery 5.x | NO — broker publish | NO — static canvas only | NO (Redis/RabbitMQ) | Medium (broker + result backend) | Good (mature, sync-first) | BSD-3 | Production-ready |
| Dagster 1.x | NO — daemon into own DB | Partial — dynamic fan-out only, no cross-flow deps | NO (separate DB) | High (daemon + webserver + DB) | Poor (data-pipeline oriented) | Apache-2.0 | Production-ready |
| Dramatiq | NO — broker publish | NO — no primitive | NO (Redis/RabbitMQ) | Medium (broker) | Good (sync/async) | LGPL-3.0 | Production-ready |
| RQ | NO — Redis only | NO — static `depends_on` only | NO (Redis only) | Low (Redis) | Fair (sync-focused) | BSD-3 | Production-ready |
| Huey | NO — broker API | NO — no primitive | Partial (PG as broker option) | Low (Redis or PG) | Good (FastAPI recipe) | MIT | Production-ready |
| Conductor | NO — HTTP/gRPC to server | YES — `FORK_JOIN_DYNAMIC` | NO (separate server) | High (JVM server + persistence) | Poor (client SDK only) | Apache-2.0 | Production-ready |
| Windmill | NO — separate server | YES — flows with branches/loops | NO (separate PG) | High (Docker self-host) | Fair (polyglot) | Apache-2.0 | Production-ready |

---

## Implemented Engines (5 branches)

### 1. procrastinate (main branch) — Baseline

**Transactional scheduling: YES.** The `configure(connection=conn).defer()`
method inserts the job row on the caller's SQLAlchemy connection, within the
caller's transaction. If the transaction rolls back, the job is never enqueued.

```python
# Transactional defer — same tx as the Dataset insert
conn = session.connection().connection
process_dataset.configure(connection=conn).defer(
    dataset_id=str(dataset_id),
    filename=filename,
)
session.commit()
```

**Runtime Task DAG: NO — polling workaround.** Procrastinate has no native
dependency edges. The `preprocess` task polls the dataset status from within
the task body: if the dataset is not yet `ready`, it sleeps 1s and retries
(up to 60 attempts). If the dataset never becomes ready, the simulation is
marked as `failed`. The frontend starts simulations immediately after
uploading — no API-level waiting. The DAG edge `simulation → dataset` exists
only in the application code's polling logic, not in the engine's task model.

**Infrastructure:** Postgres only (no extra services). Worker runs as
`procrastinate worker -a simapp.tasks.app`.

**Key test:** `tests/test_transactional.py` — proves that rollback cancels
the deferred job. `tests/test_simulations.py` — demonstrates immediate
simulation start with engine-level dataset waiting.

**Pros:** Simplest infra. Transactional enqueue is the gold standard.
SQLAlchemy connector shares the app's connection pool. MIT licensed.

**Cons:** No DAG primitives. No workflow visualization. No automatic retry
of failed tasks (must be handled manually). No durability/recovery of
multi-step workflows. Inter-workflow dependency requires polling the DB from
within the task body (hacky).

---

### 2. DBOS Transact (feat/dbos branch)

**Transactional scheduling: YES.** The `DBOSClient.enqueue_in_transaction(session, ...)`
method inserts the workflow registration row on the caller's SQLAlchemy
connection, within the caller's transaction.

```python
# Transactional enqueue — same tx as the Dataset insert
client.enqueue_in_transaction(
    session,
    options,
    str(dataset_id),
    filename,
)
session.commit()
```

**Runtime Task DAG: YES — native `set_event`/`recv`.** DBOS has no DAG
concept at all — workflows are plain Python functions. The simulation
workflow calls `preprocess_step`, fans out N `simulate_chunk` steps via
`Queue.enqueue()`, then collects results and calls `postprocess_step`. N is
determined at runtime. The "graph" IS the call stack, shaped by runtime
values.

The inter-workflow dependency `simulation → dataset` is expressed natively:
`process_dataset_wf` calls `DBOS.set_event()` to signal completion.
`simulation_wf` calls `DBOS.recv()` to block (with timeout) until the
dataset is ready. The frontend starts simulations immediately — the engine
handles the waiting natively. Each connected component (upload + simulations)
is one workflow by reachability.

```python
@DBOS.workflow()
def process_dataset_wf(dataset_id: str, filename: str) -> str:
    _process_dataset_step(dataset_id)
    DBOS.set_event(f"dataset-ready-{dataset_id}", "ready")
    return "processed"

@DBOS.workflow()
def simulation_wf(simulation_id: str, dataset_id: str, chunks: int) -> str:
    # Block until dataset ready — engine-native dependency edge
    DBOS.recv(topic=f"dataset-ready-{dataset_id}", timeout_seconds=120)
    _preprocess_step(simulation_id)
    handles = [
        sim_queue.enqueue(_simulate_chunk_step, simulation_id, i)
        for i in range(chunks)  # N determined at runtime
    ]
    chunk_results = [h.get_result() for h in handles]  # fan-in
    _postprocess_step(simulation_id, chunk_results)
    return "completed"
```

**Infrastructure:** Postgres only (separate system database `simapp_dbos` on
the same instance). Worker runs as `python -m simapp.dbos_worker`.

**Key test:** `tests/test_transactional.py` — proves that rollback cancels
the enqueued workflow. `tests/test_simulations.py` — demonstrates immediate
simulation start with native event-based waiting.

**Pros:** Satisfies BOTH hard requirements. Plain Python workflows (no DSL).
Durable execution with automatic recovery from checkpoints. FastAPI-native.
MIT licensed. Postgres-only (no extra infra). Native inter-workflow
dependencies via `set_event`/`recv`.

**Cons:** Younger ecosystem than Temporal. System database is separate from
app database (though same Postgres instance). Sync-only `enqueue_in_transaction`
(async callers must bridge via `run_sync`). `recv` timeout is finite — a very
slow dataset processing could exceed it.

---

### 3. Temporal (feat/temporal branch)

**Transactional scheduling: NO.** `client.start_workflow()` is a gRPC call
to the Temporal Server, which has its own persistence database. There is no
API to insert a workflow-start record into the caller's SQL transaction.

```python
# Fire-after-commit: DB commits first, then workflow starts
session.commit()
self._loop.run_until_complete(
    self._client.start_workflow(
        SimulationWorkflow.run,
        args=[str(simulation_id), str(dataset_id), chunks],
        id=workflow_id,
        task_queue="simapp-task-queue",
    )
)
```

**Runtime Task DAG: YES — retry-bounded polling + signals.** Temporal workflows
are imperative code. The simulation workflow calls `preprocess_activity`,
fans out N `simulate_chunk` activities via `asyncio.gather`, then calls
`postprocess_activity`. N is determined at runtime. This is the closest
analogue to PyTorch's dynamic graph — there is no pre-declared DAG at all.

The inter-workflow dependency `simulation → dataset` is expressed as a retry
activity: `wait_dataset_activity` raises if the dataset isn't ready; Temporal's
built-in retry mechanism (with exponential backoff) re-runs the activity until
it succeeds. The frontend starts simulations immediately — Temporal makes the
activity poll until the dataset becomes ready.

```python
@workflow.defn
class SimulationWorkflow:
    @workflow.run
    async def run(self, simulation_id: str, dataset_id: str, chunks: int) -> str:
        # Wait for dataset — Temporal retry makes it poll with backoff
        await workflow.execute_activity(
            wait_dataset_activity, args=[dataset_id],
            retry_policy=RetryPolicy(maximum_attempts=100),
        )
        await workflow.execute_activity(preprocess_activity, ...)
        chunk_results = await asyncio.gather(*[
            workflow.execute_activity(simulate_chunk_activity, args=[simulation_id, i], ...)
            for i in range(chunks)  # N determined at runtime
        ])
        return await workflow.execute_activity(postprocess_activity, ...)
```

**Infrastructure:** Temporal Server + its own Postgres + Temporal UI. Docker
images: `temporalio/server:1.31.2`, `temporalio/ui:2.52.1`.

**Key test:** `tests/test_consistency_gap.py` — demonstrates the
inconsistency window: DB commits but workflow start fails → orphaned row.
`tests/test_simulations.py` — demonstrates immediate simulation start with
engine-level dataset waiting.

**Pros:** Best dynamic DAG support. Mature, battle-tested. Excellent
durability and retry semantics. Rich UI for workflow visualization. Strong
typing in the Python SDK. Inter-workflow dependency via retry-bounded
activity polling.

**Cons:** CANNOT do transactional enqueue (architectural). Heavy infra
(Server + DB + UI). Learning curve for the workflow programming model.
Separate datastore from app DB. Inter-workflow dependency is a polling
workaround (not a native signal primitive like DBOS's `recv`).

**Gap test result:** When the Temporal server is down, the dataset row is
committed to Postgres but no workflow is started. The simulation stays in
`pending` status forever — an orphaned row that will never be processed.

---

### 4. Prefect (feat/prefect branch)

**Transactional scheduling: NO.** Flow runs are created via the Prefect
Server API (separate datastore). No hook to piggyback on the app's
SQLAlchemy transaction.

**Runtime Task DAG: YES — `.map()` + retry task.** Prefect 3's `.map()`
creates task runs at runtime. The simulation flow preprocesses, maps
`simulate_chunk_task` over `range(chunks)`, then aggregates with
`.collect()`. N is determined at runtime.

The inter-workflow dependency `simulation → dataset` is expressed as a retry
task: `wait_dataset_task` raises if the dataset isn't ready; Prefect's
built-in retry mechanism re-runs the task until it succeeds. The frontend
starts simulations immediately — the engine handles the waiting.

```python
@flow(name="simulation_flow")
def simulation_flow(simulation_id: str, dataset_id: str, chunks: int) -> str:
    wait_dataset_task(dataset_id)      # raises to retry until dataset ready
    preprocess_task(simulation_id, dataset_id)
    chunk_results = simulate_chunk_task.map(
        [simulation_id] * chunks,
        list(range(chunks)),  # N determined at runtime
    )
    wait(chunk_results)
    results_list = [r.result() for r in chunk_results]
    return postprocess_task(simulation_id, results_list)
```

**Infrastructure:** Prefect Server + its own Postgres. Docker image:
`prefecthq/prefect:3-python3.12`. Worker runs as `python -m simapp.prefect_worker`.

**Key test:** `tests/test_consistency_gap.py` — same gap as Temporal.
`tests/test_simulations.py` — demonstrates immediate simulation start with
engine-level dataset waiting.

**Pros:** Popular in the Python ecosystem. Good `.map()` dynamic fan-out.
Clean `@flow` / `@task` decorator API. Built-in UI at port 4200.
Inter-workflow dependency via retry task.

**Cons:** CANNOT do transactional enqueue. Medium-high infra (Server + DB).
Separate datastore from app DB. The `flow.serve()` model can be confusing
vs. traditional deployment-based scheduling. Inter-workflow dependency is a
polling workaround (not a native signal primitive).

**Gap test result:** Same as Temporal — when the Prefect server is down, the
DB row is committed but no flow is started.

---

### 5. Transactional Outbox + Debezium + Kafka (feat/outbox-cdc branch)

**Transactional scheduling: YES — architectural pattern.** Instead of
directly deferring tasks, the scheduler INSERTs an outbox event row in the
SAME transaction as the business data. Debezium captures the outbox row via
Postgres WAL → publishes to Kafka → a consumer reads from Kafka and
dispatches the work.

```python
# Outbox event written in the SAME tx as the Dataset insert
event = OutboxEvent(
    aggregate_type="dataset",
    aggregate_id=str(dataset_id),
    event_type="process_dataset",
    payload={"dataset_id": str(dataset_id), "filename": filename},
)
session.add(event)
session.commit()
# → Debezium captures the INSERT via WAL
# → Publishes to Kafka topic "simapp.dataset"
# → Consumer reads and calls process_dataset()
```

**Runtime Task DAG: NO — polling workaround.** Kafka has no native
concept of dependency edges between tasks. The simulation handler in the
consumer polls the dataset status from within the handler: if the dataset is
not yet `ready`, it sleeps 1s and retries (up to 60 attempts). The DAG
structure exists only in the consumer code's polling logic. Internal fan-out
(simulate_chunk × N) is handled in-process by the consumer.

**Infrastructure:** Postgres 17 (with `wal_level=logical`) + Zookeeper +
Kafka + Debezium Connect. Docker images: `quay.io/debezium/{zookeeper,kafka,connect}:3.6`.

**Key test:** `tests/test_transactional.py` — proves that rollback cancels
both the business row and the outbox event. `tests/test_simulations.py` —
demonstrates immediate simulation start with consumer-side dataset waiting.

**Pros:** Universal bridge — makes ANY engine transactional. Decouples the
app from the task engine. CDC is reliable (WAL-based, no polling). Industry
standard pattern. Can be combined with Temporal/Prefect/Celery on the
consumer side.

**Cons:** Heavy infra (Kafka + Debezium + Zookeeper + Connect). CDC adds
~100ms-1s latency. Consumers must be idempotent (use outbox event ID for
dedup). Operational complexity of Kafka cluster. Requires `wal_level=logical`
on Postgres. Inter-workflow dependency requires polling (same as
procrastinate).

---

## Documented-Only Engines (not implemented)

### Celery

**Transactional: NO.** `apply_async()` publishes to Redis/RabbitMQ. The
official workaround is `transaction.on_commit()` which fires AFTER commit —
not atomic. If the process dies between commit and `on_commit`, the task is
lost.

**DAG: Partial.** `chain`, `group`, `chord` build a static DAG at
composition time. Dynamic fan-out only if you construct the canvas inside a
task body from prior results.

**Why not implemented:** Same transactional limitation as Temporal/Prefect,
with less compelling DAG support.

### Dagster

**Transactional: NO.** Orchestrated by the Dagster daemon into its own PG.

**DAG: YES.** `DynamicOut` / `.map()` / `.collect()` for runtime fan-out.

**Why not implemented:** Dagster is a data orchestrator, not designed for
request-path FastAPI workloads. Overkill for the simulation scenario.

### Dramatiq

**Transactional: NO.** Broker-based (Redis/RabbitMQ).

**DAG: Partial.** Pipelines (`msg1 | msg2`) chain tasks; no native chord/group.

**Why not implemented:** Same transactional limitation, weaker DAG than
Celery.

### RQ (Redis Queue)

**Transactional: NO.** Redis-only.

**DAG: Partial.** `depends_on` for static deps; `Group` for tracking.

**Why not implemented:** Redis-only (no Postgres integration at all).

### Huey

**Transactional: NO.** Broker API call (even when broker is PG).

**DAG: YES.** `group` (fan-out) + `chord` (map/reduce). Dynamic fan-out recipe
documented.

**Why not implemented:** Same transactional limitation as Celery, despite
supporting PG as a broker.

### Conductor (Netflix/OSS)

**Transactional: NO.** HTTP/gRPC to separate JVM server.

**DAG: YES.** `FORK_JOIN_DYNAMIC` task type — fork count and task types
resolved at runtime.

**Why not implemented:** Java server, Python SDK is client-only (not
in-process). Heavy infra.

### Windmill

**Transactional: NO.** Separate Windmill server with own PG.

**DAG: YES.** Flows with dynamic branches, loops, iterators.

**Why not implemented:** Polyglot platform, Python is a client not an
in-process library. Heavy infra.

---

## Recommendations by Use Case

### If transactional scheduling is non-negotiable (your case)

1. **DBOS Transact** — the only engine that satisfies both requirements.
   `set_event`/`recv` is native and elegant. `enqueue_in_transaction` is
   transactional. Recommended for your simulation scenario.
2. **procrastinate** — if you only need a flat transactional queue and can
   tolerate polling workarounds for the runtime task DAG. Stays on your
   current architecture. Minimal infra (PG only).
3. **Outbox + CDC** — if you're already invested in Temporal/Prefect/Celery
   and can't switch. Write an outbox table in-tx, use Debezium to dispatch to
   your existing engine. More infra but no vendor lock-in.

### If you can relax the transactional requirement

4. **Temporal** — the strongest runtime task DAG + durability story. Best
   workflow visualization. Accept the fire-after-commit gap or bridge it with
   the outbox pattern.

### If you want minimal infra + real runtime task DAG

5. **DBOS** (PG only) — no Kafka, no Redis, no separate server. Just Postgres.
   It's the only engine that gives you transactional enqueue AND runtime task
   DAG (native `recv`) with minimal infrastructure.

---

## Architecture Diagram

See `docs/architecture.md` for a visual representation of the branch
structure and decision axes.
