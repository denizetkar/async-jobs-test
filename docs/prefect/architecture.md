# Prefect Architecture

Deep architectural analysis of the Prefect-based solution for the simulation
scenario. This document covers how Prefect works internally, what topology we
run, how it behaves under failure, and how to operate it on the open-source
Prefect Server plus a dedicated Prefect Postgres.

**Scope:** everything here uses Prefect 3 (`prefecthq/prefect:3-python3.12`)
self-hosted via `prefect server start`, with a separate Postgres instance for
the Prefect API database. No Prefect Cloud. See
[`../comparison-report.md`](../comparison-report.md) for the engine-selection
rationale.

The central narrative of this branch is a consistency gap that Prefect
cannot close by itself. Prefect creates flow runs through an API call to a
separate server with its own datastore, so the act of scheduling a workflow
is not a database write the caller can bind to their transaction. We commit
the application row first, then trigger the flow. If the trigger fails, the
row is committed and nothing will process it. This is the same shape as
Temporal, and the opposite of DBOS. The rest of the document explains the
mechanics, the topology, and the tradeoffs that follow from that one fact.

---

## 1. Internal Mechanics

### 1.1 Execution model

Prefect is a **server-orchestrated** engine. A Python library in your
application process defines flows and tasks with decorators, and a separate
long-running server process (the Prefect Server API) tracks the state of
every flow run and task run. The engine is split across three roles:

1. **Prefect Server API**: a stateless HTTP service (the `prefect-server`
   container) backed by its own Postgres. It is the system of record for
   flow-run and task-run state. It does not execute your code.
2. **Worker**: a long-lived process that calls `serve(deployments)` and
   polls the API for scheduled runs. When it claims a run, it executes the
   flow body in-process, reporting state transitions back to the API.
3. **Client library**: `@flow` and `@task` decorators that turn Python
   functions into tracked units, plus `run_deployment` for triggering runs.

The critical consequence: **workflow state lives in a separate datastore
from your application data.** The Prefect Postgres holds flow runs, task
runs, and deployment metadata; your application Postgres holds `datasets`
and `simulations`. There is no shared transaction between them. This is the
root cause of the consistency gap documented in §1.7 and §5.

### 1.2 The Prefect database

Prefect stores its internal tables in a dedicated `prefect` database, served
by a separate Postgres container (`prefect-postgres`). In our deployment
(`feat/prefect` branch) the topology has two Postgres instances:

- `simapp` (the `postgres` container), application data: `datasets`,
  `simulations`, and the SQLAlchemy models defined in `simapp.models`.
- `prefect` (the `prefect-postgres` container), Prefect's own tables:
  `flow_run`, `task_run`, `deployment`, `work_pool`, `worker`, and the
  state-transition history that powers the UI.

The connection URL is configured on the server container:

```yaml
prefect-server:
  image: prefecthq/prefect:3-python3.12
  command: prefect server start --host 0.0.0.0 --ui
  environment:
    PREFECT_API_DATABASE_CONNECTION_URL: postgresql+asyncpg://prefect:prefect@prefect-postgres:5432/prefect
```

Prefect supports SQLite for local development (ephemeral, in-process) and
Postgres with the `asyncpg` driver for production. There is no way to point
Prefect at your application database for its own tables without making
Prefect's schema coexist with your domain model, which Prefect does not
support as a first-class scenario. The two datastores are separate by
design.

Key Prefect tables (managed by the server, not by us):

| Table | Contents |
|---|---|
| `flow_run` | One row per flow invocation: ID, name, state, parameters, scheduled time, deployment ID |
| `task_run` | One row per task invocation: parent flow run, task name, state, parameters, map index |
| `deployment` | Registered deployments: flow name, version, schedule, work pool, entrypoint |
| `work_pool` / `worker` | Work pools and the workers registered against them, with last heartbeat |
| `log` | Structured logs emitted by flows and tasks, surfaced in the UI |

These are Prefect-managed tables. We never write to them directly. We
observe them through the Prefect UI at `:4200` or the API.

### 1.3 Workflows and tasks

Flows and tasks are Python functions decorated with `@flow` and `@task`:

```python
from prefect import flow, task, unmapped

@task(retries=3, retry_delay_seconds=5)
def wait_dataset_task(dataset_id: str) -> str:
    """Block until the dataset is ready. Polls in-task; raises after timeout."""
    SessionFactory = _get_session_factory()
    for _ in range(120):
        with SessionFactory() as session:
            dataset = session.get(Dataset, UUID(dataset_id))
            if dataset is not None and dataset.status == DatasetStatus.ready:
                return "ready"
        time.sleep(1)
    raise RuntimeError(f"Dataset {dataset_id} not ready after 120s")

@flow(name="simulation_flow")
def simulation_flow(simulation_id: str, dataset_id: str, chunks: int) -> str:
    wait_dataset_task(dataset_id)
    preprocess_task(simulation_id, dataset_id)

    chunk_indices = list(range(chunks))
    chunk_results = simulate_chunk_task.map(
        simulation_id=unmapped(simulation_id),
        chunk_index=chunk_indices,
    )
    results_list = chunk_results.result()

    return postprocess_task(simulation_id, results_list)
```

Two hard rules come from the execution model:

1. **Task functions must be importable by the worker.** When a worker claims
   a flow run, it imports the flow function from your code (by entrypoint)
   and re-executes the flow body. Tasks called inside the flow are tracked
   as task runs. This is why we avoid module-level `SessionLocal` (§1.8):
   the closure must be picklable for transport to the worker.

2. **Tasks should be idempotent.** Prefect retries tasks at the task level,
   not the flow level. A retried task runs from the start. Our tasks satisfy
   this: `process_dataset_task` does `UPDATE datasets SET status='ready'`
   (idempotent), `simulate_chunk_task` is pure sleep+return,
   `postprocess_task` overwrites the result row (full-row upsert).

### 1.4 Recovery: task retries, not workflow replay

Prefect does not promise exactly-once workflow completion. It promises
**at-least-once task execution with task-level retries**:

- If a task succeeds, its state is recorded in the Prefect DB and the task
  does not re-run within the same flow run.
- If a task fails (raises) and has `retries=N`, Prefect re-invokes the task
  up to N times after `retry_delay_seconds`. Only the failed task reruns;
  the rest of the flow's task-run history is preserved.
- If the worker process dies mid-flow, the flow run is left in a `Running`
  state with no worker claiming it. There is no automatic replay of the
  flow from the start. A new worker will not pick up an in-progress flow
  run unless you configure a work pool with a corresponding
  `infrastructure` process that re-executes the flow entrypoint.

The contrast with DBOS is sharp. DBOS replays the workflow function from
the top, skipping steps whose checkpoints exist. Prefect does not replay
flows. A flow run is a single forward pass; only individual failed tasks
retry. If the worker dies mid-flow, recovery is manual: you re-run the flow
from the UI or the CLI, or you accept that the flow run is dead and write
a reconciliation job for your application state.

### 1.5 Queues and work pools

Prefect's queue primitive is the **work pool**. A work pool is a named
target for flow runs. Workers register against a work pool and poll the API
for runs assigned to it.

In our branch, deployments created by `serve()` use the default process
work pool. The worker process polls the API:

```python
d1 = process_dataset_flow.to_deployment(name="process-dataset-deployment")
d2 = simulation_flow.to_deployment(name="simulation-deployment")
serve(d1, d2)
```

`serve(d1, d2)` is a long-lived blocking call. It registers the deployments
with the API and then polls for scheduled runs in a loop. When a run is
scheduled, the worker executes the flow body in-process. There is no push
from the API to the worker. Workers poll on a short interval.

Work pools come in three flavors:

- **Process** (default), the worker runs flows in its own Python process.
  Our branch uses this. The worker and the flow share a process and an
  import space.
- **Hybrid** (Docker, Kubernetes), the worker spins up a container or pod
  per flow run. Requires a worker process running somewhere with Docker or
  cluster credentials.
- **Push / managed**: Prefect Cloud runs the flow server-side, no worker
  process needed. Cloud only.

The polling model matters for latency. Because workers poll rather than
receive push, a flow scheduled by `run_deployment` will not start
instantly; it starts on the next poll cycle. For our workload (seconds to
minutes per simulation) this is negligible. For low-latency use cases,
Prefect recommends event-driven automations, which we do not use.

### 1.6 Inter-workflow dependency: in-task polling

The scenario requires the simulation flow to wait until the dataset
processing flow has finished marking the dataset `ready`. DBOS does this
with a durable `get_event` primitive. Prefect has no equivalent
cross-flow primitive. We implement the wait as an in-task polling loop:

```python
@task(retries=3, retry_delay_seconds=5)
def wait_dataset_task(dataset_id: str) -> str:
    """Block until the dataset is ready. Polls in-task; raises after timeout."""
    SessionFactory = _get_session_factory()
    for _ in range(120):
        with SessionFactory() as session:
            dataset = session.get(Dataset, UUID(dataset_id))
            if dataset is not None and dataset.status == DatasetStatus.ready:
                return "ready"
        time.sleep(1)
    raise RuntimeError(f"Dataset {dataset_id} not ready after 120s")
```

This is application-level polling against the application database, not an
engine-level wait. Consequences:

- The wait holds a worker slot for up to 120 seconds per simulation. With
  `concurrency` limits on the work pool, this can starve other runs.
- The task has `retries=3, retry_delay_seconds=5`. If the 120-second loop
  times out and raises `RuntimeError`, the task retries after 5 seconds
  and polls again. Total worst-case wait before the flow fails permanently:
  4 attempts of 120 seconds each, plus 15 seconds of retry delay, about 8.5
  minutes. The `retry_delay_seconds` can be a list for per-attempt delays;
  here it is a scalar so all three retries use 5 seconds.
- `retry_jitter_factor` can add randomness to retry delays to avoid
  thundering-herd polling, which we do not set.

This is a deliberate, visible workaround for an engine primitive Prefect
does not provide. The DBOS branch gets this for free with `get_event`.

### 1.7 Transactional enqueue: impossible

This is the headline limitation of the Prefect branch. Prefect creates flow
runs via the Prefect Server API. That is an HTTP call, not a SQL write
against the caller's transaction. There is no way to enlist the flow-run
creation in the caller's SQLAlchemy session.

The scheduler commits the application row first, then triggers the flow:

```python
class PrefectScheduler:
    def schedule_dataset_processing(
        self,
        session: Session,
        dataset_id: UUID,
        filename: str,
    ) -> None:
        dataset = session.get(Dataset, dataset_id)
        if dataset is not None:
            dataset.status = DatasetStatus.pending
        session.commit()

        run_deployment(
            name="process_dataset_flow/process-dataset-deployment",
            parameters={"dataset_id": str(dataset_id), "filename": filename},
            timeout=0,
        )
```

`run_deployment` with `timeout=0` is fire-and-forget. It returns immediately
after the API accepts the flow run. The caller does not wait for the flow
to execute.

The gap is the window between `session.commit()` and the return of
`run_deployment`. If `run_deployment` raises (server down, network error,
API 500), the application row is already committed. The HTTP handler sees
the exception and returns 500, but the dataset or simulation row exists in
the database with no corresponding flow run. It is an orphan, and nothing
will process it.

The test `tests/test_consistency_gap.py` proves this:

```python
with patch(
    "simapp.prefect_scheduler.run_deployment"
) as mock_flow:
    mock_flow.side_effect = RuntimeError("Prefect server down")

    with open(filepath, "rb") as f:
        response = gap_client.post(
            "/datasets", files={"file": ("test.csv", f, "text/csv")}
        )

    assert response.status_code == 500, (
        f"Expected 500 when Prefect is down, got {response.status_code}"
    )

with db_engine.connect() as conn:
    count = conn.execute(
        text("SELECT count(*) FROM datasets WHERE filename = 'test.csv'")
    ).scalar()
    assert count >= 1, "Expected orphaned dataset row (the gap) but found none"
```

The test patches `run_deployment` to raise, asserts the HTTP response is
500, and then asserts the dataset row exists in the database anyway. That
is the gap: the client sees a failure, but the database has a committed
row that nothing will process. There is no rollback because the commit
already happened.

The only workaround within Prefect's model is the **Transactional Outbox
pattern**: write an `outbox` row in the same transaction as the application
row, then have a separate process read the outbox and trigger the flow.
That pattern is implemented on the `feat/outbox-cdc` branch and is outside
the scope of this branch. Within Prefect alone, the gap is structural.

### 1.8 Per-task session factory

A subtle but important design decision in this branch is that tasks do not
use a module-level `SessionLocal`. Each task that touches the database
calls a local helper:

```python
def _get_session_factory():
    """Build a session factory on demand, avoids capturing module-level engine in closures."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from simapp.config import settings

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    return sessionmaker(bind=engine, expire_on_commit=False)
```

The reason is `cloudpickle`. When Prefect sends a task to a worker (which
it does for mapped tasks and for work-pool execution), the task function
and its closure are pickled. A module-level SQLAlchemy `Engine` or
`SessionLocal` carries a `thread.Lock` and a connection pool, neither of
which pickles cleanly. `cloudpickle` will fail or produce a broken
session factory on the worker side.

Building the engine inside the task, on demand, sidesteps the problem. The
task carries only its arguments (strings, UUIDs) and imports `settings` at
runtime on the worker. The cost is a new engine per task invocation, which
matters less for our workload (seconds per task) than it would for
high-throughput tasks. A `scoped_session` or a worker-level engine cache
would be the next step if throughput warranted it.

The test suite enforces this:

```python
def test_prefect_flows_no_module_level_session() -> None:
    """Regression: module-level SessionLocal causes cloudpickle RLock error."""
    import simapp.prefect_flows as mod

    assert "SessionLocal" not in mod.__dict__, "SessionLocal must not be at module level"
```

---

## 2. Our Topology

### 2.1 Services

```mermaid
graph LR
  subgraph Docker Host
    Client[HTTP client] -->|POST /datasets| Server
    Client -->|POST /simulations| Server
    Client -->|GET ...| Server

    subgraph Server[FastAPI server container]
      App[simapp.main:app]
      Sched[PrefectScheduler]
      App --> Sched
    end

    subgraph PrefectServer[prefect-server container]
      API["Prefect Server API<br/>prefect server start --ui<br/>:4200"]
    end

    subgraph Worker[worker container]
      Srv["serve(d1, d2)<br/>polls API for runs"]
    end

    subgraph PG[postgres container]
      AppDB[(simapp<br/>datasets, simulations)]
    end

    subgraph PGPrefect[prefect-postgres container]
      PrefDB[(prefect<br/>flow_run, task_run,<br/>deployment, work_pool)]
    end

    Sched -->|"run_deployment(timeout=0)<br/>fire-after-commit"| API
    Sched -->|CRUD commit first| AppDB
    App -->|CRUD| AppDB
    API -->|state writes| PrefDB
    Srv -->|poll + claim runs| API
    Srv -->|"UPDATE simulations/<br/>datasets (per task)"| AppDB
  end
```

Five containers: `postgres` (app DB), `prefect-postgres` (Prefect DB),
`prefect-server` (API + UI), `server` (FastAPI), `worker` (Prefect worker).
The two Postgres instances are separate by design. The application server
talks to both: it writes to the app DB within its handlers, and it triggers
flows via the Prefect API. The worker talks to both as well: it polls the
Prefect API for runs and writes to the app DB from inside task bodies.

### 2.2 Request lifecycle

**POST /datasets:**

1. FastAPI handler opens a SQLAlchemy session against the app DB.
2. `INSERT INTO datasets (status='pending')`.
3. `PrefectScheduler.schedule_dataset_processing(session, ...)`, commits
   the dataset row, then calls `run_deployment` against the Prefect API.
4. If `run_deployment` succeeds, return 201 with `{id, status: "pending"}`.
5. If `run_deployment` fails, return 500. **The dataset row is already
   committed.** This is the gap.
6. Worker polls the API, claims the `process_dataset_flow` run, executes
   `process_dataset_task` in-process: sleep 2s, `UPDATE datasets SET
   status='ready'`.

**POST /simulations:**

1. FastAPI inserts the simulation row (status `pending`) and commits.
2. Scheduler calls `run_deployment` for `simulation_flow`, fire-and-forget.
3. Worker claims the run and executes `simulation_flow`:
   - `wait_dataset_task`, polls the app DB for up to 120 s waiting for the
     dataset to be `ready`. Retries 3 times if it times out.
   - `preprocess_task`, `UPDATE simulations SET status='running'`.
   - `simulate_chunk_task.map(...)`, dynamic fan-out, one task run per
     chunk index. Returns futures.
   - `chunk_results.result()`, fan-in. Blocks until all mapped task runs
     finish and returns their results as a list.
   - `postprocess_task`, aggregate, `UPDATE simulations SET
     status='completed', result=...`.

### 2.3 Why two datastores

The two-datastore design is forced by Prefect's architecture. The Prefect
Server API needs its own database to track flow runs, task runs, and
deployments. It does not know about your application tables and cannot
share a transaction with them. The application needs its own database for
domain data.

Two consequences follow:

1. **No transactional enqueue.** You cannot insert a flow run in the same
   transaction as an application row. The two databases are separate, and
   the flow run is created via an API call, not a SQL insert. This is the
   gap (§1.7).
2. **Two backup and operations surfaces.** You back up the app DB and the
   Prefect DB independently. You observe application state via SQL and
   workflow state via the Prefect UI. There is no single source of truth
   that spans both.

The `serve(d1, d2)` pattern packs both deployments into one worker
process. This is the simplest worker topology: one process, two flows, one
poll loop. Scaling out means running more worker processes (§3). The
deployments are created with `to_deployment` and only registered with the
API when `serve()` is called, so the worker is both the registration point
and the execution point. If the worker is down, deployments are not
registered (or fall out of registration after a heartbeat timeout), and
`run_deployment` will fail with a "no deployment found" error. This makes
the worker a single point of failure for the scheduling path, mitigated by
running multiple workers.

---

## 3. Scaling Path

The architecture scales by running more workers, then by adopting work
pools, then by moving to Prefect Cloud:

| Load increase | Change | Infra delta |
|---|---|---|
| 1 worker (current) | `serve(d1, d2)` in one container | postgres, prefect-postgres, prefect-server, server, worker |
| Worker CPU-bound | Run more worker containers, each `serve(d1, d2)` | none, workers share the work pool |
| Heterogeneous runtimes | Move to a Docker or Kubernetes work pool; workers spawn per-flow containers | worker container replaced by a process worker plus Docker/K8s credentials |
| Beyond self-hosted | Prefect Cloud with a managed work pool | drop `prefect-server` and `prefect-postgres`; Cloud hosts the API |
| Postgres I/O-bound (Prefect DB) | Bigger `prefect-postgres` instance | none |
| App DB I/O-bound | Bigger `postgres` instance or read replicas | none to engine |

Throughput ceiling: a single Prefect Server handles thousands of flow runs
per second in principle; in practice the bottleneck is the worker process
and the app DB. The simulation workload (one flow run per HTTP request,
seconds per task) is far below the ceiling.

One real constraint: **workers poll, they do not receive push.** Scheduling
latency is bounded by the poll interval, which defaults to a few seconds.
For interactive-latency workloads, configure a shorter poll interval or
move to Prefect Cloud's event-driven automations. For our workload, poll
latency is acceptable.

A second constraint: **mapped tasks share the worker's process and
concurrency limits.** `simulate_chunk_task.map(...)` with `chunks=100`
creates 100 task runs on the same worker. The worker's concurrency setting
determines how many run in parallel; the rest queue in the API. Scaling
chunk count means scaling worker concurrency or worker count.

---

## 4. Operating

### 4.1 Recovery playbook

**Normal worker restart** (deploy, OOM, crash): Docker restarts the
container. The worker re-registers its deployments via `serve()` and
resumes polling. Flow runs that were `Scheduled` or `Pending` are claimed
by the new worker. **Flow runs that were `Running` when the worker died
stay `Running` in the API.** No other worker will pick them up, because a
`Running` run is considered owned. They are stuck until you cancel them
from the UI or the API. This is the operational cost of no workflow replay.

**Task failure with retries.** A task that raises and has `retries=N` is
retried by the same worker after `retry_delay_seconds`. The flow body is
not re-executed; only the failed task reruns. State from prior tasks in
the same flow run is preserved in the Prefect DB.

**Task failure after retries exhausted.** The flow run is marked `Failed`.
Application state is whatever the tasks left it in. For our simulation
flow, if `wait_dataset_task` exhausts its retries, the simulation is left
in `pending` (because `preprocess_task` never ran). For
`postprocess_task` failure, the simulation is left in `running`. There is
no automatic reconciliation. A janitor query against the app DB is needed
to find `running` simulations whose flow runs are `Failed`.

**Prefect Server restart.** The API is stateless; it reads and writes the
Prefect Postgres. Restarting the server container reconnects to the DB and
resumes. Workers reconnect and resume polling. Flow runs in flight are
unaffected because they execute in the worker, not the server.

### 4.2 Observability

The Prefect UI runs at `http://prefect-server:4200` (exposed on the host
at `:4200`). It shows flow runs, task runs, deployments, work pools, logs,
and state transitions. For ad-hoc queries, the Prefect API and the
`prefect` CLI are available.

Application state is observed by querying the app DB directly:

```sql
SELECT id, filename, status FROM datasets WHERE status = 'pending';
SELECT id, status, result FROM simulations WHERE status = 'running';
```

The two views do not automatically agree. A simulation can be `running` in
the app DB while its flow run is `Failed` in the Prefect DB. Reconciling
the two is an operations task, not an engine feature.

### 4.3 Versioning

Prefect deployments have a version, derived from the flow function's code.
When you change a flow's body and restart the worker, `serve()` registers a
new version of the deployment. In-flight runs continue with the version
they were scheduled with only if you use versioned deployments explicitly;
by default, new runs pick up the new code. There is no automatic
deterministic replay against a pinned version.

For our workload, this means a deploy that changes `simulate_chunk_task`
will affect runs scheduled after the deploy. Runs that were mid-execution
when the worker restarted are stuck (§4.1). There is no migration story
for in-flight runs.

### 4.4 Idempotency

| Operation | Idempotent? | Why |
|---|---|---|
| `process_dataset_task` | Yes | `UPDATE datasets SET status='ready'` is idempotent |
| `preprocess_task` | Yes | `UPDATE simulations SET status='running'` is idempotent |
| `simulate_chunk_task` | Yes | Pure sleep + return, no side effects |
| `postprocess_task` | Yes | Full-row write of `result` and `status='completed'` |
| `run_deployment` (dataset) | No | Each call creates a new flow run. A retried HTTP request creates a duplicate flow run and a duplicate `process_dataset_task` execution |
| `run_deployment` (simulation) | No | Same. Duplicate simulation runs will both `UPDATE simulations SET status`, racing |

There is no flow-run deduplication by a client-supplied key. DBOS gets this
with explicit `workflow_id`; Prefect has no equivalent in the open-source
server. Duplicate HTTP requests create duplicate flow runs. Mitigations
are application-side: an idempotency key table in the app DB, checked
before scheduling.

---

## 5. Failure Modes

| Failure | Behavior | User-visible effect |
|---|---|---|
| Server crash mid-request | Application row may be committed, flow trigger may not have fired | Client sees 500, but the row exists. Orphaned row. This is the gap. |
| Worker crash mid-task | Task run marked `Running` indefinitely; flow run stuck `Running` | Simulation stuck in `pending` or `running`. No automatic recovery. |
| Worker crash between tasks | Same: flow run stuck `Running`, no other worker claims it | Simulation stuck. Manual cancel and re-run from UI. |
| Prefect Server down at request time | `run_deployment` raises; row already committed | 500 to client, orphaned row in DB (the gap test) |
| Prefect Postgres restart | API reconnects; in-flight runs unaffected; brief API blip | Workers fail to poll briefly, retry |
| App Postgres restart | Tasks fail to get sessions; `wait_dataset_task` raises; retries kick in | Simulations take longer; may fail after retries |
| `wait_dataset_task` timeout after 120s | Task raises `RuntimeError`; retries 3 times after 5s each | Worst case ~8.5 min before flow marked `Failed`; simulation left `pending` |
| Duplicate HTTP request | Two flow runs created; both write to the same app rows | Race on `simulations.status`; last writer wins, no dedup |

The gap row is the central failure mode. The test in
`tests/test_consistency_gap.py` is the proof: a 500 response with a
committed dataset row. The fix is not in Prefect; it is in the application
architecture. The `feat/outbox-cdc` branch implements the Transactional
Outbox pattern to close this gap: the application row and an outbox row
are written in the same transaction, and a relay process reads the outbox
and calls `run_deployment`. The relay failing is recoverable because the
outbox row is durable.

The `wait_dataset_task` timeout is the second rough edge. After retries
exhaust, the flow is `Failed` and the simulation is left in `pending`. No
one will pick it up. Options:

1. Wrap the wait in a try/except inside the flow, write a `failed` status
   to the simulations table on timeout. Owned by us, deterministic.
2. A janitor task that periodically scans the Prefect API for `Failed`
   flow runs and reconciles `simulations.status`.
3. Surface the failure UX-side ("simulation timed out waiting for
   dataset").

This is flagged as known follow-up work, not yet implemented on the
branch.

---

## 6. Comparison With What We Rejected

The Prefect architecture sits in a different corner of the design space
from DBOS, Temporal, and procrastinate:

| Component | Temporal | Prefect | procrastinate | Outbox+CDC | **DBOS** |
|---|---|---|---|---|---|
| Orchestrator server | ✅ required | ✅ required | n/a |, | **library** |
| Separate engine datastore | Cassandra | own Postgres | app Postgres | app Postgres | **app Postgres (shared)** |
| Message broker | internal | internal | app Postgres | app Postgres + CDC | **app Postgres** |
| UI/dashboard | temporal-ui | prefect-server (built-in) | n/a |, | **Conductor (optional)** |
| Transactional enqueue | no (API) | **no (API)** | yes (same tx) | yes (outbox row) | **yes (same tx)** |
| Workflow replay | yes | no (task retries only) | no | no | **yes** |
| Cross-flow wait primitive | Signal/Query | no (in-task poll) | no | no | **get_event** |
| Extra infra services | 4 | 2 | 0 | 0 to 1 | **0** |

Prefect shares Temporal's shape: an orchestrator server with its own
datastore, API-based scheduling, no transactional enqueue. It shares
procrastinate's ease of adoption (Python decorators, no separate broker)
but adds the server back. Compared to DBOS, it gives up transactional
enqueue and workflow replay in exchange for a built-in UI, a richer
deployment model, and a Cloud upgrade path.

The Outbox+CDC branch is not a different engine; it is a pattern layered
on top of an engine like Prefect to recover transactional enqueue. It is
the workaround you adopt if you must use Prefect and cannot tolerate the
gap.

---

## 7. Known Tradeoffs

Honest list, so we're not surprised later:

1. **No transactional enqueue.** The headline. Scheduling a flow is an
   API call to a separate server, not a SQL write. The application row and
   the flow run are not atomic. This produces orphaned rows under server
   failure. The gap test proves it. The Outbox pattern is the only fix
   within Prefect's model.

2. **Medium-high infrastructure footprint.** Two Postgres instances plus a
   Prefect Server plus a worker, versus DBOS's one Postgres. The Prefect
   Server is stateless and cheap to run, but it is a moving part that
   fails independently and a version to track.

3. **Separate datastore for engine state.** The Prefect DB and the app DB
   are separate. Observability spans two surfaces (UI and SQL). Backups
   are independent. There is no single source of truth. Reconciling the
   two is an operations task.

4. **No workflow replay.** A failed worker leaves flow runs stuck in
   `Running`. Recovery is manual (cancel and re-run from UI) or requires a
   custom reconciliation job. Only individual tasks retry.

5. **No cross-flow wait primitive.** We poll the app DB inside
   `wait_dataset_task`. This holds a worker slot for up to 120 s per
   simulation and requires an application-level timeout. DBOS's
   `get_event` is a durable, engine-level wait with no slot cost.

6. **`.map()` returns futures you must wait on.** `simulate_chunk_task.map(...)`
   returns a list of futures; `chunk_results.result()` blocks until all
   complete. If one mapped task fails, the fan-in raises and the flow
   fails (subject to the task's retries). There is no partial-result
   handling without writing it yourself.

7. **Workers poll, not push.** Scheduling latency is bounded by the poll
   interval. For our workload this is fine. For low-latency work, you need
   event-driven automations (Cloud) or a shorter poll interval.

8. **No flow-run deduplication by client key.** Duplicate HTTP requests
   create duplicate flow runs. Idempotency must be enforced application-side
   (idempotency key table).

9. **SQLite for dev only.** Local development uses SQLite; production
   requires Postgres with `asyncpg`. The two can behave differently under
   concurrency, so dev/prod parity is not free.

10. **`serve()` is both registration and execution.** If the worker is
    down, deployments are not registered. `run_deployment` will fail. The
    worker is a single point of failure for the scheduling path until you
    run more than one.

---

## 8. Decision

For this project, Prefect is a viable but not preferred choice. The
workload (simulation flows of minutes, single-digit workers, existing
Postgres) is well within Prefect's capacity, and the built-in UI and
deployment model are genuine operational wins over a library-only engine.
The Cloud upgrade path is real if we ever need managed execution.

The blocker is transactional scheduling. The consistency gap is not a bug;
it is the shape of an API-orchestrated engine. For a workload where a
committed dataset row with no flow run is acceptable, or where the
application can tolerate an idempotency-key layer, Prefect is fine. For a
workload where the application row and the workflow must commit together,
Prefect alone is insufficient, and the Outbox pattern is required.

The recommendation is:

- ⚠️ Do not adopt `feat/prefect` as the reference implementation if
  transactional scheduling is a hard requirement. Use `feat/dbos` or
  `feat/outbox-cdc` instead.
- ✅ If transactional scheduling is not a hard requirement, `feat/prefect`
  is acceptable. Adopt with the following caveats.
- ✅ Add the failed-`wait_dataset_task` reconciliation (§5 option 1)
  before treating this as production-ready.
- ✅ Add an idempotency key table in the app DB to deduplicate retried
  HTTP requests.
- ✅ Run at least two worker containers to avoid `serve()` being a single
  point of failure for the scheduling path.
- ⏸️ Re-evaluate Prefect Cloud if we want managed execution or
  event-driven automations for low-latency scheduling.
