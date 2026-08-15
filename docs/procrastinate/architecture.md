# Procrastinate Architecture

Deep architectural analysis of the procrastinate-based solution for the
simulation scenario. This document covers how procrastinate works
internally, what topology we run, how it behaves under failure, and how to
operate it on the open-source library alone.

**Scope:** everything here is `procrastinate` 3.x (MIT) + your own Postgres.
No SaaS, no broker, no separate orchestrator. See
[`../comparison-report.md`](../comparison-report.md) for the engine-selection
rationale.

---

## 1. Internal Mechanics

### 1.1 Execution model

Procrastinate implements **background jobs** as a library plus a CLI worker.
There is no orchestrator server, no message broker, no Raft cluster. The
engine is a Python package that:

1. Registers `@app.task` functions in-process at import time, keyed by
   their full dotted module path (`simapp.tasks.process_dataset`).
2. Defers each job by inserting a row into the `procrastinate_jobs` table
   in your existing Postgres.
3. Runs a separate worker process (CLI: `procrastinate -a simapp.tasks.app
   worker`) that polls the table, claims a row with `FOR UPDATE SKIP
   LOCKED`, executes the task function, and writes the final status back.
4. On crash, jobs left in `doing` state are not auto-recovered. They sit
   there until an operator manually calls `retry_job_by_id()` or schedules
   a sweep.

The critical consequence: **Postgres is the single source of truth.** Job
rows, queue state, locks, and event channels all live in your existing
Postgres database. If you can back up Postgres, you have backed up the
entire engine state. There is no separate broker to also back up and no
separate cluster to also keep alive.

### 1.2 The job table

Procrastinate stores its internal state in a single logical schema inside
the application database. In our deployment (`main` branch) the
application data and the procrastinate system tables share the same
`simapp` database on a single Postgres 17 container.

The schema is **not** managed by Alembic. Procrastinate ships its own DDL
and applies it out-of-band via `app.schema_manager`. Our
`scripts/post_migrate.py` runs after Alembic migrations and applies the
schema via the same `app.schema_manager.get_schema()` call the official
CLI uses. The two systems share a database but not a migration tool, which
matters for upgrade ordering (see §4.3).

Key system table:

| Table | Contents |
|---|---|
| `procrastinate_jobs` | One row per job: id, queue_name, task_name (full dotted path), job_args, status (`todo`/`doing`/`succeeded`/`failed`/`aborted`/`aborting`/`cancelled`), attempts, scheduled_at, locked_by, events |
| `procrastinate_events` | Per-job lifecycle events (deferred, started, succeeded, failed) for audit |
| `procrastinate_locks` | Queueing-lock rows backing `queueing_lock="..."`. A second defer with the same lock raises `AlreadyEnqueued` |

This is documented SQL. You can `SELECT * FROM procrastinate_jobs` for
live observability without any UI (see §4.2).

### 1.3 Workflows and tasks

Procrastinate has no workflow primitive, no DAG primitive, no fan-in
primitive. A "workflow" is just a chain of tasks where each task defers
the next from inside its own body. The module docstring on `main` says
this explicitly and we'll quote it verbatim:

```python
"""Procrastinate tasks and scheduler implementation.

This is the baseline (main branch). Key characteristics:
- Transactional enqueue: uses `task.configure(connection=conn).defer()` to insert
  the job row within the caller's SQLAlchemy transaction.
- DAG limitation: procrastinate has no DAG primitives. The simulation "DAG" is
  achieved via ad-hoc task chaining — each task defers the next task(s) from
  within its body. There is no engine-level dependency tracking, no wait-for-
  completion, and no fan-in primitive. The `simulate_chunk` tasks coordinate
  via a counter in the Simulation row to detect when all chunks are done, then
  the last one defers `postprocess`. This is intentionally awkward — it
  highlights why a real workflow engine is needed.
"""
```

The four tasks that implement the scenario:

```python
@app.task
def process_dataset(dataset_id: str, filename: str) -> None:
    time.sleep(2)
    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is None:
            return
        dataset.status = DatasetStatus.ready
        session.commit()


# Raising while the dataset is not ready schedules a 1s retry (up to
# max_attempts); after that the worker marks the job failed rather
# than silently swallowing it.
@app.task(retry=RetryStrategy(max_attempts=60, wait=1))
def preprocess(simulation_id: str, dataset_id: str) -> None:
    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is None or dataset.status != DatasetStatus.ready:
            raise RuntimeError(f"Dataset {dataset_id} not ready")

    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is None:
            return
        simulation.status = SimulationStatus.running
        session.commit()

    chunks = _get_chunks(simulation_id)
    for i in range(chunks):
        simulate_chunk.configure().defer(simulation_id=simulation_id, chunk_index=i)


@app.task
def simulate_chunk(simulation_id: str, chunk_index: int) -> None:
    time.sleep(1)
    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is None:
            return
        result = dict(simulation.result or {"chunks": []})
        result["chunks"] = list(result.get("chunks", [])) + [
            {"chunk_index": chunk_index, "value": chunk_index * 2}
        ]
        result["completed_count"] = result.get("completed_count", 0) + 1
        simulation.result = result
        session.commit()
        if result["completed_count"] >= _get_chunks(simulation_id):
            postprocess.configure().defer(simulation_id=simulation_id)


@app.task
def postprocess(simulation_id: str) -> None:
    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is None:
            return
        result = simulation.result or {}
        chunks = result.get("chunks", [])
        total = sum(c["value"] for c in chunks)
        result["total"] = total
        result["chunk_count"] = len(chunks)
        simulation.result = result
        simulation.status = SimulationStatus.completed
        session.commit()
```

Two properties fall out of this shape:

1. **There is no engine-level determinism contract.** A task function is
   plain Python; the worker simply calls it. Recovery does not replay, so
   non-deterministic code (wall-clock reads, `random`, partial DB writes)
   is not a correctness risk the way it is in DBOS/Temporal. The price is
   that recovery is also not automatic (§1.4).

2. **Tasks should be idempotent if they're retryable.** `process_dataset`
   does `UPDATE datasets SET status='ready'` (idempotent). The chunk
   fan-in is **not** idempotent: it appends to a JSON list and increments
   a counter. If a chunk were retried after partially committing, the
   counter would advance twice. Today the engine never retries
   `simulate_chunk` (no `RetryStrategy`), so this is latent. See §7.

### 1.4 Recovery: there is none

Procrastinate does not promise auto-recovery of in-flight jobs. If a
worker dies with a job in `doing` state, the row stays in `doing` with
its `locked_by` set to the dead worker. Nothing automatically re-queues
it.

What the engine provides instead is a **manual** recovery primitive:

```python
await app.job_manager.retry_job_by_id(job_id=...)
```

This resets a `failed` (or stuck `doing`) job back to `todo` so any live
worker can pick it up. The caller decides when and whether to call it.
There is no built-in sweeper that detects `doing` rows with a stale
`locked_by`, no heartbeat lease, no timeout. That is operator work.

In our single-worker deployment this is mostly fine. If the worker
container dies, Docker restarts it; new jobs flow as soon as it reconnects.
Stuck `doing` rows from the crash are surfaced by the §4.2 queries and
manually retried. It's a known sharp edge, not a hidden one.

### 1.5 Queues: SKIP LOCKED + LISTEN/NOTIFY

The `procrastinate_jobs` table is the queue. Workers consume it with
`SELECT ... FOR UPDATE SKIP LOCKED`, which lets multiple worker processes
share one table without double-claiming rows. Each claim sets
`locked_by` to a worker-provided id and `status='doing'`.

For low latency, procrastinate also uses Postgres `LISTEN`/`NOTIFY`: every
`defer()` notifies the channel for the job's queue, and a worker blocked
on `LISTEN` wakes immediately. There's a polling fallback
(`fetch_job_polling_interval`, default 5s; our test fixture sets 1.0s) in
case notifications are missed. The default queue in our app is
`"queueing"` (the built-in default), and all four tasks land there. We do
not partition by task type.

`SKIP LOCKED` is the core scaling primitive. Adding workers means adding
processes that compete for the same rows, and Postgres row-level locking
keeps them disjoint. There is no separate broker to scale and no
partition assignment to rebalance.

### 1.6 Inter-task dependency: a polling workaround

This is the section where procrastinate's lack of DAG primitives hurts
most. Our scenario requires: *start the simulation only after the dataset
is ready, with the engine itself enforcing the ordering.* DBOS does this
with `get_event`/`set_event` (durable blocking). Temporal and Prefect do
it with retry-until-ready. Procrastinate has no durable wait, so we use
the same retry-until-ready idea, expressed as a `RetryStrategy`:

```python
@app.task(retry=RetryStrategy(max_attempts=60, wait=1))
def preprocess(simulation_id: str, dataset_id: str) -> None:
    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is None or dataset.status != DatasetStatus.ready:
            raise RuntimeError(f"Dataset {dataset_id} not ready")
    ...
```

`RetryStrategy(max_attempts=60, wait=1)` means the worker will retry this
job up to 60 times, sleeping 1 second between attempts, before marking it
`failed`. Each retry is a fresh `defer()` of the same task. The engine
re-enqueues and the worker claims the new row on its next poll. So the
"wait for dataset ready" is a 60-second polling loop with a hard ceiling.

This works. It is also visibly ad-hoc: there's no first-class
"dependency" concept, no event the dataset task can fire to wake the
simulation task early, no guarantee that 60 seconds is enough. The
module docstring calls this out as "intentionally awkward," to make the
case for a real workflow engine.

Fan-in is the same story. `simulate_chunk` tasks do not return to a
parent. There is no parent. They coordinate through a counter on the
Simulation row:

```python
        result["completed_count"] = result.get("completed_count", 0) + 1
        simulation.result = result
        session.commit()
        if result["completed_count"] >= _get_chunks(simulation_id):
            postprocess.configure().defer(simulation_id=simulation_id)
```

The last chunk to commit sees `completed_count >= chunks` and defers
`postprocess`. The engine has no idea that these tasks are related; the
graph exists only in our code. This is fan-in via DB row counter, not
fan-in via engine primitive.

### 1.7 Transactional enqueue

The headline feature for our use case. The scheduler defers jobs on the
caller's DBAPI connection, inside the caller's transaction:

```python
class ProcrastinateScheduler:
    """Scheduler using procrastinate with transactional enqueue.

    `configure(connection=conn).defer()` inserts the job row on the caller's
    SQLAlchemy connection, within the caller's transaction. If the transaction
    rolls back, the job is never enqueued.
    """

    def schedule_dataset_processing(
        self,
        session: Session,
        dataset_id: UUID,
        filename: str,
    ) -> None:
        conn = session.connection().connection
        process_dataset.configure(
            connection=conn,
            queueing_lock=f"process_dataset:{dataset_id}",
        ).defer(
            dataset_id=str(dataset_id),
            filename=filename,
        )

    def schedule_simulation(
        self,
        session: Session,
        simulation_id: UUID,
        dataset_id: UUID,
        parameters: dict,
    ) -> None:
        conn = session.connection().connection
        preprocess.configure(connection=conn).defer(
            simulation_id=str(simulation_id),
            dataset_id=str(dataset_id),
        )
```

`task.configure(connection=conn).defer()` runs the `INSERT INTO
procrastinate_jobs` on the DBAPI connection you pass in, not on a
procrastinate-owned pool connection. That INSERT is part of the caller's
transaction. Commit and the job is visible to workers; rollback and the
job is gone. The `queueing_lock=f"process_dataset:{dataset_id}"` argument
also inserts a row into `procrastinate_locks` inside the same transaction,
so the dedup lock and the job row share a fate.

This is what the official docs call the **Transactional Outbox pattern**:
the application's writes and the "send a message" action are one atomic
step, with no `on_commit` callback gap (as with Celery/Django) and no
separate broker to lose messages in transit.

The proof that this works is `tests/test_transactional.py` (§1.8). Both
the dataset row and the job row live or die together.

### 1.8 Test: rollback cancels the deferred job

Verbatim from `tests/test_transactional.py`:

```python
def test_rollback_cancels_deferred_job():
    """If a transaction rolls back after deferring a task, no job row should exist."""
    engine = _fresh_engine_and_schema()

    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        conn.commit()

    dataset_id = uuid.uuid4()
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        dataset = Dataset(id=dataset_id, filename="rollback_test.csv", status=DatasetStatus.pending)
        session.add(dataset)
        session.flush()

        conn = session.connection().connection
        process_dataset.configure(connection=conn).defer(
            dataset_id=str(dataset_id),
            filename="rollback_test.csv",
        )

        result = session.execute(
            text("SELECT count(*) FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        assert result.scalar() >= 1, "Job should exist within the transaction"

        session.rollback()
    finally:
        session.close()

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT count(*) FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        assert result.scalar() == 0, "Expected 0 jobs after rollback"

    with SessionLocal() as session:
        assert session.get(Dataset, dataset_id) is None, "Dataset should not exist after rollback"

    engine.dispose()
```

The companion test (`test_commit_persists_deferred_job`) defers the same
task, commits, and asserts `count(*) == 1`. Together they prove the
atomicity contract both ways: rollback removes the job, commit keeps it.
The dataset row and the job row share a transaction, and there is no
window where one is visible and the other is not.

The test uses `process_dataset.full_path` as the `task_name` filter
because procrastinate stores job rows under the task's full dotted path
(`simapp.tasks.process_dataset`), not the bare name. This is also why our
test fixture copies task objects verbatim into `worker_app.tasks` rather
than relying on `import_paths` discovery (see §4.1).

### 1.9 Schema application

The procrastinate schema is not in Alembic. We apply it from
`scripts/post_migrate.py`, which runs after `alembic upgrade head` in both
dev and test paths:

```python
def apply_schema(engine=None) -> None:
    """Apply procrastinate schema to the target database.

    When engine is provided (conftest/test DB), execute the schema SQL
    directly via that engine's connection. When engine is None (verify.py
    dev DB), use the async app path.
    """
    from simapp.tasks import app

    schema_sql = app.schema_manager.get_schema()

    if engine is not None:
        with engine.connect() as conn:
            from sqlalchemy import text

            exists = conn.execute(
                text("SELECT 1 FROM pg_tables WHERE tablename = 'procrastinate_jobs'")
            ).fetchone()
            if exists is not None:
                return
            for statement in _split_statements(schema_sql):
                conn.execute(text(statement))
            conn.commit()
        return

    async def _run() -> None:
        await app.open_async()
        await app.schema_manager.apply_schema_async()

    asyncio.run(_run())
```

The sync path fetches the engine's `schema_manager.get_schema()` SQL (the
same DDL the `procrastinate schema --apply` CLI emits), splits it on
top-level semicolons while respecting `$$`-quoted bodies, and runs each
statement over a SQLAlchemy connection. The idempotency guard (`IF EXISTS
procrastinate_jobs`) keeps it safe to run repeatedly.

The async path (used by `verify.py` against the dev DB) just calls
`app.schema_manager.apply_schema_async()`. Both paths produce the same
schema; the sync path exists so the test fixture can apply schema over a
SQLAlchemy engine it already controls.

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
      Sched[ProcrastinateScheduler]
      App --> Sched
    end

    subgraph Worker[worker container]
      CLI["procrastinate -a simapp.tasks.app worker<br/>poll procrastinate_jobs<br/>LISTEN/NOTIFY"]
    end

    subgraph PG[postgres container]
      AppDB[(simapp<br/>datasets, simulations)]
      JobTable[(simapp<br/>procrastinate_jobs,<br/>procrastinate_events,<br/>procrastinate_locks)]
    end

    Sched -->|"INSERT INTO procrastinate_jobs<br/>(same SQLAlchemy tx)"| JobTable
    App -->|CRUD| AppDB
    CLI -->|"SELECT ... FOR UPDATE SKIP LOCKED"| JobTable
    CLI -->|"UPDATE simulations/<br/>datasets"| AppDB
    JobTable -.->|"NOTIFY queue channel"| CLI
  end
```

Two processes (`server`, `worker`) share one Postgres. The worker runs the
procrastinate CLI directly. There is no `worker.py` in our code. From
`docker-compose.yml`:

```yaml
  worker:
    build: .
    command: uv run procrastinate -a simapp.tasks.app worker
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      SIMAPP_DATABASE_URL: postgresql+psycopg://simapp:simapp@postgres:5432/simapp
    volumes:
      - ./src:/app/src
```

The `-a simapp.tasks.app` flag tells the CLI which `App` object to load.
The worker process imports `simapp.tasks`, finds `app`, opens its
connector, and starts the poll/notify loop. No separate scheduler, no
broker, no orchestrator container.

### 2.2 Request lifecycle

**POST /datasets:**

1. FastAPI handler opens a SQLAlchemy session.
2. `INSERT INTO datasets (status='pending')`.
3. `ProcrastinateScheduler.schedule_dataset_processing(session, ...)`
   inserts a row into `procrastinate_jobs` **and** a row into
   `procrastinate_locks`, both on the caller's connection, inside the
   same transaction.
4. Commit. Atomically: dataset row + job row + lock row.
5. Return 201 with `{id, status: "pending"}`.
6. Worker wakes (LISTEN/NOTIFY or 5s poll), claims the job with
   `FOR UPDATE SKIP LOCKED`, runs `process_dataset`:
   - `time.sleep(2)`.
   - `UPDATE datasets SET status='ready'`.

**POST /simulations:**

1. FastAPI inserts simulation row (status `pending`) + enqueues
   `preprocess`, same in-tx pattern. Note **no check that the dataset is
   ready**. This is the design choice that removed the 409 gate. The
   engine handles ordering.
2. Worker picks up `preprocess`:
   - Checks `dataset.status`. If not `ready`, raises `RuntimeError`.
     `RetryStrategy(max_attempts=60, wait=1)` re-defers the job; the
     worker sleeps 1s and retries up to 60 times.
   - When the dataset is `ready`: sets `simulation.status='running'`,
     reads `chunks` from `simulation.parameters`, and defers N
     `simulate_chunk` jobs.
3. Each `simulate_chunk`:
   - `time.sleep(1)`.
   - Appends its result to `simulation.result["chunks"]`, increments
     `result["completed_count"]`, commits.
   - If `completed_count >= chunks`, defers `postprocess`. (Fan-in: the
     last chunk to commit is responsible for chaining.)
4. `postprocess`:
   - Sums chunk values, sets `result["total"]` and `result["chunk_count"]`.
   - Sets `simulation.status='completed'`, commits.

### 2.3 Why no separate worker process

Honestly, we could run the worker in the server process. Procrastinate
supports `run_worker_async(concurrency=N)` in-process, which would let us
ship one container and reduce operational surface. We don't, for three
reasons:

- **Isolation.** A misbehaving task (long sleep, infinite loop) should
  not starve HTTP request handling. Separate processes give us a free
  backpressure valve: the worker can lag without affecting API latency.
- **Scaling independently.** If we ever need more workers, we can scale
  the worker service without touching the server (§3).
- **Crash containment.** A worker crash (segfault in a C dependency, OOM)
  does not take down the API. Docker restarts the worker container; the
  API keeps serving.

The trade-off is one extra container in the compose file. For our scale
that's cheap. The CLI invocation is one line in `docker-compose.yml`
and there is no `worker.py` to maintain.

---

## 3. Scaling Path

The architecture scales without redesign:

| Load increase | Change | Infra delta |
|---|---|---|
| 1 worker (current) | (baseline) | postgres + server + worker |
| Worker CPU-bound | `docker compose up -d --scale worker=N` | none. `SKIP LOCKED` handles distribution |
| Worker I/O-bound on Postgres | Increase `fetch_job_polling_interval`, partition by task type into separate queues | none |
| Postgres I/O-bound | Bigger instance, or read replicas for observability queries | single instance swap |
| Beyond 1 Postgres | Shard by tenant across multiple Postgres hosts, one `procrastinate_jobs` per shard | replication of compose stack |
| Multi-host workers | Same `SKIP LOCKED` semantics work across hosts; just point each worker at the same Postgres | none |

`SKIP LOCKED` is the lever. Adding workers means adding processes that
compete for the same `procrastinate_jobs` rows; Postgres row-level locks
guarantee no two workers claim the same job. There is no broker
partition assignment to rebalance and no coordinator to elect.

One real constraint from the official docs: *"Do not use concurrency if
you have synchronous blocking tasks."* Our tasks (`time.sleep`, sync
SQLAlchemy) are blocking, so `run_worker_async(concurrency=N)` in a
single process would not help us. We scale by adding **processes**
(`--scale worker=N`), not by raising `concurrency=N`. Each process runs
the default single-threaded loop and consumes one job at a time. That's
the correct shape for our workload.

Throughput ceiling: a single Postgres can handle thousands of
`INSERT`/`UPDATE`/`SKIP LOCKED` operations per second. Our workload (a
handful of jobs per HTTP request, sleeps of 1 to 2 seconds each) is
nowhere near the ceiling.

---

## 4. Operating

### 4.1 Recovery playbook

Procrastinate does not auto-recover in-flight jobs. If a worker dies with
a job in `doing` state, that row stays in `doing` with `locked_by` set to
the dead worker id. The engine will not pick it up again on its own.

**Normal worker restart** (deploy, OOM, container crash): Docker restarts
the container. The new worker process opens its connector and starts
polling. New jobs (`todo`) flow immediately. Any job that was `doing` when
the old worker died is still `doing`. It's now an orphan. The operator's
job is to detect those and retry them manually:

```python
await app.job_manager.retry_job_by_id(job_id=orphan_id)
```

This resets the row to `todo` and lets any live worker claim it. There is
no built-in sweeper that does this automatically. You write one or you
do it by hand.

**Worker is permanently lost** (host dies, scaled down): same story,
slightly worse. Any `doing` row whose `locked_by` no longer matches a
live worker is an orphan. Our §4.2 observability queries surface them.
A 30-line cron script that scans for `status='doing'` rows older than
`fetch_job_polling_interval × 3` and calls `retry_job_by_id` would close
the gap. We have not written it; on a single-worker deployment it's a
non-event, and on a multi-worker deployment it's the first thing to add.

**Test fixture note.** `tests/conftest.py` runs the worker in a daemon
thread against the test database. The fixture builds a **separate**
procrastinate `App` for the worker (so the test DB's worker doesn't
disturb the main app's lazily-created sync connector) and copies the task
objects verbatim into `worker_app.tasks`. The `task.blueprint` pointer is
deliberately left pointing at the original `app`, so chained `defer()`
calls inside a running task route through `app.job_manager` (opened by
the fixture). This is more involved than a DBOS test fixture because
procrastinate mixes sync and async connector lifecycles; see the long
docstring in `conftest.py` for the full reasoning.

### 4.2 Observability

There is no UI. All observability is SQL against `procrastinate_jobs`:

```sql
-- Currently running
SELECT id, task_name, queue_name, attempts,
       started_at
FROM procrastinate_jobs
WHERE status = 'doing'
ORDER BY started_at DESC;

-- Pending (waiting to be picked up)
SELECT id, task_name, scheduled_at
FROM procrastinate_jobs
WHERE status = 'todo'
ORDER BY scheduled_at;

-- Failures in the last hour
SELECT id, task_name, attempts,
       to_timestamp(scheduled_at / 1000000) AS scheduled
FROM procrastinate_jobs
WHERE status = 'failed'
  AND scheduled_at > extract(epoch from now() - interval '1 hour') * 1000000;

-- Orphaned doing rows (worker died mid-job)
SELECT id, task_name, locked_by, started_at
FROM procrastinate_jobs
WHERE status = 'doing'
  AND locked_by NOT IN (< live worker ids >);

-- Throughput per minute over last 10 min
SELECT date_trunc('minute', to_timestamp(scheduled_at / 1000000)) AS minute,
       count(*)
FROM procrastinate_jobs
WHERE scheduled_at > extract(epoch from now() - interval '10 minutes') * 1000000
GROUP BY 1 ORDER BY 1 DESC;
```

`procrastinate_events` gives a per-job audit trail (deferred → started →
succeeded/failed) for free, useful for post-mortems on a specific stuck
job. For aggregate metrics, a scheduled rollup view over
`procrastinate_jobs` is enough; the table is append-mostly and small for
our scale.

### 4.3 Versioning of task code

The `task_name` column stores the task's **full dotted path**:
`simapp.tasks.process_dataset`. Rename the function or move it to a
different module and existing `procrastinate_jobs` rows become
undeferable: the worker looks up `task_name` in `app.tasks` and finds
nothing. The job fails on the next attempt.

This is a stricter versioning constraint than DBOS's `application_version`
field. Practical implications:

- Never rename `@app.task` functions once they have in-flight jobs.
- Never move a `@app.task` between modules.
- If you must rename, write a shim: register the old name as a task that
  forwards to the new function, drain the queue, then remove the shim.

The full-path requirement is also why
`tests/test_transactional.py` uses `process_dataset.full_path` as the
filter, not the bare function name:

```python
TASK_NAME = process_dataset.full_path
```

Bare names match nothing in `procrastinate_jobs`. This is a documented
behavior of the library, not a bug, but it's an easy thing to get wrong
when writing queries by hand.

### 4.4 Idempotency table

| Task | Idempotent? | Why |
|---|---|---|
| `process_dataset` | Yes | `UPDATE datasets SET status='ready'`. Same result on retry |
| `preprocess` | Mostly | First section (raise-if-not-ready) is idempotent; second section (`status='running'`) is idempotent; the fan-out loop is **not**. A retry after partial fan-out will defer some chunks twice |
| `simulate_chunk` | **No** | Appends to a JSON list and increments a counter; a retry after partial commit double-counts |
| `postprocess` | Yes | Overwrites `simulations.result` wholesale |

The `simulate_chunk` non-idempotency is latent because the engine never
retries it today (no `RetryStrategy`). The day we add retry to
`simulate_chunk`, we need to also add a dedup mechanism (e.g., a
`processed_chunks` table keyed on `simulation_id + chunk_index` that the
task checks at start). Same for the `preprocess` fan-out loop.

The `queueing_lock` on `process_dataset` (`queueing_lock=f"process_dataset:{dataset_id}"`)
protects against double-enqueue from retried HTTP requests: a second
`POST /datasets` for the same id (unlikely but possible) raises
`AlreadyEnqueued` inside the caller's transaction, and the whole
request rolls back. We do not currently set a `queueing_lock` on
`preprocess` (§7).

---

## 5. Failure Modes

| Failure | Behavior | User-visible effect |
|---|---|---|
| Server crash mid-request | Transaction rolls back | Client sees connection error, retries safely (no partial writes, no orphan jobs) |
| Worker crash mid-task | Job stays in `doing`; engine does not retry it | Stuck simulation, needs manual `retry_job_by_id` or sweeper |
| Worker crash between tasks | Next-task defer was already committed; new worker picks up where it left off | No visible effect (assuming the dead job's row is retried) |
| Postgres restart | Worker reconnects, polling resumes; LISTEN/NOTIFY channel re-subscribes | Brief delay; nothing lost |
| Duplicate enqueue (client retry) | `queueing_lock=f"process_dataset:{dataset_id}"` rejects second defer with `AlreadyEnqueued` inside the tx | Client sees 500, but no duplicate job; safe to retry |
| `preprocess` exhausts 60 retries (dataset still not ready after 60s) | Job marked `failed`; `simulation.status` left at `pending` | Frontend shows `pending` forever, needs reconciliation |
| `simulate_chunk` partial commit then crash | Counter incremented, chunk result appended, but `postprocess` never deferred | Stuck at `running`, `completed_count < chunks`, needs manual intervention |
| Schema drift (Alembic moves a column, post_migrate not re-run) | Worker can still defer/claim, but task bodies fail on ORM access | Job fails with `UndefinedColumn`-style errors; needs schema fix and retry |

The `preprocess` retry exhaustion is the most visible rough edge. After
60 attempts (60 seconds of polling), the job is `failed` and
`simulation.status` is still `pending`. Options:

1. Wrap the `preprocess` body so that on final-attempt failure it writes
   `simulation.status='failed'`. Requires peeking at the attempt count
   from inside the task, which is possible but ugly.
2. A janitor task that scans `procrastinate_jobs` for `failed` rows
   matching `preprocess` and reconciles `simulations.status`.
3. Surface the timeout UX-side ("dataset never became ready").

This is flagged as known follow-up work, not yet implemented on the
branch. The `simulate_chunk` partial-commit case is worse but rarer: it
requires a worker crash between the `session.commit()` and the
`postprocess.configure().defer()` call, in the brief window where the
counter has advanced but the next task hasn't been enqueued. There's no
current protection against it.

---

## 6. Comparison With What We Rejected

The procrastinate architecture replaces what other engines need as
separate infrastructure:

| Component | Temporal | Prefect | DBOS | Outbox+CDC | **Procrastinate** |
|---|---|---|---|---|---|
| Orchestrator server | required | required | library | required (Kafka+Connect) | **none** |
| Message broker | internal | internal | Postgres | Kafka | **Postgres** |
| Result/checkpoint store | Cassandra | own Postgres | Postgres | Postgres | **Postgres** |
| UI/dashboard | temporal-ui container | prefect-server container | Conductor (paid) | none | **none** |
| Schema migration | own | own | Alembic | Alembic + Debezium | **post_migrate.py (out of Alembic)** |
| App database | yours | yours | yours | yours | **yours** |
| Extra infra services | 4 (server, UI, cassandra, ES) | 2 (server, PG) | 0 | 3 (kafka, connect, zookeeper) | **0. Just your existing PG** |

The trade-off is explicit: procrastinate gives us Temporal-class
infrastructure simplicity (zero extra services) in exchange for
Temporal-class workflow primitives (none). What we get is the
Transactional Outbox pattern for free, because the job row lives in the
same Postgres as the business data and `defer(connection=conn)` uses the
caller's transaction. What we don't get is any of the orchestration
features that make DBOS, Temporal, or Prefect worth their weight.

For our scenario, the question is whether the orchestration features
matter. The `preprocess` polling workaround and the `simulate_chunk`
counter-based fan-in show that they do, at least a little. The next
section is honest about that.

---

## 7. Known Tradeoffs

Honest list, so we're not surprised later:

1. **No DAG primitive.** The simulation graph exists only in our code.
   Each task defers the next from inside its body. There's no engine
   view of "this simulation has 4 chunks, 2 done, 2 pending"; you
   reconstruct it by querying `procrastinate_jobs` and the
   `simulations.result` JSON.

2. **No auto-retry of in-flight jobs.** A crashed worker leaves `doing`
   rows that the engine will not pick up. Recovery is operator work via
   `retry_job_by_id()`. On a single-worker deployment this is rare and
   tolerable; on a multi-worker deployment, write a sweeper.

3. **Fan-in is a DB counter.** `simulate_chunk` tasks coordinate through
   `simulation.result["completed_count"]`. The "last chunk defers
   postprocess" pattern is correct but fragile: it depends on every
   chunk committing exactly once. A retry-after-partial-commit would
   double-count and could trigger `postprocess` early. We have no
   `RetryStrategy` on `simulate_chunk` today, which is why this is
   latent rather than active.

4. **Determinism is not enforced.** Procrastinate doesn't replay task
   bodies, so non-deterministic code in a task is not a correctness
   hazard the way it is in DBOS/Temporal. The flip side: there's no
   replay checker catching bugs in task code either. A task that reads
   `time.time()` and stores it in the result will silently produce
   different results on retry, and nothing warns you.

5. **No native UI.** Observability is SQL queries (§4.2). Fine for
   developers, awkward for ops/support staff who want to click a
   "failed jobs" view. There's no `procrastinate-ui` container to add;
   you'd build a small admin page over `procrastinate_jobs` if you
   needed one.

6. **Schema lives outside Alembic.** `scripts/post_migrate.py` applies
   the procrastinate schema after `alembic upgrade head`. The two
   systems don't know about each other. A deploy that runs Alembic but
   forgets `post_migrate.py` will start the server fine, accept
   requests, and fail at `defer()` time with a missing-table error.
   `verify.py` runs both in order, but the ordering is a convention,
   not enforced by tooling.

7. **`preprocess` retry exhaustion leaves app state inconsistent** (§5).
   After 60 attempts, the job is `failed` but `simulation.status` is
   still `pending`. Needs a small amount of application-side
   reconciliation we haven't written yet.

8. **`queueing_lock` only on `process_dataset`.** `preprocess` has no
   queueing lock. A retried `POST /simulations` for the same
   `simulation_id` will defer a second `preprocess` job; both will
   run, both will fan out chunks, and the counter will double-count.
   The frontend prevents this by not offering a retry button, but the
   engine doesn't enforce it. Adding
   `queueing_lock=f"preprocess:{simulation_id}"` is a one-line fix.

---

## 8. Decision

For this project, procrastinate is the **baseline**, not the
recommendation. It's what we run on `main` to compare against. The two
requirements that drove the engine comparison (transactional scheduling,
runtime task DAG) are split: transactional scheduling is satisfied
natively (§1.7), and the runtime DAG is **not** satisfied natively (§1.6).
The DAG is faked with a polling retry and a DB counter.

The recommendation is:

- [x] Keep procrastinate on `main` as the infra-minimal baseline.
- [x] Use it as the reference for "what zero extra services buys you."
- [x] Do not promote it to production for the simulation workload unless
      we accept the §5 failure modes and the §7 tradeoffs as-is.
- [ ] Add the `queueing_lock` on `preprocess` (§7.8) before any
      production use.
- [ ] Add a `doing`-row sweeper (§4.1) before scaling past one worker.
- [ ] Add `preprocess`-failure reconciliation (§5 option 1 or 2) before
      treating this as production-ready.
- [ ] Re-evaluate if the simulation workload ever needs real fan-in,
      child workflows, or cross-task signals; at that point the
      ad-hoc chaining pattern in `tasks.py` becomes a liability, not a
      quirk.

The branch exists to make this trade-off visible. The other branches
(`feat/dbos`, `feat/temporal`, `feat/prefect`, `feat/outbox-cdc`) are
what you read when deciding whether the trade-offs in this section are
worth escaping.
