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

from __future__ import annotations

import time
from uuid import UUID

from procrastinate import App, PsycopgConnector
from sqlalchemy.orm import Session

from simapp.config import settings
from simapp.db import SessionLocal
from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus

conninfo = settings.database_url.replace("postgresql+psycopg://", "postgresql://")
connector = PsycopgConnector(conninfo=conninfo)
app = App(connector=connector)


@app.task
def process_dataset(dataset_id: str, filename: str) -> None:
    time.sleep(2)
    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is None:
            return
        dataset.status = DatasetStatus.ready
        session.commit()


@app.task
def preprocess(simulation_id: str, dataset_id: str) -> None:
    max_retries = 60
    for _ in range(max_retries):
        with SessionLocal() as session:
            dataset = session.get(Dataset, UUID(dataset_id))
            if dataset is not None and dataset.status == DatasetStatus.ready:
                break
        time.sleep(1)
    else:
        with SessionLocal() as session:
            simulation = session.get(Simulation, UUID(simulation_id))
            if simulation is not None:
                simulation.status = SimulationStatus.failed
                simulation.result = {"error": f"Dataset {dataset_id} not ready after {max_retries}s"}
                session.commit()
        return

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


def _get_chunks(simulation_id: str) -> int:
    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is None:
            return 0
        return simulation.parameters.get("chunks", 4)


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
