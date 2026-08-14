"""DBOS workflows and scheduler implementation.

Key characteristics of this branch:
- Transactional enqueue: uses `DBOSClient.enqueue_in_transaction(session, ...)`
  to enqueue a workflow within the caller's SQLAlchemy transaction. If the tx
  rolls back, the workflow is never enqueued.
- Dynamic DAG: DBOS has no DAG concept — workflows are plain Python. The
  simulation workflow calls preprocess, fans out N chunk workflows via
  Queue.enqueue, then collects results and calls postprocess. N is determined
  at runtime from the simulation parameters. This is the most PyTorch-like
  model: the "graph" IS the call stack, shaped by runtime values.
"""

from __future__ import annotations

import time
from uuid import UUID

from dbos import DBOS
from sqlalchemy.orm import Session

from simapp.db import SessionLocal
from simapp.dbos_config import dbos, sim_queue
from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus


@DBOS.step()
def _process_dataset_step(dataset_id: str) -> None:
    time.sleep(2)
    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is not None:
            dataset.status = DatasetStatus.ready
            session.commit()


@DBOS.workflow()
def process_dataset_wf(dataset_id: str, filename: str) -> str:
    _process_dataset_step(dataset_id)
    DBOS.set_event("ready", "ready")
    return "processed"


@DBOS.step()
def _preprocess_step(simulation_id: str) -> None:
    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is not None:
            simulation.status = SimulationStatus.running
            session.commit()


@DBOS.workflow()
def _simulate_chunk_wf(simulation_id: str, chunk_index: int) -> dict:
    DBOS.sleep(1)
    return {"chunk_index": chunk_index, "value": chunk_index * 2}


@DBOS.step()
def _postprocess_step(simulation_id: str, chunk_results: list[dict]) -> None:
    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is None:
            return
        total = sum(c["value"] for c in chunk_results)
        simulation.result = {
            "chunks": chunk_results,
            "total": total,
            "chunk_count": len(chunk_results),
        }
        simulation.status = SimulationStatus.completed
        session.commit()


@DBOS.workflow()
def simulation_wf(simulation_id: str, dataset_id: str, chunks: int) -> str:
    """Dynamic DAG with inter-workflow dependency: wait for dataset, then
    preprocess → fan-out N chunks → fan-in postprocess.
    """
    _preprocess_step(simulation_id)

    # set_event("ready", ...) in process_dataset_wf pairs with this get_event;
    # workflow_id is f"dataset-{dataset_id}" set at enqueue time.
    DBOS.get_event(f"dataset-{dataset_id}", "ready", timeout_seconds=120)

    handles = [
        sim_queue.enqueue(_simulate_chunk_wf, simulation_id, i)
        for i in range(chunks)
    ]

    chunk_results = [h.get_result() for h in handles]

    _postprocess_step(simulation_id, chunk_results)
    return "completed"


class DBOSScheduler:
    """Scheduler implementation using DBOS with transactional enqueue.

    Uses `DBOSClient.enqueue_in_transaction(session, ...)` to insert the
    workflow registration row on the caller's SQLAlchemy connection, within
    the caller's transaction. If the transaction rolls back, the workflow
    is never enqueued.
    """

    def __init__(self) -> None:
        from dbos import DBOSClient

        from simapp.dbos_config import _system_db_url

        self._client = DBOSClient(system_database_url=_system_db_url)

    def destroy(self) -> None:
        self._client.destroy()

    def schedule_dataset_processing(
        self,
        session: Session,
        dataset_id: UUID,
        filename: str,
    ) -> None:
        from dbos import EnqueueOptions

        options: EnqueueOptions = {
            "queue_name": "sim_queue",
            "workflow_name": "process_dataset_wf",
            "workflow_id": f"dataset-{dataset_id}",
        }
        self._client.enqueue_in_transaction(
            session,
            options,
            str(dataset_id),
            filename,
        )

    def schedule_simulation(
        self,
        session: Session,
        simulation_id: UUID,
        dataset_id: UUID,
        parameters: dict,
    ) -> None:
        from dbos import EnqueueOptions

        chunks = parameters.get("chunks", 4)
        options: EnqueueOptions = {
            "queue_name": "sim_queue",
            "workflow_name": "simulation_wf",
            "workflow_id": f"simulation-{simulation_id}",
        }
        self._client.enqueue_in_transaction(
            session,
            options,
            str(simulation_id),
            str(dataset_id),
            chunks,
        )
