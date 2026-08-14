"""Prefect flows and tasks.

Key characteristics of this branch:
- Dynamic DAG: Prefect .map() creates task runs at runtime. The simulation flow
  preprocesses, maps simulate_chunk over range(chunks), then aggregates with
  .result(). N is determined at runtime from the simulation parameters.
- Transactional scheduling: NOT POSSIBLE. Prefect creates task runs via the
  Prefect Server API (separate datastore). This branch uses fire-after-commit:
  commit the DB row, then trigger the flow. The gap test demonstrates the
  inconsistency window (commit succeeds, flow trigger fails -> orphaned row).
"""

from __future__ import annotations

import time
from uuid import UUID

from prefect import flow, task, unmapped

from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus


def _get_session_factory():
    """Build a session factory on demand — avoids capturing module-level engine in closures."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from simapp.config import settings

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


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


@task
def process_dataset_task(dataset_id: str, filename: str) -> str:
    time.sleep(2)
    SessionFactory = _get_session_factory()
    with SessionFactory() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is not None:
            dataset.status = DatasetStatus.ready
            session.commit()
    return "processed"


@task
def preprocess_task(simulation_id: str, dataset_id: str) -> str:
    SessionFactory = _get_session_factory()
    with SessionFactory() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is not None:
            simulation.status = SimulationStatus.running
            session.commit()
    return "preprocessed"


@task
def simulate_chunk_task(simulation_id: str, chunk_index: int) -> dict:
    time.sleep(1)
    return {"chunk_index": chunk_index, "value": chunk_index * 2}


@task
def postprocess_task(simulation_id: str, chunk_results: list[dict]) -> str:
    SessionFactory = _get_session_factory()
    with SessionFactory() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is None:
            return "not_found"
        total = sum(c["value"] for c in chunk_results)
        simulation.result = {
            "chunks": chunk_results,
            "total": total,
            "chunk_count": len(chunk_results),
        }
        simulation.status = SimulationStatus.completed
        session.commit()
    return "completed"


@flow(name="process_dataset_flow")
def process_dataset_flow(dataset_id: str, filename: str) -> str:
    return process_dataset_task(dataset_id, filename)


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
