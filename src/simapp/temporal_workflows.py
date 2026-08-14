"""Temporal workflows and activities.

Key characteristics of this branch:
- Dynamic DAG: Temporal workflows are imperative code. The simulation workflow
  calls preprocess, fans out N chunk activities via asyncio.gather, then calls
  postprocess. N is determined at runtime from the simulation parameters.
- Inter-workflow dependency: the simulation workflow calls wait_dataset_activity
  which raises a RuntimeError if the dataset isn't ready — Temporal's built-in
  retry (with backoff) makes the activity poll until it succeeds.
- Transactional scheduling: NOT POSSIBLE. Temporal start_workflow() is an RPC
  to the Temporal Server (separate datastore). This branch uses fire-after-commit:
  commit the DB row, then start the workflow. The gap test demonstrates the
  inconsistency window (commit succeeds, workflow start fails -> orphaned row).
"""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from uuid import UUID

from temporalio import activity, workflow
from temporalio.common import RetryPolicy


@activity.defn
def wait_dataset_activity(dataset_id: str) -> str:
    """Block until the dataset is ready. Raises if not — Temporal retries."""
    from simapp.db import SessionLocal
    from simapp.models import Dataset, DatasetStatus

    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is not None and dataset.status == DatasetStatus.ready:
            return "ready"
    raise RuntimeError(f"Dataset {dataset_id} not yet ready")


@activity.defn
def process_dataset_activity(dataset_id: str, filename: str) -> str:
    from simapp.db import SessionLocal
    from simapp.models import Dataset, DatasetStatus

    time.sleep(2)
    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is not None:
            dataset.status = DatasetStatus.ready
            session.commit()
    return "processed"


@activity.defn
def preprocess_activity(simulation_id: str, dataset_id: str) -> str:
    from simapp.db import SessionLocal
    from simapp.models import Simulation, SimulationStatus

    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is not None:
            simulation.status = SimulationStatus.running
            session.commit()
    return "preprocessed"


@activity.defn
def simulate_chunk_activity(simulation_id: str, chunk_index: int) -> dict:
    time.sleep(1)
    return {"chunk_index": chunk_index, "value": chunk_index * 2}


@activity.defn
def postprocess_activity(simulation_id: str, chunk_results_json: str) -> str:
    import json

    from simapp.db import SessionLocal
    from simapp.models import Simulation, SimulationStatus

    chunk_results: list[dict] = json.loads(chunk_results_json)
    with SessionLocal() as session:
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


@workflow.defn
class DatasetProcessWorkflow:
    @workflow.run
    async def run(self, dataset_id: str, filename: str) -> str:
        return await workflow.execute_activity(
            process_dataset_activity,
            args=[dataset_id, filename],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )


@workflow.defn
class SimulationWorkflow:
    @workflow.run
    async def run(self, simulation_id: str, dataset_id: str, chunks: int) -> str:
        # Inter-workflow dependency: wait for dataset — retry with 1s backoff
        await workflow.execute_activity(
            wait_dataset_activity,
            args=[dataset_id],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(seconds=5),
                maximum_attempts=100,
            ),
        )

        await workflow.execute_activity(
            preprocess_activity,
            args=[simulation_id, dataset_id],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        chunk_results = await asyncio.gather(*[
            workflow.execute_activity(
                simulate_chunk_activity,
                args=[simulation_id, i],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            for i in range(chunks)
        ])

        import json

        return await workflow.execute_activity(
            postprocess_activity,
            args=[simulation_id, json.dumps(chunk_results)],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
