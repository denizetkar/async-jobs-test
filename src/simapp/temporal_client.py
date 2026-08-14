"""Temporal scheduler — fire-after-commit with gap demonstration.

Temporal CANNOT do transactional enqueue: start_workflow() is an RPC to the
Temporal Server (separate datastore). This scheduler commits the DB row first,
then starts the workflow. If the workflow start fails (server down, network
error), the DB row is committed but no workflow exists — an orphaned simulation.
The gap test in tests/test_consistency_gap.py demonstrates this.
"""

from __future__ import annotations

import os
from uuid import UUID

from sqlalchemy.orm import Session

from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus
from simapp.temporal_workflows import DatasetProcessWorkflow, SimulationWorkflow


class TemporalScheduler:
    """Scheduler using Temporal with fire-after-commit.

    The DB transaction commits BEFORE the workflow starts. If the workflow
    start fails, the DB row is orphaned (committed but never processed).
    """

    def __init__(self) -> None:
        import asyncio

        from temporalio.client import Client

        self._client: Client | None = None
        self._loop = asyncio.new_event_loop()
        self._temporal_address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")

    def _ensure_client(self) -> None:
        if self._client is None:
            import asyncio

            from temporalio.client import Client

            self._client = self._loop.run_until_complete(
                Client.connect(self._temporal_address, namespace="default")
            )

    def schedule_dataset_processing(
        self,
        session: Session,
        dataset_id: UUID,
        filename: str,
    ) -> None:
        import asyncio

        from temporalio.client import Client

        dataset = session.get(Dataset, dataset_id)
        if dataset is not None:
            dataset.status = DatasetStatus.pending
        session.commit()

        self._ensure_client()
        workflow_id = f"dataset-{dataset_id}"

        self._loop.run_until_complete(
            self._client.start_workflow(
                DatasetProcessWorkflow.run,
                args=[str(dataset_id), filename],
                id=workflow_id,
                task_queue="simapp-task-queue",
            )
        )

    def schedule_simulation(
        self,
        session: Session,
        simulation_id: UUID,
        dataset_id: UUID,
        parameters: dict,
    ) -> None:
        chunks = parameters.get("chunks", 4)

        simulation = session.get(Simulation, simulation_id)
        if simulation is not None:
            simulation.status = SimulationStatus.pending
        session.commit()

        self._ensure_client()
        workflow_id = f"simulation-{simulation_id}"

        self._loop.run_until_complete(
            self._client.start_workflow(
                SimulationWorkflow.run,
                args=[str(simulation_id), str(dataset_id), chunks],
                id=workflow_id,
                task_queue="simapp-task-queue",
            )
        )
