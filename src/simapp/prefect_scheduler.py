"""Prefect scheduler — fire-after-commit with gap demonstration.

Prefect CANNOT do transactional enqueue: flow runs are created via the Prefect
Server API (separate datastore). This scheduler commits the DB row first,
then triggers the flow. If the flow trigger fails (server down, network error),
the DB row is committed but no flow exists — an orphaned simulation.
The gap test in tests/test_consistency_gap.py demonstrates this.
"""

from __future__ import annotations

from uuid import UUID

from prefect.deployments import run_deployment
from sqlalchemy.orm import Session

from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus


class PrefectScheduler:
    """Scheduler using Prefect with fire-after-commit.

    The DB transaction commits BEFORE the flow is triggered. If the flow
    trigger fails, the DB row is orphaned (committed but never processed).
    """

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

        run_deployment(
            name="simulation_flow/simulation-deployment",
            parameters={
                "simulation_id": str(simulation_id),
                "dataset_id": str(dataset_id),
                "chunks": chunks,
            },
            timeout=0,
        )
