"""Scheduler interface — each engine branch provides its own implementation."""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy.orm import Session


@runtime_checkable
class SimulationScheduler(Protocol):
    """Abstract interface for scheduling background work.

    Each engine branch provides its own implementation that the FastAPI
    app uses via dependency injection.
    """

    def schedule_dataset_processing(
        self,
        session: Session,
        dataset_id: UUID,
        filename: str,
    ) -> None:
        """Schedule the background processing of an uploaded dataset.

        Implementations should enqueue a task that:
        1. Simulates slow work (sleep ~2s)
        2. Marks the Dataset status as 'ready' or 'failed'

        The session is passed so that transactional schedulers can defer
        the task within the caller's DB transaction. Non-transactional
        schedulers commit first, then dispatch.
        """
        ...

    def schedule_simulation(
        self,
        session: Session,
        simulation_id: UUID,
        dataset_id: UUID,
        parameters: dict,
    ) -> None:
        """Schedule a simulation workflow that executes a DAG:
        1. preprocess(dataset_id)
        2. simulate_chunk(chunk_index) x N  (fan-out, N = parameters['chunks'])
        3. postprocess(simulation_id, chunk_results)  (fan-in)
        """
        ...
