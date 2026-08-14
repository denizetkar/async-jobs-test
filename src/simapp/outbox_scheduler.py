"""Outbox scheduler — transactional outbox pattern with CDC.

This scheduler writes an outbox event row IN THE SAME TRANSACTION as the
Dataset/Simulation row. The actual task dispatch happens asynchronously:
Debezium captures the outbox row via Postgres WAL → publishes to Kafka →
the consumer reads from Kafka and defers to the procrastinate worker.

This is the architectural bridge that makes ANY engine transactional.
"""

from __future__ import annotations

import json
from uuid import UUID

from sqlalchemy.orm import Session

from simapp.models import OutboxEvent


class OutboxScheduler:
    """Scheduler using the Transactional Outbox pattern.

    Instead of directly deferring tasks, it INSERTs an outbox event row
    in the SAME transaction as the business data. CDC (Debezium) captures
    the row and dispatches it to Kafka, where a consumer triggers the work.
    """

    def schedule_dataset_processing(
        self,
        session: Session,
        dataset_id: UUID,
        filename: str,
    ) -> None:
        event = OutboxEvent(
            aggregate_type="dataset",
            aggregate_id=str(dataset_id),
            event_type="process_dataset",
            payload={"dataset_id": str(dataset_id), "filename": filename},
        )
        session.add(event)

    def schedule_simulation(
        self,
        session: Session,
        simulation_id: UUID,
        dataset_id: UUID,
        parameters: dict,
    ) -> None:
        event = OutboxEvent(
            aggregate_type="simulation",
            aggregate_id=str(simulation_id),
            event_type="start_simulation",
            payload={
                "simulation_id": str(simulation_id),
                "dataset_id": str(dataset_id),
                "parameters": parameters,
            },
        )
        session.add(event)
