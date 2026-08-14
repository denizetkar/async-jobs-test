"""Kafka consumer that reads outbox events and dispatches to procrastinate workers.

This process subscribes to the Kafka topic that Debezium publishes outbox
events to. For each event, it calls the appropriate procrastinate task.
"""

from __future__ import annotations

import json
import logging
import os
import time
from uuid import UUID

from kafka import KafkaConsumer
from sqlalchemy import create_engine, inspect

from simapp.config import settings
from simapp.db import SessionLocal
from simapp.models import (
    Dataset,
    DatasetStatus,
    ProcessedEvent,
    Simulation,
    SimulationStatus,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _event_id_from_headers(headers) -> UUID | None:
    """Extract the Debezium EventRouter event ID from Kafka headers.

    Debezium's EventRouter plugin writes the event ID to the ``id`` header
    (configured via ``table.field.event.id=id``). kafka-python exposes
    headers as a list of ``(name, bytes)`` tuples.
    """
    if not headers:
        return None
    for name, value in headers:
        if name == "id" and value is not None:
            try:
                return UUID(str(value.decode("utf-8")))
            except (ValueError, UnicodeDecodeError):
                logger.warning("Could not parse event id header: %r", value)
                return None
    return None


def process_dataset(dataset_id: str, filename: str) -> None:
    time.sleep(2)
    with SessionLocal() as session:
        dataset = session.get(Dataset, UUID(dataset_id))
        if dataset is not None:
            dataset.status = DatasetStatus.ready
            session.commit()


def start_simulation(simulation_id: str, dataset_id: str, chunks: int) -> None:
    # Poll for dataset readiness — same workaround as procrastinate. The
    # timeout is configurable via SIMAPP_SIMULATION_TIMEOUT (seconds, default
    # 120); we poll once per second so the iteration count equals the timeout.
    timeout_seconds = int(os.getenv("SIMAPP_SIMULATION_TIMEOUT", "120"))
    max_retries = max(1, timeout_seconds)
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
                simulation.result = {
                    "error": (
                        f"Dataset {dataset_id} not ready after {timeout_seconds}s "
                        f"(SIMAPP_SIMULATION_TIMEOUT={timeout_seconds})"
                    ),
                }
                session.commit()
        return

    with SessionLocal() as session:
        simulation = session.get(Simulation, UUID(simulation_id))
        if simulation is not None:
            simulation.status = SimulationStatus.running
            session.commit()

    import concurrent.futures

    def simulate_chunk(chunk_index: int) -> dict:
        time.sleep(1)
        return {"chunk_index": chunk_index, "value": chunk_index * 2}

    with concurrent.futures.ThreadPoolExecutor(max_workers=chunks) as executor:
        chunk_results = list(executor.map(simulate_chunk, range(chunks)))

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


def _build_consumer(bootstrap: str) -> KafkaConsumer:
    """Build the Kafka consumer for the given bootstrap servers.

    Bootstrap is resolved by the caller at call time (never at module level)
    so env overrides and test monkeypatching work reliably.
    """
    return KafkaConsumer(
        "simapp.outbox_events",
        bootstrap_servers=bootstrap,
        group_id="simapp-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def _dispatch(event_type: str, payload: dict) -> None:
    if event_type == "process_dataset":
        process_dataset(
            payload["dataset_id"],
            payload["filename"],
        )
    elif event_type == "start_simulation":
        start_simulation(
            payload["simulation_id"],
            payload["dataset_id"],
            payload["parameters"].get("chunks", 4),
        )


def main() -> None:
    # Ensure the processed_events table exists before consuming. The table is
    # normally created by scripts/post_migrate.py, but if the worker starts
    # without that having run (e.g. in Docker), it would crash on first use.
    db_url = os.environ.get(
        "SIMAPP_DATABASE_URL",
        "postgresql+psycopg://simapp:simapp@localhost:5432/simapp",
    )
    engine = create_engine(db_url)
    inspector = inspect(engine)
    if not inspector.has_table("processed_events"):
        ProcessedEvent.__table__.create(engine)
    engine.dispose()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info("Starting outbox consumer, bootstrap=%s", bootstrap)
    consumer = _build_consumer(bootstrap)
    logger.info("Consumer ready, listening on topic 'simapp.outbox_events'...")

    for message in consumer:
        event = message.value
        event_type = event.get("event_type")
        payload = event.get("payload", {})
        event_id = _event_id_from_headers(message.headers)
        logger.info("Received event: %s (id=%s)", event_type, event_id)

        if event_id is not None:
            with SessionLocal() as session:
                already = session.get(ProcessedEvent, event_id)
                if already is not None:
                    logger.info("Skipping duplicate event id=%s", event_id)
                    consumer.commit()
                    continue

        _dispatch(event_type, payload)

        if event_id is not None:
            with SessionLocal() as session:
                session.add(ProcessedEvent(id=event_id))
                session.commit()
            consumer.commit()


if __name__ == "__main__":
    main()
