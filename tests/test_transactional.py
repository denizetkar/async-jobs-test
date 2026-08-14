"""Transactional outbox proof: rollback cancels both the business row and the outbox event.

This test demonstrates that the outbox event and the business data (Dataset)
are in the SAME transaction. If the transaction rolls back, neither exists.
"""

from __future__ import annotations

import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from simapp.config import settings
from simapp.models import Dataset, DatasetStatus, OutboxEvent
from simapp.outbox_scheduler import OutboxScheduler


def test_rollback_cancels_outbox_event():
    """If a transaction rolls back, neither the dataset nor the outbox event exist."""
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    dataset_id = uuid.uuid4()
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    scheduler = OutboxScheduler()
    session = SessionLocal()
    try:
        dataset = Dataset(id=dataset_id, filename="rollback_test.csv", status=DatasetStatus.pending)
        session.add(dataset)
        session.flush()

        scheduler.schedule_dataset_processing(
            session=session,
            dataset_id=dataset_id,
            filename="rollback_test.csv",
        )
        session.flush()

        result = session.execute(
            text("SELECT count(*) FROM outbox_events WHERE aggregate_id = :id"),
            {"id": str(dataset_id)},
        )
        assert result.scalar() >= 1, "Outbox event should exist within the transaction"

        session.rollback()
    finally:
        session.close()

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT count(*) FROM outbox_events WHERE aggregate_id = :id"),
            {"id": str(dataset_id)},
        )
        assert result.scalar() == 0, "Outbox event should not exist after rollback"

    with SessionLocal() as session:
        assert session.get(Dataset, dataset_id) is None, "Dataset should not exist after rollback"

    engine.dispose()


def test_commit_persists_outbox_event():
    """If a transaction commits, both the dataset and the outbox event persist."""
    engine = create_engine(settings.database_url, pool_pre_ping=True)

    dataset_id = uuid.uuid4()
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    scheduler = OutboxScheduler()
    session = SessionLocal()
    try:
        dataset = Dataset(id=dataset_id, filename="commit_test.csv", status=DatasetStatus.pending)
        session.add(dataset)
        session.flush()

        scheduler.schedule_dataset_processing(
            session=session,
            dataset_id=dataset_id,
            filename="commit_test.csv",
        )
        session.commit()
    finally:
        session.close()

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT count(*) FROM outbox_events WHERE aggregate_id = :id"),
            {"id": str(dataset_id)},
        )
        assert result.scalar() == 1, "Outbox event should exist after commit"

    with SessionLocal() as session:
        assert session.get(Dataset, dataset_id) is not None, "Dataset should exist after commit"

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM outbox_events WHERE aggregate_id = :id"), {"id": str(dataset_id)})
        conn.execute(text("DELETE FROM datasets WHERE id = :id"), {"id": str(dataset_id)})
        conn.commit()

    engine.dispose()
