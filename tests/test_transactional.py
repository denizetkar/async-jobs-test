"""Transactional enqueue proof: rollback cancels the procrastinate job."""

from __future__ import annotations

import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from simapp.config import settings
from simapp.db import Base
from simapp.models import Dataset, DatasetStatus  # noqa: F401  (registers models)
from simapp.tasks import process_dataset

from scripts.post_migrate import apply_schema

# procrastinate stores job rows under the task's full module path (e.g.
# "simapp.tasks.process_dataset"); the bare name matches nothing.
TASK_NAME = process_dataset.full_path


def _fresh_engine_and_schema():
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    apply_schema(engine)
    return engine


def test_rollback_cancels_deferred_job():
    """If a transaction rolls back after deferring a task, no job row should exist."""
    engine = _fresh_engine_and_schema()

    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        conn.commit()

    dataset_id = uuid.uuid4()
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        dataset = Dataset(id=dataset_id, filename="rollback_test.csv", status=DatasetStatus.pending)
        session.add(dataset)
        session.flush()

        conn = session.connection().connection
        process_dataset.configure(connection=conn).defer(
            dataset_id=str(dataset_id),
            filename="rollback_test.csv",
        )

        result = session.execute(
            text("SELECT count(*) FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        assert result.scalar() >= 1, "Job should exist within the transaction"

        session.rollback()
    finally:
        session.close()

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT count(*) FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        assert result.scalar() == 0, "Expected 0 jobs after rollback"

    with SessionLocal() as session:
        assert session.get(Dataset, dataset_id) is None, "Dataset should not exist after rollback"

    engine.dispose()


def test_commit_persists_deferred_job():
    """If a transaction commits after deferring a task, the job row should persist."""
    engine = _fresh_engine_and_schema()

    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        conn.commit()

    dataset_id = uuid.uuid4()
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        dataset = Dataset(id=dataset_id, filename="commit_test.csv", status=DatasetStatus.pending)
        session.add(dataset)
        session.flush()

        conn = session.connection().connection
        process_dataset.configure(connection=conn).defer(
            dataset_id=str(dataset_id),
            filename="commit_test.csv",
        )
        session.commit()
    finally:
        session.close()

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT count(*) FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        assert result.scalar() == 1, "Expected 1 job after commit"

    with engine.connect() as conn:
        conn.execute(
            text("DELETE FROM procrastinate_jobs WHERE task_name = :task_name"),
            {"task_name": TASK_NAME},
        )
        conn.execute(text("DELETE FROM datasets WHERE id = :id"), {"id": str(dataset_id)})
        conn.commit()

    engine.dispose()
