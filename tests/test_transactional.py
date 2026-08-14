"""Transactional enqueue proof: rollback cancels the DBOS workflow.

This is the KEY test that demonstrates DBOS's transactional scheduling.
If the DB transaction rolls back after `enqueue_in_transaction()`,
the workflow must NOT exist in the DBOS system database.
"""

from __future__ import annotations

import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import dbos
from dbos import DBOSClient, EnqueueOptions

from simapp.config import settings
from simapp.dbos_config import _system_db_url
from simapp.models import Dataset, DatasetStatus


def test_rollback_cancels_enqueued_workflow(dbos_launched):
    """If a transaction rolls back after enqueueing a workflow, no workflow should exist."""
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    client = DBOSClient(system_database_url=_system_db_url)

    dataset_id = uuid.uuid4()
    workflow_id = f"dataset-{dataset_id}"

    try:
        client.delete_workflow(workflow_id)
    except dbos.error.DBOSNonExistentWorkflowError:
        pass

    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        dataset = Dataset(id=dataset_id, filename="rollback_test.csv", status=DatasetStatus.pending)
        session.add(dataset)
        session.flush()

        options: EnqueueOptions = {
            "queue_name": "sim_queue",
            "workflow_name": "process_dataset_wf",
            "workflow_id": workflow_id,
        }
        client.enqueue_in_transaction(
            session,
            options,
            str(dataset_id),
            "rollback_test.csv",
        )
        session.rollback()
    finally:
        session.close()

    try:
        client.retrieve_workflow(workflow_id)
        raise AssertionError(f"Expected no workflow after rollback, but found {workflow_id}")
    except dbos.error.DBOSNonExistentWorkflowError:
        pass  # Expected: workflow was never committed
    finally:
        client.destroy()

    with SessionLocal() as session:
        dataset = session.get(Dataset, dataset_id)
        assert dataset is None, "Dataset should not exist after rollback"

    engine.dispose()


def test_commit_persists_enqueued_workflow(dbos_launched):
    """If a transaction commits after enqueueing a workflow, the workflow should persist."""
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    client = DBOSClient(system_database_url=_system_db_url)

    dataset_id = uuid.uuid4()
    workflow_id = f"dataset-{dataset_id}"

    try:
        client.delete_workflow(workflow_id)
    except dbos.error.DBOSNonExistentWorkflowError:
        pass

    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionLocal()
    try:
        dataset = Dataset(id=dataset_id, filename="commit_test.csv", status=DatasetStatus.pending)
        session.add(dataset)
        session.flush()

        options: EnqueueOptions = {
            "queue_name": "sim_queue",
            "workflow_name": "process_dataset_wf",
            "workflow_id": workflow_id,
        }
        client.enqueue_in_transaction(
            session,
            options,
            str(dataset_id),
            "commit_test.csv",
        )
        session.commit()
    finally:
        session.close()

    handle = client.retrieve_workflow(workflow_id)
    status = handle.get_status()
    assert status is not None, "Expected workflow to exist after commit"

    try:
        client.delete_workflow(workflow_id)
    except dbos.error.DBOSNonExistentWorkflowError:
        pass
    finally:
        client.destroy()

    with engine.connect() as conn:
        conn.execute(text("DELETE FROM datasets WHERE id = :id"), {"id": str(dataset_id)})
        conn.commit()

    engine.dispose()
