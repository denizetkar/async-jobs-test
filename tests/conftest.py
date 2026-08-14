"""Shared pytest fixtures — engine-specific overrides."""

from __future__ import annotations

import importlib.util
import os
import threading
from collections.abc import Generator
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

TEST_DATABASE_URL = os.environ.get(
    "SIMAPP_TEST_DATABASE_URL",
    "postgresql+psycopg://simapp:simapp@localhost:5432/simapp_test",
)


def _apply_engine_schema(engine) -> None:
    """Apply engine-specific schema via post_migrate.py hook."""
    hook = Path("scripts/post_migrate.py")
    if not hook.exists():
        return
    spec = importlib.util.spec_from_file_location("post_migrate", hook)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if hasattr(mod, "apply_schema"):
        mod.apply_schema(engine)


class StubScheduler:
    """Test-only scheduler preserving the outbox pattern.

    Inserts an OutboxEvent row (same as OutboxScheduler) AND processes it in a
    background daemon thread so ``sample_dataset_id`` can observe status=ready
    without a real Kafka/Debezium round-trip. ``SessionLocal`` is imported at
    call time so the monkeypatched factory from the ``client`` fixture is used.
    """

    def schedule_dataset_processing(
        self,
        session: Session,
        dataset_id: UUID,
        filename: str,
    ) -> None:
        from simapp.models import OutboxEvent

        session.add(
            OutboxEvent(
                aggregate_type="dataset",
                aggregate_id=str(dataset_id),
                event_type="process_dataset",
                payload={"dataset_id": str(dataset_id), "filename": filename},
            )
        )

        def _process() -> None:
            import time

            from simapp.db import SessionLocal
            from simapp.models import Dataset, DatasetStatus

            time.sleep(2)
            with SessionLocal() as s:
                dataset = s.get(Dataset, dataset_id)
                if dataset is not None:
                    dataset.status = DatasetStatus.ready
                    s.commit()

        threading.Thread(target=_process, daemon=True).start()

    def schedule_simulation(
        self,
        session: Session,
        simulation_id: UUID,
        dataset_id: UUID,
        parameters: dict,
    ) -> None:
        from simapp.models import OutboxEvent

        session.add(
            OutboxEvent(
                aggregate_type="simulation",
                aggregate_id=str(simulation_id),
                event_type="start_simulation",
                payload={
                    "simulation_id": str(simulation_id),
                    "dataset_id": str(dataset_id),
                    "parameters": parameters,
                },
            )
        )

        def _process() -> None:
            import time

            from simapp.db import SessionLocal
            from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus

            for _ in range(30):
                with SessionLocal() as s:
                    dataset = s.get(Dataset, dataset_id)
                    if dataset is not None and dataset.status == DatasetStatus.ready:
                        break
                time.sleep(1)

            with SessionLocal() as s:
                simulation = s.get(Simulation, simulation_id)
                if simulation is not None:
                    simulation.status = SimulationStatus.running
                    s.commit()

            chunks = parameters.get("chunks", 4)
            chunk_results = [
                {"chunk_index": i, "value": i * 2}
                for i in range(chunks)
            ]

            with SessionLocal() as s:
                simulation = s.get(Simulation, simulation_id)
                if simulation is None:
                    return
                total = sum(c["value"] for c in chunk_results)
                simulation.result = {
                    "chunks": chunk_results,
                    "total": total,
                    "chunk_count": len(chunk_results),
                }
                simulation.status = SimulationStatus.completed
                s.commit()

        threading.Thread(target=_process, daemon=True).start()


@pytest.fixture(scope="session")
def db_engine():
    """Session-scoped engine pointing at the test database."""
    engine = create_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS simulations CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS datasets CASCADE"))
        conn.execute(text("DROP TYPE IF EXISTS datasetstatus CASCADE"))
        conn.execute(text("DROP TYPE IF EXISTS simulationstatus CASCADE"))
        conn.commit()

    from simapp.db import Base
    from simapp import models  # noqa: F401

    Base.metadata.create_all(engine)
    _apply_engine_schema(engine)

    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine) -> Generator[Session, None]:
    """Function-scoped DB session that rolls back after each test."""
    connection = db_engine.connect()
    transaction = connection.begin()
    SessionLocal = sessionmaker(bind=connection, expire_on_commit=False)
    session = SessionLocal()

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture(scope="function")
def client(db_engine, monkeypatch) -> Generator[TestClient, None]:
    """FastAPI TestClient with the DB engine patched to use the test database."""
    import simapp.db as db_module
    import simapp.outbox_consumer as tasks_module

    test_engine = db_engine
    test_session_factory = sessionmaker(bind=test_engine, expire_on_commit=False)

    monkeypatch.setattr(db_module, "engine", test_engine)
    monkeypatch.setattr(db_module, "SessionLocal", test_session_factory)
    monkeypatch.setattr(tasks_module, "SessionLocal", test_session_factory)

    from simapp.deps import get_scheduler
    from simapp.main import app

    app.dependency_overrides[get_scheduler] = lambda: StubScheduler()

    from simapp.deps import get_session

    def _test_get_session():
        session = test_session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_session] = _test_get_session

    with TestClient(app) as c:
        yield c

    app.dependency_overrides.clear()


@pytest.fixture
def sample_dataset_id(client) -> str:
    """Create a dataset and wait for it to be ready. Returns the dataset ID."""
    import time
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("col1,col2\n1,2\n3,4\n")
        f.flush()
        filepath = f.name

    try:
        with open(filepath, "rb") as f:
            response = client.post("/datasets", files={"file": ("test.csv", f, "text/csv")})
        assert response.status_code == 201, response.text
        dataset_id = response.json()["id"]

        for _ in range(30):
            response = client.get(f"/datasets/{dataset_id}")
            status = response.json()["status"]
            if status == "ready":
                return dataset_id
            if status == "failed":
                pytest.fail("Dataset processing failed")
            time.sleep(1)

        pytest.fail("Dataset not ready after 30s")
    finally:
        os.unlink(filepath)
