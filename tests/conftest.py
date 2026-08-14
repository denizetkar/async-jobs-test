"""Shared pytest fixtures — engine-specific overrides."""

from __future__ import annotations

import importlib.util
import os
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

TEST_DATABASE_URL = os.environ.get(
    "SIMAPP_TEST_DATABASE_URL",
    "postgresql+psycopg://simapp:simapp@localhost:5432/simapp_test",
)


class StubScheduler:
    """Test-only scheduler that processes inline in background threads.

    Mimics async-engine behavior without contacting a real Temporal server:
    dataset processing flips status to ready after a short delay, and
    simulation processing flips status to running then completed.
    """

    def schedule_dataset_processing(self, session, dataset_id, filename):
        import threading
        import time

        def _process():
            time.sleep(2)
            from simapp.db import SessionLocal
            from simapp.models import Dataset, DatasetStatus

            with SessionLocal() as s:
                d = s.get(Dataset, dataset_id)
                if d:
                    d.status = DatasetStatus.ready
                    s.commit()

        threading.Thread(target=_process, daemon=True).start()

    def schedule_simulation(self, session, simulation_id, dataset_id, parameters):
        import threading
        import time

        chunks = parameters.get("chunks", 4)

        def _process():
            from simapp.db import SessionLocal
            from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus

            for _ in range(30):
                with SessionLocal() as s:
                    d = s.get(Dataset, dataset_id)
                    if d and d.status == DatasetStatus.ready:
                        break
                time.sleep(1)
            with SessionLocal() as s:
                sim = s.get(Simulation, simulation_id)
                if sim:
                    sim.status = SimulationStatus.running
                    s.commit()
            chunk_results = [{"chunk_index": i, "value": i * 2} for i in range(chunks)]
            with SessionLocal() as s:
                sim = s.get(Simulation, simulation_id)
                if sim:
                    sim.result = {
                        "chunks": chunk_results,
                        "total": sum(c["value"] for c in chunk_results),
                        "chunk_count": chunks,
                    }
                    sim.status = SimulationStatus.completed
                    s.commit()

        threading.Thread(target=_process, daemon=True).start()


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
    """FastAPI TestClient with the DB engine patched to use the test database.

    Overrides get_scheduler with StubScheduler so tests don't need a real
    Temporal server.
    """
    import simapp.db as db_module
    import simapp.temporal_workflows as tasks_module

    test_engine = db_engine
    test_session_factory = sessionmaker(bind=test_engine, expire_on_commit=False)

    monkeypatch.setattr(db_module, "engine", test_engine)
    monkeypatch.setattr(db_module, "SessionLocal", test_session_factory)
    monkeypatch.setattr(tasks_module, "SessionLocal", test_session_factory, raising=False)

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

    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


@pytest.fixture(scope="function")
def gap_client(db_engine, monkeypatch) -> Generator[TestClient, None]:
    """TestClient WITHOUT the scheduler override.

    The gap test patches the real TemporalScheduler._ensure_client, so it
    needs the real scheduler wired in (not the StubScheduler).
    """
    import simapp.db as db_module
    import simapp.temporal_workflows as tasks_module

    test_engine = db_engine
    test_session_factory = sessionmaker(bind=test_engine, expire_on_commit=False)

    monkeypatch.setattr(db_module, "engine", test_engine)
    monkeypatch.setattr(db_module, "SessionLocal", test_session_factory)
    monkeypatch.setattr(tasks_module, "SessionLocal", test_session_factory, raising=False)

    from simapp.deps import get_session
    from simapp.main import app

    def _test_get_session():
        session = test_session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_session] = _test_get_session

    with TestClient(app, raise_server_exceptions=False) as c:
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
