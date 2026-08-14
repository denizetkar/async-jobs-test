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
    """Test-only scheduler that processes inline in background threads.

    SessionLocal is imported at call time inside each method so it resolves
    to the monkeypatched test-DB session factory (conftest patches
    simapp.db.SessionLocal before the stub runs).
    """

    def schedule_dataset_processing(self, session, dataset_id, filename) -> None:
        import threading
        import time

        from simapp.models import Dataset, DatasetStatus

        def _process() -> None:
            time.sleep(2)
            from simapp.db import SessionLocal

            with SessionLocal() as s:
                d = s.get(Dataset, dataset_id)
                if d is not None:
                    d.status = DatasetStatus.ready
                    s.commit()

        threading.Thread(target=_process, daemon=True).start()

    def schedule_simulation(self, session, simulation_id, dataset_id, parameters) -> None:
        import threading
        import time

        from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus

        chunks = parameters.get("chunks", 4)

        def _process() -> None:
            for _ in range(30):
                from simapp.db import SessionLocal

                with SessionLocal() as s:
                    d = s.get(Dataset, dataset_id)
                    if d is not None and d.status == DatasetStatus.ready:
                        break
                time.sleep(1)
            from simapp.db import SessionLocal

            with SessionLocal() as s:
                sim = s.get(Simulation, simulation_id)
                if sim is not None:
                    sim.status = SimulationStatus.running
                    s.commit()
            chunk_results = [{"chunk_index": i, "value": i * 2} for i in range(chunks)]
            with SessionLocal() as s:
                sim = s.get(Simulation, simulation_id)
                if sim is not None:
                    sim.result = {
                        "chunks": chunk_results,
                        "total": sum(c["value"] for c in chunk_results),
                        "chunk_count": chunks,
                    }
                    sim.status = SimulationStatus.completed
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

    from simapp import models  # noqa: F401
    from simapp.db import Base

    Base.metadata.create_all(engine)
    _apply_engine_schema(engine)

    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def dbos_launched(db_engine):
    """Ensure DBOS system schema is migrated for transactional tests."""
    import simapp.tasks_dbos  # noqa: F401
    from dbos import DBOS
    from simapp.dbos_config import dbos
    dbos.launch()
    DBOS.register_queue("sim_queue", concurrency=10)
    yield
    dbos.destroy()


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
    import simapp.tasks_dbos as tasks_module

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
    import tempfile
    import time

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
