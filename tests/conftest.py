"""Shared pytest fixtures.

Tests use FastAPI's TestClient (sync) which runs the app in-process.
A live Postgres is required — these are integration tests, not unit tests.
Set SIMAPP_DATABASE_URL to point at a test database.
"""

from __future__ import annotations

import importlib.util
import os
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

TEST_DATABASE_URL = os.environ.get(
    "SIMAPP_TEST_DATABASE_URL",
    "postgresql+psycopg://simapp:simapp@localhost:5432/simapp_test",
)


def _apply_engine_schema(engine) -> None:
    """Apply engine-specific schema via post_migrate.py hook if present."""
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
    engine = create_engine(TEST_DATABASE_URL, echo=False, pool_pre_ping=True)

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

    test_engine = db_engine
    test_session_factory = sessionmaker(bind=test_engine, expire_on_commit=False)

    monkeypatch.setattr(db_module, "engine", test_engine)
    monkeypatch.setattr(db_module, "SessionLocal", test_session_factory)

    from simapp.main import app

    with TestClient(app) as c:
        yield c


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
