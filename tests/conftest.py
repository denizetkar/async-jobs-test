"""Shared pytest fixtures — procrastinate-specific overrides for main branch."""

from __future__ import annotations

import importlib.util
import os
import threading
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
TEST_CONNINFO = TEST_DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")

# Host-visible worker log: ./tests is mounted into the run container, so
# tests/inprocess_worker.log survives the throwaway container's exit;
# a CWD-relative path would land in /app and be lost.
LOG_PATH = Path(__file__).parent / "inprocess_worker.log"


def _apply_engine_schema(engine) -> None:
    """Apply procrastinate schema via post_migrate.py hook."""
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


@pytest.fixture(scope="session")
def inprocess_worker(db_engine):
    """Run a procrastinate worker in-process against the TEST database.

    The Docker background worker reads jobs from the DEV database (simapp).
    Tests that enqueue jobs into simapp_test (via the patched app) therefore
    need a worker on the same database or the jobs never run. This fixture
    rebinds simapp.tasks.app to a test-DB connector
    (`App.replace_connector`, which also swaps `self.job_manager.connector`),
    launches an explicitly built `procrastinate.Worker` in a daemon thread
    so it can be stopped from the session thread via `Worker.stop()`
    (thread-safe by design, see worker.py `Worker.stop`), and restores the
    connector at session end. MUST run after db_engine applies the schema,
    which the fixture dependency guarantees.
    """
    # Ordering dependency only: db_engine applies the procrastinate schema
    # before the worker starts.
    _ = db_engine

    import asyncio
    import traceback

    from procrastinate import PsycopgConnector
    from procrastinate.worker import Worker  # procrastinate/__init__ does not export Worker

    from simapp.tasks import app

    connector = PsycopgConnector(conninfo=TEST_CONNINFO)
    # Open the app's own connector pool so bare defer() calls (no
    # connection= kwarg — e.g. task chaining inside a running worker)
    # enqueue through the app's conninfo database (= TEST db in the
    # pytest lane) instead of raising AppNotOpen.
    app.open()
    # with_connector returns a NEW App sharing the task registry (app.py:
    # with_connector). The worker must NOT become the app's current connector
    # via replace_connector: once a PsycopgConnector is opened async,
    # get_sync_connector() returns the ASYNC connector (psycopg_connector.py
    # `if self._async_pool: return self`), and defer(connection=fairy).defer()
    # then bridges into the async cursor path, which raises
    # TypeError: 'Cursor' object does not support the asynchronous context
    # manager protocol on the sync SQLAlchemy connection (psycopg_connector.py
    # `_get_cursor`). Keeping the original app connector unopened keeps
    # defer-with-connection on the sync bridge (lazily created
    # SyncPsycopgConnector), while this worker still drains the TEST db
    # (both conninfos target the same TEST database in the pytest lane).
    worker_app = app.with_connector(connector)
    started = threading.Event()
    startup_error: list[BaseException] = []
    worker_holder: list[Worker] = []

    async def _serve() -> None:
        async with worker_app.open_async():
            worker = Worker(
                app=worker_app,
                install_signal_handlers=False,
                fetch_job_polling_interval=1.0,
            )
            worker_holder.append(worker)
            started.set()
            try:
                await worker.run()
            finally:
                worker_holder.clear()

    def _run() -> None:
        # Dedicated log file: caplog interleaves worker output across the
        # whole session; a session-scoped file makes startup failures debuggable.
        with open(LOG_PATH, "w", buffering=1) as log:
            try:
                import logging as _logging

                handler = _logging.StreamHandler(log)
                _logging.getLogger("procrastinate").addHandler(handler)
                asyncio.run(_serve())
            except BaseException as exc:  # noqa: BLE001
                traceback.print_exception(exc, file=log)
                startup_error.append(exc)
                started.set()  # unblock the setup thread

    worker_thread = threading.Thread(target=_run, name="inprocess-worker", daemon=True)
    worker_thread.start()

    if not started.wait(timeout=15):
        pytest.fail(f"inprocess_worker startup timed out; see {LOG_PATH}")
    if startup_error:
        pytest.fail(
            f"inprocess_worker failed to start: {startup_error[0]!r}; "
            f"see {LOG_PATH}"
        )
    worker = worker_holder[0]

    try:
        yield worker
    finally:
        # Worker.stop() is thread-safe (worker.py): from this thread it uses
        # self._loop.call_soon_threadsafe to wake the worker loop.
        worker.stop()
        worker_thread.join(timeout=30)
        if worker_thread.is_alive():
            pytest.fail(f"inprocess_worker did not stop cleanly; see {LOG_PATH}")


@pytest.fixture(scope="function")
def db_session(db_engine, inprocess_worker) -> Generator[Session, None]:
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
def client(db_engine, inprocess_worker, monkeypatch) -> Generator[TestClient, None]:
    """FastAPI TestClient with the DB engine patched to use the test database."""
    import simapp.db as db_module
    import simapp.tasks as tasks_module

    test_engine = db_engine
    test_session_factory = sessionmaker(bind=test_engine, expire_on_commit=False)

    monkeypatch.setattr(db_module, "engine", test_engine)
    monkeypatch.setattr(db_module, "SessionLocal", test_session_factory)
    monkeypatch.setattr(tasks_module, "SessionLocal", test_session_factory)

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
