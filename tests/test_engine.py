"""Unit tests for procrastinate engine — no DB connection needed.

Tests connector configuration, task registration, and scheduler instantiation.
"""

from __future__ import annotations

import socket
import time
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from procrastinate.connector import BaseAsyncConnector
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

import simapp.db as db_module
import tests.conftest as conftest
from scripts import post_migrate
from scripts.post_migrate import apply_schema
from simapp.deps import get_scheduler
from simapp.tasks import ProcrastinateScheduler
from simapp.tasks import app as proc_app
from simapp.tasks import connector, postprocess, preprocess, process_dataset, simulate_chunk


def test_procrastinate_app_imports() -> None:
    assert proc_app is not None


def test_worker_importable_from_module_path() -> None:
    """Regression: conftest's inprocess_worker fixture imported
    `Worker` from the top-level `procrastinate` package, where procrastinate
    3.9.0 does not export it (ImportError at fixture setup in Docker E2E).
    The reachable location is the `procrastinate.worker` module."""
    import importlib

    pkg = importlib.import_module("procrastinate")
    assert not hasattr(pkg, "Worker")
    mod = importlib.import_module("procrastinate.worker")
    assert isinstance(mod.Worker, type)


def test_conftest_module_imports_cleanly() -> None:
    """Regression: tests/conftest.py must import without error — the worker
    fixture's ImportError previously killed every DB-fixtured test's setup.
    The fixture only needs imports to succeed; no worker is started here."""
    import importlib

    mod = importlib.import_module("tests.conftest")
    assert callable(mod.inprocess_worker)


def test_tasks_registered() -> None:
    assert callable(process_dataset)
    assert callable(preprocess)
    assert callable(simulate_chunk)
    assert callable(postprocess)


def test_connector_is_async() -> None:
    assert isinstance(connector, BaseAsyncConnector)


def test_connector_conninfo_stripped() -> None:
    conninfo = connector._pool_args.get("conninfo", "")
    assert "+psycopg://" not in conninfo
    assert "+psycopg2://" not in conninfo
    assert conninfo.startswith("postgresql://")


def test_scheduler_instantiates() -> None:
    s = ProcrastinateScheduler()
    assert s is not None


def test_deps_returns_scheduler() -> None:
    s = get_scheduler()
    assert isinstance(s, ProcrastinateScheduler)


def test_post_migrate_exposes_apply_schema() -> None:
    assert callable(apply_schema)


def test_inprocess_worker_fixture_binds_test_database() -> None:
    """Behavioral regression: the in-process worker must serve the TEST db.

    Rationale: without it, dag fixture's uploads land jobs in simapp_test
    while the background worker drains simapp (dev db) — dataset never
    becomes ready; dag tests ERROR at fixture (observed in Docker E2E).
    This test proves the fixture SCOPE and conninfo binding through pytest's
    own fixture discovery machinery (the `_pytestfixturefunction` marker that
    pytest itself uses to collect fixtures) rather than matching source text.
    """
    # pytest >=9 decorates fixtures as FixtureFunctionDefinition carrying
    # `_fixture_function_marker` (scope/params/autouse) instead of the old
    # `_pytestfixturefunction` attribute; drive the same marker pytest's
    # own FixtureManager reads during collection.
    marker_by_name = {
        name: getattr(obj, "_fixture_function_marker", None)
        for name, obj in vars(conftest).items()
        if getattr(obj, "_fixture_function_marker", None) is not None
    }
    assert "inprocess_worker" in marker_by_name, (
        "dag-wait tests must have an in-process worker against the test DB"
    )
    assert marker_by_name["inprocess_worker"].scope == "session", (
        "in-process worker fixture must be session-scoped so the connector "
        "rebind covers every dag-wait test in the session"
    )

    # And the test-database path: the fixture's conninfo must default to the
    # TEST database, not the dev one.
    assert "simapp_test" in conftest.TEST_DATABASE_URL


def test_post_migrate_apply_schema_executes_statements_individually() -> None:
    """Regression: apply_schema must (a) use Result.fetchone(), not
    Connection.fetchone() (AttributeError crash seen in Docker E2E), and
    (b) execute the multi-statement schema SQL one statement at a time,
    keeping $$-quoted PL/pgSQL function bodies intact."""

    class StubResult:
        def __init__(self, rows): self._rows = rows
        def fetchone(self): return self._rows[0] if self._rows else None

    class StubConn:
        def __init__(self, probe_rows):
            self.probe_rows = probe_rows
            self.execs = []
            self.committed = False
        def execute(self, clause):
            sql = str(clause)
            self.execs.append(sql)
            if "pg_tables" in sql:
                return StubResult(self.probe_rows)
            return StubResult([])
        def commit(self): self.committed = True
        def __enter__(self): return self
        def __exit__(self, *a): return False

    class StubEngine:
        def __init__(self, probe_rows): self.conn = StubConn(probe_rows)
        def connect(self): return self.conn

    eng = StubEngine(probe_rows=[(1,)])
    post_migrate.apply_schema(eng)
    assert len(eng.conn.execs) == 1, eng.conn.execs

    eng = StubEngine(probe_rows=[])
    post_migrate.apply_schema(eng)
    ddl = [s for s in eng.conn.execs[1:] if s.strip()]
    assert len(ddl) > 1, "multi-statement schema must be split into individual executions"
    assert eng.conn.committed


def test_dataset_processing_uses_per_dataset_queueing_lock(monkeypatch) -> None:
    """Regression for global 'process_dataset' queueing lock serializing all
    uploads (AlreadyEnqueued on overlapping uploads, seen in Docker E2E)."""
    captured = {}

    class FakeTask:
        def configure(self, **kwargs):
            captured.update(kwargs)
            return self
        def defer(self, **kwargs):
            captured.setdefault("deferred", kwargs)

    monkeypatch.setattr("simapp.tasks.process_dataset", FakeTask())

    session = MagicMock()
    session.connection.return_value.connection = MagicMock()

    scheduler = ProcrastinateScheduler()
    dsid = uuid4()
    scheduler.schedule_dataset_processing(session=session, dataset_id=dsid, filename="x.csv")

    lock = captured.get("queueing_lock", "")
    assert str(dsid) in lock, f"queueing lock must be per-dataset, got {lock!r}"
    assert lock != "process_dataset", "global lock must not be used"


def test_verify_script_writes_artifact_log_for_failed_stage(
    tmp_path, monkeypatch
) -> None:
    """Behavioral regression: verify.py must persist each failed stage's FULL
    log to CWD-relative .omo/verify-artifacts/*.log files.

    Rationale: the failing stage's log was only printed via `log[:2000]` on
    stdout, which truncated the pytest error detail and made the traceback
    unobtainable. This test drives the real verify() flow with stubbed
    git/docker calls (one stage forced to fail) and asserts the artifact
    exists and carries the failing stage's complete log.
    """
    import subprocess

    import typer

    import scripts.verify as verify_mod

    marker = "PYTEST_STAGE_FAILURE_LOG_MARKER"
    ok = lambda: verify_mod.StageResult("ok", True, "")  # noqa: E731
    for fn_name in (
        "stage_compose_build",
        "stage_postgres",
        "stage_migrations",
        "stage_post_migrate",
        "stage_server",
        "stage_worker",
        "stage_demo",
        "stage_test_db",
    ):
        monkeypatch.setattr(verify_mod, fn_name, ok)
    monkeypatch.setattr(
        verify_mod,
        "stage_pytest",
        lambda: verify_mod.StageResult("pytest", False, marker),
    )
    monkeypatch.setattr(
        verify_mod,
        "run",
        lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0, "", ""),
    )
    monkeypatch.setattr(
        verify_mod,
        "compose",
        lambda *a: subprocess.CompletedProcess(a, 0, "", ""),
    )
    monkeypatch.chdir(tmp_path)

    # Failed stage must keep exit-code semantics (typer.Exit(1)).
    with pytest.raises(typer.Exit) as excinfo:
        verify_mod.verify()
    assert excinfo.value.exit_code == 1

    artifacts = list((tmp_path / ".omo" / "verify-artifacts").glob("*.log"))
    assert len(artifacts) == 1, artifacts
    assert marker in artifacts[0].read_text()


def test_conftest_inprocess_worker_log_path_is_durable() -> None:
    """Behavioral regression: the in-process worker's log file must resolve
    under tests/ (mounted into the run container == host ./tests), not the
    worker thread's CWD (/app in the throwaway container) where the file
    vanished when the container exited."""
    assert conftest.LOG_PATH.name == "inprocess_worker.log"
    assert conftest.LOG_PATH.parent == Path(conftest.__file__).parent


def _test_db_reachable() -> bool:
    try:
        socket.create_connection(("localhost", 5432), timeout=1).close()
        return True
    except OSError:
        return False


requires_test_db = pytest.mark.skipif(
    not _test_db_reachable(), reason="test DB not reachable"
)


@requires_test_db
def test_inprocess_worker_processes_job_from_test_db(
    db_engine, inprocess_worker, monkeypatch
) -> None:
    """End-to-end behavioral proof: a job deferred to the TEST db is consumed
    by the in-process worker and its side effect lands in the TEST db.

    The `inprocess_worker` fixture holds `app.replace_connector` for its whole
    lifetime, so a bare `defer()` (no connection kwarg) enqueues through the
    app's CURRENT connector — the test-DB one. `SessionLocal` is patched to
    the test engine so the worker's task body also writes to the test db
    (same wiring the `client` fixture uses). Runs in the Docker/WSL2 lane;
    skipped in the sandbox where localhost:5432 is unreachable.
    """
    import simapp.tasks as tasks_module

    test_session_factory = sessionmaker(bind=db_engine, expire_on_commit=False)
    monkeypatch.setattr(db_module, "SessionLocal", test_session_factory)
    monkeypatch.setattr(tasks_module, "SessionLocal", test_session_factory)

    with db_engine.connect() as conn:
        conn.execute(
            text(
                "INSERT INTO datasets (id, filename, status) "
                "VALUES (gen_random_uuid(), 't.csv', 'pending')"
            )
        )
        conn.commit()
        dsid = conn.execute(
            text("SELECT id FROM datasets WHERE filename = 't.csv'")
        ).fetchone()[0]

    process_dataset.configure(queueing_lock=f"process_dataset:{dsid}").defer(
        dataset_id=str(dsid), filename="t.csv"
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        with db_engine.connect() as conn:
            status = conn.execute(
                text("SELECT status FROM datasets WHERE id = :i"), {"i": dsid}
            ).fetchone()[0]
        if status == "ready":
            return
        time.sleep(1)
    raise AssertionError("in-process worker did not process job from test db in 30s")
