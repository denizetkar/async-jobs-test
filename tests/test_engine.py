"""Unit tests for DBOS engine — sandbox-safe via socket probe."""
import socket
import sys

import pytest


def _env_text(env: dict | list | None) -> str:
    if isinstance(env, dict):
        return "\n".join(f"{k}={v}" for k, v in env.items())
    return "\n".join(env or [])

def _db_reachable() -> bool:
    try:
        socket.create_connection(("localhost", 5432), timeout=1).close()
        return True
    except OSError:
        return False

dbos_sdk = pytest.importorskip("dbos")

if not _db_reachable():
    pytest.skip(
        "DBOS engine tests require Postgres at localhost:5432",
        allow_module_level=True,
    )

# Only executed when DB is reachable (WSL2 verify lane):

def test_dbos_tasks_import() -> None:
    from simapp.tasks_dbos import (
        _postprocess_step,
        _preprocess_step,
        _simulate_chunk_wf,
        process_dataset_wf,
        simulation_wf,
    )

    for fn in (
        process_dataset_wf,
        simulation_wf,
        _preprocess_step,
        _simulate_chunk_wf,
        _postprocess_step,
    ):
        assert callable(fn)

def test_dbos_scheduler_instantiation() -> None:
    from simapp.tasks_dbos import DBOSScheduler

    assert DBOSScheduler() is not None

def test_deps_returns_dbos_scheduler() -> None:
    from simapp.deps import get_scheduler
    from simapp.tasks_dbos import DBOSScheduler

    assert isinstance(get_scheduler(), DBOSScheduler)

def test_docker_compose_worker_command() -> None:
    import pathlib

    import yaml

    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    assert cfg["services"]["worker"]["command"] == (
        "uv run python -m simapp.dbos_worker"
    )
    env = _env_text(cfg["services"]["worker"].get("environment"))
    assert "postgresql+psycopg://" in env
    assert "psycopg2" not in env


def test_dbos_worker_imports_tasks_module() -> None:
    """Regression: dbos_worker must import tasks_dbos before launch so the
    Queue and @DBOS.workflow registrations are in place when launch runs."""

    assert "simapp.tasks_dbos" in sys.modules


def test_dbos_config_uses_single_db() -> None:
    """Regression: enqueue_in_transaction requires the session to target the
    system DB; both URLs must point at the same database."""
    from simapp.dbos_config import _db_url, _system_db_url

    assert _system_db_url == _db_url


def test_dbos_uses_set_event_get_event() -> None:
    """Regression: set_event/get_event pair used for dataset readiness signaling."""
    import simapp.tasks_dbos as mod
    source = open(mod.__file__).read()
    assert "DBOS.set_event" in source, "process_dataset_wf must call DBOS.set_event('ready', ...)"
    assert "DBOS.get_event" in source, "simulation_wf must call DBOS.get_event(...) instead of polling"
    assert "_simulate_chunk_wf" in source, "Chunk should be a workflow, not a step"


def test_db_engine_no_destroy() -> None:
    """Regression: dbos.destroy() must not be called in db_engine fixture.

    The dbos_launched fixture's teardown may call dbos.destroy() — that's
    correct (it runs at session end after all tests). This test checks only
    the db_engine fixture body.
    """
    import ast
    import pathlib

    source = pathlib.Path("tests/conftest.py").read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "db_engine":
            func_source = ast.get_source_segment(source, node)
            assert func_source is not None
            assert "dbos.destroy" not in func_source, (
                "dbos.destroy() must not be called in db_engine — it kills the runtime mid-session"
            )
            return
    assert False, "db_engine fixture not found in conftest.py"
