"""Unit tests for common code — no DB connection needed.

Tests imports, model definitions, schema validation, config, and API structure.
Engine-specific tests live on each engine branch.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Syntax validation
# ---------------------------------------------------------------------------

def test_all_python_files_parse() -> None:
    root = Path(".")
    for f in root.rglob("*.py"):
        if ".venv" in f.parts or "__pycache__" in f.parts:
            continue
        ast.parse(f.read_text(), filename=str(f))


def test_conftest_imports_engine_task_module_for_this_branch() -> None:
    """Regression for ModuleNotFoundError cascade (verify 2026-08-14: every
    client-fixture test ERROR'd because conftest imported simapp.tasks, a main-only
    module; this branch's engine module is tasks_dbos)."""
    import importlib

    # conftest module itself must import cleanly in sandbox...
    mod = importlib.import_module("tests.conftest")
    # ... and its client fixture's wiring must resolve against THIS branch's engine
    # module: drive the fixture's module-resolution by checking the engine module's
    # SessionLocal attribute exists (the symbol the fixture monkeypatches).
    import simapp.tasks_dbos as tasks_module  # the import form conftest now uses

    assert hasattr(mod, "client")
    assert hasattr(tasks_module, "SessionLocal"), (
        "conftest's client fixture monkeypatches <engine>.SessionLocal; "
        "the module it imports must define it"
    )


# ---------------------------------------------------------------------------
# Import checks
# ---------------------------------------------------------------------------

def test_main_app_imports() -> None:
    from simapp.main import app
    assert app.title == "SimApp"


def test_models_import() -> None:
    from simapp.models import Dataset, Simulation, DatasetStatus, SimulationStatus
    assert DatasetStatus.pending == "pending"
    assert DatasetStatus.ready == "ready"
    assert DatasetStatus.failed == "failed"
    assert SimulationStatus.pending == "pending"
    assert SimulationStatus.running == "running"
    assert SimulationStatus.completed == "completed"
    assert SimulationStatus.failed == "failed"


def test_scheduler_protocol_import() -> None:
    from simapp.scheduler import SimulationScheduler
    assert SimulationScheduler is not None


def test_schemas_import() -> None:
    from simapp.schemas import DatasetResponse, SimulationRequest, SimulationResponse
    assert DatasetResponse is not None
    assert SimulationRequest is not None
    assert SimulationResponse is not None


def test_config_uses_psycopg3() -> None:
    from simapp.config import settings
    assert "+psycopg://" in settings.database_url
    assert "+psycopg2://" not in settings.database_url


def test_db_module_import() -> None:
    from simapp.db import Base, SessionLocal, engine, get_session
    assert Base is not None
    assert SessionLocal is not None
    assert engine is not None
    assert get_session is not None


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def test_simulation_request_validates() -> None:
    from simapp.schemas import SimulationRequest
    req = SimulationRequest(
        dataset_id="00000000-0000-0000-0000-000000000000",
        parameters={"chunks": 4},
    )
    assert req.parameters.chunks == 4


def test_dataset_response_serializes() -> None:
    from simapp.schemas import DatasetResponse
    assert DatasetResponse.model_json_schema()["type"] == "object"


def test_simulation_response_serializes() -> None:
    from simapp.schemas import SimulationResponse
    assert SimulationResponse.model_json_schema()["type"] == "object"


# ---------------------------------------------------------------------------
# Script checks
# ---------------------------------------------------------------------------

def test_verify_script_imports() -> None:
    from scripts.verify import app
    assert app is not None


def test_demo_script_imports() -> None:
    from scripts.demo import app
    assert app is not None
