"""Unit tests for Temporal engine — no server needed."""
import asyncio
import importlib
import pathlib

import pytest
import yaml

def _env_text(env: dict | list | None) -> str:
    if isinstance(env, dict):
        return "\n".join(f"{k}={v}" for k, v in env.items())
    return "\n".join(env or [])

def test_temporal_imports() -> None:
    from simapp.temporal_client import TemporalScheduler
    from simapp.temporal_worker import main
    from simapp.temporal_workflows import (
        DatasetProcessWorkflow,
        SimulationWorkflow,
    )

    assert callable(main)
    assert DatasetProcessWorkflow is not None
    assert SimulationWorkflow is not None

def test_temporal_scheduler_instantiation() -> None:
    from simapp.temporal_client import TemporalScheduler

    assert TemporalScheduler() is not None

def test_deps_returns_temporal_scheduler() -> None:
    from simapp.deps import get_scheduler
    from simapp.temporal_client import TemporalScheduler

    assert isinstance(get_scheduler(), TemporalScheduler)

def test_docker_compose_worker_command() -> None:
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    assert cfg["services"]["worker"]["command"] == (
        "uv run python -m simapp.temporal_worker"
    )
    env = _env_text(cfg["services"]["worker"].get("environment"))
    assert "postgresql+psycopg://" in env
    assert "psycopg2" not in env
    assert "temporal" in cfg["services"]

def test_temporal_address_env_override(monkeypatch) -> None:
    """D2 regression: TEMPORAL_ADDRESS env must override the hardcoded localhost."""
    monkeypatch.setenv("TEMPORAL_ADDRESS", "temporal:7233")
    from simapp.temporal_client import TemporalScheduler
    assert TemporalScheduler()._temporal_address == "temporal:7233"
    monkeypatch.delenv("TEMPORAL_ADDRESS")
    assert TemporalScheduler()._temporal_address == "localhost:7233"

def test_temporal_worker_reads_env_at_call_time(monkeypatch) -> None:
    """D2 regression part 2: worker connect target is env-driven at call time."""
    called = {}

    class FakeClient:
        @classmethod
        async def connect(cls, address, namespace="default"):
            called["address"] = address
            raise RuntimeError("stop-after-capture")

    monkeypatch.setenv("TEMPORAL_ADDRESS", "temporal:7233")
    monkeypatch.setattr("simapp.temporal_worker.Client", FakeClient)
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        asyncio.run(__import__("simapp.temporal_worker", fromlist=["main"]).main())
    assert called["address"] == "temporal:7233"

def test_docker_compose_temporal_address_env() -> None:
    """D2 regression part 3: server+worker compose env must set TEMPORAL_ADDRESS."""
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    for svc in ("server", "worker"):
        env = _env_text(cfg["services"][svc].get("environment"))
        assert "TEMPORAL_ADDRESS" in env, f"{svc} missing TEMPORAL_ADDRESS"

def test_docker_compose_uses_auto_setup_image() -> None:
    """Temporal service must use the auto-setup image (handles schema + namespace)."""
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    image = cfg["services"]["temporal"]["image"]
    assert image.startswith("temporalio/auto-setup"), (
        f"Expected temporalio/auto-setup image, got {image}"
    )
    env = _env_text(cfg["services"]["temporal"].get("environment"))
    assert "POSTGRES_SEEDS=temporal-postgres" in env
    assert "POSTGRES_USER=temporal" in env
    assert "DB=postgres12" in env

def test_temporal_worker_has_activity_executor() -> None:
    """Regression: Worker must have activity_executor for sync activities."""
    import simapp.temporal_worker as mod
    source = open(mod.__file__).read()
    assert "ThreadPoolExecutor" in source, "Worker must import ThreadPoolExecutor"
    assert "activity_executor" in source, "Worker must pass activity_executor"

def test_conftest_imports_engine_task_module_for_this_branch() -> None:
    """conftest must import this branch's engine module (temporal_workflows)."""
    mod = importlib.import_module("tests.conftest")
    import simapp.temporal_workflows as tasks_module
    assert hasattr(mod, "client")
    assert hasattr(tasks_module, "DatasetProcessWorkflow")


def test_temporal_workflows_no_module_level_db_import() -> None:
    """Regression: module-level DB import triggers Settings() in Temporal sandbox."""
    import simapp.temporal_workflows as mod
    assert "SessionLocal" not in mod.__dict__, "SessionLocal must not be at module level"
