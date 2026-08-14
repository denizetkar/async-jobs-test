"""Unit tests for Prefect engine — no API server needed."""
import pathlib

import yaml

def _env_text(env: dict | list | None) -> str:
    if isinstance(env, dict):
        return "\n".join(f"{k}={v}" for k, v in env.items())
    return "\n".join(env or [])

def test_prefect_imports() -> None:
    from simapp.prefect_flows import process_dataset_flow, simulation_flow
    from simapp.prefect_scheduler import PrefectScheduler

    assert callable(process_dataset_flow)
    assert callable(simulation_flow)

def test_prefect_scheduler_instantiation() -> None:
    from simapp.prefect_scheduler import PrefectScheduler

    assert PrefectScheduler() is not None

def test_deps_returns_prefect_scheduler() -> None:
    from simapp.deps import get_scheduler
    from simapp.prefect_scheduler import PrefectScheduler

    assert isinstance(get_scheduler(), PrefectScheduler)

def test_docker_compose_env_and_command() -> None:
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    assert cfg["services"]["worker"]["command"] == (
        "uv run python -m simapp.prefect_worker"
    )
    for svc in ("server", "worker"):
        env = _env_text(cfg["services"][svc].get("environment"))
        assert "postgresql+psycopg://" in env
        assert "psycopg2" not in env
        assert "PREFECT_API_URL" in env


def test_conftest_imports_engine_task_module_for_this_branch() -> None:
    """Regression for ModuleNotFoundError cascade: conftest must import THIS
    branch's engine module (prefect_flows), never main's simapp.tasks."""
    import importlib

    mod = importlib.import_module("tests.conftest")
    import simapp.prefect_flows as tasks_module  # the import form conftest now uses

    assert hasattr(mod, "client")
    assert hasattr(tasks_module, "_get_session_factory"), (
        "conftest's client fixture patches the engine module; "
        "prefect_flows must define _get_session_factory (per-task session factory)"
    )


def test_prefect_flows_no_module_level_session() -> None:
    """Regression: module-level SessionLocal causes cloudpickle RLock error."""
    import simapp.prefect_flows as mod

    assert "SessionLocal" not in mod.__dict__, "SessionLocal must not be at module level"
    source = pathlib.Path(mod.__file__).read_text()
    assert "from prefect import wait" not in source, "from prefect import wait is bogus"


def test_prefect_scheduler_uses_run_deployment() -> None:
    """schedule_dataset_processing must trigger the flow via run_deployment
    with timeout=0 (fire-and-forget), not a direct flow call."""
    from unittest.mock import MagicMock, patch
    from uuid import uuid4

    from simapp.prefect_scheduler import PrefectScheduler

    session = MagicMock()
    dataset_id = uuid4()
    filename = "test.csv"

    with patch("simapp.prefect_scheduler.run_deployment") as mock_run:
        scheduler = PrefectScheduler()
        scheduler.schedule_dataset_processing(
            session=session,
            dataset_id=dataset_id,
            filename=filename,
        )

    session.commit.assert_called_once()
    mock_run.assert_called_once()
    _, kwargs = mock_run.call_args
    assert kwargs["name"] == "process_dataset_flow/process-dataset-deployment"
    assert kwargs["parameters"] == {
        "dataset_id": str(dataset_id),
        "filename": filename,
    }
    assert kwargs["timeout"] == 0


def test_prefect_worker_serves_both_flows() -> None:
    """Regression: both flows must be served (not just the first blocking one)."""
    import pathlib
    source = pathlib.Path("src/simapp/prefect_worker.py").read_text()
    assert "process_dataset_flow" in source, "Worker must serve process_dataset_flow"
    assert "simulation_flow" in source, "Worker must serve simulation_flow"
    assert source.count("to_deployment") >= 2, "Must build both deployments via to_deployment"
    assert "serve(" in source, "Must serve deployments via serve()"
