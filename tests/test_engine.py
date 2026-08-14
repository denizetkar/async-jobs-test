"""Unit tests for Outbox+CDC engine — no kafka/debezium needed."""
import pathlib
import re

import yaml

def _env_text(env: dict | list | None) -> str:
    if isinstance(env, dict):
        return "\n".join(f"{k}={v}" for k, v in env.items())
    return "\n".join(env or [])

def test_outbox_imports() -> None:
    from simapp.outbox_consumer import main as consumer_main
    from simapp.outbox_scheduler import OutboxScheduler

    assert callable(consumer_main)

def test_outbox_table_in_models() -> None:
    from simapp.models import OutboxEvent

    assert "outbox_events" in OutboxEvent.metadata.tables

def test_outbox_table_in_migrations() -> None:
    mig = pathlib.Path("migrations/versions/0001_initial.py").read_text()
    assert re.search(r"create_table\(\s*['\"]outbox_events['\"]", mig)
    assert re.search(r"drop_table\(\s*['\"]outbox_events['\"]", mig)

def test_deps_returns_outbox_scheduler() -> None:
    from simapp.deps import get_scheduler
    from simapp.outbox_scheduler import OutboxScheduler

    assert isinstance(get_scheduler(), OutboxScheduler)

def test_docker_compose_consumer_command_and_infra() -> None:
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    assert cfg["services"]["worker"]["command"] == (
        "uv run python -m simapp.outbox_consumer"
    )
    env = _env_text(cfg["services"]["worker"].get("environment"))
    assert "SIMAPP_DATABASE_URL" in env
    assert "postgresql+psycopg://" in env
    assert "psycopg2" not in env
    for svc in ("kafka", "connect"):
        assert svc in cfg["services"]
    assert "zookeeper" not in cfg["services"]


def test_docker_compose_kafka_kraft_mode() -> None:
    """Kafka runs in KRaft mode (no Zookeeper): the service env must carry the
    KRaft controller role and a cluster id."""
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    env = _env_text(cfg["services"]["kafka"].get("environment"))
    assert "NODE_ROLE" in env
    assert "CLUSTER_ID" in env


def test_debezium_connector_routes_to_single_topic() -> None:
    """All outbox events route to one topic so the consumer needs a single
    subscription regardless of aggregate_type."""
    from scripts.register_debezium_connector import build_connector_config

    config = build_connector_config()
    assert config["transforms.outbox.route.topic.replacement"] == "simapp.outbox_events"


def test_outbox_consumer_bootstrap_env_override(monkeypatch) -> None:
    """D3a regression: KAFKA_BOOTSTRAP_SERVERS env overrides hardcoded localhost.

    The consumer must read the env var at call time so monkeypatch works;
    a module-level constant would freeze the value at import.
    """
    captured: dict = {}

    class _FakeConsumer:
        def __init__(self, *args, **kwargs) -> None:
            captured.update(kwargs)

        def __iter__(self):
            return iter([])

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setattr("simapp.outbox_consumer.KafkaConsumer", _FakeConsumer)

    from simapp.outbox_consumer import main as consumer_main

    consumer_main()  # terminates immediately: fake consumer yields no messages
    assert captured["bootstrap_servers"] == "kafka:9092"


def test_compose_has_debezium_init_and_connect_healthcheck() -> None:
    """D3b regression: connect service healthcheck + one-shot debezium-init
    service that waits for connect to be healthy before registering."""
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    assert "healthcheck" in cfg["services"]["connect"]
    init = cfg["services"]["debezium-init"]
    assert init["command"] == "uv run python scripts/register_debezium_connector.py"
    assert "CONNECT_URL" in _env_text(init.get("environment"))
    assert init["depends_on"]["connect"]["condition"] == "service_healthy"


def test_conftest_imports_engine_task_module_for_this_branch() -> None:
    """Regression for ModuleNotFoundError cascade: conftest must import THIS
    branch's engine module (outbox_consumer), never main's simapp.tasks."""
    import importlib

    mod = importlib.import_module("tests.conftest")
    import simapp.outbox_consumer as tasks_module  # the import form conftest must use

    assert hasattr(mod, "client")
    assert hasattr(tasks_module, "SessionLocal"), (
        "conftest's client fixture monkeypatches <engine>.SessionLocal; "
        "the module it imports must define it"
    )


def test_docker_compose_worker_depends_on_connect() -> None:
    """Regression: worker must depend on connect+debezium-init for CDC chain."""
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    worker_deps = cfg["services"]["worker"].get("depends_on", {})
    assert "connect" in worker_deps, "Worker must depend on connect"
    assert "debezium-init" in worker_deps, "Worker must depend on debezium-init"


def test_connect_healthcheck_not_python() -> None:
    """Regression: connect healthcheck must not use python (image has no Python)."""
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    hc = cfg["services"]["connect"].get("healthcheck", {})
    test_cmd = str(hc.get("test", ""))
    assert "python" not in test_cmd, "Connect healthcheck must not use python"
    assert "curl" in test_cmd or "wget" in test_cmd, "Connect healthcheck must use curl or wget"


def test_kafka_has_listener_security_protocol_map() -> None:
    """Regression: kafka KRaft needs listener security protocol map."""
    cfg = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text())
    kafka_env = cfg["services"]["kafka"].get("environment", {})
    # Handle both dict and list env forms
    if isinstance(kafka_env, list):
        env_str = " ".join(kafka_env)
        assert "LISTENER_SECURITY_PROTOCOL_MAP" in env_str
    else:
        assert "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" in kafka_env or "LISTENER_SECURITY_PROTOCOL_MAP" in kafka_env
