# async-jobs-test

Async job / workflow engine comparison repo. Each branch implements the same
simulation scenario (upload file -> process -> start simulation with dynamic
chunk fan-out/fan-in) using a different workflow engine.

## Branches

| Branch | Engine | Transactional scheduling | Dynamic DAG |
|---|---|---|---|
| `main` | procrastinate 3.9 | YES in-tx defer() | NO ad-hoc chaining |
| `feat/dbos` | DBOS 2.29 | YES in-tx enqueue | YES plain Python workflows |
| `feat/temporal` | Temporal SDK 1.31 | NO fire-after-commit | YES child workflows |
| `feat/prefect` | Prefect 3.8 | NO fire-after-commit | YES .map() |
| `feat/outbox-cdc` | Debezium 3.6 + Kafka | YES outbox in-tx -> CDC | NO — polling workaround (in-process ThreadPoolExecutor) |

## Quick start

```bash
uv sync
docker compose up -d
bash scripts/demo_scenario.sh
uv run pytest tests/ -v
docker compose down -v
```

See docs/comparison-report.md for the full engine comparison.
