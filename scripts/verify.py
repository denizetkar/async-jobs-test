#!/usr/bin/env python3
"""Runtime verification for async-jobs-test branches.

Usage:
    uv run python scripts/verify.py

Replaces the bash verify.sh with proper health checks, retries, and
structured output. Requires: docker compose v2, uv. Run on the branch to verify.
"""

from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import typer


@dataclass
class StageResult:
    name: str
    passed: bool
    log: str = ""


app = typer.Typer()


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


def compose(*args: str) -> subprocess.CompletedProcess:
    return run(["docker", "compose", *args])


def wait_healthy(service: str, timeout: int = 30) -> StageResult:
    for _ in range(timeout):
        r = compose("ps", service, "--format", "{{.Health}}")
        if r.returncode == 0 and r.stdout.strip() == "healthy":
            return StageResult(f"{service} healthy", True)
        time.sleep(1)
    return StageResult(f"{service} healthy", False, compose("logs", service).stdout)


def wait_http(url: str, timeout: int = 30) -> StageResult:
    for _ in range(timeout):
        try:
            r = httpx.get(f"{url}/health", timeout=2)
            if r.status_code == 200:
                return StageResult("server healthy", True)
        except Exception:
            pass
        time.sleep(1)
    return StageResult("server healthy", False, f"no 200 from {url}")


def run_stage(name: str, cmd: list[str], cwd: Path | None = None) -> StageResult:
    r = run(cmd, cwd=cwd)
    log = f"$ {' '.join(cmd)}\n{r.stdout}{r.stderr}"
    return StageResult(name, r.returncode == 0, log)


def stage_compose_build() -> StageResult:
    return run_stage("compose build", ["docker", "compose", "build"])


def stage_postgres() -> StageResult:
    r = compose("up", "-d", "postgres")
    if r.returncode != 0:
        return StageResult("postgres up", False, r.stderr)
    return wait_healthy("postgres")


def stage_migrations() -> StageResult:
    return run_stage("alembic migrations", [
        "docker", "compose", "run", "--rm", "--no-deps",
        "-e", "SIMAPP_DATABASE_URL=postgresql+psycopg://simapp:simapp@postgres:5432/simapp",
        "server", "uv", "run", "alembic", "upgrade", "head",
    ])


def stage_post_migrate() -> StageResult:
    hook = Path("scripts/post_migrate.py")
    if not hook.exists():
        return StageResult("post-migrate hook", True, "no hook, skipped")
    return run_stage("post-migrate hook", [
        "docker", "compose", "run", "--rm", "--no-deps",
        "-e", "SIMAPP_DATABASE_URL=postgresql+psycopg://simapp:simapp@postgres:5432/simapp",
        "server", "uv", "run", "python", "scripts/post_migrate.py",
    ])


def stage_server() -> StageResult:
    r = compose("up", "-d", "server")
    if r.returncode != 0:
        return StageResult("server up", False, r.stderr)
    return wait_http("http://localhost:8000")


def stage_worker() -> StageResult:
    r = compose("up", "-d", "worker")
    if r.returncode != 0:
        return StageResult("worker up", False, r.stderr)
    time.sleep(3)
    r = compose("ps", "worker", "--format", "{{.State}}")
    if r.stdout.strip() == "running":
        return StageResult("worker running", True)
    logs = compose("logs", "worker").stdout
    return StageResult("worker running", False, f"State: {r.stdout}\nLogs:\n{logs}")


def stage_demo() -> StageResult:
    return run_stage("demo scenario", ["uv", "run", "python", "scripts/demo.py", "README.md", "4"])


def stage_test_db() -> StageResult:
    r = compose("exec", "-T", "postgres", "psql", "-U", "simapp", "-d", "postgres",
                "-tAc", "SELECT 1 FROM pg_database WHERE datname='simapp_test'")
    if r.stdout.strip() == "1":
        return StageResult("create test db", True, "already exists")
    r = compose("exec", "-T", "postgres", "psql", "-U", "simapp", "-d", "postgres",
                "-c", "CREATE DATABASE simapp_test")
    return StageResult("create test db", r.returncode == 0, r.stderr)


def stage_pytest() -> StageResult:
    return run_stage("pytest", [
        "docker", "compose", "run", "--rm", "--no-deps",
        "-e", "SIMAPP_DATABASE_URL=postgresql+psycopg://simapp:simapp@postgres:5432/simapp_test",
        "-e", "SIMAPP_TEST_DATABASE_URL=postgresql+psycopg://simapp:simapp@postgres:5432/simapp_test",
        "-e", "SIMAPP_BASE_URL=http://server:8000",
        "server", "uv", "run", "pytest", "tests/", "-v", "--tb=short",
    ])


@app.command()
def verify():
    repo_root = Path(__file__).parent.parent
    results: list[StageResult] = []

    dirty = run(["git", "status", "--porcelain"], cwd=repo_root)
    if dirty.stdout.strip():
        print("ERROR: working tree is dirty. Commit or stash first.", file=sys.stderr)
        raise typer.Exit(1)

    current = run(["git", "branch", "--show-current"], cwd=repo_root).stdout.strip()

    stages: list[tuple[str, object]] = [
        ("build", stage_compose_build),
        ("postgres", stage_postgres),
        ("migrations", stage_migrations),
        ("post-migrate hook", stage_post_migrate),
        ("server", stage_server),
        ("worker", stage_worker),
        ("demo", stage_demo),
        ("test db", stage_test_db),
        ("pytest", stage_pytest),
    ]

    for name, fn in stages:
        result = fn()
        results.append(result)
        status = "PASS" if result.passed else "FAIL"
        print(f"  {status}  {result.name}")

    failed = [r for r in results if not r.passed]

    if failed:
        print("\n--- worker logs (last 50 lines) ---")
        print(compose("logs", "worker", "--tail", "50").stdout)

    compose("down", "-v")

    # Summary
    failed = [r for r in results if not r.passed]
    print(f"\n{'='*60}")
    print(f"Branch: {current}")
    print(f"Result: {len(results)-len(failed)} passed, {len(failed)} failed")
    if failed:
        print("Failures:")
        for f in failed:
            print(f"  x {f.name}")
            if f.log:
                print(f"    log: {f.log[:2000]}")
    print(f"{'='*60}")

    raise typer.Exit(0 if not failed else 1)


if __name__ == "__main__":
    app()
