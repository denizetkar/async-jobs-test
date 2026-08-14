#!/usr/bin/env python3
"""End-to-end demo scenario for simulation workflows.

Usage:
    uv run python scripts/demo.py [file] [chunks]
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import httpx
import typer

app = typer.Typer()


@app.command()
def demo(
    file: str = typer.Argument("README.md", help="File to upload as dataset"),
    chunks: int = typer.Argument(4, help="Number of simulation chunks"),
    base_url: str = typer.Option("http://localhost:8000", envvar="SIMAPP_BASE_URL"),
):
    filepath = Path(file)
    if not filepath.exists():
        print(f"Error: file '{file}' not found", file=sys.stderr)
        raise typer.Exit(1)

    client = httpx.Client(base_url=base_url, timeout=10)

    print("=== SimApp Demo Scenario ===")
    print(f"Upload file: {file}")
    print(f"Chunks: {chunks}\n")

    print("1. Uploading dataset...")
    with filepath.open("rb") as f:
        r = client.post("/datasets", files={"file": (filepath.name, f)})
    r.raise_for_status()
    dataset_id = r.json()["id"]
    print(f"   Dataset ID: {dataset_id}")

    print(f"2. Starting simulation with {chunks} chunks...")
    r = client.post("/simulations", json={"dataset_id": dataset_id, "parameters": {"chunks": chunks}})
    r.raise_for_status()
    sim_id = r.json()["id"]
    print(f"   Simulation ID: {sim_id}")

    print("3. Waiting for simulation to complete...")
    for _ in range(60):
        sim = client.get(f"/simulations/{sim_id}").json()
        ds = client.get(f"/datasets/{dataset_id}").json()
        print(f"   Dataset: {ds['status']} | Simulation: {sim['status']}")
        if sim["status"] == "completed":
            break
        if sim["status"] == "failed":
            print("   ERROR: Simulation failed!", file=sys.stderr)
            raise typer.Exit(1)
        time.sleep(1)
    else:
        print(f"   ERROR: Simulation not completed after 60s (status={sim['status']})", file=sys.stderr)
        raise typer.Exit(1)

    print("4. Result:")
    result = client.get(f"/simulations/{sim_id}").json()
    print(f"   chunks: {result['result']['chunk_count']}, total: {result['result']['total']}")
    print("\n=== Demo complete! ===")


if __name__ == "__main__":
    app()
