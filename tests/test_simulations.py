"""Simulation start and retrieval tests."""

from __future__ import annotations

import time


def test_start_simulation_waits_for_dataset(client):
    """Upload a dataset, immediately start a simulation — the engine waits."""
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("data\n1\n")
        f.flush()
        filepath = f.name

    try:
        with open(filepath, "rb") as f:
            upload_resp = client.post("/datasets", files={"file": ("data.csv", f, "text/csv")})
        dataset_id = upload_resp.json()["id"]

        response = client.post(
            "/simulations",
            json={"dataset_id": dataset_id, "parameters": {"chunks": 2}},
        )
        assert response.status_code == 201
        sim_id = response.json()["id"]

        for _ in range(60):
            response = client.get(f"/simulations/{sim_id}")
            data = response.json()
            if data["status"] == "completed":
                break
            if data["status"] == "failed":
                raise AssertionError(f"Simulation failed: {data.get('result')}")
            time.sleep(1)
        else:
            raise AssertionError("Simulation did not complete in 60s")

        result = data["result"]
        assert result["chunk_count"] == 2
        assert result["total"] == 2

    finally:
        import os

        os.unlink(filepath)


def test_get_simulation_not_found(client):
    response = client.get("/simulations/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


def test_start_simulation_dataset_not_found(client):
    response = client.post(
        "/simulations",
        json={
            "dataset_id": "00000000-0000-0000-0000-000000000000",
            "parameters": {"chunks": 2},
        },
    )
    assert response.status_code == 404
