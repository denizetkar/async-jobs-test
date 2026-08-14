"""Simulation DAG tests: fan-out, fan-in, and result aggregation."""

from __future__ import annotations

import time


def test_simulation_dag_completes(client, sample_dataset_id):
    """Full E2E: start simulation, wait for completion, verify DAG results."""
    response = client.post(
        "/simulations",
        json={"dataset_id": sample_dataset_id, "parameters": {"chunks": 4}},
    )
    assert response.status_code == 201
    sim_id = response.json()["id"]

    for _ in range(60):
        response = client.get(f"/simulations/{sim_id}")
        data = response.json()
        if data["status"] == "completed":
            break
        if data["status"] == "failed":
            raise AssertionError("Simulation failed")
        time.sleep(1)
    else:
        raise AssertionError("Simulation did not complete in 60s")

    result = data["result"]
    assert result["chunk_count"] == 4
    assert result["total"] == sum(c["value"] for c in result["chunks"])
    chunk_indices = sorted(c["chunk_index"] for c in result["chunks"])
    assert chunk_indices == [0, 1, 2, 3]


def test_simulation_dag_different_chunk_counts(client, sample_dataset_id):
    """Verify dynamic fan-out: different chunk counts produce different numbers of results."""
    for chunks in [1, 3, 8]:
        response = client.post(
            "/simulations",
            json={"dataset_id": sample_dataset_id, "parameters": {"chunks": chunks}},
        )
        assert response.status_code == 201
        sim_id = response.json()["id"]

        for _ in range(60):
            response = client.get(f"/simulations/{sim_id}")
            data = response.json()
            if data["status"] == "completed":
                break
            if data["status"] == "failed":
                raise AssertionError(f"Simulation with {chunks} chunks failed")
            time.sleep(1)
        else:
            raise AssertionError(f"Simulation with {chunks} chunks did not complete in 60s")

        result = data["result"]
        assert result["chunk_count"] == chunks, f"Expected {chunks} chunks, got {result['chunk_count']}"
