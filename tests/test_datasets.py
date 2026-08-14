"""Dataset upload and retrieval tests."""

import tempfile

import pytest


def test_upload_dataset(client):
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("col1,col2\n1,2\n")
        f.flush()
        filepath = f.name

    try:
        with open(filepath, "rb") as f:
            response = client.post("/datasets", files={"file": ("test.csv", f, "text/csv")})
        assert response.status_code == 201
        data = response.json()
        assert "id" in data
        assert data["filename"] == "test.csv"
        assert data["status"] == "pending"
    finally:
        import os

        os.unlink(filepath)


def test_get_dataset_not_found(client):
    response = client.get("/datasets/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


def test_get_dataset(client):
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("data\n1\n")
        f.flush()
        filepath = f.name

    try:
        with open(filepath, "rb") as f:
            upload_resp = client.post("/datasets", files={"file": ("data.csv", f, "text/csv")})
        assert upload_resp.status_code == 201
        dataset_id = upload_resp.json()["id"]

        response = client.get(f"/datasets/{dataset_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == dataset_id
        assert data["filename"] == "data.csv"
    finally:
        import os

        os.unlink(filepath)
