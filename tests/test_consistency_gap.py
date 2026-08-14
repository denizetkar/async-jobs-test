"""Consistency gap test: demonstrates the fire-after-commit inconsistency window.

Temporal CANNOT do transactional enqueue. The scheduler commits the DB row
first, then starts the workflow via RPC. If the workflow start fails (Temporal
server down, network error), the DB row is committed but no workflow exists —
an orphaned simulation that will never be processed.

This test patches the Temporal client's start_workflow to raise an exception,
then verifies that the DB row was committed anyway (the gap).
"""

from __future__ import annotations

import tempfile
from unittest.mock import patch


def test_consistency_gap_simulation(gap_client):
    """If Temporal is down, a simulation row is committed but never processed."""
    from sqlalchemy import create_engine, text
    from simapp.config import settings

    # Setup cleanup: delete any pre-existing test.csv rows so the count assertion is exact
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM datasets WHERE filename = 'test.csv'"))
        conn.commit()
    engine.dispose()

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("col1,col2\n1,2\n3,4\n")
        f.flush()
        filepath = f.name

    try:
        # Patch Client.connect so _ensure_client succeeds with a mock connected
        # client, then patch start_workflow (the actual RPC call) to raise —
        # this is the gap: commit succeeded, workflow start failed.
        from temporalio.client import Client

        async def _fake_connect(*args, **kwargs):
            from unittest.mock import AsyncMock

            mock_client = AsyncMock(spec=Client)
            mock_client.start_workflow = AsyncMock(side_effect=RuntimeError("Temporal server down"))
            return mock_client

        with patch("temporalio.client.Client.connect", side_effect=_fake_connect):
            # Upload a dataset — this should fail because Temporal is "down"
            with open(filepath, "rb") as f:
                response = gap_client.post("/datasets", files={"file": ("test.csv", f, "text/csv")})

            # The server should raise a 500 because the workflow start failed
            # BUT the DB row was already committed (the gap!)
            assert response.status_code == 500, (
                f"Expected 500 when Temporal is down, got {response.status_code}"
            )

        # Verify: the dataset row EXISTS in the DB despite the workflow failure
        # This is the inconsistency — a committed row with no corresponding workflow
        engine = create_engine(settings.database_url, pool_pre_ping=True)
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT count(*) FROM datasets WHERE filename = 'test.csv'")
            )
            count = result.scalar()
            assert count == 1, (
                f"Expected exactly one orphaned dataset row (the gap) but found {count}"
            )
        engine.dispose()

    finally:
        import os

        os.unlink(filepath)
        # Cleanup orphaned rows
        from sqlalchemy import create_engine, text
        from simapp.config import settings

        engine = create_engine(settings.database_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM datasets WHERE filename = 'test.csv'"))
            conn.commit()
        engine.dispose()
