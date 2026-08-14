"""Consistency gap test: demonstrates the fire-after-commit inconsistency window.

Prefect CANNOT do transactional enqueue. The scheduler commits the DB row
first, then triggers the flow via the Prefect Server API. If the flow trigger
fails (server down, network error), the DB row is committed but no flow exists
— an orphaned simulation that will never be processed.

This test patches run_deployment (the Prefect Server API trigger) to raise an
exception, then verifies that the DB row was committed anyway (the gap).
"""

from __future__ import annotations

import os
import tempfile
from unittest.mock import patch


def test_consistency_gap_dataset(gap_client, db_engine):
    """If Prefect is down, a dataset row is committed but never processed."""
    from sqlalchemy import text

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
        f.write("col1,col2\n1,2\n3,4\n")
        f.flush()
        filepath = f.name

    try:
        with patch(
            "simapp.prefect_scheduler.run_deployment"
        ) as mock_flow:
            mock_flow.side_effect = RuntimeError("Prefect server down")

            with open(filepath, "rb") as f:
                response = gap_client.post(
                    "/datasets", files={"file": ("test.csv", f, "text/csv")}
                )

            assert response.status_code == 500, (
                f"Expected 500 when Prefect is down, got {response.status_code}"
            )

        with db_engine.connect() as conn:
            count = conn.execute(
                text("SELECT count(*) FROM datasets WHERE filename = 'test.csv'")
            ).scalar()
            assert count >= 1, "Expected orphaned dataset row (the gap) but found none"

    finally:
        os.unlink(filepath)
        with db_engine.connect() as conn:
            conn.execute(text("DELETE FROM datasets WHERE filename = 'test.csv'"))
            conn.commit()
