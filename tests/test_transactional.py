"""Transactional scheduling is NOT supported by Temporal.

This file replaces the procrastinate transactional tests with a documentation
test that explains why Temporal cannot do transactional enqueue.

The actual gap demonstration is in test_consistency_gap.py.
"""

from __future__ import annotations


def test_temporal_cannot_do_transactional_enqueue():
    """Temporal uses a separate datastore (its own Postgres), so start_workflow()
    is an RPC call, not a SQL operation on the caller's transaction.

    There is no way to enlist the Temporal workflow start in the caller's
    SQLAlchemy transaction. The only workaround is the Transactional Outbox
    pattern (see feat/outbox-cdc branch).
    """
    # This test is a documentation marker — no assertions needed.
    # The gap is demonstrated in test_consistency_gap.py.
    assert True
