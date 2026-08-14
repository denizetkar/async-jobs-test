"""Transactional scheduling is NOT supported by Prefect.

This file replaces the procrastinate transactional tests with a documentation
test that explains why Prefect cannot do transactional enqueue.

The actual gap demonstration is in test_consistency_gap.py.
"""

from __future__ import annotations


def test_prefect_cannot_do_transactional_enqueue():
    """Prefect creates flow runs via the Prefect Server API (separate datastore),
    so triggering a flow is an API call, not a SQL operation on the caller's
    transaction.

    There is no way to enlist the Prefect flow start in the caller's SQLAlchemy
    transaction. The only workaround is the Transactional Outbox pattern
    (see feat/outbox-cdc branch).
    """
    assert True
