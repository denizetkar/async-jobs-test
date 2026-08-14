"""Post-migration schema hooks.

Applied after Alembic migrations (and by the test harness via
``conftest._apply_engine_schema``) to create branch-specific tables that do
not warrant a full Alembic revision on this feature branch.

Expose ``apply_schema(engine)`` so callers can run the hooks against an
arbitrary SQLAlchemy engine.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import CreateTable

from simapp.models import ProcessedEvent


def apply_schema(engine) -> None:
    """Create branch-specific tables if they do not already exist."""
    with engine.begin() as conn:
        if not conn.dialect.has_table(conn, ProcessedEvent.__tablename__):
            conn.execute(
                CreateTable(
                    ProcessedEvent.__table__,
                )
            )


# Reuse the column definitions for an explicit, readable DDL alternative.
# Kept for clarity / debugging; ``apply_schema`` uses the ORM metadata above.
_PROCESSED_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS processed_events (
    id           UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _apply_ddl(engine) -> None:
    """Direct-DDL variant (unused; kept for ad-hoc introspection)."""
    with engine.begin() as conn:
        conn.exec_driver_sql(_PROCESSED_EVENTS_DDL)


# Silence unused-import warnings for the explicit DDL column references.
_ = (Column, DateTime, UUID, func)
