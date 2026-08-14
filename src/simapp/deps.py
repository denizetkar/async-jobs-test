"""FastAPI dependency providers."""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy.orm import Session

from simapp.db import SessionLocal


def get_session() -> Generator[Session, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


_scheduler_instance: object | None = None


def get_scheduler() -> object:
    """Returns the OutboxScheduler implementation."""
    global _scheduler_instance
    if _scheduler_instance is None:
        from simapp.outbox_scheduler import OutboxScheduler
        _scheduler_instance = OutboxScheduler()
    return _scheduler_instance
