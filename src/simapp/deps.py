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
    """Returns the active SimulationScheduler implementation.

    Each engine branch overrides this with its own scheduler.
    On common, this raises NotImplementedError — there is no default engine.
    """
    global _scheduler_instance
    if _scheduler_instance is None:
        raise NotImplementedError(
            "No scheduler configured. Each engine branch provides its own "
            "implementation via simapp.tasks and deps.py."
        )
    return _scheduler_instance
