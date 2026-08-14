#!/usr/bin/env python3
"""Post-migration hook: apply procrastinate schema to the database."""

from __future__ import annotations

import asyncio


def _split_statements(schema_sql: str) -> list[str]:
    """Split schema SQL on top-level semicolons, keeping $$-quoted bodies intact."""
    statements: list[str] = []
    buf: list[str] = []
    in_dollar = False
    i = 0
    while i < len(schema_sql):
        if schema_sql.startswith("$$", i):
            in_dollar = not in_dollar
            buf.append("$$")
            i += 2
            continue
        ch = schema_sql[i]
        if ch == ";" and not in_dollar:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def apply_schema(engine=None) -> None:
    """Apply procrastinate schema to the target database.

    When engine is provided (conftest/test DB), execute the schema SQL
    directly via that engine's connection. When engine is None (verify.py
    dev DB), use the async app path.
    """
    from simapp.tasks import app

    schema_sql = app.schema_manager.get_schema()

    if engine is not None:
        with engine.connect() as conn:
            from sqlalchemy import text

            exists = conn.execute(
                text("SELECT 1 FROM pg_tables WHERE tablename = 'procrastinate_jobs'")
            ).fetchone()
            if exists is not None:
                return
            for statement in _split_statements(schema_sql):
                conn.execute(text(statement))
            conn.commit()
        return

    async def _run() -> None:
        await app.open_async()
        await app.schema_manager.apply_schema_async()

    asyncio.run(_run())


def main() -> None:
    apply_schema()


if __name__ == "__main__":
    main()
