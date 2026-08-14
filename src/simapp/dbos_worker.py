"""DBOS worker entrypoint — launches the DBOS executor to process queued workflows."""

from __future__ import annotations

from dbos import DBOS

from simapp.dbos_config import dbos


def main() -> None:
    import simapp.tasks_dbos  # noqa: F401

    dbos.launch()
    DBOS.register_queue("sim_queue", concurrency=10)
    import threading

    threading.Event().wait()


if __name__ == "__main__":
    main()
