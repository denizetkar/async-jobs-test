"""DBOS configuration and initialization."""

from __future__ import annotations

from dbos import DBOS, Queue
from dbos._dbos_config import DBOSConfig

from simapp.config import settings

# DBOS SDK's enqueue_in_transaction requires the session to target the system
# DB. Use the same database for both the app session and the DBOSClient so
# enqueue_in_transaction operates within the app's existing connection.
_db_url = settings.database_url
_system_db_url = _db_url

dbos_config: DBOSConfig = {
    "name": "simapp",
    "application_database_url": _db_url,
    "system_database_url": _system_db_url,
}

dbos = DBOS(config=dbos_config)

# Queue config is import-safe; DBOS.register_queue() persists it to the
# system DB and must run AFTER dbos.launch() (see dbos_worker.main).
sim_queue = Queue("sim_queue", concurrency=10)
