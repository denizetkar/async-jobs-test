#!/usr/bin/env python3
"""Register the Debezium Postgres connector via Kafka Connect REST API.

Usage: uv run python scripts/register_debezium_connector.py
"""

from __future__ import annotations

import os
import sys
import time

import httpx
import typer

app = typer.Typer()


def build_connector_config() -> dict[str, str]:
    """Build the Debezium Postgres connector config.

    All outbox events route to a single topic (``simapp.outbox_events``) so the
    consumer only needs one subscription regardless of ``aggregate_type``.
    """
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "simapp",
        "database.password": "simapp",
        "database.dbname": "simapp",
        "topic.prefix": "simapp",
        "plugin.name": "pgoutput",
        "slot.name": "simapp_slot",
        "publication.name": "simapp_pub",
        "table.include.list": "public.outbox_events",
        "tombstones.on.delete": "false",
        "transforms": "outbox",
        "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
        "transforms.outbox.table.field.event.id": "id",
        "transforms.outbox.table.field.event.key": "aggregate_id",
        "transforms.outbox.table.field.event.type": "event_type",
        "transforms.outbox.table.field.event.payload": "payload",
        "transforms.outbox.table.fields.additional.placement": "event_type:envelope:event_type",
        "transforms.outbox.table.expand.json.payload": "true",
        "transforms.outbox.route.by.field": "aggregate_type",
        "transforms.outbox.route.topic.regex": ".*",
        "transforms.outbox.route.topic.replacement": "simapp.outbox_events",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }


@app.command()
def register(
    connect_url: str = typer.Option("http://localhost:8083", envvar="CONNECT_URL"),
):
    config = build_connector_config()

    print("Registering Debezium Postgres connector...")
    r = httpx.put(
        f"{connect_url}/connectors/simapp-connector/config",
        headers={"Content-Type": "application/json"},
        json=config,
        timeout=30,
    )
    r.raise_for_status()

    print("Connector registered. Verifying...")
    deadline = time.time() + 180
    while time.time() < deadline:
        r = httpx.get(f"{connect_url}/connectors/simapp-connector/status", timeout=10)
        r.raise_for_status()
        blob = r.json()
        state = blob.get("connector", {}).get("state", "UNKNOWN")
        task_state = (blob.get("tasks") or [{}])[0].get("state", "UNKNOWN")
        if state == "RUNNING" and task_state == "RUNNING":
            print("Debezium connector is running!")
            return
        if state == "FAILED" or task_state == "FAILED":
            print(f"Connector FAILED: {blob}", file=sys.stderr)
            raise typer.Exit(1)
        time.sleep(2)
    print(f"Connector never reached RUNNING: {blob}", file=sys.stderr)
    raise typer.Exit(1)


if __name__ == "__main__":
    app()
