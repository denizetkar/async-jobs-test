"""Temporal worker entrypoint — registers activities and workflows, polls for tasks."""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from simapp.temporal_workflows import (
    DatasetProcessWorkflow,
    SimulationWorkflow,
    postprocess_activity,
    preprocess_activity,
    process_dataset_activity,
    simulate_chunk_activity,
    wait_dataset_activity,
)


async def main() -> None:
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"), namespace="default")

    with ThreadPoolExecutor(max_workers=10) as activity_executor:
        worker = Worker(
            client,
            task_queue="simapp-task-queue",
            workflows=[DatasetProcessWorkflow, SimulationWorkflow],
            activities=[
                process_dataset_activity,
                preprocess_activity,
                simulate_chunk_activity,
                postprocess_activity,
                wait_dataset_activity,
            ],
            activity_executor=activity_executor,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
