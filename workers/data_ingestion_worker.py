import asyncio

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from temporal_client import get_temporal_client
from temporalio.worker import Worker

from workflows.data_ingestion_workflow import DataIngestionWorkflow, perform_db_operation

async def main():
    client = await get_temporal_client()

    worker = Worker(
        client,
        task_queue="data-ingestion-task-queue",
        workflows=[DataIngestionWorkflow],
        activities=[perform_db_operation],
    )

    print("🚀 Worker is running and polling Temporal Cloud...")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
