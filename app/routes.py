from fastapi import APIRouter, Depends, HTTPException
from app.auth import verify_token
from app.models import DataRequest
from temporal_client import get_temporal_client
import uuid

router = APIRouter()

@router.post("/event")
async def trigger_workflow(request: DataRequest, token=Depends(verify_token)):
    try:
        client = await get_temporal_client()

        handle = await client.start_workflow(
            workflow="DataIngestionWorkflow",
            id=f"workflow-{uuid.uuid4()}",
            task_queue="data-ingestion-task-queue",
            args=[request.dict()],
        )

        return {"workflow_id": handle.id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

