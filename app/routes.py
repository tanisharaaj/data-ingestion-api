from fastapi import APIRouter, Depends, HTTPException, Query
from app.auth import verify_token
from app.models import DataRequest
from temporal_client import get_temporal_client
import uuid

router = APIRouter()

@router.post("/event")
async def trigger_workflow(
    request: DataRequest,
    token=Depends(verify_token),
    db: str = Query("default", description="Database key")
):
    try:
        client = await get_temporal_client()

        handle = await client.start_workflow(
            workflow="DataIngestionWorkflow",
            id=f"workflow-{uuid.uuid4()}",
            task_queue="data-ingestion-task-queue",
            args=[request.dict(), db],  # ✅ pass db_key to workflow
        )

        return {"workflow_id": handle.id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

