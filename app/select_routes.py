from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from app.auth import verify_token
from temporal_client import get_temporal_client
import uuid

router = APIRouter(prefix="/select", tags=["select"])

class SelectRequest(BaseModel):
    table: str
    columns: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None
    limit: Optional[int] = 100
    offset: Optional[int] = 0

@router.post("", summary="Run a SELECT using Temporal workflow")
async def select_via_temporal(
    req: SelectRequest,
    token=Depends(verify_token),
    db: str = Query("default", description="Database key to select target DB"),
):
    try:
        client = await get_temporal_client()

        payload = {
            "operation": "select",
            "table": req.table,
            "columns": req.columns or [],
            "filters": req.filters or {},
            "limit": req.limit,
            "offset": req.offset
        }

        handle = await client.start_workflow(
            workflow="DataIngestionWorkflow",
            id=f"select-workflow-{uuid.uuid4()}",
            task_queue="data-ingestion-task-queue",
            args=[payload, db],
        )

        result = await handle.result()
        return {"result": result, "workflow_id": handle.id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
