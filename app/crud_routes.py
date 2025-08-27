from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Union
from app.auth import verify_token
from temporal_client import get_temporal_client
import uuid

router = APIRouter(prefix="/crud", tags=["crud"])

class CRUDRequest(BaseModel):
    operation: str = Field(..., pattern="^(insert|update|delete|select)$", description="CRUD operation")
    table: str
    fields: Optional[Dict[str, Any]] = None       # For insert/update
    filters: Optional[Dict[str, Any]] = None      # For update/delete/select
    columns: Optional[List[str]] = None           # For select
    primary_key: Optional[Dict[str, Any]] = None  # For update/delete

@router.post("", summary="Trigger INSERT, UPDATE, DELETE, or SELECT via Temporal Workflow")
async def trigger_crud_via_temporal(
    req: CRUDRequest,
    token=Depends(verify_token),
    db: str = Query("default", description="Database key to select target DB"),
):
    try:
        client = await get_temporal_client()

        # Assemble full payload for the workflow
        payload = req.dict(exclude_unset=True)

        handle = await client.start_workflow(
            workflow="DataIngestionWorkflow",
            id=f"crud-workflow-{uuid.uuid4()}",
            task_queue="data-ingestion-task-queue",
            args=[payload, db],
        )

        # Optionally wait for the result and return it directly
        result: Union[str, list] = await handle.result()

        return {
            "workflow_id": handle.id,
            "status": f"{req.operation.upper()} operation completed via Temporal",
            "result": result,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
