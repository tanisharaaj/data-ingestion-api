# app/select_routes.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from sqlalchemy import Table, select, MetaData
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import Session

from app.db_engine import get_session
from app.auth import verify_token

router = APIRouter(prefix="/select", tags=["select"])

class SelectRequest(BaseModel):
    table: str
    columns: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None
    limit: Optional[int] = 100
    offset: Optional[int] = 0

@router.post("", summary="Run a SELECT with a dynamic WHERE (equality only)")
def dynamic_select(
    req: SelectRequest,
    token=Depends(verify_token),
    session: Session = Depends(get_session)
):
    metadata = MetaData()
    try:
        table_obj: Table = Table(req.table, metadata, autoload_with=session.bind)
    except NoSuchTableError:
        raise HTTPException(status_code=400, detail=f"Table '{req.table}' not found")

    # Validate and resolve columns
    if req.columns:
        bad = [c for c in req.columns if c not in table_obj.c]
        if bad:
            raise HTTPException(status_code=400, detail=f"Invalid columns: {bad}")
        cols = [table_obj.c[c] for c in req.columns]
    else:
        cols = list(table_obj.c)

    # Build SELECT statement
    stmt = select(*cols)
    if req.filters:
        for col, val in req.filters.items():
            if col not in table_obj.c:
                raise HTTPException(status_code=400, detail=f"Invalid filter column: {col}")
            stmt = stmt.where(table_obj.c[col] == val)

    if req.limit is not None:
        stmt = stmt.limit(max(1, min(int(req.limit), 1000)))
    if req.offset:
        stmt = stmt.offset(max(0, int(req.offset)))

    # Run the query on the selected DB session
    rows = [dict(r._mapping) for r in session.execute(stmt)]
    return {"rows": rows}
