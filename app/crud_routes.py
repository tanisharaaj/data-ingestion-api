# app/crud_routes.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from sqlalchemy import Table, MetaData, select, insert, update, delete
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.engine import Result
from sqlalchemy.orm import Session

from app.db_engine import get_session
from app.auth import verify_token

router = APIRouter(prefix="/crud", tags=["crud"])

class CRUDRequest(BaseModel):
    operation: str = Field(..., pattern="^(INSERT|UPDATE|DELETE|SELECT)$")
    table: str
    fields: Optional[Dict[str, Any]] = None      # data to insert or update
    filters: Optional[Dict[str, Any]] = None     # dynamic equality WHERE for update/delete/select
    columns: Optional[List[str]] = None          # for SELECT

def _load_table(table_name: str, session: Session) -> Table:
    metadata = MetaData()
    try:
        return Table(table_name, metadata, autoload_with=session.bind)
    except NoSuchTableError:
        raise HTTPException(status_code=400, detail=f"Table '{table_name}' not found")

def _validate_columns(tbl: Table, cols: Optional[List[str]]) -> List:
    if not cols:
        return list(tbl.c)
    bad = [c for c in cols if c not in tbl.c]
    if bad:
        raise HTTPException(status_code=400, detail=f"Unknown column(s): {bad}")
    return [tbl.c[c] for c in cols]

def _build_where(tbl: Table, filters: Optional[Dict[str, Any]]):
    if not filters:
        return None
    exprs = []
    for col, val in filters.items():
        if col not in tbl.c:
            raise HTTPException(status_code=400, detail=f"Invalid filter column: {col}")
        exprs.append(tbl.c[col] == val)
    from sqlalchemy import and_
    return and_(*exprs) if exprs else None

@router.post("", summary="INSERT, UPDATE, DELETE, or SELECT with dynamic equality WHERE")
def run_crud(
    req: CRUDRequest,
    token=Depends(verify_token),
    session: Session = Depends(get_session),
):
    tbl = _load_table(req.table, session)
    op = req.operation.upper()

    # SELECT
    if op == "SELECT":
        cols = _validate_columns(tbl, req.columns)
        stmt = select(*cols)
        where_expr = _build_where(tbl, req.filters)
        if where_expr is not None:
            stmt = stmt.where(where_expr)
        rows = [dict(r._mapping) for r in session.execute(stmt)]
        return {"rows": rows}

    # INSERT
    if op == "INSERT":
        if not req.fields or not isinstance(req.fields, dict):
            raise HTTPException(status_code=400, detail="INSERT requires 'fields'")
        bad = [k for k in req.fields.keys() if k not in tbl.c]
        if bad:
            raise HTTPException(status_code=400, detail=f"Unknown field(s): {bad}")
        stmt = insert(tbl).values(**req.fields)
        try:
            stmt = stmt.returning(*list(tbl.c))
        except Exception:
            pass
        res: Result = session.execute(stmt)
        session.commit()
        try:
            rows = [dict(r._mapping) for r in res]
            if rows:
                return {"inserted": 1, "row": rows[0]}
        except Exception:
            pass
        return {"inserted": res.rowcount or 1}

    # UPDATE
    if op == "UPDATE":
        if not req.fields:
            raise HTTPException(status_code=400, detail="UPDATE requires 'fields'")
        where_expr = _build_where(tbl, req.filters)
        if where_expr is None:
            raise HTTPException(status_code=400, detail="UPDATE requires 'filters' to avoid full-table update")
        bad = [k for k in req.fields.keys() if k not in tbl.c]
        if bad:
            raise HTTPException(status_code=400, detail=f"Unknown field(s): {bad}")
        stmt = update(tbl).where(where_expr).values(**req.fields)
        try:
            stmt = stmt.returning(*list(tbl.c))
        except Exception:
            pass
        res: Result = session.execute(stmt)
        session.commit()
        try:
            rows = [dict(r._mapping) for r in res]
            return {"updated": len(rows), "rows": rows}
        except Exception:
            return {"updated": res.rowcount or 0}

    # DELETE
    if op == "DELETE":
        where_expr = _build_where(tbl, req.filters)
        if where_expr is None:
            raise HTTPException(status_code=400, detail="DELETE requires 'filters' to avoid full-table delete")
        stmt = delete(tbl).where(where_expr)
        try:
            stmt = stmt.returning(*list(tbl.c))
        except Exception:
            pass
        res: Result = session.execute(stmt)
        session.commit()
        try:
            rows = [dict(r._mapping) for r in res]
            return {"deleted": len(rows), "rows": rows}
        except Exception:
            return {"deleted": res.rowcount or 0}

    # Unsupported op
    raise HTTPException(status_code=400, detail=f"Unsupported operation '{req.operation}'")
