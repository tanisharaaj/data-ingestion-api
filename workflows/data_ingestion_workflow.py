from temporalio import workflow, activity
from datetime import timedelta
from typing import Union

@workflow.defn
class DataIngestionWorkflow:
    @workflow.run
    async def run(self, payload: dict, db_key: str) -> Union[str, list]:
        workflow.logger.info(f"Starting data ingestion with: {payload}, DB: {db_key}")

        result = await workflow.execute_activity(
            perform_db_operation,
            args=[payload, db_key],
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        return result


@activity.defn
async def perform_db_operation(payload: dict, db_key: str) -> Union[str, list]:
    from sqlalchemy import text
    from app.db_engine import SESSIONS

    sessionmaker = SESSIONS.get(db_key)
    if not sessionmaker:
        raise ValueError(f"Unknown DB key: '{db_key}'")

    operation = payload.get("operation")
    table = payload.get("table")
    fields = payload.get("fields", {})
    filters = payload.get("filters", {})
    columns = payload.get("columns", [])
    primary_key = payload.get("primary_key", {})

    try:
        with sessionmaker.begin() as conn:
            if operation == "insert":
                fields.pop("id", None)
                if not fields:
                    raise ValueError("No fields provided for insert")
                keys = ", ".join(fields.keys())
                vals = ", ".join([f":{k}" for k in fields])
                query = text(f"INSERT INTO {table} ({keys}) VALUES ({vals})")
                conn.execute(query, fields)
                return f"Insert operation completed on {table}"

            elif operation == "update":
                if not primary_key:
                    raise ValueError("Primary key required for update")
                if not fields:
                    raise ValueError("No fields provided for update")
                set_clause = ", ".join([f"{k} = :{k}" for k in fields])
                where_clause = " AND ".join([f"{k} = :pk_{k}" for k in primary_key])
                params = {**fields, **{f"pk_{k}": v for k, v in primary_key.items()}}
                query = text(f"UPDATE {table} SET {set_clause} WHERE {where_clause}")
                conn.execute(query, params)
                return f"Update operation completed on {table}"

            elif operation == "delete":
                if not primary_key:
                    raise ValueError("Primary key required for delete")
                where_clause = " AND ".join([f"{k} = :{k}" for k in primary_key])
                query = text(f"DELETE FROM {table} WHERE {where_clause}")
                conn.execute(query, primary_key)
                return f"Delete operation completed on {table}"

            elif operation == "select":
                selected_cols = ", ".join(columns) if columns else "*"
                query = f"SELECT {selected_cols} FROM {table}"
                if filters:
                    where_clause = " AND ".join([f"{k} = :{k}" for k in filters])
                    query += f" WHERE {where_clause}"
                result = conn.execute(text(query), filters)
                rows = [dict(r._mapping) for r in result]
                return rows  # list of dicts

            else:
                raise ValueError(f"Unsupported operation '{operation}'")

    except Exception as e:
        raise RuntimeError(f"Database operation failed: {str(e)}")
