from temporalio import workflow, activity
from datetime import timedelta

@workflow.defn
class DataIngestionWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> str:
        workflow.logger.info(f"Starting data ingestion with: {payload}")
        result = await workflow.execute_activity(
            perform_db_operation,
            payload,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        return result


@activity.defn
async def perform_db_operation(payload: dict) -> str:
    from sqlalchemy import text
    from app.db_engine import engine

    operation = payload.get("operation")
    table = payload.get("table")
    fields = payload.get("fields", {})
    primary_key = payload.get("primary_key", {})

    try:
        with engine.begin() as conn:
            if operation == "insert":
                keys = ", ".join(fields.keys())
                vals = ", ".join([f":{k}" for k in fields])
                query = text(f"INSERT INTO {table} ({keys}) VALUES ({vals})")
                conn.execute(query, fields)

            elif operation == "update":
                if not primary_key:
                    raise ValueError("Primary key required for update")
                set_clause = ", ".join([f"{k} = :{k}" for k in fields])
                where_clause = " AND ".join([f"{k} = :pk_{k}" for k in primary_key])
                params = {**fields, **{f"pk_{k}": v for k, v in primary_key.items()}}
                query = text(f"UPDATE {table} SET {set_clause} WHERE {where_clause}")
                conn.execute(query, params)

            elif operation == "delete":
                if not primary_key:
                    raise ValueError("Primary key required for delete")
                where_clause = " AND ".join([f"{k} = :{k}" for k in primary_key])
                query = text(f"DELETE FROM {table} WHERE {where_clause}")
                conn.execute(query, primary_key)

            else:
                raise ValueError("Unsupported operation")

            return f"{operation.capitalize()} operation completed on {table}"

    except Exception as e:
        raise RuntimeError(f"Database operation failed: {e}")
