# app/db_engine.py

import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi import Depends, Query, Header, HTTPException
from sqlalchemy.orm import Session

load_dotenv()

# Load the DB URL mapping from env
raw_db_urls = os.getenv("DB_URLS_JSON")
if not raw_db_urls:
    raise RuntimeError("DB_URLS_JSON environment variable is missing.")

# Parse it as a dict
try:
    DB_URLS = json.loads(raw_db_urls)
except json.JSONDecodeError:
    raise RuntimeError("Failed to parse DB_URLS_JSON as valid JSON.")

# Create an engine + sessionmaker for each DB
ENGINES = {}
SESSIONS = {}

for key, url in DB_URLS.items():
    engine = create_engine(url, pool_pre_ping=True)
    ENGINES[key] = engine
    SESSIONS[key] = sessionmaker(bind=engine, autocommit=False, autoflush=False)

# Optional: expose a default session (for one-off scripts or CLI use)
SessionLocal = SESSIONS["default"]






def resolve_db_key(
    db: str | None = Query(None, description="DB key from allowlist"),
    x_db_key: str | None = Header(None, convert_underscores=False)
) -> str:
    key = db or x_db_key or "default"
    if key not in SESSIONS:
        raise HTTPException(status_code=400, detail=f"Unknown db key '{key}'")
    return key

def get_session(db_key: str = Depends(resolve_db_key)) -> Session:
    session = SESSIONS[db_key]()
    try:
        yield session
    finally:
        session.close()
