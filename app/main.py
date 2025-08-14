# app/main.py
import os
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import router as event_router  # adjust if your import is different
from app.select_routes import router as select_router

app = FastAPI(title="Universal Data Ingestion API")

# Read allowed origins from environment variable
allowed_origins = json.loads(os.getenv("ALLOWED_ORIGINS_JSON", '["http://localhost:5173"]'))

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
    max_age=86400
)

# Include routes
app.include_router(event_router)
app.include_router(select_router)
from app.crud_routes import router as crud_router
app.include_router(crud_router)
