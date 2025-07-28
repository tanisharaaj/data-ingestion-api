from fastapi import FastAPI
from app.routes import router as event_router

app = FastAPI(title="Universal Data Ingestion API")

app.include_router(event_router)
