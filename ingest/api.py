from contextlib import asynccontextmanager

from fastapi import FastAPI
from .main import build_pipeline

pipe = build_pipeline()


@asynccontextmanager
async def lifespan(app: FastAPI):
    if hasattr(pipe.store, "open"):
        await pipe.store.open()
    try:
        yield
    finally:
        if hasattr(pipe.store, "close"):
            await pipe.store.close()


app = FastAPI(title="Academic Ingestion Engine", lifespan=lifespan)


@app.get("/health")
async def health(): return {"ok": True}

@app.post("/ingest")
async def ingest(payload: dict): return (await pipe.process(payload)).model_dump()
