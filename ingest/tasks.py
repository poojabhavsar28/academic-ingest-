"""Optional Celery front door. Task id == task_key, so a re-enqueue of the same paper is deduplicated.
Run: celery -A ingest.tasks worker -c 4"""
import asyncio, os
from celery import Celery
from .main import build_pipeline
from .pipeline import task_key

app = Celery("ingest", broker=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
app.conf.update(task_acks_late=True, worker_prefetch_multiplier=1)


async def _run(raw):
    pipe = build_pipeline()
    if hasattr(pipe.store, "open"): await pipe.store.open()  # pool is bound to this event loop
    try: return (await pipe.process(raw)).model_dump()
    finally:
        if hasattr(pipe.store, "close"): await pipe.store.close()


@app.task(name="ingest.paper", autoretry_for=(Exception,), retry_backoff=True, max_retries=3)
def ingest_paper(raw: dict): return asyncio.run(_run(raw))


def enqueue(raw: dict):
    return ingest_paper.apply_async(args=[raw], task_id=task_key(raw))