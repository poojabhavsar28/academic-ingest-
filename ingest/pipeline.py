import asyncio, hashlib
from pydantic import BaseModel, Field
from .cypher import build_cypher, render
from .embed import embed_paper
from .extract import RuleExtractor
from .schema import AcademicPaper, normalize_doi
from .store import Store


class Result(BaseModel):
    task_key: str
    status: str  # complete | partial | failed | duplicate
    paper: dict | None = None
    cypher: list[str] = Field(default_factory=list)
    embedding_dim: int = 0
    errors: list[str] = Field(default_factory=list)


def task_key(raw: dict) -> str:
    """Stable key: same DOI -> same key, regardless of casing or URL prefix."""
    doi = normalize_doi(str(raw.get("doi", "")))
    return hashlib.sha256(doi.encode()).hexdigest()[:32]


class Pipeline:
    def __init__(self, extractor=None, store=None, concurrency=8, embedder=None):
        self.extractor, self.store = extractor or RuleExtractor(), store or Store()
        self.embedder = embedder or embed_paper
        self.sem, self.inflight = asyncio.Semaphore(concurrency), {}

    async def process(self, raw: dict) -> Result:
        key = task_key(raw)
        if key in self.inflight:                       # concurrent duplicate -> share the work
            r = await self.inflight[key]
            return r.model_copy(update={"status": "duplicate"})
        fut = asyncio.ensure_future(self._guarded(key, raw))  # registered before any await
        self.inflight[key] = fut
        try:
            return await fut
        finally:
            if self.inflight.get(key) is fut:
                self.inflight.pop(key, None)

    async def _guarded(self, key, raw) -> Result:
        if await self.store.status(key) == "complete":
            return Result(task_key=key, status="duplicate")  # replay is a no-op
        return await self._run(key, raw)

    async def _run(self, key, raw) -> Result:
        async with self.sem:
            errors = []
            try:                                       # stage 1: required
                paper: AcademicPaper = await self.extractor.extract(raw)
            except Exception as e:
                return Result(task_key=key, status="failed", errors=[f"normalize: {e}"])
            outbox, cy, dim = {}, [], 0
            try:                                       # stage 2: optional (partial success)
                vec = await asyncio.to_thread(self.embedder, paper, raw); dim = len(vec)
                outbox["qdrant"] = {"id": key, "vector": vec, "payload": {"doi": paper.doi}}
            except Exception as e:
                errors.append(f"embed: {e}")
            try:                                       # stage 3: optional
                stmts = build_cypher(paper)
                cy = [render(q, p) for q, p in stmts]
                outbox["neo4j"] = {"statements": [{"query": q, "params": p} for q, p in stmts]}
            except Exception as e:
                errors.append(f"graph: {e}")
            status = "complete" if not errors else "partial"
            await self.store.commit(key, paper.model_dump(), status, outbox)
            return Result(task_key=key, status=status, paper=paper.model_dump(), cypher=cy, embedding_dim=dim, errors=errors)

    async def run_batch(self, raws: list[dict]) -> list[Result]:
        async def safe(r):                             # isolate: one bad payload never halts the batch
            try: return await self.process(r)
            except Exception as e: return Result(task_key=task_key(r) if isinstance(r, dict) else "?", status="failed", errors=[str(e)])
        return await asyncio.gather(*(safe(r) for r in raws))
