"""Outbox relay: drains Postgres outbox into Qdrant + Neo4j. Safe to run N copies (SKIP LOCKED).
Both sinks are idempotent (Qdrant upsert by id, Cypher MERGE) so at-least-once delivery is fine.
Run: python -m ingest.relay"""
import asyncio, os, uuid
import psycopg
from neo4j import AsyncGraphDatabase
from .asyncio_compat import configure_windows_event_loop
from .embed import DIM

COLLECTION = "papers"


async def _cypher(tx, q, p): await tx.run(q, p)


async def main():
    targets = [target.strip() for target in os.getenv("RELAY_TARGETS", "qdrant,neo4j").split(",") if target.strip()]
    invalid_targets = set(targets) - {"qdrant", "neo4j"}
    if invalid_targets or not targets:
        raise ValueError(f"RELAY_TARGETS must contain qdrant and/or neo4j; got {targets}")

    qd = None
    neo = None
    try:
        if "qdrant" in targets:
            from qdrant_client import AsyncQdrantClient
            from qdrant_client.models import Distance, VectorParams

            qd = AsyncQdrantClient(url=os.getenv("QDRANT_URL", "http://localhost:6333"))
            if not await qd.collection_exists(COLLECTION):
                await qd.create_collection(COLLECTION, vectors_config=VectorParams(size=DIM, distance=Distance.COSINE))
        if "neo4j" in targets:
            neo = AsyncGraphDatabase.driver(os.getenv("NEO4J_URL", "bolt://localhost:7687"),
                                            auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password")))
        async with await psycopg.AsyncConnection.connect(os.environ["DATABASE_URL"]) as c:
            while True:
                async with c.transaction():
                    rows = await (await c.execute("SELECT task_key,target,payload FROM outbox WHERE NOT done AND target = ANY(%s) "
                                                  "ORDER BY task_key LIMIT 50 FOR UPDATE SKIP LOCKED", (targets,))).fetchall()
                    for key, target, pl in rows:
                        try:
                            if target == "qdrant":
                                from qdrant_client.models import PointStruct

                                await qd.upsert(COLLECTION, [PointStruct(id=str(uuid.UUID(pl["id"])), vector=pl["vector"], payload=pl["payload"])])
                            else:
                                async with neo.session() as s:
                                    for st in pl["statements"]:
                                        await s.execute_write(_cypher, st["query"], st["params"])
                            await c.execute("UPDATE outbox SET done=TRUE WHERE task_key=%s AND target=%s", (key, target))
                        except Exception as e:
                            print("relay error", key, target, e)
                            await c.execute("UPDATE outbox SET attempts=attempts+1 WHERE task_key=%s AND target=%s", (key, target))
                if not rows:
                    await asyncio.sleep(2)
    finally:
        if qd is not None:
            await qd.close()
        if neo is not None:
            await neo.close()

if __name__ == "__main__":
    configure_windows_event_loop()
    asyncio.run(main())