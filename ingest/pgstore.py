"""Real PostgreSQL store. Same interface as store.Store (status / commit)."""
from pathlib import Path
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

SCHEMA = (Path(__file__).parent.parent / "migrations" / "001_init.sql").read_text()


class PgStore:
    def __init__(self, dsn: str):
        self.pool = AsyncConnectionPool(dsn, open=False)

    async def open(self):
        await self.pool.open()
        async with self.pool.connection() as c:
            await c.execute(SCHEMA)

    async def close(self): await self.pool.close()

    async def status(self, key):
        async with self.pool.connection() as c:
            r = await (await c.execute("SELECT status FROM papers WHERE task_key=%s", (key,))).fetchone()
        return r[0] if r else None

    async def commit(self, key, paper: dict, status: str, outbox: dict):
        async with self.pool.connection() as c, c.transaction():  # one tx: row + outbox
            await c.execute("INSERT INTO papers(task_key,payload,status) VALUES(%s,%s,%s) "
                            "ON CONFLICT(task_key) DO UPDATE SET payload=EXCLUDED.payload,status=EXCLUDED.status,updated_at=now()",
                            (key, Jsonb(paper), status))
            for target, payload in outbox.items():
                await c.execute("INSERT INTO outbox(task_key,target,payload) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                                (key, target, Jsonb(payload)))