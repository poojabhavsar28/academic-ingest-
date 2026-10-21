"""PostgreSQL stand-in: SQLite as canonical store + transactional outbox (swap DSN for Postgres)."""
import asyncio, json, sqlite3, threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS papers(task_key TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbox(task_key TEXT, target TEXT, payload TEXT, done INTEGER DEFAULT 0, PRIMARY KEY(task_key,target));
"""


class Store:
    def __init__(self, path=":memory:"):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.executescript(SCHEMA)
        self.lock = threading.Lock()

    def _get(self, key):
        with self.lock:
            r = self.db.execute("SELECT status FROM papers WHERE task_key=?", (key,)).fetchone()
        return r[0] if r else None

    def _commit(self, key, paper: dict, status: str, outbox: dict):
        with self.lock, self.db:  # one transaction: canonical row + outbox rows
            self.db.execute("INSERT INTO papers VALUES(?,?,?) ON CONFLICT(task_key) DO UPDATE SET payload=excluded.payload,status=excluded.status",
                            (key, json.dumps(paper), status))
            for target, payload in outbox.items():
                self.db.execute("INSERT OR IGNORE INTO outbox(task_key,target,payload) VALUES(?,?,?)", (key, target, json.dumps(payload)))

    async def status(self, key): return await asyncio.to_thread(self._get, key)
    async def commit(self, *a): return await asyncio.to_thread(self._commit, *a)
    def outbox_count(self):
        with self.lock: return self.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
