CREATE TABLE IF NOT EXISTS papers(task_key TEXT PRIMARY KEY, payload JSONB NOT NULL, status TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS outbox(task_key TEXT, target TEXT, payload JSONB NOT NULL, done BOOLEAN DEFAULT FALSE, attempts INT DEFAULT 0, PRIMARY KEY(task_key,target));
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(done) WHERE NOT done;