# Academic Ingestion Engine

An asynchronous ingestion service that normalizes academic paper metadata, extracts research metrics and formulas, generates semantic dense vectors, and persists records to Qdrant and Neo4j through a retryable outbox relay.

## What works

The local demo and API run without Docker or external services. Given a valid paper, the pipeline:

1. Validates and normalizes the DOI and paper fields with Pydantic.
2. Extracts hardware metrics, LaTeX formulas, and taxonomy tags using deterministic rules.
3. Generates a normalized, 384-dimensional semantic embedding using `sentence-transformers/all-MiniLM-L6-v2` by default.
4. Builds parameterized, idempotent Cypher statements using `MERGE`.
5. Saves the normalized paper and pending Qdrant/Neo4j outbox entries to the configured store.

The default store is **in-memory SQLite**. In this mode, the app does not contact PostgreSQL, Neo4j, or Qdrant; the vector and Cypher work is generated and queued locally. Data in in-memory SQLite is lost when the process exits. To fulfill the dual-index persistence requirement, configure `DATABASE_URL` to use PostgreSQL and run the relay with reachable Neo4j and Qdrant services.

With PostgreSQL configured, the API transactionally stores the paper and both outbox entries; the relay asynchronously delivers each entry to its index and retries failed deliveries. The API's `complete` status means normalization, embedding, graph statement generation, and outbox commit succeeded. It means delivery is queued, not that both remote sinks have acknowledged it yet. Verify final delivery in Neo4j and Qdrant.

## How an ingestion works

The same pipeline is used by the command-line batch runner and the HTTP API:

```text
JSON paper
    -> stable task key from normalized DOI
    -> Pydantic validation and rule-based extraction
    -> semantic embedding + Neo4j Cypher generation
    -> paper and sink work saved to the outbox
    -> relay sends vector to Qdrant and Cypher to Neo4j
```

The task key is the first 32 hexadecimal characters of the SHA-256 hash of the normalized DOI. This makes DOI casing and common DOI URL prefixes resolve to the same key. Concurrent copies share in-flight processing, and a successfully completed task is treated as a duplicate on replay.

The response status describes pipeline processing, not necessarily remote database delivery:

| Status | Meaning |
| --- | --- |
| `complete` | Validation and preparation succeeded and the work was saved. In PostgreSQL mode the relay still has to deliver the outbox entries. |
| `duplicate` | This DOI was already completed, or an identical request is already being processed. |
| `partial` | Required paper validation succeeded, but one optional preparation step (embedding or graph generation) failed. Resubmitting allows the missing work to be retried. |
| `failed` | Required validation or processing failed. For example, a DOI that does not start with `10.` is rejected. |

With the default local SQLite store, the outbox is in memory and the relay is not running; the CLI output proves extraction, embedding, and Cypher generation, but not persistence to either external index. PostgreSQL mode plus the relay is needed for durable, actual Qdrant and Neo4j writes.

## Exercise requirement map

| Requirement | Implementation | Verification |
| --- | --- | --- |
| Async pipeline and stable idempotency | `asyncio` batch processing, DOI-derived SHA-256 task keys, shared in-flight work, and replay detection in `ingest/pipeline.py` | `test_idempotent_replay_and_concurrency`; DOI URL/casing forms in `test_task_key_stable` |
| AcademicPaper schema and normalization | Pydantic model in `ingest/schema.py`; deterministic extractor and optional vLLM extractor in `ingest/extract.py` | `test_disclaimer_forced`, schema assertions in `test_ingestion_outputs_required_schema_vector_and_dual_index_outbox` |
| Semantic embeddings and Neo4j Cypher | Normalized 384-dimensional Sentence Transformers vector in `ingest/embed.py`; parameterized Cypher `MERGE` statements in `ingest/cypher.py`; both destinations recorded in the outbox and delivered by `ingest/relay.py` | `test_semantic_embedder_uses_paper_text_and_requests_normalized_dense_vector`, `test_cypher_is_merge_only`, dual-index outbox assertion |
| Mandatory disclaimer | Pydantic validator always injects the required exact text | `test_disclaimer_forced` and integrated ingestion test |

## Run locally

### Windows Command Prompt

From the project directory:

```cmd
python -m venv .venv
call .venv\Scripts\activate.bat
python -m pip install -r requirements.txt -r requirements-api.txt
python -m pytest -q
python -m ingest.main samples\papers.jsonl
```

Expected tests: `8 passed`. The sample batch includes two valid papers, one repeated DOI, and one invalid DOI; the invalid record should report `failed` validation while the rest of the batch continues.

The first semantic embedding may download the configured model from Hugging Face and can take longer than subsequent runs. The Hugging Face unauthenticated-request message is a rate-limit warning, not an ingestion failure; a token is optional for this public model. Override `EMBEDDING_MODEL` to use another Sentence Transformers model only if it produces exactly 384 dimensions.

Run the API locally using SQLite:

```cmd
set DATABASE_URL=
python -m ingest.server
```

Open <http://127.0.0.1:8080/docs>, expand **POST /ingest**, click **Try it out**, enter a paper with a valid and preferably new DOI, and click **Execute**. The response appears under **Server response**. The first submission should be `complete`; a later submission with the same DOI should be `duplicate`. Stop the server with Ctrl+C.

For each new Command Prompt, activate the virtual environment before running project commands:

```cmd
cd /d C:\path\to\academic-ingest
call .venv\Scripts\activate.bat
```

### Linux or macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements-api.txt
python -m pytest -q
python -m ingest.main samples/papers.jsonl
```

Start the local API with `unset DATABASE_URL && python -m ingest.server`.

## Example API request

Use this JSON as the body for `POST /ingest` in Swagger:

```json
{
  "doi": "10.1000/example.123",
  "title": "Example paper on quantum systems",
  "authors": ["A. Researcher"],
  "abstract": "We measured 127 qubits at 15 mK.",
  "body": "The fidelity was 99.9%. The result is $F = 1 - epsilon$."
}
```

The response includes the normalized `paper`, generated `cypher` statements, `embedding_dim`, `status`, and any `errors`. The disclaimer is always set to `100% AI-Generated & Categorized Data Insight`.

`embedding_dim` reports the vector length, not the vector itself. The API response includes Cypher statements so the graph work can be inspected; the relay uses the parameterized form stored in the outbox when it writes to Neo4j.

## Test coverage

Run all unit tests with:

```bash
python -m pytest -q
```

The eight tests cover:

- Stable idempotency keys for HTTPS, HTTP, and `doi:` DOI forms.
- Enforcement of the required disclaimer.
- Idempotent handling of concurrent and repeated requests.
- Required normalized paper fields and dual-index outbox entries.
- Retry of incomplete pipeline stages after a partial result.
- Normalized semantic 384-dimensional vector generation via the mocked model contract.
- Cypher generation using `MERGE`, not `CREATE`.
- Isolation of invalid inputs in a batch.

The unit tests use an in-memory store and a fake embedding model; they do not require a running PostgreSQL, Neo4j, Qdrant, Docker, or Fly.io account. They do not prove connectivity to external services. Run a configured integration test/deployment and confirm the new DOI in Neo4j and its point in Qdrant to verify real dual-index delivery.

## Design decisions

- **Stable idempotency:** a SHA-256 key derived from the normalized DOI identifies a paper. Replays do not reprocess a successfully stored paper; concurrent identical requests share in-flight work.
- **Canonical record and outbox:** the paper and its downstream Qdrant/Neo4j work are committed together. PostgreSQL is the durable option; SQLite is the lightweight local stand-in.
- **At-least-once delivery:** the relay marks outbox entries done only after a sink write succeeds. Qdrant upserts by point ID and Neo4j uses `MERGE`, allowing safe retries.
- **Partial success:** normalization is required, while vector and graph preparation are independent optional stages. A failure in an optional stage can produce `partial` rather than block the whole batch.
- **Semantic embedding:** Sentence Transformers `all-MiniLM-L6-v2` embeds title, authors, abstract/body, extracted metrics, formulas, and taxonomy tags into a normalized 384-dimensional vector.
- **Safe graph parameters:** Cypher is stored with parameters for execution; rendered statements in the API response are illustrative output.

## Optional SLM extraction

The deterministic rule extractor is the default. To try an OpenAI-compatible vLLM endpoint, set `USE_VLLM=1` and `VLLM_URL` before starting the app. Invalid or unavailable SLM output falls back to rule extraction. Embedding is performed separately using the Sentence Transformers model.

## Full Fly.io demo (API + PostgreSQL + Neo4j + Qdrant)

This deploys the API and outbox relay as separate process groups, with PostgreSQL, Neo4j, and Qdrant in their own private Fly apps. Fly builds the API image remotely from the Dockerfile; Docker Desktop is not required on your Windows PC. Install `flyctl` using Fly's official Windows instructions and sign in:

```cmd
fly auth login
```

Choose one Fly region and use it for all apps. Replace each `REPLACE-WITH-...` placeholder in the `deploy\fly\*.toml` files with your unique app names and chosen region. The app names must match the command arguments and `.internal` hostnames below. Choose a private alphanumeric database password; do not publish it or commit it to the repository.

Create and deploy PostgreSQL:

```cmd
fly apps create YOUR-POSTGRES-APP
fly volumes create pg_data --size 10 --region YOUR-REGION --app YOUR-POSTGRES-APP
fly secrets set --app YOUR-POSTGRES-APP "POSTGRES_PASSWORD=YOUR_ALPHANUMERIC_PASSWORD"
fly deploy --config deploy\fly\postgres.toml
```

Create and deploy Neo4j:

```cmd
fly apps create YOUR-NEO4J-APP
fly volumes create neo4j_data --size 10 --region YOUR-REGION --app YOUR-NEO4J-APP
fly secrets set --app YOUR-NEO4J-APP "NEO4J_AUTH=neo4j/YOUR_ALPHANUMERIC_PASSWORD"
fly deploy --config deploy\fly\neo4j.toml
```

Create and deploy Qdrant:

```cmd
fly apps create YOUR-QDRANT-APP
fly volumes create qdrant_data --size 10 --region YOUR-REGION --app YOUR-QDRANT-APP
fly deploy --config deploy\fly\qdrant.toml
```

Edit `deploy\fly\api.toml`: replace the API app name, region, Neo4j app hostname, and Qdrant app hostname. Then create and deploy the API/relay app, storing credentials as Fly secrets:

```cmd
fly apps create YOUR-API-APP
fly secrets set --app YOUR-API-APP "DATABASE_URL=postgresql://postgres:YOUR_ALPHANUMERIC_PASSWORD@YOUR-POSTGRES-APP.internal:5432/ingest" "NEO4J_PASSWORD=YOUR_ALPHANUMERIC_PASSWORD"
fly deploy --config deploy\fly\api.toml
fly status --app YOUR-API-APP
```

Allow the API and relay to start and download the embedding model on first use. Check each app's logs if it does not become healthy:

```cmd
fly logs --app YOUR-API-APP
fly logs --app YOUR-POSTGRES-APP
fly logs --app YOUR-NEO4J-APP
fly logs --app YOUR-QDRANT-APP
```

Open `https://YOUR-API-APP.fly.dev/docs` and submit a valid paper with a new DOI. Confirm the API health endpoint returns `{"ok":true}`:

```cmd
curl https://YOUR-API-APP.fly.dev/health
```

To verify Neo4j without making its browser public, start a local proxy:

```cmd
fly proxy 7474:7474 --app YOUR-NEO4J-APP
```

Open `http://127.0.0.1:7474`, sign in as `neo4j` with the configured password, and run:

```cypher
MATCH (p:Paper) RETURN p LIMIT 10
```

Verify Qdrant with a second local proxy:

```cmd
fly proxy 6333:6333 --app YOUR-QDRANT-APP
```

Take the response's `task_key`, format its 32 hexadecimal characters as a UUID (`8-4-4-4-12`), and query the point:

```cmd
curl "http://127.0.0.1:6333/collections/papers/points/YOUR-TASK-KEY-AS-UUID?with_payload=true&with_vector=false"
```

The full persistence requirement is verified only when the API health check passes and the newly submitted DOI appears in Neo4j and its vector point appears in Qdrant. API `complete` means the PostgreSQL outbox commit succeeded; the separate relay performs eventual delivery and retries failures.

This self-hosted Fly setup is for a demo, not high availability: each database has a single Fly volume, and volumes are not automatically replicated. Set up and test backups before storing important data. No Fly deployment is included in the local unit-test results.
