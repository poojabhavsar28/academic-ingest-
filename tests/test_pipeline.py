import asyncio
import math
from ingest.pipeline import Pipeline, task_key
from ingest.schema import DISCLAIMER, AcademicPaper
from ingest.embed import DIM, embed_paper, paper_text

RAW = {"doi": "10.1000/x.1", "title": "Qubit paper", "authors": ["A"], "abstract": "10 qubits $E=mc^2$"}


def fake_embedder(paper, source):
    return [1.0 / math.sqrt(DIM)] * DIM


def test_task_key_stable():
    keys = {
        task_key(RAW),
        task_key({**RAW, "doi": "https://doi.org/10.1000/X.1"}),
        task_key({**RAW, "doi": "http://doi.org/10.1000/X.1"}),
        task_key({**RAW, "doi": "doi:10.1000/X.1"}),
    }
    assert len(keys) == 1

def test_disclaimer_forced():
    assert AcademicPaper(doi="10.1/a", title="t", disclaimer="nope").disclaimer == DISCLAIMER

def test_idempotent_replay_and_concurrency():
    async def go():
        p = Pipeline(embedder=fake_embedder)
        rs = await asyncio.gather(*(p.process(dict(RAW)) for _ in range(5)))
        again = await p.process(dict(RAW))
        return rs, again, p.store.outbox_count()
    rs, again, n = asyncio.run(go())
    assert sum(r.status == "complete" for r in rs) == 1 and again.status == "duplicate" and n == 2

def test_cypher_is_merge_only():
    r = asyncio.run(Pipeline(embedder=fake_embedder).process(dict(RAW)))
    assert all("MERGE" in q for q in r.cypher) and "CREATE" not in " ".join(r.cypher)


def test_ingestion_outputs_required_schema_vector_and_dual_index_outbox():
    async def go():
        pipeline = Pipeline(embedder=fake_embedder)
        result = await pipeline.process(dict(RAW))
        return result, pipeline.store.outbox_count()

    result, outbox_count = asyncio.run(go())
    assert result.status == "complete"
    assert result.paper["doi"] == RAW["doi"]
    assert result.paper["title"] == RAW["title"]
    assert result.paper["authors"] == RAW["authors"]
    assert result.paper["hardware_metrics"][0]["name"] == "qubits"
    assert result.paper["latex_formulas"] == ["E=mc^2"]
    assert result.paper["taxonomy_tags"] == ["quantum"]
    assert result.paper["disclaimer"] == DISCLAIMER
    assert result.embedding_dim == 384
    assert result.cypher and all("MERGE" in statement for statement in result.cypher)
    assert outbox_count == 2


def test_partial_ingestion_can_retry_missing_index_work():
    calls = 0

    def flaky_embedder(paper, source):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("model temporarily unavailable")
        return fake_embedder(paper, source)

    async def go():
        pipeline = Pipeline(embedder=flaky_embedder)
        first = await pipeline.process(dict(RAW))
        second = await pipeline.process(dict(RAW))
        return first, second, pipeline.store.outbox_count()

    first, second, outbox_count = asyncio.run(go())
    assert first.status == "partial"
    assert second.status == "complete"
    assert outbox_count == 2


def test_semantic_embedder_uses_paper_text_and_requests_normalized_dense_vector(monkeypatch):
    paper = AcademicPaper.model_validate({
        **RAW,
        "taxonomy_tags": ["quantum"],
        "hardware_metrics": [{"name": "qubits", "value": 10, "unit": "qubits"}],
    })
    class FakeModel:
        def encode(self, text, **kwargs):
            assert "Qubit paper" in text
            assert "10 qubits" in text
            assert "qubits 10.0 qubits" in text
            assert "quantum" in text
            assert kwargs["normalize_embeddings"] is True
            assert kwargs["convert_to_numpy"] is True
            class FakeVector:
                def tolist(self):
                    return [1 / math.sqrt(DIM)] * DIM

            return FakeVector()

    monkeypatch.setattr("ingest.embed._load_model", lambda: FakeModel())
    vector = embed_paper(paper, RAW)
    assert len(vector) == DIM
    assert all(math.isfinite(value) for value in vector)
    assert math.isclose(math.sqrt(sum(value * value for value in vector)), 1.0)
    assert paper_text(paper, RAW).find("10 qubits") >= 0


def test_failures_isolated_in_batch():
    rs = asyncio.run(Pipeline(embedder=fake_embedder).run_batch([RAW, {"doi": "bad", "title": "x"}, {"nope": 1}]))
    assert [r.status for r in rs] == ["complete", "failed", "failed"]
