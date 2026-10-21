import asyncio, json, os, sys
from .asyncio_compat import configure_windows_event_loop
from .extract import RuleExtractor, VLLMExtractor
from .pipeline import Pipeline


def build_pipeline() -> Pipeline:
    ex = VLLMExtractor() if os.getenv("USE_VLLM") == "1" else RuleExtractor()
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        from .pgstore import PgStore

        return Pipeline(extractor=ex, store=PgStore(database_url))
    return Pipeline(extractor=ex)


async def _cli(path):
    raws = []
    for line in open(path, encoding="utf-8"):
        try: raws.append(json.loads(line))
        except json.JSONDecodeError: raws.append({"doi": "corrupt", "title": ""})
    pipe = build_pipeline()
    if hasattr(pipe.store, "open"):
        await pipe.store.open()
    try:
        for r in await pipe.run_batch(raws):
            print(json.dumps(r.model_dump(), indent=2))
    finally:
        if hasattr(pipe.store, "close"):
            await pipe.store.close()

if __name__ == "__main__":
    configure_windows_event_loop()
    asyncio.run(_cli(sys.argv[1] if len(sys.argv) > 1 else "samples/papers.jsonl"))
