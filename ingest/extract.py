"""Schema normalization. SLM (Llama-3.2-3B on vLLM) first; deterministic fallback."""
import asyncio, json, os, re, urllib.request
from .schema import AcademicPaper, HardwareMetric

UNITS = r"qubits?|GHz|MHz|mW|W|GB|TOPS|ms|µs|us|mK|K|%"
METRIC = re.compile(rf"(\d+(?:\.\d+)?)\s*({UNITS})(?![A-Za-z])")
LATEX = re.compile(r"\$\$(.+?)\$\$|\$([^$\n]+?)\$", re.S)
TAGS = {
    "quantum": ["quantum", "qubit"], "neuromorphic": ["neuromorphic", "spiking"],
    "solid-state-battery": ["solid-state", "electrolyte"], "thermoelectric": ["thermoelectric"],
    "fusion": ["fusion", "tokamak"], "bio-computing": ["protein", "bio-computing"],
}


class RuleExtractor:
    async def extract(self, raw: dict) -> AcademicPaper:
        text = f"{raw.get('abstract', '')}\n{raw.get('body', '')}"
        metrics = [HardwareMetric(name=u.lower(), value=float(v), unit=u) for v, u in METRIC.findall(text)]
        formulas = [(a or b).strip() for a, b in LATEX.findall(text)]
        low = text.lower() + " " + str(raw.get("title", "")).lower()
        tags = [t for t, kws in TAGS.items() if any(k in low for k in kws)]
        return AcademicPaper(doi=raw["doi"], title=raw["title"], authors=raw.get("authors", []),
                             hardware_metrics=metrics, latex_formulas=formulas, taxonomy_tags=tags)


class VLLMExtractor:
    """OpenAI-compatible vLLM endpoint serving meta-llama/Llama-3.2-3B-Instruct."""
    def __init__(self, url=None, model="meta-llama/Llama-3.2-3B-Instruct"):
        self.url = url or os.getenv("VLLM_URL", "http://localhost:8000/v1/chat/completions")
        self.model, self.fallback = model, RuleExtractor()

    def _call(self, raw: dict) -> dict:
        body = json.dumps({"model": self.model, "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [{"role": "system", "content": "Return ONLY JSON matching: doi,title,authors[],hardware_metrics[{name,value,unit}],latex_formulas[],taxonomy_tags[]"},
                         {"role": "user", "content": json.dumps(raw)[:12000]}]}).encode()
        req = urllib.request.Request(self.url, body, {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(json.load(r)["choices"][0]["message"]["content"])

    async def extract(self, raw: dict) -> AcademicPaper:
        try:
            return AcademicPaper.model_validate(await asyncio.to_thread(self._call, raw))
        except Exception:  # SLM down or invalid JSON -> degrade, never halt
            return await self.fallback.extract(raw)
