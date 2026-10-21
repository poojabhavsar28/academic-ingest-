"""Idempotent Cypher: every write is MERGE, so replays never duplicate nodes/edges."""
import json
from .schema import AcademicPaper


def build_cypher(p: AcademicPaper) -> list[tuple[str, dict]]:
    base = {"doi": p.doi}
    out = [("MERGE (p:Paper {doi:$doi}) SET p.title=$title, p.disclaimer=$disclaimer",
            {**base, "title": p.title, "disclaimer": p.disclaimer})]
    for a in p.authors:
        out.append(("MATCH (p:Paper {doi:$doi}) MERGE (a:Author {name:$name}) MERGE (a)-[:AUTHORED]->(p)", {**base, "name": a}))
    for t in p.taxonomy_tags:
        out.append(("MATCH (p:Paper {doi:$doi}) MERGE (t:Tag {name:$name}) MERGE (p)-[:TAGGED]->(t)", {**base, "name": t}))
    for m in p.hardware_metrics:
        key = f"{p.doi}|{m.name}|{m.value}"
        out.append(("MATCH (p:Paper {doi:$doi}) MERGE (m:HardwareMetric {key:$key}) SET m.name=$name, m.value=$value, m.unit=$unit MERGE (p)-[:REPORTS]->(m)",
                    {**base, "key": key, "name": m.name, "value": m.value, "unit": m.unit}))
    return out


def render(query: str, params: dict) -> str:
    """Literal query string (for the exercise output); production uses bound parameters."""
    for k in sorted(params, key=len, reverse=True):
        query = query.replace(f"${k}", json.dumps(params[k], ensure_ascii=False))
    return query + ";"
