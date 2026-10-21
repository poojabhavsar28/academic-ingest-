"""Pydantic contract for SLM output. The disclaimer is enforced, not trusted."""
from pydantic import BaseModel, Field, field_validator

DISCLAIMER = "100% AI-Generated & Categorized Data Insight"


def normalize_doi(value: str) -> str:
    normalized = value.strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        normalized = normalized.removeprefix(prefix)
    return normalized


class HardwareMetric(BaseModel):
    name: str
    value: float
    unit: str | None = None


class AcademicPaper(BaseModel):
    doi: str
    title: str
    authors: list[str] = Field(default_factory=list)
    hardware_metrics: list[HardwareMetric] = Field(default_factory=list)
    latex_formulas: list[str] = Field(default_factory=list)
    taxonomy_tags: list[str] = Field(default_factory=list)
    disclaimer: str = DISCLAIMER

    @field_validator("doi")
    @classmethod
    def _doi(cls, v: str) -> str:
        v = normalize_doi(v)
        if not v.startswith("10."):
            raise ValueError("invalid DOI")
        return v

    @field_validator("disclaimer")
    @classmethod
    def _force_disclaimer(cls, v: str) -> str:  # mandatory metadata, always injected
        return DISCLAIMER
