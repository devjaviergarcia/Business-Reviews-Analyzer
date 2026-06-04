from __future__ import annotations

from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, field_validator

BrowserJobSource = Literal["google_maps", "tripadvisor"]
BrowserExecutionMode = Literal["automatic", "live"]
BrowserRuntimeTarget = Literal["local_browser", "server_worker"]
BrowserFallbackPolicy = Literal["none", "suggest_live", "auto_escalate_to_live"]

DEFAULT_BROWSER_EXECUTION_MODE: BrowserExecutionMode = "automatic"
DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET: BrowserRuntimeTarget = "local_browser"
DEFAULT_SERVER_WORKER_RUNTIME_TARGET: BrowserRuntimeTarget = "server_worker"
DEFAULT_BROWSER_FALLBACK_POLICY: BrowserFallbackPolicy = "suggest_live"

LOCAL_BROWSER_JOB_TYPES = frozenset({"business_analyze", "crm_lead_discovery", "geo_grid_study"})
LOCAL_BROWSER_QUEUE_NAMES = frozenset({"scrape", "scrape_google_maps", "scrape_tripadvisor"})


class BrowserJobRuntimeOptions(BaseModel):
    source: BrowserJobSource
    execution_mode: BrowserExecutionMode = DEFAULT_BROWSER_EXECUTION_MODE
    runtime_target: BrowserRuntimeTarget = DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET
    requested_by: str = "internal_api"
    fallback_policy: BrowserFallbackPolicy = DEFAULT_BROWSER_FALLBACK_POLICY
    human_session_id: str | None = None
    source_display_name: str | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("requested_by", mode="before")
    @classmethod
    def normalize_requested_by(cls, value: object) -> str:
        cleaned = str(value or "").strip().lower().replace(" ", "_")
        return cleaned or "internal_api"

    @field_validator("human_session_id", "source_display_name", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: object) -> object:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None


def normalize_browser_source(value: object) -> BrowserJobSource | None:
    cleaned = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if cleaned in {"google_maps", "tripadvisor"}:
        return cleaned  # type: ignore[return-value]
    return None


def infer_browser_source(
    *,
    queue_name: object,
    payload: Mapping[str, Any] | None = None,
    explicit_source: object = None,
) -> BrowserJobSource | None:
    explicit = normalize_browser_source(explicit_source)
    if explicit is not None:
        return explicit

    if isinstance(payload, Mapping):
        from_payload = normalize_browser_source(payload.get("source"))
        if from_payload is not None:
            return from_payload

    normalized_queue = str(queue_name or "").strip().lower()
    if "tripadvisor" in normalized_queue:
        return "tripadvisor"
    if "google_maps" in normalized_queue or normalized_queue == "scrape":
        return "google_maps"
    return None


def default_runtime_target_for_job(*, queue_name: object, job_type: object) -> BrowserRuntimeTarget:
    normalized_queue = str(queue_name or "").strip().lower()
    normalized_job_type = str(job_type or "").strip().lower()
    if normalized_queue in LOCAL_BROWSER_QUEUE_NAMES and normalized_job_type in LOCAL_BROWSER_JOB_TYPES:
        return DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET
    return DEFAULT_SERVER_WORKER_RUNTIME_TARGET


def default_fallback_policy_for_runtime(runtime_target: object) -> BrowserFallbackPolicy:
    normalized_runtime_target = str(runtime_target or "").strip().lower()
    if normalized_runtime_target == DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET:
        return DEFAULT_BROWSER_FALLBACK_POLICY
    return "none"


def default_source_display_name(source: BrowserJobSource | None) -> str | None:
    if source == "google_maps":
        return "Google Maps"
    if source == "tripadvisor":
        return "Tripadvisor"
    return None


def is_local_browser_job(job_doc: Mapping[str, Any]) -> bool:
    runtime_target = str(job_doc.get("runtime_target") or "").strip().lower()
    if runtime_target:
        return runtime_target == DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET

    queue_name = job_doc.get("queue_name")
    job_type = job_doc.get("job_type")
    return default_runtime_target_for_job(queue_name=queue_name, job_type=job_type) == DEFAULT_LOCAL_BROWSER_RUNTIME_TARGET

