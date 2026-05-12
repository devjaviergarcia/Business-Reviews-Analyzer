from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AnalysisJobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    NEEDS_HUMAN = "needs_human"
    RETRYING = "retrying"
    PARTIAL = "partial"


ScrapeQueueName = Literal["scrape", "scrape_google_maps", "scrape_tripadvisor"]
JobQueueName = Literal["scrape", "scrape_google_maps", "scrape_tripadvisor", "analysis", "report", "crm"]
JobType = Literal[
    "business_analyze",
    "business_reanalyze",
    "analysis_generate",
    "report_generate",
    "crm_lead_discovery",
    "crm_lead_pipeline",
    "crm_campaign_dispatch",
]

ReportFormat = Literal["pdf", "typst", "html", "json"]
ReportSourceMode = Literal["auto", "combined", "single"]
ReportSelectedSource = Literal["google_maps", "tripadvisor"]
CRMLeadPipelineSource = Literal["google_maps", "tripadvisor"]


class JobProgressEvent(BaseModel):
    status: AnalysisJobStatus
    stage: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class JobProgressState(BaseModel):
    status: AnalysisJobStatus
    stage: str
    message: str
    updated_at: datetime

    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class AnalyzeBusinessTaskPayload(BaseModel):
    name: str
    canonical_name: str | None = None
    canonical_name_normalized: str | None = None
    source_name: str | None = None
    source_name_normalized: str | None = None
    root_business_id: str | None = None
    force: bool = False
    strategy: str | None = None
    force_mode: str | None = None
    interactive_max_rounds: int | None = None
    html_scroll_max_rounds: int | None = None
    html_stable_rounds: int | None = None
    tripadvisor_max_pages: int | None = None
    tripadvisor_pages_percent: float | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("Business name cannot be empty.")
        return cleaned

    @field_validator(
        "canonical_name",
        "canonical_name_normalized",
        "source_name",
        "source_name_normalized",
        "root_business_id",
        mode="before",
    )
    @classmethod
    def normalize_optional_text_fields(cls, value: object) -> object:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @field_validator("strategy", mode="before")
    @classmethod
    def normalize_strategy(cls, value: object) -> object:
        if value is None:
            return None
        raw = str(value).strip()
        return raw or None

    @field_validator("force_mode", mode="before")
    @classmethod
    def normalize_force_mode(cls, value: object) -> object:
        if value is None:
            return None
        raw = str(value).strip().lower().replace("-", "_").replace(" ", "_")
        return raw or None

    @field_validator("interactive_max_rounds", mode="before")
    @classmethod
    def normalize_interactive_max_rounds(cls, value: object) -> object:
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed < 1:
            raise ValueError("interactive_max_rounds must be >= 1.")
        return parsed

    @field_validator("html_scroll_max_rounds", mode="before")
    @classmethod
    def normalize_html_scroll_max_rounds(cls, value: object) -> object:
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed < 0:
            raise ValueError("html_scroll_max_rounds must be >= 0.")
        return parsed

    @field_validator("html_stable_rounds", mode="before")
    @classmethod
    def normalize_html_stable_rounds(cls, value: object) -> object:
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed < 2:
            raise ValueError("html_stable_rounds must be >= 2.")
        return parsed

    @field_validator("tripadvisor_max_pages", mode="before")
    @classmethod
    def normalize_tripadvisor_max_pages(cls, value: object) -> object:
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed < 1:
            raise ValueError("tripadvisor_max_pages must be >= 1.")
        return parsed

    @field_validator("tripadvisor_pages_percent", mode="before")
    @classmethod
    def normalize_tripadvisor_pages_percent(cls, value: object) -> object:
        if value is None or value == "":
            return None
        parsed = float(value)
        if parsed <= 0 or parsed > 100:
            raise ValueError("tripadvisor_pages_percent must be > 0 and <= 100.")
        return parsed


class AnalyzeBusinessJobEnvelope(BaseModel):
    queue_name: ScrapeQueueName = "scrape"
    job_type: Literal["business_analyze"] = "business_analyze"
    payload: AnalyzeBusinessTaskPayload

    model_config = ConfigDict(extra="forbid")


class AnalysisGenerateTaskPayload(BaseModel):
    business_id: str
    dataset_id: str | None = None
    source_profile_id: str | None = None
    scrape_run_id: str | None = None
    batchers: list[str] | None = None
    batch_size: int | None = None
    max_reviews_pool: int | None = None
    source_job_id: str | None = None
    source_mode: ReportSourceMode = "auto"
    selected_source: ReportSelectedSource | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("business_id")
    @classmethod
    def validate_business_id(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("business_id cannot be empty.")
        return cleaned

    @field_validator("batchers", mode="before")
    @classmethod
    def normalize_batchers(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("dataset_id", "source_profile_id", "scrape_run_id", "source_job_id", mode="before")
    @classmethod
    def normalize_optional_ids(cls, value: object) -> object:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @field_validator("source_mode", mode="before")
    @classmethod
    def normalize_source_mode(cls, value: object) -> object:
        if value is None:
            return "auto"
        cleaned = str(value).strip().lower()
        return cleaned or "auto"

    @field_validator("selected_source", mode="before")
    @classmethod
    def normalize_selected_source(cls, value: object) -> object:
        if value is None:
            return None
        cleaned = str(value).strip().lower()
        return cleaned or None


class AnalysisGenerateJobEnvelope(BaseModel):
    queue_name: Literal["analysis"] = "analysis"
    job_type: Literal["analysis_generate"] = "analysis_generate"
    payload: AnalysisGenerateTaskPayload

    model_config = ConfigDict(extra="forbid")


class ReportGenerateTaskPayload(BaseModel):
    business_id: str
    analysis_id: str
    output_format: ReportFormat = "pdf"
    locale: str | None = None
    template_id: str | None = None
    source_job_id: str | None = None
    source_mode: ReportSourceMode = "auto"
    selected_source: ReportSelectedSource | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("business_id", "analysis_id")
    @classmethod
    def validate_required_ids(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("Required id field cannot be empty.")
        return cleaned

    @field_validator("output_format", mode="before")
    @classmethod
    def normalize_output_format(cls, value: object) -> object:
        if value is None:
            return "pdf"
        cleaned = str(value).strip().lower()
        return cleaned or "pdf"

    @field_validator("source_mode", mode="before")
    @classmethod
    def normalize_source_mode(cls, value: object) -> object:
        if value is None:
            return "auto"
        cleaned = str(value).strip().lower()
        return cleaned or "auto"

    @field_validator("selected_source", mode="before")
    @classmethod
    def normalize_selected_source(cls, value: object) -> object:
        if value is None:
            return None
        cleaned = str(value).strip().lower()
        return cleaned or None


class ReportGenerateJobEnvelope(BaseModel):
    queue_name: Literal["report"] = "report"
    job_type: Literal["report_generate"] = "report_generate"
    payload: ReportGenerateTaskPayload

    model_config = ConfigDict(extra="forbid")


class CRMLeadDiscoveryTaskPayload(BaseModel):
    query: str
    city: str | None = None
    category: str | None = None
    limit: int = 100
    source: str = "auto_live_google_maps"
    discovery_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("query cannot be empty.")
        return cleaned

    @field_validator("city", "category", "discovery_run_id", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: object) -> object:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @field_validator("source", mode="before")
    @classmethod
    def normalize_source(cls, value: object) -> object:
        cleaned = str(value or "").strip().lower()
        return cleaned or "auto_live_google_maps"

    @field_validator("limit", mode="before")
    @classmethod
    def normalize_limit(cls, value: object) -> object:
        if value is None or value == "":
            return 100
        parsed = int(value)
        if parsed < 1:
            raise ValueError("limit must be >= 1.")
        return min(parsed, 5000)


class CRMLeadPipelineTaskPayload(BaseModel):
    lead_id: str
    force: bool = False
    sources: list[CRMLeadPipelineSource] = Field(default_factory=lambda: ["google_maps", "tripadvisor"])
    google_maps_name: str | None = None
    tripadvisor_name: str | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("lead_id")
    @classmethod
    def validate_lead_id(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("lead_id cannot be empty.")
        return cleaned

    @field_validator("google_maps_name", "tripadvisor_name", mode="before")
    @classmethod
    def normalize_optional_names(cls, value: object) -> object:
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @field_validator("sources", mode="before")
    @classmethod
    def normalize_sources(cls, value: object) -> object:
        if value is None:
            return ["google_maps", "tripadvisor"]
        if isinstance(value, str):
            normalized = str(value).strip().lower()
            return [normalized] if normalized else ["google_maps", "tripadvisor"]
        if isinstance(value, (list, tuple)):
            normalized_list: list[str] = []
            for item in value:
                cleaned = str(item or "").strip().lower()
                if cleaned and cleaned not in normalized_list:
                    normalized_list.append(cleaned)
            return normalized_list or ["google_maps", "tripadvisor"]
        return value


class CRMCampaignDispatchTaskPayload(BaseModel):
    campaign_id: str
    message_id: str

    model_config = ConfigDict(extra="forbid")

    @field_validator("campaign_id", "message_id")
    @classmethod
    def validate_required_ids(cls, value: str) -> str:
        cleaned = str(value or "").strip()
        if not cleaned:
            raise ValueError("Required id field cannot be empty.")
        return cleaned


class CRMLeadDiscoveryJobEnvelope(BaseModel):
    queue_name: Literal["crm", "scrape_google_maps"] = "crm"
    job_type: Literal["crm_lead_discovery"] = "crm_lead_discovery"
    payload: CRMLeadDiscoveryTaskPayload

    model_config = ConfigDict(extra="forbid")


class CRMLeadPipelineJobEnvelope(BaseModel):
    queue_name: Literal["crm"] = "crm"
    job_type: Literal["crm_lead_pipeline"] = "crm_lead_pipeline"
    payload: CRMLeadPipelineTaskPayload

    model_config = ConfigDict(extra="forbid")


class CRMCampaignDispatchJobEnvelope(BaseModel):
    queue_name: Literal["crm"] = "crm"
    job_type: Literal["crm_campaign_dispatch"] = "crm_campaign_dispatch"
    payload: CRMCampaignDispatchTaskPayload

    model_config = ConfigDict(extra="forbid")


WorkerTaskPayload = (
    AnalyzeBusinessTaskPayload
    | AnalysisGenerateTaskPayload
    | ReportGenerateTaskPayload
    | CRMLeadDiscoveryTaskPayload
    | CRMLeadPipelineTaskPayload
    | CRMCampaignDispatchTaskPayload
)


def parse_analyze_business_payload(job_doc: Mapping[str, Any]) -> AnalyzeBusinessTaskPayload:
    raw_payload = job_doc.get("payload")
    if isinstance(raw_payload, dict):
        envelope = AnalyzeBusinessJobEnvelope.model_validate(
            {
                "queue_name": str(job_doc.get("queue_name") or "scrape"),
                "job_type": str(job_doc.get("job_type") or "business_analyze"),
                "payload": raw_payload,
            }
        )
        return envelope.payload

    # Backward compatibility for legacy queued jobs without explicit payload.
    return AnalyzeBusinessTaskPayload.model_validate(
        {
            "name": str(job_doc.get("name", "")).strip(),
            "canonical_name": str(job_doc.get("canonical_name") or "").strip() or None,
            "canonical_name_normalized": str(job_doc.get("canonical_name_normalized") or "").strip() or None,
            "source_name": str(job_doc.get("source_name") or "").strip() or None,
            "source_name_normalized": str(job_doc.get("source_name_normalized") or "").strip() or None,
            "root_business_id": str(job_doc.get("root_business_id") or "").strip() or None,
            "force": bool(job_doc.get("force", False)),
            "strategy": str(job_doc.get("strategy") or "").strip() or None,
            "force_mode": str(job_doc.get("force_mode") or "").strip() or None,
            "interactive_max_rounds": job_doc.get("interactive_max_rounds"),
            "html_scroll_max_rounds": job_doc.get("html_scroll_max_rounds"),
            "html_stable_rounds": job_doc.get("html_stable_rounds"),
            "tripadvisor_max_pages": job_doc.get("tripadvisor_max_pages"),
            "tripadvisor_pages_percent": job_doc.get("tripadvisor_pages_percent"),
        }
    )


def parse_analysis_generate_payload(job_doc: Mapping[str, Any]) -> AnalysisGenerateTaskPayload:
    raw_payload = job_doc.get("payload")
    if isinstance(raw_payload, dict):
        envelope = AnalysisGenerateJobEnvelope.model_validate(
            {
                "queue_name": str(job_doc.get("queue_name") or "analysis"),
                "job_type": str(job_doc.get("job_type") or "analysis_generate"),
                "payload": raw_payload,
            }
        )
        return envelope.payload

    # Legacy fallback for top-level fields.
    return AnalysisGenerateTaskPayload.model_validate(
        {
            "business_id": str(job_doc.get("business_id", "")).strip(),
            "dataset_id": str(job_doc.get("dataset_id") or "").strip() or None,
            "source_profile_id": str(job_doc.get("source_profile_id") or "").strip() or None,
            "scrape_run_id": str(job_doc.get("scrape_run_id") or "").strip() or None,
            "batchers": job_doc.get("batchers"),
            "batch_size": job_doc.get("batch_size"),
            "max_reviews_pool": job_doc.get("max_reviews_pool"),
            "source_job_id": str(job_doc.get("source_job_id") or "").strip() or None,
            "source_mode": str(job_doc.get("source_mode") or "auto"),
            "selected_source": (
                str(job_doc.get("selected_source")).strip()
                if job_doc.get("selected_source") is not None
                else None
            ),
        }
    )


def parse_report_generate_payload(job_doc: Mapping[str, Any]) -> ReportGenerateTaskPayload:
    raw_payload = job_doc.get("payload")
    if isinstance(raw_payload, dict):
        envelope = ReportGenerateJobEnvelope.model_validate(
            {
                "queue_name": str(job_doc.get("queue_name") or "report"),
                "job_type": str(job_doc.get("job_type") or "report_generate"),
                "payload": raw_payload,
            }
        )
        return envelope.payload

    # Legacy fallback for top-level fields.
    return ReportGenerateTaskPayload.model_validate(
        {
            "business_id": str(job_doc.get("business_id", "")).strip(),
            "analysis_id": str(job_doc.get("analysis_id", "")).strip(),
            "output_format": str(job_doc.get("output_format") or job_doc.get("format") or "pdf"),
            "locale": str(job_doc.get("locale") or "").strip() or None,
            "template_id": str(job_doc.get("template_id") or "").strip() or None,
            "source_job_id": str(job_doc.get("source_job_id") or "").strip() or None,
            "source_mode": str(job_doc.get("source_mode") or "auto"),
            "selected_source": (
                str(job_doc.get("selected_source")).strip()
                if job_doc.get("selected_source") is not None
                else None
            ),
        }
    )


def parse_crm_lead_discovery_payload(job_doc: Mapping[str, Any]) -> CRMLeadDiscoveryTaskPayload:
    raw_payload = job_doc.get("payload")
    if isinstance(raw_payload, dict):
        envelope = CRMLeadDiscoveryJobEnvelope.model_validate(
            {
                "queue_name": str(job_doc.get("queue_name") or "crm"),
                "job_type": str(job_doc.get("job_type") or "crm_lead_discovery"),
                "payload": raw_payload,
            }
        )
        return envelope.payload

    return CRMLeadDiscoveryTaskPayload.model_validate(
        {
            "query": str(job_doc.get("query", "")).strip(),
            "city": str(job_doc.get("city") or "").strip() or None,
            "category": str(job_doc.get("category") or "").strip() or None,
            "limit": job_doc.get("limit"),
            "source": str(job_doc.get("source") or "auto_live_google_maps"),
        }
    )


def parse_crm_lead_pipeline_payload(job_doc: Mapping[str, Any]) -> CRMLeadPipelineTaskPayload:
    raw_payload = job_doc.get("payload")
    if isinstance(raw_payload, dict):
        envelope = CRMLeadPipelineJobEnvelope.model_validate(
            {
                "queue_name": str(job_doc.get("queue_name") or "crm"),
                "job_type": str(job_doc.get("job_type") or "crm_lead_pipeline"),
                "payload": raw_payload,
            }
        )
        return envelope.payload

    return CRMLeadPipelineTaskPayload.model_validate(
        {
            "lead_id": str(job_doc.get("lead_id") or "").strip(),
            "force": bool(job_doc.get("force", False)),
            "sources": job_doc.get("sources"),
            "google_maps_name": str(job_doc.get("google_maps_name") or "").strip() or None,
            "tripadvisor_name": str(job_doc.get("tripadvisor_name") or "").strip() or None,
        }
    )


def parse_crm_campaign_dispatch_payload(job_doc: Mapping[str, Any]) -> CRMCampaignDispatchTaskPayload:
    raw_payload = job_doc.get("payload")
    if isinstance(raw_payload, dict):
        envelope = CRMCampaignDispatchJobEnvelope.model_validate(
            {
                "queue_name": str(job_doc.get("queue_name") or "crm"),
                "job_type": str(job_doc.get("job_type") or "crm_campaign_dispatch"),
                "payload": raw_payload,
            }
        )
        return envelope.payload

    return CRMCampaignDispatchTaskPayload.model_validate(
        {
            "campaign_id": str(job_doc.get("campaign_id") or "").strip(),
            "message_id": str(job_doc.get("message_id") or "").strip(),
        }
    )


def build_worker_job_envelope(
    *,
    queue_name: JobQueueName,
    job_type: JobType,
    task_payload: WorkerTaskPayload,
) -> (
    AnalyzeBusinessJobEnvelope
    | AnalysisGenerateJobEnvelope
    | ReportGenerateJobEnvelope
    | CRMLeadDiscoveryJobEnvelope
    | CRMLeadPipelineJobEnvelope
    | CRMCampaignDispatchJobEnvelope
):
    normalized_queue = str(queue_name or "").strip().lower()
    normalized_job_type = str(job_type or "").strip().lower()

    if normalized_queue in {"scrape", "scrape_google_maps", "scrape_tripadvisor"} and normalized_job_type == "business_analyze":
        if not isinstance(task_payload, AnalyzeBusinessTaskPayload):
            raise TypeError("Expected AnalyzeBusinessTaskPayload for scrape/business_analyze.")
        return AnalyzeBusinessJobEnvelope(
            queue_name=normalized_queue,
            job_type="business_analyze",
            payload=task_payload,
        )

    if normalized_queue == "analysis" and normalized_job_type == "analysis_generate":
        if not isinstance(task_payload, AnalysisGenerateTaskPayload):
            raise TypeError("Expected AnalysisGenerateTaskPayload for analysis/analysis_generate.")
        return AnalysisGenerateJobEnvelope(
            queue_name="analysis",
            job_type="analysis_generate",
            payload=task_payload,
        )

    if normalized_queue == "report" and normalized_job_type == "report_generate":
        if not isinstance(task_payload, ReportGenerateTaskPayload):
            raise TypeError("Expected ReportGenerateTaskPayload for report/report_generate.")
        return ReportGenerateJobEnvelope(
            queue_name="report",
            job_type="report_generate",
            payload=task_payload,
        )

    if normalized_queue in {"crm", "scrape_google_maps"} and normalized_job_type == "crm_lead_discovery":
        if not isinstance(task_payload, CRMLeadDiscoveryTaskPayload):
            raise TypeError(
                "Expected CRMLeadDiscoveryTaskPayload for crm|scrape_google_maps/crm_lead_discovery."
            )
        return CRMLeadDiscoveryJobEnvelope(
            queue_name=normalized_queue,
            job_type="crm_lead_discovery",
            payload=task_payload,
        )

    if normalized_queue == "crm" and normalized_job_type == "crm_lead_pipeline":
        if not isinstance(task_payload, CRMLeadPipelineTaskPayload):
            raise TypeError("Expected CRMLeadPipelineTaskPayload for crm/crm_lead_pipeline.")
        return CRMLeadPipelineJobEnvelope(
            queue_name="crm",
            job_type="crm_lead_pipeline",
            payload=task_payload,
        )

    if normalized_queue == "crm" and normalized_job_type == "crm_campaign_dispatch":
        if not isinstance(task_payload, CRMCampaignDispatchTaskPayload):
            raise TypeError("Expected CRMCampaignDispatchTaskPayload for crm/crm_campaign_dispatch.")
        return CRMCampaignDispatchJobEnvelope(
            queue_name="crm",
            job_type="crm_campaign_dispatch",
            payload=task_payload,
        )

    raise ValueError(f"Unsupported queue/job pair: queue_name={queue_name!r}, job_type={job_type!r}.")


class AnalysisJobQueueDocument(BaseModel):
    queue_name: JobQueueName = "scrape"
    job_type: JobType = "business_analyze"
    payload: dict[str, Any] = Field(default_factory=dict)
    name: str | None = None
    name_normalized: str | None = None
    canonical_name: str | None = None
    canonical_name_normalized: str | None = None
    source_name: str | None = None
    source_name_normalized: str | None = None
    root_business_id: str | None = None
    force: bool | None = None
    strategy: str | None = None
    force_mode: str | None = None
    interactive_max_rounds: int | None = None
    html_scroll_max_rounds: int | None = None
    html_stable_rounds: int | None = None
    tripadvisor_max_pages: int | None = None
    tripadvisor_pages_percent: float | None = None
    status: AnalysisJobStatus
    progress: JobProgressState
    events: list[JobProgressEvent] = Field(default_factory=list)
    cancel_requested: bool = False
    cancel_requested_at: datetime | None = None
    cancel_reason: str | None = None
    attempts: int = 0
    error: str | None = None
    result: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None

    model_config = ConfigDict(extra="forbid", use_enum_values=True)
