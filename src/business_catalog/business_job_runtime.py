from __future__ import annotations

from typing import Any, Callable

from src.workers.contracts import AnalysisGenerateTaskPayload


DatabaseFactory = Callable[[], Any]
ParseObjectIdFn = Callable[..., Any]


class BusinessJobRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        parse_object_id: ParseObjectIdFn,
        job_service: Any,
        businesses_collection_name: str,
    ) -> None:
        self._database_factory = database_factory
        self._parse_object_id = parse_object_id
        self._job_service = job_service
        self._businesses_collection_name = businesses_collection_name

    async def enqueue_business_analysis_generate_job(
        self,
        *,
        business_id: str,
        dataset_id: str | None = None,
        batchers: list[str] | None = None,
        batch_size: int | None = None,
        max_reviews_pool: int | None = None,
        source_job_id: str | None = None,
        source_mode: str | None = None,
        selected_source: str | None = None,
    ) -> dict[str, Any]:
        parsed_business_id = self._parse_object_id(business_id, field_name='business_id')
        businesses = self._database_factory()[self._businesses_collection_name]
        business_doc = await businesses.find_one({'_id': parsed_business_id}, projection={'_id': 1})
        if business_doc is None:
            raise LookupError(f"Business '{business_id}' not found.")
        payload = AnalysisGenerateTaskPayload(
            business_id=str(parsed_business_id),
            dataset_id=str(dataset_id or '').strip() or None,
            batchers=batchers,
            batch_size=batch_size,
            max_reviews_pool=max_reviews_pool,
            source_job_id=str(source_job_id or '').strip() or None,
            source_mode=str(source_mode or 'auto').strip().lower() or 'auto',
            selected_source=str(selected_source).strip().lower() if selected_source is not None else None,
        )
        return await self._job_service.enqueue_analysis_generate_job(task_payload=payload)

    @staticmethod
    def ensure_job_is_scrape(job_payload: dict[str, Any]) -> None:
        queue_name = str(job_payload.get('queue_name') or '').strip().lower()
        job_type = str(job_payload.get('job_type') or '').strip().lower()
        if job_type != 'business_analyze' or queue_name not in {'scrape', 'scrape_google_maps', 'scrape_tripadvisor'}:
            raise ValueError(
                'Job is not a scrape job. Expected job_type=business_analyze and '
                'queue_name in scrape/scrape_google_maps/scrape_tripadvisor.'
            )

    @staticmethod
    def ensure_job_is_analysis(job_payload: dict[str, Any]) -> None:
        queue_name = str(job_payload.get('queue_name') or '').strip().lower()
        job_type = str(job_payload.get('job_type') or '').strip().lower()
        if job_type != 'analysis_generate' or queue_name != 'analysis':
            raise ValueError(
                'Job is not an analysis job. Expected job_type=analysis_generate and queue_name=analysis.'
            )

    @staticmethod
    def ensure_job_is_report(job_payload: dict[str, Any]) -> None:
        queue_name = str(job_payload.get('queue_name') or '').strip().lower()
        job_type = str(job_payload.get('job_type') or '').strip().lower()
        if job_type != 'report_generate' or queue_name != 'report':
            raise ValueError(
                'Job is not a report job. Expected job_type=report_generate and queue_name=report.'
            )
