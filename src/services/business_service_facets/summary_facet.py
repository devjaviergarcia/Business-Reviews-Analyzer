from __future__ import annotations

from typing import Any

from bson import ObjectId

from src.config import settings


class BusinessServiceSummaryFacet:

    def _validate_business_name(self, name: str) -> str:
        return self._business_common_runtime.validate_business_name(name)

    def _normalize_text(self, value: str) -> str:
        return self._business_common_runtime.normalize_text(value)

    def _normalize_scraped_review(self, review: dict[str, Any]) -> dict[str, Any]:
        return self._reanalysis_support_runtime.normalize_scraped_review(review)

    def _normalize_stored_review(self, review: dict[str, Any]) -> dict[str, Any]:
        return self._reanalysis_support_runtime.normalize_stored_review(review)

    def _resolve_reanalysis_batchers(self, batchers: list[str] | None) -> list[str]:
        return self._reanalysis_support_runtime.resolve_reanalysis_batchers(
            batchers,
            list(settings.analysis_reanalyze_default_batchers),
        )

    def _build_reanalysis_batches(
        self,
        reviews: list[dict[str, Any]],
        *,
        batcher_names: list[str],
        batch_size: int,
    ) -> list[tuple[str, list[dict[str, Any]]]]:
        return self._reanalysis_support_runtime.build_reanalysis_batches(
            reviews,
            batcher_names=batcher_names,
            batch_size=batch_size,
        )

    def _build_priority_batch(
        self,
        reviews: list[dict[str, Any]],
        *,
        batch_size: int,
        primary_predicate,
    ) -> list[dict[str, Any]]:
        return self._reanalysis_support_runtime.build_priority_batch(
            reviews,
            batch_size=batch_size,
            primary_predicate=primary_predicate,
        )

    def _build_balanced_rating_batch(self, reviews: list[dict[str, Any]], *, batch_size: int) -> list[dict[str, Any]]:
        return self._reanalysis_support_runtime.build_balanced_rating_batch(
            reviews,
            batch_size=batch_size,
        )

    def _review_identity(self, review: dict[str, Any]) -> str:
        return self._reanalysis_support_runtime.review_identity(review)

    def _safe_rating(self, review: dict[str, Any]) -> float:
        return self._reanalysis_support_runtime.safe_rating(review)

    def _analysis_quality_score(self, analysis_payload: dict[str, Any]) -> float:
        return self._reanalysis_support_runtime.analysis_quality_score(analysis_payload)

    def _merge_reanalysis_runs(self, run_results: list[dict[str, Any]]) -> dict[str, Any]:
        return self._reanalysis_support_runtime.merge_reanalysis_runs(run_results)

    def _merge_reanalysis_terms(self, run_results: list[dict[str, Any]], *, key: str, limit: int) -> list[str]:
        return self._reanalysis_support_runtime.merge_reanalysis_terms(
            run_results,
            key=key,
            limit=limit,
        )

    def _review_fingerprint(self, review: dict[str, Any]) -> str:
        return self._reanalysis_support_runtime.review_fingerprint(review)

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        return self._business_common_runtime.parse_object_id(value, field_name=field_name)

    def _coerce_pagination(self, *, page: int, page_size: int, max_page_size: int) -> tuple[int, int]:
        try:
            page_value = int(page)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid page. It must be an integer >= 1.") from exc
        try:
            page_size_value = int(page_size)
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid page_size. It must be an integer >= 1.") from exc

        if page_value < 1:
            raise ValueError("Invalid page. It must be >= 1.")
        if page_size_value < 1:
            raise ValueError("Invalid page_size. It must be >= 1.")
        return page_value, min(page_size_value, max_page_size)

    def _pagination_payload(self, *, items: list[dict[str, Any]], page: int, page_size: int, total: int) -> dict[str, Any]:
        total_value = max(0, int(total))
        total_pages = ((total_value + page_size - 1) // page_size) if total_value else 0
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total_value,
            "total_pages": total_pages,
            "has_next": bool(total_pages and page < total_pages),
            "has_prev": bool(total_pages and page > 1),
        }

    def _serialize_business_doc(self, *, business_doc: dict[str, Any], review_count: int, include_listing: bool) -> dict:
        return self._business_summary_runtime.serialize_business_doc(
            business_doc=business_doc,
            review_count=review_count,
            include_listing=include_listing,
        )

    def _serialize_business_summary_doc(
        self,
        *,
        business_doc: dict[str, Any],
        latest_analysis: dict[str, Any] | None,
        include_listing: bool,
    ) -> dict[str, Any]:
        return self._business_summary_runtime.serialize_business_summary_doc(
            business_doc=business_doc,
            latest_analysis=latest_analysis,
            include_listing=include_listing,
        )

    def _build_business_description(
        self,
        *,
        business_doc: dict[str, Any],
        latest_analysis: dict[str, Any] | None,
        categories: list[str],
    ) -> str:
        return self._business_summary_runtime.build_business_description(
            business_doc=business_doc,
            latest_analysis=latest_analysis,
            categories=categories,
        )

    def _serialize_review_doc(self, review_doc: dict[str, Any]) -> dict:
        return self._business_summary_runtime.serialize_review_doc(review_doc)

    def _serialize_analysis_doc(self, analysis_doc: dict[str, Any]) -> dict:
        return self._business_summary_runtime.serialize_analysis_doc(analysis_doc)

    def _serialize_analysis_job_doc(self, job_doc: dict[str, Any]) -> dict:
        return self._business_summary_runtime.serialize_analysis_job_doc(job_doc)

    def _summarize_tripadvisor_antibot_job(
        self,
        job_doc: dict[str, Any],
    ) -> dict[str, Any] | None:
        return self._tripadvisor_antibot_job_runtime.summarize_job(job_doc)

    def _extract_tripadvisor_antibot_event_summary(
        self,
        event: dict[str, Any],
        *,
        index: int,
    ) -> dict[str, Any] | None:
        return self._tripadvisor_antibot_job_runtime.extract_event_summary(event, index=index)

    def _looks_like_antibot_text(self, value: Any) -> bool:
        return self._tripadvisor_antibot_job_runtime.looks_like_antibot_text(value)

    def _extract_diagnostic_id_from_text(self, value: str) -> str | None:
        return self._tripadvisor_antibot_job_runtime.extract_diagnostic_id_from_text(value)

    def _sanitize_response_payload(self, value: Any) -> Any:
        return self._business_common_runtime.sanitize_response_payload(value)
