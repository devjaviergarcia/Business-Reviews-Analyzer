from __future__ import annotations

from typing import Any, Callable


DatabaseFactory = Callable[[], Any]


class BusinessCleanupRuntime:
    def __init__(
        self,
        *,
        database_factory: DatabaseFactory,
        job_service: Any,
        parse_object_id: Callable[..., Any],
        sanitize_response_payload: Callable[[Any], Any],
        build_related_business_jobs_query: Callable[..., dict[str, Any]],
        businesses_collection_name: str,
        reviews_collection_name: str,
        comments_collection_name: str,
        analyses_collection_name: str,
        source_profiles_collection_name: str,
        datasets_collection_name: str,
        scrape_runs_collection_name: str,
        jobs_collection_name: str,
    ) -> None:
        self._database_factory = database_factory
        self._job_service = job_service
        self._parse_object_id = parse_object_id
        self._sanitize_response_payload = sanitize_response_payload
        self._build_related_business_jobs_query = build_related_business_jobs_query
        self._businesses_collection_name = businesses_collection_name
        self._reviews_collection_name = reviews_collection_name
        self._comments_collection_name = comments_collection_name
        self._analyses_collection_name = analyses_collection_name
        self._source_profiles_collection_name = source_profiles_collection_name
        self._datasets_collection_name = datasets_collection_name
        self._scrape_runs_collection_name = scrape_runs_collection_name
        self._jobs_collection_name = jobs_collection_name

    async def delete_business(
        self,
        *,
        business_id: str,
        wait_active_stop_seconds: float,
        poll_seconds: float,
        force_delete_on_timeout: bool,
        delete_related_jobs: bool,
    ) -> dict[str, Any]:
        parsed_business_id = self._parse_object_id(business_id, field_name="business_id")
        normalized_business_id = str(parsed_business_id)
        database = self._database_factory()
        businesses = database[self._businesses_collection_name]
        reviews = database[self._reviews_collection_name]
        comments = database[self._comments_collection_name]
        analyses = database[self._analyses_collection_name]
        source_profiles = database[self._source_profiles_collection_name]
        datasets = database[self._datasets_collection_name]
        scrape_runs = database[self._scrape_runs_collection_name]
        jobs_collection = database[self._jobs_collection_name]

        business_doc = await businesses.find_one({"_id": parsed_business_id})
        if business_doc is None:
            raise LookupError(f"Business '{business_id}' not found.")

        canonical_name_normalized = str(business_doc.get("name_normalized") or "").strip()
        deleted_jobs: list[dict[str, Any]] = []
        job_delete_errors: list[dict[str, str]] = []

        if delete_related_jobs:
            jobs_query = self._build_related_business_jobs_query(
                business_id=normalized_business_id,
                canonical_name_normalized=canonical_name_normalized,
            )
            related_jobs_docs = await jobs_collection.find(jobs_query, projection={"_id": 1}).to_list(length=None)
            for job_doc in related_jobs_docs:
                current_job_id = str(job_doc.get("_id") or "").strip()
                if not current_job_id:
                    continue
                try:
                    delete_result = await self._job_service.delete_job(
                        job_id=current_job_id,
                        wait_active_stop_seconds=wait_active_stop_seconds,
                        poll_seconds=poll_seconds,
                        force_delete_on_timeout=force_delete_on_timeout,
                    )
                    deleted_jobs.append(
                        {
                            "job_id": current_job_id,
                            "status_at_delete": delete_result.get("status_at_delete"),
                            "forced_delete": bool(delete_result.get("forced_delete")),
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    job_delete_errors.append({"job_id": current_job_id, "error": str(exc)})

        reviews_result = await reviews.delete_many({"business_id": normalized_business_id})
        comments_result = await comments.delete_many({"business_id": normalized_business_id})
        analyses_result = await analyses.delete_many({"business_id": normalized_business_id})
        source_profiles_result = await source_profiles.delete_many({"business_id": normalized_business_id})
        datasets_result = await datasets.delete_many({"business_id": normalized_business_id})
        scrape_runs_result = await scrape_runs.delete_many({"business_id": normalized_business_id})
        business_delete_result = await businesses.delete_one({"_id": parsed_business_id})

        if business_delete_result.deleted_count == 0:
            raise RuntimeError(f"Business '{business_id}' could not be deleted.")

        return self._sanitize_response_payload(
            {
                "business_id": normalized_business_id,
                "deleted": True,
                "business_name": str(business_doc.get("name") or ""),
                "canonical_name_normalized": canonical_name_normalized or None,
                "delete_related_jobs": bool(delete_related_jobs),
                "jobs": {
                    "deleted_count": len(deleted_jobs),
                    "deleted_jobs": deleted_jobs,
                    "errors": job_delete_errors,
                },
                "collections": {
                    "businesses_deleted": int(business_delete_result.deleted_count),
                    "reviews_deleted": int(reviews_result.deleted_count),
                    "comments_deleted": int(comments_result.deleted_count),
                    "analyses_deleted": int(analyses_result.deleted_count),
                    "source_profiles_deleted": int(source_profiles_result.deleted_count),
                    "datasets_deleted": int(datasets_result.deleted_count),
                    "scrape_runs_deleted": int(scrape_runs_result.deleted_count),
                },
            }
        )
