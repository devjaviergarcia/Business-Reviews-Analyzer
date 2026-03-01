from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from src.config import settings
from src.database import get_database


class ReanalyzeUseCase:
    def __init__(
        self,
        *,
        preprocessor,
        llm_analyzer,
        parse_object_id: Callable[..., Any],
        resolve_reanalysis_batchers: Callable[[list[str] | None], list[str]],
        normalize_stored_review: Callable[[dict[str, Any]], dict[str, Any]],
        serialize_review_doc: Callable[[dict[str, Any]], dict[str, Any]],
        build_reanalysis_batches: Callable[..., list[tuple[str, list[dict[str, Any]]]]],
        analysis_quality_score: Callable[[dict[str, Any]], float],
        merge_reanalysis_runs: Callable[[list[dict[str, Any]]], dict[str, Any]],
        sanitize_response_payload: Callable[[Any], Any],
        businesses_collection_name: str,
        reviews_collection_name: str,
        analyses_collection_name: str,
    ) -> None:
        self.preprocessor = preprocessor
        self.llm_analyzer = llm_analyzer
        self._parse_object_id = parse_object_id
        self._resolve_reanalysis_batchers = resolve_reanalysis_batchers
        self._normalize_stored_review = normalize_stored_review
        self._serialize_review_doc = serialize_review_doc
        self._build_reanalysis_batches = build_reanalysis_batches
        self._analysis_quality_score = analysis_quality_score
        self._merge_reanalysis_runs = merge_reanalysis_runs
        self._sanitize_response_payload = sanitize_response_payload
        self._businesses_collection_name = businesses_collection_name
        self._reviews_collection_name = reviews_collection_name
        self._analyses_collection_name = analyses_collection_name

    async def execute(
        self,
        *,
        business_id: str,
        dataset_id: str | None = None,
        batchers: list[str] | None = None,
        batch_size: int | None = None,
        max_reviews_pool: int | None = None,
    ) -> dict[str, Any]:
        parsed_business_id = self._parse_object_id(business_id, field_name="business_id")
        database = get_database()
        businesses = database[self._businesses_collection_name]
        reviews = database[self._reviews_collection_name]
        analyses = database[self._analyses_collection_name]

        business_doc = await businesses.find_one({"_id": parsed_business_id})
        if business_doc is None:
            raise LookupError(f"Business '{business_id}' not found.")

        business_name = str(business_doc.get("name", "")).strip() or str(business_doc.get("name_normalized", ""))
        listing_payload = business_doc.get("listing")

        pool_size = max_reviews_pool if max_reviews_pool is not None else settings.analysis_reanalyze_pool_size
        pool_size = max(20, min(int(pool_size), 1000))
        batch_size_value = batch_size if batch_size is not None else settings.analysis_reanalyze_batch_size
        batch_size_value = max(10, min(int(batch_size_value), 120))

        selected_batchers = self._resolve_reanalysis_batchers(batchers)

        reviews_query: dict[str, Any] = {"business_id": business_id}
        selected_dataset_id = str(dataset_id or "").strip() or None
        if selected_dataset_id is not None:
            reviews_query["dataset_id"] = selected_dataset_id

        review_docs = (
            await reviews.find(reviews_query)
            .sort([("scraped_at", -1), ("_id", -1)])
            .limit(pool_size)
            .to_list(length=pool_size)
        )
        if not review_docs:
            if selected_dataset_id is not None:
                raise LookupError(
                    f"No stored reviews found for business '{business_id}' and dataset '{selected_dataset_id}'."
                )
            raise LookupError(f"No stored reviews found for business '{business_id}'.")

        normalized_stored_reviews = [
            self._normalize_stored_review(self._serialize_review_doc(doc)) for doc in review_docs
        ]
        processed_reviews = self.preprocessor.process(normalized_stored_reviews)
        stats = self.preprocessor.compute_stats(processed_reviews)

        batches = self._build_reanalysis_batches(
            processed_reviews,
            batcher_names=selected_batchers,
            batch_size=batch_size_value,
        )
        if not batches:
            raise RuntimeError("Could not prepare review batches for reanalysis.")

        run_results: list[dict[str, Any]] = []
        for batcher_name, batch_reviews in batches:
            analysis = await self.llm_analyzer.analyze(
                business_name=business_name,
                reviews=batch_reviews,
                stats=stats,
            )
            analysis_payload = analysis.model_dump(mode="python")
            run_results.append(
                {
                    "batcher": batcher_name,
                    "sample_size": len(batch_reviews),
                    "analysis": analysis_payload,
                    "quality_score": round(self._analysis_quality_score(analysis_payload), 4),
                }
            )

        merged_analysis_payload = self._merge_reanalysis_runs(run_results)
        now = datetime.now(timezone.utc)
        merged_analysis_payload["business_id"] = business_id
        if selected_dataset_id is not None:
            merged_analysis_payload["dataset_id"] = selected_dataset_id
        merged_analysis_payload["created_at"] = now
        merged_analysis_payload["meta"] = {
            "type": "stored_reviews_reanalysis",
            "dataset_id": selected_dataset_id,
            "batchers": selected_batchers,
            "batch_size": batch_size_value,
            "pool_size": pool_size,
            "runs": [
                {
                    "batcher": item["batcher"],
                    "sample_size": item["sample_size"],
                    "quality_score": item["quality_score"],
                }
                for item in run_results
            ],
        }

        inserted_analysis = await analyses.insert_one(merged_analysis_payload)
        review_count_query = {"business_id": business_id}
        if selected_dataset_id is not None:
            review_count_query["dataset_id"] = selected_dataset_id
        review_count = await reviews.count_documents(review_count_query)

        await businesses.update_one(
            {"_id": parsed_business_id},
            {
                "$set": {
                    "stats": stats,
                    "review_count": review_count,
                    "latest_analysis_id": str(inserted_analysis.inserted_id),
                    "updated_at": now,
                }
            },
        )

        payload = {
            "business_id": business_id,
            "name": business_name,
            "cached": False,
            "reanalyzed": True,
            "listing": listing_payload,
            "stats": stats,
            "review_count": review_count,
            "dataset_id": selected_dataset_id,
            "listing_total_reviews": (listing_payload or {}).get("total_reviews") if isinstance(listing_payload, dict) else None,
            "processed_review_count": len(processed_reviews),
            "analysis": merged_analysis_payload,
            "batchers_used": selected_batchers,
        }
        return self._sanitize_response_payload(payload)
