from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId


async def build_benchmarking(
    *,
    business_id: str,
    business_name: str,
    listing: dict[str, Any] | None,
    stats: dict[str, Any],
    review_metrics: list[dict[str, Any]],
    businesses_collection,
    safe_float,
    safe_int,
    negative_ratio,
    compute_reputation_score,
    average_dimension,
) -> dict[str, Any]:
    categories_raw = (listing or {}).get("categories")
    categories = [str(item).strip() for item in categories_raw] if isinstance(categories_raw, list) else []
    categories = [item for item in categories if item][:3]

    target_query_id = None
    try:
        target_query_id = ObjectId(str(business_id))
    except (InvalidId, TypeError):
        target_query_id = None

    base_query: dict[str, Any] = {"listing.overall_rating": {"$gt": 0}}
    if target_query_id is not None:
        base_query["_id"] = {"$ne": target_query_id}
    if categories:
        base_query["listing.categories"] = {"$in": categories}

    competitor_docs = await businesses_collection.find(base_query).limit(120).to_list(length=120)
    if len(competitor_docs) < 5 and categories:
        fallback_query = {"listing.overall_rating": {"$gt": 0}}
        if target_query_id is not None:
            fallback_query["_id"] = {"$ne": target_query_id}
        competitor_docs = await businesses_collection.find(fallback_query).limit(120).to_list(length=120)

    target_avg_rating = safe_float((stats or {}).get("avg_rating"))
    target_response_rate = safe_float((stats or {}).get("response_rate"))
    target_neg_ratio = negative_ratio(review_metrics=review_metrics)
    target_score = compute_reputation_score(
        avg_rating=target_avg_rating,
        response_rate=target_response_rate,
        negative_ratio=target_neg_ratio,
        sentiment_avg=average_dimension(review_metrics, "sentiment"),
        tranquility_avg=average_dimension(review_metrics, "tranquility_aggressiveness"),
    )

    target_record = {
        "business_id": business_id,
        "name": business_name,
        "avg_rating": round(target_avg_rating, 3),
        "review_count": len(review_metrics),
        "reputation_score": target_score,
        "is_target": True,
    }
    competitors: list[dict[str, Any]] = []
    for doc in competitor_docs:
        listing_payload = doc.get("listing")
        listing_dict = listing_payload if isinstance(listing_payload, dict) else {}
        avg_rating = safe_float(
            listing_dict.get("overall_rating")
            if listing_dict.get("overall_rating") is not None
            else (doc.get("stats") or {}).get("avg_rating")
        )
        review_count = safe_int(doc.get("review_count") or 0)
        if avg_rating <= 0:
            continue
        score = round((avg_rating / 5.0) * 80.0 + min(20.0, math.log1p(max(0, review_count)) * 4.5), 2)
        competitors.append(
            {
                "business_id": str(doc.get("_id")),
                "name": str(doc.get("name", "") or ""),
                "avg_rating": round(avg_rating, 3),
                "review_count": review_count,
                "reputation_score": score,
                "is_target": False,
            }
        )

    ranking = [target_record, *competitors]
    ranking.sort(
        key=lambda item: (float(item.get("reputation_score", 0.0)), float(item.get("avg_rating", 0.0))),
        reverse=True,
    )
    target_rank = 1
    for idx, item in enumerate(ranking, start=1):
        if item.get("is_target"):
            target_rank = idx
            break

    top_competitors = [item for item in ranking if not item.get("is_target")][:8]
    nearest_competitors = sorted(
        top_competitors,
        key=lambda item: abs(float(item.get("avg_rating", 0.0)) - target_avg_rating),
    )[:5]

    return {
        "target": target_record,
        "target_rank": target_rank,
        "total_competitors_compared": len(competitors),
        "total_businesses_compared": len(competitors) + 1,
        "top_competitors": top_competitors,
        "nearest_by_rating": nearest_competitors,
        "comparison_note": "Benchmark calculado sobre negocios con rating público en la base de datos.",
    }


async def build_score_and_evolution(
    *,
    business_id: str,
    stats: dict[str, Any],
    review_metrics: list[dict[str, Any]],
    analyses_collection,
    safe_float,
    negative_ratio,
    average_dimension,
    compute_reputation_score,
    linear_slope,
) -> dict[str, Any]:
    avg_rating = safe_float((stats or {}).get("avg_rating"))
    response_rate = safe_float((stats or {}).get("response_rate"))
    negative_ratio_value = negative_ratio(review_metrics=review_metrics)
    sentiment_avg = average_dimension(review_metrics, "sentiment")
    tranquility_avg = average_dimension(review_metrics, "tranquility_aggressiveness")
    score = compute_reputation_score(
        avg_rating=avg_rating,
        response_rate=response_rate,
        negative_ratio=negative_ratio_value,
        sentiment_avg=sentiment_avg,
        tranquility_avg=tranquility_avg,
    )

    analysis_docs = (
        await analyses_collection.find({"business_id": business_id})
        .sort([("created_at", -1), ("_id", -1)])
        .limit(12)
        .to_list(length=12)
    )
    history = []
    for doc in reversed(analysis_docs):
        sentiment_value = str(doc.get("overall_sentiment", "") or "").strip().lower()
        sentiment_score = {"positive": 1.0, "mixed": 0.0, "negative": -1.0}.get(sentiment_value, 0.0)
        sentiment_label = {"positive": "positivo", "mixed": "mixto", "negative": "negativo"}.get(
            sentiment_value,
            "mixto",
        )
        created_at_value = doc.get("created_at")
        created_at_text = (
            created_at_value.isoformat()
            if isinstance(created_at_value, datetime)
            else str(created_at_value or "")
        ).strip()
        history.append(
            {
                "created_at": created_at_text,
                "overall_sentiment": sentiment_label,
                "sentiment_score": sentiment_score,
            }
        )

    slope = linear_slope([float(item.get("sentiment_score", 0.0)) for item in history])
    trend = "estable"
    if slope >= 0.07:
        trend = "al_alza"
    elif slope <= -0.07:
        trend = "a_la_baja"

    buckets = defaultdict(list)
    for item in review_metrics:
        bucket = str(item.get("relative_time_bucket", "unknown") or "unknown")
        buckets[bucket].append(float(item.get("dimensions", {}).get("satisfaction", 0.0)))

    bucket_summary = {
        key: round(statistics.mean(values), 4) if values else 0.0
        for key, values in buckets.items()
    }

    return {
        "reputation_score": score,
        "score_scale": "0-100",
        "components": {
            "avg_rating": round(avg_rating, 3),
            "response_rate": round(response_rate, 4),
            "negative_ratio": round(negative_ratio_value, 4),
            "sentiment_avg": round(sentiment_avg, 4),
            "tranquility_avg": round(tranquility_avg, 4),
        },
        "evolution": {
            "trend": trend,
            "trend_slope": round(slope, 5),
            "analyses_history": history,
            "satisfaccion_por_antiguedad_resena": bucket_summary,
        },
    }
