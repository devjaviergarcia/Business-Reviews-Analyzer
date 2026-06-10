from __future__ import annotations

from typing import Any


def score_review_dimensions(
    *,
    index: int,
    review: dict[str, Any],
    positive_tokens: tuple[str, ...],
    negative_tokens: tuple[str, ...],
    expectation_tokens: tuple[str, ...],
    improvement_tokens: tuple[str, ...],
    aggressive_tokens: tuple[str, ...],
    safe_rating,
    normalize_text,
    count_keyword_hits,
    clamp,
    clamp01,
    upper_ratio,
    theme_scores,
    resolve_dominant_problem,
) -> dict[str, Any]:
    text = str(review.get("text", "") or "").strip()
    author_name = str(review.get("author_name", "") or "").strip()
    rating = safe_rating(review.get("rating"))
    text_norm = normalize_text(text)

    pos_hits = count_keyword_hits(text_norm, positive_tokens)
    neg_hits = count_keyword_hits(text_norm, negative_tokens)
    exp_hits = count_keyword_hits(text_norm, expectation_tokens)
    imp_hits = count_keyword_hits(text_norm, improvement_tokens)
    aggr_hits = count_keyword_hits(text_norm, aggressive_tokens)

    lexical_sentiment = (pos_hits - neg_hits) / max(1, pos_hits + neg_hits)
    rating_sentiment = (rating - 3.0) / 2.0
    sentiment = clamp(rating_sentiment * 0.75 + lexical_sentiment * 0.25, -1.0, 1.0)

    expectation_gap = clamp01((max(0.0, 3.0 - rating) / 3.0) * 0.55 + min(1.0, exp_hits / 3.0) * 0.45)
    satisfaction = clamp01((rating / 5.0) * 0.8 + max(0.0, lexical_sentiment) * 0.2)

    punctuation_aggr = 1.0 if "!!" in text else 0.0
    uppercase_ratio = upper_ratio(text)
    upper_aggr = 1.0 if uppercase_ratio >= 0.35 and len(text) >= 20 else 0.0
    aggr_score = clamp01((aggr_hits + punctuation_aggr + upper_aggr) / 4.0)
    tranquility_aggressiveness = clamp(1.0 - (2.0 * aggr_score), -1.0, 1.0)

    improvement_intent = clamp01(min(1.0, imp_hits / 3.0) * 0.7 + expectation_gap * 0.3)

    review_theme_scores = theme_scores(text_norm)
    dominant_problem = resolve_dominant_problem(
        rating=rating,
        text_norm=text_norm,
        sentiment=sentiment,
        theme_scores=review_theme_scores,
    )

    customer_key = normalize_text(author_name)
    if not customer_key:
        review_id = str(review.get("review_id") or "").strip()
        customer_key = review_id or f"anon_{index}"

    return {
        "index": index,
        "customer_key": customer_key,
        "author_name": author_name or "Cliente anónimo",
        "rating": rating,
        "source": str(review.get("source", "") or "").strip() or "unknown",
        "text": text,
        "relative_time_bucket": str(review.get("relative_time_bucket", "unknown") or "unknown"),
        "has_owner_reply": bool(review.get("has_owner_reply")),
        "owner_reply": str(review.get("owner_reply", "") or "").strip(),
        "dimensions": {
            "sentiment": round(sentiment, 4),
            "expectation_gap": round(expectation_gap, 4),
            "satisfaction": round(satisfaction, 4),
            "tranquility_aggressiveness": round(tranquility_aggressiveness, 4),
            "improvement_intent": round(improvement_intent, 4),
        },
        "theme_scores": review_theme_scores,
        "dominant_problem": dominant_problem,
    }
