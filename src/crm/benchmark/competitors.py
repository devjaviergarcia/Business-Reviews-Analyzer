from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class _ScoredCompetitor:
    payload: dict[str, Any]
    score: float
    relative_position: str
    why_selected: str


class CompetitorSelector:
    def __init__(self, *, max_competitors: int = 5) -> None:
        self._max_competitors = max(1, int(max_competitors))

    def select(
        self,
        *,
        target_business: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        scored: list[_ScoredCompetitor] = []
        for candidate in candidates:
            if self._is_same_business(target_business, candidate):
                continue
            business_name = str(candidate.get("business_name") or candidate.get("name") or "").strip()
            if not business_name:
                continue
            scored.append(self._score_candidate(target_business=target_business, candidate=candidate))

        selected = self._pick_balanced(scored)
        return [item.payload for item in selected]

    def _pick_balanced(self, scored: list[_ScoredCompetitor]) -> list[_ScoredCompetitor]:
        selected: list[_ScoredCompetitor] = []
        used_keys: set[str] = set()

        def add_from(group: str, limit: int) -> None:
            for item in self._sorted_items([candidate for candidate in scored if candidate.relative_position == group]):
                if len(selected) >= self._max_competitors:
                    return
                key = self._candidate_key(item.payload)
                if key in used_keys:
                    continue
                selected.append(item)
                used_keys.add(key)
                if sum(1 for selected_item in selected if selected_item.relative_position == group) >= limit:
                    return

        add_from("leader", 2)
        add_from("similar", 2)
        add_from("aspirational", 1)

        for item in self._sorted_items(scored):
            if len(selected) >= self._max_competitors:
                break
            key = self._candidate_key(item.payload)
            if key in used_keys:
                continue
            selected.append(item)
            used_keys.add(key)

        return selected[: self._max_competitors]

    def _score_candidate(self, *, target_business: dict[str, Any], candidate: dict[str, Any]) -> _ScoredCompetitor:
        target_category = _normalize_text(target_business.get("category"))
        candidate_category = _normalize_text(candidate.get("category"))
        target_city = _normalize_text(target_business.get("city") or _city_from_address(target_business.get("address")))
        candidate_city = _normalize_text(candidate.get("city") or _city_from_address(candidate.get("address")))
        target_rating = _coerce_float(target_business.get("rating"))
        candidate_rating = _coerce_float(candidate.get("rating"))
        target_reviews = _coerce_int(target_business.get("review_count"))
        candidate_reviews = _coerce_int(candidate.get("review_count"))
        target_rank = _coerce_int(target_business.get("discovery_rank"))
        candidate_rank = _coerce_int(candidate.get("discovery_rank"))

        category_score = _category_similarity(target_category, candidate_category) * 35.0
        city_score = 15.0 if target_city and candidate_city and target_city == candidate_city else 0.0
        rating_score = _rating_score(target_rating=target_rating, candidate_rating=candidate_rating)
        review_score = _review_score(target_reviews=target_reviews, candidate_reviews=candidate_reviews)
        quality_score = _quality_score(candidate_rating=candidate_rating, candidate_reviews=candidate_reviews)
        position_score = _position_score(target_rank=target_rank, candidate_rank=candidate_rank)
        total_score = round(category_score + city_score + rating_score + review_score + quality_score + position_score, 2)

        relative_position = _relative_position(
            target_rating=target_rating,
            candidate_rating=candidate_rating,
            target_reviews=target_reviews,
            candidate_reviews=candidate_reviews,
            target_rank=target_rank,
            candidate_rank=candidate_rank,
        )
        why_selected = _why_selected(
            same_city=bool(target_city and candidate_city and target_city == candidate_city),
            category_similarity=category_score / 35.0 if category_score else 0.0,
            relative_position=relative_position,
            candidate_rating=candidate_rating,
            candidate_reviews=candidate_reviews,
            candidate_rank=candidate_rank,
            rank_better=bool(target_rank and candidate_rank and candidate_rank < target_rank),
        )
        payload = {
            "benchmark_business_id": str(candidate.get("benchmark_business_id") or candidate.get("id") or "").strip() or None,
            "business_name": str(candidate.get("business_name") or candidate.get("name") or "").strip(),
            "maps_url": str(candidate.get("maps_url") or "").strip() or None,
            "discovery_rank": candidate_rank,
            "rating": candidate_rating,
            "review_count": candidate_reviews,
            "website": str(candidate.get("website") or "").strip() or None,
            "category": str(candidate.get("category") or "").strip() or None,
            "distance_hint": "same_city" if target_city and candidate_city and target_city == candidate_city else None,
            "why_selected": why_selected,
            "relative_position": relative_position,
            "similarity_score": total_score,
        }
        return _ScoredCompetitor(
            payload=payload,
            score=total_score,
            relative_position=relative_position,
            why_selected=why_selected,
        )

    def _sorted_items(self, items: list[_ScoredCompetitor]) -> list[_ScoredCompetitor]:
        return sorted(
            items,
            key=lambda item: (
                -item.score,
                -float(item.payload.get("rating") or 0),
                -int(item.payload.get("review_count") or 0),
                str(item.payload.get("business_name") or "").lower(),
            ),
        )

    def _is_same_business(self, target: dict[str, Any], candidate: dict[str, Any]) -> bool:
        target_id = str(target.get("benchmark_business_id") or target.get("id") or "").strip()
        candidate_id = str(candidate.get("benchmark_business_id") or candidate.get("id") or "").strip()
        if target_id and candidate_id and target_id == candidate_id:
            return True

        target_maps = str(target.get("maps_url_canonical") or target.get("maps_url") or "").strip()
        candidate_maps = str(candidate.get("maps_url_canonical") or candidate.get("maps_url") or "").strip()
        if target_maps and candidate_maps and target_maps == candidate_maps:
            return True

        target_key = (_normalize_text(target.get("business_name")), _normalize_text(target.get("address")))
        candidate_key = (_normalize_text(candidate.get("business_name")), _normalize_text(candidate.get("address")))
        return bool(target_key[0] and target_key == candidate_key)

    def _candidate_key(self, candidate: dict[str, Any]) -> str:
        return "|".join(
            [
                str(candidate.get("benchmark_business_id") or ""),
                _normalize_text(candidate.get("business_name")),
                str(candidate.get("maps_url") or ""),
            ]
        )


def select_competitors_for_business(
    business: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    max_competitors: int = 5,
) -> list[dict[str, Any]]:
    return CompetitorSelector(max_competitors=max_competitors).select(
        target_business=business,
        candidates=candidates,
    )


def _normalize_text(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    collapsed = re.sub(r"[^a-z0-9]+", " ", ascii_value)
    return re.sub(r"\s+", " ", collapsed).strip()


def _city_from_address(value: Any) -> str | None:
    parts = [part.strip() for part in str(value or "").split(",") if part.strip()]
    return parts[-1] if parts else None


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value).replace(".", "").replace(",", "")))
    except (TypeError, ValueError):
        return None


def _category_similarity(target_category: str, candidate_category: str) -> float:
    if not target_category or not candidate_category:
        return 0.2
    if target_category == candidate_category:
        return 1.0
    target_tokens = set(target_category.split())
    candidate_tokens = set(candidate_category.split())
    if not target_tokens or not candidate_tokens:
        return 0.0
    overlap = len(target_tokens & candidate_tokens)
    union = len(target_tokens | candidate_tokens)
    return overlap / union if union else 0.0


def _rating_score(*, target_rating: float | None, candidate_rating: float | None) -> float:
    if candidate_rating is None:
        return 6.0
    if target_rating is None:
        return min(candidate_rating / 5.0, 1.0) * 20.0
    delta = candidate_rating - target_rating
    if delta >= 0.0:
        return 20.0
    if delta >= -0.3:
        return 16.0
    if delta >= -0.7:
        return 10.0
    return 4.0


def _review_score(*, target_reviews: int | None, candidate_reviews: int | None) -> float:
    if candidate_reviews is None:
        return 5.0
    if not target_reviews or target_reviews <= 0:
        return min(candidate_reviews / 500.0, 1.0) * 15.0
    ratio = candidate_reviews / max(target_reviews, 1)
    if ratio >= 1.0:
        return 15.0
    if ratio >= 0.5:
        return 10.0
    if ratio >= 0.25:
        return 6.0
    return 2.0


def _quality_score(*, candidate_rating: float | None, candidate_reviews: int | None) -> float:
    rating_component = min(max(candidate_rating or 0.0, 0.0) / 5.0, 1.0) * 6.0
    reviews = max(candidate_reviews or 0, 0)
    volume_component = min(reviews / 1000.0, 1.0) * 4.0
    return rating_component + volume_component


def _position_score(*, target_rank: int | None, candidate_rank: int | None) -> float:
    if candidate_rank is None:
        return 0.0
    base = max(0.0, 10.0 - min(candidate_rank - 1, 10) * 0.8)
    if target_rank is not None and candidate_rank < target_rank:
        base += 4.0
    return min(base, 14.0)


def _relative_position(
    *,
    target_rating: float | None,
    candidate_rating: float | None,
    target_reviews: int | None,
    candidate_reviews: int | None,
    target_rank: int | None,
    candidate_rank: int | None,
) -> str:
    target_rating_value = target_rating if target_rating is not None else 0.0
    candidate_rating_value = candidate_rating if candidate_rating is not None else 0.0
    target_review_value = target_reviews or 0
    candidate_review_value = candidate_reviews or 0

    if (
        candidate_rating_value >= target_rating_value
        and candidate_review_value >= target_review_value
        and (target_rank is None or candidate_rank is None or candidate_rank <= target_rank)
    ):
        return "leader"
    if (
        candidate_rating_value >= target_rating_value + 0.3
        or candidate_review_value >= target_review_value * 2
        or (target_rank is not None and candidate_rank is not None and candidate_rank < target_rank)
    ):
        return "aspirational"
    return "similar"


def _why_selected(
    *,
    same_city: bool,
    category_similarity: float,
    relative_position: str,
    candidate_rating: float | None,
    candidate_reviews: int | None,
    candidate_rank: int | None,
    rank_better: bool,
) -> str:
    reasons: list[str] = []
    if same_city:
        reasons.append("misma ciudad")
    if category_similarity >= 0.95:
        reasons.append("misma categoria")
    elif category_similarity >= 0.35:
        reasons.append("categoria similar")
    if relative_position == "leader":
        reasons.append("mejor o igual en rating y volumen")
    elif relative_position == "aspirational":
        reasons.append("referente aspiracional")
    else:
        reasons.append("perfil comparable")
    if candidate_rank is not None:
        reasons.append(f"posicion benchmark {candidate_rank}")
    if rank_better:
        reasons.append("aparece antes en discovery")
    if candidate_rating is not None:
        reasons.append(f"rating {candidate_rating:.1f}")
    if candidate_reviews is not None:
        reasons.append(f"{candidate_reviews} reseñas")
    return "; ".join(reasons)
