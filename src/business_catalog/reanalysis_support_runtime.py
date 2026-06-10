from __future__ import annotations

import hashlib
from collections import Counter
from typing import Any, Callable


NormalizeTextFn = Callable[[str], str]


class ReanalysisSupportRuntime:
    def __init__(
        self,
        *,
        normalize_text: NormalizeTextFn,
        supported_batchers: set[str],
    ) -> None:
        self._normalize_text = normalize_text
        self._supported_batchers = supported_batchers

    def normalize_scraped_review(self, review: dict[str, Any]) -> dict[str, Any]:
        item = dict(review)
        owner_reply = item.get("owner_reply")
        if isinstance(owner_reply, dict):
            item["owner_reply"] = str(owner_reply.get("text", "") or "").strip()
            item["owner_reply_relative_time"] = str(owner_reply.get("relative_time", "") or "").strip()
            item["owner_reply_author_name"] = str(
                item.get("owner_reply_author_name", "") or owner_reply.get("author_name", "") or ""
            ).strip()
            item["owner_reply_written_date"] = str(
                item.get("owner_reply_written_date", "") or owner_reply.get("written_date", "") or ""
            ).strip()
        elif isinstance(owner_reply, str):
            item["owner_reply"] = owner_reply.strip()
            item["owner_reply_relative_time"] = ""
            item["owner_reply_author_name"] = str(item.get("owner_reply_author_name", "") or "").strip()
            item["owner_reply_written_date"] = str(item.get("owner_reply_written_date", "") or "").strip()
        else:
            item["owner_reply"] = ""
            item["owner_reply_relative_time"] = ""
            item["owner_reply_author_name"] = str(item.get("owner_reply_author_name", "") or "").strip()
            item["owner_reply_written_date"] = str(item.get("owner_reply_written_date", "") or "").strip()
        return item

    def normalize_stored_review(self, review: dict[str, Any]) -> dict[str, Any]:
        item = dict(review)
        owner_reply = item.get("owner_reply")
        if isinstance(owner_reply, dict):
            item["owner_reply"] = str(owner_reply.get("text", "") or "").strip()
            item["owner_reply_relative_time"] = str(owner_reply.get("relative_time", "") or "").strip()
        elif isinstance(owner_reply, str):
            item["owner_reply"] = owner_reply.strip()
            item["owner_reply_relative_time"] = str(item.get("owner_reply_relative_time", "") or "").strip()
        else:
            item["owner_reply"] = ""
            item["owner_reply_relative_time"] = ""
        item["owner_reply_author_name"] = str(item.get("owner_reply_author_name", "") or "").strip()
        item["owner_reply_written_date"] = str(item.get("owner_reply_written_date", "") or "").strip()
        item["source"] = str(item.get("source", "google_maps") or "google_maps")
        item["author_name"] = str(item.get("author_name", "") or "").strip()
        item["text"] = str(item.get("text", "") or "").strip()
        item["relative_time"] = str(item.get("relative_time", "") or "").strip()
        item["review_id"] = str(item.get("review_id") or item.get("id") or "").strip() or None
        return item

    def resolve_reanalysis_batchers(self, batchers: list[str] | None, default_batchers: list[str]) -> list[str]:
        source = batchers if batchers else default_batchers
        normalized: list[str] = []
        for raw in source:
            value = self._normalize_text(str(raw or "")).replace("-", "_").replace(" ", "_")
            if not value:
                continue
            if value not in self._supported_batchers:
                supported = ", ".join(sorted(self._supported_batchers))
                raise ValueError(f"Unknown batcher '{raw}'. Supported: {supported}.")
            if value not in normalized:
                normalized.append(value)
        if not normalized:
            raise ValueError("At least one valid batcher is required.")
        return normalized

    def build_reanalysis_batches(
        self,
        reviews: list[dict[str, Any]],
        *,
        batcher_names: list[str],
        batch_size: int,
    ) -> list[tuple[str, list[dict[str, Any]]]]:
        if not reviews:
            return []
        batch_size = max(10, min(batch_size, 120))
        text_reviews = [item for item in reviews if bool(item.get("has_text") or item.get("text"))]
        source_reviews = text_reviews or reviews
        batches: list[tuple[str, list[dict[str, Any]]]] = []
        for batcher_name in batcher_names:
            if batcher_name == "latest_text":
                selected = source_reviews[:batch_size]
            elif batcher_name == "low_rating_focus":
                selected = self.build_priority_batch(
                    source_reviews,
                    batch_size=batch_size,
                    primary_predicate=lambda item: self.safe_rating(item) <= 3.0,
                )
            elif batcher_name == "high_rating_focus":
                selected = self.build_priority_batch(
                    source_reviews,
                    batch_size=batch_size,
                    primary_predicate=lambda item: self.safe_rating(item) >= 4.0,
                )
            elif batcher_name == "balanced_rating":
                selected = self.build_balanced_rating_batch(source_reviews, batch_size=batch_size)
            else:
                selected = []
            if selected:
                batches.append((batcher_name, selected))
        return batches

    def build_priority_batch(
        self,
        reviews: list[dict[str, Any]],
        *,
        batch_size: int,
        primary_predicate: Callable[[dict[str, Any]], bool],
    ) -> list[dict[str, Any]]:
        primary: list[dict[str, Any]] = []
        secondary: list[dict[str, Any]] = []
        for item in reviews:
            if primary_predicate(item):
                primary.append(item)
            else:
                secondary.append(item)
        return (primary + secondary)[:batch_size]

    def build_balanced_rating_batch(self, reviews: list[dict[str, Any]], *, batch_size: int) -> list[dict[str, Any]]:
        buckets: dict[int, list[dict[str, Any]]] = {star: [] for star in range(1, 6)}
        for item in reviews:
            rating = self.safe_rating(item)
            star = min(max(int(round(rating)), 1), 5)
            buckets[star].append(item)
        selected: list[dict[str, Any]] = []
        used_ids: set[str] = set()
        while len(selected) < batch_size:
            added = False
            for star in range(5, 0, -1):
                if not buckets[star]:
                    continue
                candidate = buckets[star].pop(0)
                identity = self.review_identity(candidate)
                if identity in used_ids:
                    continue
                used_ids.add(identity)
                selected.append(candidate)
                added = True
                if len(selected) >= batch_size:
                    break
            if not added:
                break
        if len(selected) >= batch_size:
            return selected[:batch_size]
        for item in reviews:
            identity = self.review_identity(item)
            if identity in used_ids:
                continue
            used_ids.add(identity)
            selected.append(item)
            if len(selected) >= batch_size:
                break
        return selected[:batch_size]

    def review_identity(self, review: dict[str, Any]) -> str:
        parts = [
            str(review.get("review_id", "") or ""),
            str(review.get("id", "") or ""),
            self._normalize_text(str(review.get("author_name", "") or "")),
            self._normalize_text(str(review.get("text", "") or ""))[:120],
            str(round(self.safe_rating(review), 1)),
        ]
        return "|".join(parts)

    def safe_rating(self, review: dict[str, Any]) -> float:
        try:
            value = float(review.get("rating", 0.0))
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(5.0, value))

    def analysis_quality_score(self, analysis_payload: dict[str, Any]) -> float:
        score = 0.0
        sentiment = str(analysis_payload.get("overall_sentiment", "")).strip().lower()
        if sentiment in {"positive", "mixed", "negative"}:
            score += 1.0
        main_topics = analysis_payload.get("main_topics") or []
        strengths = analysis_payload.get("strengths") or []
        weaknesses = analysis_payload.get("weaknesses") or []
        reply = str(analysis_payload.get("suggested_owner_reply", "") or "").strip()
        score += min(len(main_topics), 8) * 1.2
        score += min(len(strengths), 8) * 1.0
        score += min(len(weaknesses), 8) * 0.8
        score += min(len(reply), 320) / 80.0
        return score

    def merge_reanalysis_runs(self, run_results: list[dict[str, Any]]) -> dict[str, Any]:
        if not run_results:
            raise RuntimeError("No reanalysis runs available to merge.")
        sentiment_counter: Counter[str] = Counter()
        for run in run_results:
            sentiment = str(run.get("analysis", {}).get("overall_sentiment", "")).strip().lower()
            if sentiment in {"positive", "mixed", "negative"}:
                sentiment_counter[sentiment] += 1
        overall_sentiment = sentiment_counter.most_common(1)[0][0] if sentiment_counter else "mixed"
        main_topics = self.merge_reanalysis_terms(run_results, key="main_topics", limit=8)
        strengths = self.merge_reanalysis_terms(run_results, key="strengths", limit=8)
        weaknesses = self.merge_reanalysis_terms(run_results, key="weaknesses", limit=8)
        best_run = max(run_results, key=lambda run: float(run.get("quality_score", 0.0)))
        suggested_owner_reply = str(best_run.get("analysis", {}).get("suggested_owner_reply", "") or "").strip()
        if not suggested_owner_reply:
            for run in run_results:
                fallback_reply = str(run.get("analysis", {}).get("suggested_owner_reply", "") or "").strip()
                if fallback_reply:
                    suggested_owner_reply = fallback_reply
                    break
        if not suggested_owner_reply:
            suggested_owner_reply = (
                "Gracias por las reseñas. Estamos revisando vuestra experiencia para mejorar el servicio."
            )
        return {
            "overall_sentiment": overall_sentiment,
            "main_topics": main_topics,
            "strengths": strengths,
            "weaknesses": weaknesses,
            "suggested_owner_reply": suggested_owner_reply,
        }

    def merge_reanalysis_terms(
        self,
        run_results: list[dict[str, Any]],
        *,
        key: str,
        limit: int,
    ) -> list[str]:
        score_by_term: Counter[str] = Counter()
        display_value_by_term: dict[str, str] = {}
        for run in run_results:
            terms = run.get("analysis", {}).get(key) or []
            if not isinstance(terms, list):
                continue
            for index, raw_term in enumerate(terms):
                term = str(raw_term or "").strip()
                normalized = self._normalize_text(term)
                if not normalized:
                    continue
                display_value_by_term.setdefault(normalized, term)
                score_by_term[normalized] += max(1, 10 - index)
        ranked = sorted(score_by_term.items(), key=lambda item: item[1], reverse=True)
        return [display_value_by_term[normalized] for normalized, _ in ranked[:limit]]

    def review_fingerprint(self, review: dict[str, Any]) -> str:
        parts = [
            str(review.get("business_id", "")),
            str(review.get("source", "")),
            str(review.get("review_id", "")),
            self._normalize_text(str(review.get("author_name", ""))),
            str(review.get("rating", 0.0)),
            self._normalize_text(str(review.get("relative_time", ""))),
            self._normalize_text(str(review.get("text", ""))),
        ]
        return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()
