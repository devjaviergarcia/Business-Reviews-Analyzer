from __future__ import annotations

import re
import unicodedata
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId


class BusinessCommonRuntime:
    def __init__(
        self,
        *,
        supported_review_strategies: set[str],
        supported_force_modes: set[str],
        scrape_sources: tuple[str, ...],
    ) -> None:
        self._supported_review_strategies = supported_review_strategies
        self._supported_force_modes = supported_force_modes
        self._scrape_sources = scrape_sources

    def validate_business_name(self, name: str) -> str:
        cleaned = re.sub(r"\s+", " ", str(name or "")).strip()
        if not cleaned:
            raise ValueError("Business name is required.")
        if len(cleaned) < 3:
            raise ValueError("Business name must contain at least 3 characters.")
        return cleaned

    def normalize_text(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def resolve_reviews_strategy(self, strategy: str | None) -> str:
        if strategy is None:
            return "scroll_copy"
        raw_value = str(strategy or "").strip()
        normalized = self.normalize_text(raw_value).replace("-", "_").replace(" ", "_")
        if normalized in {"", "default"}:
            normalized = "scroll_copy"
        if normalized not in self._supported_review_strategies:
            supported = ", ".join(sorted(self._supported_review_strategies))
            raise ValueError(f"Unknown strategy '{raw_value}'. Supported: {supported}.")
        return normalized

    def resolve_scrape_sources(self, sources: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
        if sources is None:
            return tuple(self._scrape_sources)
        normalized_sources: list[str] = []
        for raw in sources:
            normalized = self.normalize_text(str(raw or "")).replace("-", "_").replace(" ", "_")
            if not normalized:
                continue
            if normalized not in self._scrape_sources:
                supported = ", ".join(self._scrape_sources)
                raise ValueError(f"Unknown scrape source '{raw}'. Supported: {supported}.")
            if normalized not in normalized_sources:
                normalized_sources.append(normalized)
        if not normalized_sources:
            raise ValueError("At least one scrape source is required.")
        return tuple(normalized_sources)

    def resolve_force_mode(self, force_mode: str | None) -> str:
        if force_mode is None:
            return "fallback_existing"
        raw_value = str(force_mode or "").strip()
        normalized = self.normalize_text(raw_value).replace("-", "_").replace(" ", "_")
        if normalized in {"", "default"}:
            normalized = "fallback_existing"
        if normalized not in self._supported_force_modes:
            supported = ", ".join(sorted(self._supported_force_modes))
            raise ValueError(f"Unknown force_mode '{raw_value}'. Supported: {supported}.")
        return normalized

    def resolve_optional_int_override(
        self,
        *,
        value: int | None,
        fallback: int,
        min_value: int,
        field_name: str,
    ) -> int:
        if value is None:
            return int(fallback)
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be an integer.") from exc
        if parsed < min_value:
            raise ValueError(f"{field_name} must be >= {min_value}.")
        return parsed

    def resolve_optional_float_override(
        self,
        *,
        value: float | None,
        min_value: float,
        max_value: float,
        field_name: str,
    ) -> float:
        if value is None:
            raise ValueError(f"{field_name} is required when override validation is requested.")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be a number.") from exc
        if parsed < min_value:
            raise ValueError(f"{field_name} must be >= {min_value}.")
        if parsed > max_value:
            raise ValueError(f"{field_name} must be <= {max_value}.")
        return parsed

    def parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def sanitize_response_payload(self, value: Any) -> Any:
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, dict):
            return {key: self.sanitize_response_payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.sanitize_response_payload(item) for item in value]
        return value
