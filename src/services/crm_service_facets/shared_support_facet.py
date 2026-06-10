from __future__ import annotations

from datetime import datetime
from typing import Any

from src.models.crm import CRMEvent


class CRMServiceSharedSupportFacet:

    def _merge_listing_payloads(self, *, primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        return self._common_runtime.merge_listing_payloads(primary=primary, fallback=fallback)

    def _parse_rating_text(self, value: Any) -> float | None:
        return self._common_runtime.parse_rating_text(value)

    def _sanitize_listing_categories(self, raw_values: list[str]) -> list[str]:
        return self._common_runtime.sanitize_listing_categories(raw_values)

    def _is_noise_category_token(self, normalized_value: str) -> bool:
        return self._common_runtime.is_noise_category_token(normalized_value)

    def _parse_reviews_count_text(self, value: Any) -> int | None:
        return self._common_runtime.parse_reviews_count_text(value)

    def _canonicalize_maps_url(self, value: str) -> str:
        return self._common_runtime.canonicalize_maps_url(value)

    async def _record_event(
        self,
        *,
        event_type: str,
        lead_id: str | None = None,
        campaign_id: str | None = None,
        message_id: str | None = None,
        data: dict[str, Any] | None = None,
        actor: str = "system",
    ) -> None:
        event = CRMEvent(
            event_type=str(event_type or "event").strip(),
            lead_id=str(lead_id).strip() if lead_id else None,
            campaign_id=str(campaign_id).strip() if campaign_id else None,
            message_id=str(message_id).strip() if message_id else None,
            actor=str(actor or "system"),
            data=dict(data or {}),
            created_at=self._now_utc(),
        )
        await self._event_repository.insert(event.model_dump(mode="python"))

    def _extract_city_from_address(self, value: str | None) -> str | None:
        return self._common_runtime.extract_city_from_address(value)

    def _normalize_text(self, value: Any) -> str:
        return self._common_runtime.normalize_text(value)

    def _normalize_email(self, value: Any) -> str | None:
        return self._common_runtime.normalize_email(value)

    def _normalize_utm(self, value: dict[str, Any]) -> dict[str, str | None]:
        return self._common_runtime.normalize_utm(value)

    def _domain_from_email_or_website(self, *, email: str | None, website: str | None) -> str | None:
        return self._common_runtime.domain_from_email_or_website(email=email, website=website)

    def _build_lead_score(
        self,
        *,
        rating: Any,
        review_count: Any,
        has_email: bool,
        has_website: bool,
    ) -> float:
        return self._common_runtime.build_lead_score(
            rating=rating,
            review_count=review_count,
            has_email=has_email,
            has_website=has_website,
        )

    def _parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        return self._common_runtime.parse_object_id(value, field_name=field_name)

    def _serialize_mongo_doc(self, doc: dict[str, Any], *, id_key: str) -> dict[str, Any]:
        return self._common_runtime.serialize_mongo_doc(doc, id_key=id_key)

    def _sanitize_payload(self, value: Any) -> Any:
        return self._common_runtime.sanitize_payload(value)

    def _now_utc(self) -> datetime:
        return self._common_runtime.now_utc()
