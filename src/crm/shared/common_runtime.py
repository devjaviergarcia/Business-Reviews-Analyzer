from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId


class CRMCommonRuntime:
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def parse_object_id(self, value: str, *, field_name: str) -> ObjectId:
        try:
            return ObjectId(str(value))
        except (InvalidId, TypeError) as exc:
            raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc

    def serialize_mongo_doc(self, doc: dict[str, Any], *, id_key: str) -> dict[str, Any]:
        payload = dict(doc)
        payload[id_key] = str(payload.pop("_id"))
        return payload

    def sanitize_payload(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, list):
            return [self.sanitize_payload(item) for item in value]
        if isinstance(value, dict):
            return {str(key): self.sanitize_payload(item) for key, item in value.items()}
        return value

    def extract_city_from_address(self, value: str | None) -> str | None:
        cleaned = str(value or "").strip()
        if not cleaned:
            return None
        parts = [part.strip() for part in cleaned.split(",") if part.strip()]
        if not parts:
            return None
        return parts[-1]

    def normalize_text(self, value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        normalized = unicodedata.normalize("NFKD", raw)
        ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
        collapsed = re.sub(r"[^a-z0-9]+", " ", ascii_value)
        return re.sub(r"\s+", " ", collapsed).strip()

    def normalize_email(self, value: Any) -> str | None:
        raw = str(value or "").strip().lower()
        if not raw or "@" not in raw:
            return None
        return raw

    def normalize_utm(self, value: dict[str, Any]) -> dict[str, str | None]:
        allowed_keys = ("source", "medium", "campaign", "term", "content")
        normalized: dict[str, str | None] = {}
        for key in allowed_keys:
            raw = value.get(key)
            if raw is None:
                raw = value.get(f"utm_{key}")
            text = str(raw or "").strip()
            normalized[key] = text or None
        return normalized

    def domain_from_email_or_website(self, *, email: str | None, website: str | None) -> str | None:
        email_norm = self.normalize_email(email)
        if email_norm and "@" in email_norm:
            return email_norm.split("@", 1)[1].strip() or None

        website_raw = str(website or "").strip().lower()
        if not website_raw:
            return None
        if not website_raw.startswith("http://") and not website_raw.startswith("https://"):
            website_raw = f"https://{website_raw}"
        parsed = urlparse(website_raw)
        host = str(parsed.hostname or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None

    def build_lead_score(
        self,
        *,
        rating: Any,
        review_count: Any,
        has_email: bool,
        has_website: bool,
    ) -> float:
        score = 0.0
        if isinstance(rating, (int, float)):
            rating_value = max(0.0, min(5.0, float(rating)))
            if rating_value < 2.8:
                score += 20
            elif rating_value <= 4.4:
                score += 45
            else:
                score += 30
        if isinstance(review_count, (int, float)):
            reviews = max(0, int(review_count))
            if reviews >= 500:
                score += 25
            elif reviews >= 200:
                score += 20
            elif reviews >= 50:
                score += 14
            elif reviews >= 10:
                score += 8
            else:
                score += 4
        if has_email:
            score += 18
        if has_website:
            score += 12
        return round(min(100.0, score), 2)

    def parse_rating_text(self, value: Any) -> float | None:
        text = str(value or "").strip()
        if not text:
            return None
        match = re.search(r"([0-5](?:[.,]\d)?)", text)
        if not match:
            return None
        raw_value = str(match.group(1) or "").replace(",", ".")
        try:
            rating = float(raw_value)
        except ValueError:
            return None
        if rating < 0.0 or rating > 5.0:
            return None
        return rating

    def parse_reviews_count_text(self, value: Any) -> int | None:
        text = str(value or "").strip()
        if not text:
            return None
        text_no_spaces = re.sub(r"\s+", "", text)
        candidates = re.findall(r"\d[\d\.,]*", text_no_spaces)
        if not candidates:
            return None
        parsed_values: list[int] = []
        for candidate in candidates:
            digits = re.sub(r"[^0-9]", "", candidate)
            if not digits:
                continue
            try:
                parsed_values.append(int(digits))
            except ValueError:
                continue
        if not parsed_values:
            return None
        return max(parsed_values)

    def canonicalize_maps_url(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            parsed = urlparse(raw)
        except Exception:
            return raw
        path = str(parsed.path or "").strip()
        if not path:
            return raw
        return f"{parsed.scheme}://{parsed.netloc}{path}"

    def sanitize_listing_categories(self, raw_values: list[str]) -> list[str]:
        cleaned_values: list[str] = []
        seen: set[str] = set()
        for raw_value in raw_values:
            raw_text = str(raw_value or "").strip()
            if not raw_text:
                continue
            without_symbols = re.sub(r"[\uE000-\uF8FF]", " ", raw_text)
            for part in re.split(r"[,\u00b7|\u2022]+", without_symbols):
                candidate = str(part or "").strip()
                if not candidate:
                    continue
                normalized = self.normalize_text(candidate)
                if not normalized or normalized in seen:
                    continue
                if self.is_noise_category_token(normalized):
                    continue
                seen.add(normalized)
                cleaned_values.append(candidate)
                if len(cleaned_values) >= 8:
                    return cleaned_values
        return cleaned_values

    def is_noise_category_token(self, normalized_value: str) -> bool:
        if not normalized_value:
            return True
        if len(normalized_value) > 60:
            return True
        if re.search(r"\d", normalized_value):
            return True
        if not re.search(r"[a-z]", normalized_value):
            return True

        blocked_exact = {
            "copiar",
            "guardar",
            "compartir",
            "mas",
            "me gusta",
            "anadir precio",
            "sugerir nuevo horario",
            "como llegar",
            "ver mas",
            "enviar al telefono",
            "cercano",
            "resenas",
            "review",
            "reviews",
            "ordenar",
            "buscar resenas",
            "informacion",
            "vista general",
            "carta",
        }
        if normalized_value in blocked_exact:
            return True

        blocked_fragments = (
            "sugerir",
            "horario",
            "copiar",
            "compartir",
            "me gusta",
            "google maps",
            "aviso legal",
            "publicas en google maps",
            "mas informacion",
            "reservar una mesa",
        )
        return any(fragment in normalized_value for fragment in blocked_fragments)

    def merge_listing_payloads(self, *, primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = dict(primary or {})
        for key in ("business_name", "address", "phone", "website"):
            if not str(merged.get(key) or "").strip():
                value = str(fallback.get(key) or "").strip()
                if value:
                    merged[key] = value

        for key in ("price_per_person", "description", "menu_url", "reservation_url"):
            if not str(merged.get(key) or "").strip():
                value = str(fallback.get(key) or "").strip()
                if value:
                    merged[key] = value

        if merged.get("overall_rating") is None and fallback.get("overall_rating") is not None:
            merged["overall_rating"] = fallback.get("overall_rating")
        if merged.get("total_reviews") is None and fallback.get("total_reviews") is not None:
            merged["total_reviews"] = fallback.get("total_reviews")

        primary_categories = merged.get("categories") if isinstance(merged.get("categories"), list) else []
        fallback_categories = fallback.get("categories") if isinstance(fallback.get("categories"), list) else []
        merged_categories = self.sanitize_listing_categories(
            [str(item or "") for item in list(primary_categories) + list(fallback_categories)]
        )
        if merged_categories:
            merged["categories"] = merged_categories
            merged["category"] = merged_categories[0]
        elif str(fallback.get("category") or "").strip() and not str(merged.get("category") or "").strip():
            merged["category"] = str(fallback.get("category") or "").strip()

        primary_service_options = merged.get("service_options") if isinstance(merged.get("service_options"), list) else []
        fallback_service_options = fallback.get("service_options") if isinstance(fallback.get("service_options"), list) else []
        merged_service_options: list[str] = []
        seen_options: set[str] = set()
        for item in list(primary_service_options) + list(fallback_service_options):
            cleaned = str(item or "").strip()
            if not cleaned:
                continue
            key = self.normalize_text(cleaned)
            if key in seen_options:
                continue
            seen_options.add(key)
            merged_service_options.append(cleaned)
        if merged_service_options:
            merged["service_options"] = merged_service_options
        return merged
