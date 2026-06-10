from __future__ import annotations

import html
import re
import unicodedata
from typing import Any


class GoogleMapsBrowserParsingFacet:

    def extract_reviews_from_html(self, reviews_html: str, limit: int | None = None) -> list[dict]:
        if not reviews_html:
            return []

        cards = self._extract_review_card_html_fragments(reviews_html)
        if limit is not None and limit > 0:
            cards = cards[:limit]

        items: list[dict[str, Any]] = []
        for card_html in cards:
            review_id = self._extract_attr_value(card_html, "data-review-id")

            author_name = self._strip_html_markup(
                self._extract_first_html_fragment(
                    card_html,
                    r"<div[^>]*class=['\"][^'\"]*d4r55[^'\"]*['\"][^>]*>(.*?)</div>",
                )
            )
            if not author_name:
                author_name = ""

            rating_label = self._extract_first_attr_value_containing(
                card_html,
                "aria-label",
                contains_terms=("estrella", "star"),
            )
            if not rating_label:
                rating_label = self._strip_html_markup(
                    self._extract_first_html_fragment(
                        card_html,
                        r"<span[^>]*class=['\"][^'\"]*fzvQIb[^'\"]*['\"][^>]*>(.*?)</span>",
                    )
                )
            rating = self._parse_rating(rating_label)

            relative_time = self._strip_html_markup(
                self._extract_first_html_fragment(
                    card_html,
                    r"<span[^>]*class=['\"][^'\"]*rsqaWe[^'\"]*['\"][^>]*>(.*?)</span>",
                )
            )
            if not relative_time:
                relative_time = ""

            review_text = self._strip_html_markup(
                self._extract_first_html_fragment(
                    card_html,
                    r"<div[^>]*class=['\"][^'\"]*MyEned[^'\"]*['\"][^>]*>.*?<span[^>]*class=['\"][^'\"]*wiI7pd[^'\"]*['\"][^>]*>(.*?)</span>",
                )
            )
            if not review_text:
                review_text = self._strip_html_markup(
                    self._extract_first_html_fragment(
                        card_html,
                        r"<span[^>]*class=['\"][^'\"]*wiI7pd[^'\"]*['\"][^>]*>(.*?)</span>",
                    )
                )
            if not review_text:
                review_text = ""

            image_urls: list[str] = []
            seen_image_urls: set[str] = set()
            for style_value in self._extract_attr_values(card_html, "style"):
                for url in self._extract_urls_from_style(style_value):
                    if url in seen_image_urls:
                        continue
                    seen_image_urls.add(url)
                    image_urls.append(url)

            review_payload: dict[str, Any] = {
                "source": "google_maps",
                "review_id": review_id,
                "author_name": author_name,
                "rating": rating if rating is not None else 0.0,
                "relative_time": relative_time,
                "text": review_text,
                "image_urls": image_urls,
                "raw_card_html": card_html[:50_000],
            }

            owner_reply = self._extract_owner_reply_from_card_html(card_html)
            if owner_reply is not None:
                review_payload["owner_reply"] = owner_reply

            items.append(review_payload)

        return items

    def _extract_urls_from_style(self, style: str | None) -> list[str]:
        if not style:
            return []

        matches = re.findall(r"url\(([^)]+)\)", style)
        urls: list[str] = []
        for match in matches:
            cleaned = match.strip().strip("'\"")
            cleaned = html.unescape(cleaned)
            if cleaned:
                urls.append(cleaned)
        return urls

    def _extract_review_card_html_fragments(self, reviews_html: str) -> list[str]:
        open_tag_pattern = re.compile(
            r"<div\b[^>]*\bdata-review-id\s*=\s*(['\"])(?P<review_id>[^\"']+)\1[^>]*>",
            re.IGNORECASE,
        )
        div_tag_pattern = re.compile(r"</?div\b[^>]*>", re.IGNORECASE)

        fragments: list[str] = []
        seen_review_ids: set[str] = set()

        for match in open_tag_pattern.finditer(reviews_html):
            review_id = self._clean_text(match.group("review_id"))
            if not review_id or review_id in seen_review_ids:
                continue

            depth = 1
            end_index: int | None = None
            for div_match in div_tag_pattern.finditer(reviews_html, match.end()):
                token = div_match.group(0).lower()
                if token.startswith("</div"):
                    depth -= 1
                else:
                    depth += 1

                if depth == 0:
                    end_index = div_match.end()
                    break

            if end_index is None:
                continue

            fragments.append(reviews_html[match.start() : end_index])
            seen_review_ids.add(review_id)

        return fragments

    def _extract_attr_value(self, source: str, attribute: str) -> str | None:
        values = self._extract_attr_values(source, attribute)
        return values[0] if values else None

    def _extract_attr_values(self, source: str, attribute: str) -> list[str]:
        pattern = re.compile(
            rf"\b{re.escape(attribute)}\s*=\s*(['\"])(.*?)\1",
            re.IGNORECASE | re.DOTALL,
        )
        values: list[str] = []
        for match in pattern.finditer(source):
            raw_value = html.unescape(match.group(2))
            cleaned = self._clean_text(raw_value)
            if cleaned:
                values.append(cleaned)
        return values

    def _extract_first_attr_value_containing(
        self,
        source: str,
        attribute: str,
        *,
        contains_terms: tuple[str, ...],
    ) -> str | None:
        for value in self._extract_attr_values(source, attribute):
            normalized = self._normalize_text(value)
            if any(term in normalized for term in contains_terms):
                return value
        return None

    def _extract_first_html_fragment(self, source: str, pattern: str) -> str | None:
        match = re.search(pattern, source, re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return match.group(1)

    def _strip_html_markup(self, value: str | None) -> str | None:
        if not value:
            return None
        without_tags = re.sub(r"<[^>]+>", " ", value)
        decoded = html.unescape(without_tags)
        return self._clean_text(decoded)

    def _extract_owner_reply_from_card_html(self, card_html: str) -> dict[str, str] | None:
        marker_pattern = re.compile(
            r"(Respuesta del propietario|Owner response|Response from the owner)",
            re.IGNORECASE,
        )
        marker = marker_pattern.search(card_html)
        if marker is None:
            return None

        after_marker = card_html[marker.end() :]

        reply_time = self._strip_html_markup(
            self._extract_first_html_fragment(
                after_marker,
                r"<span[^>]*class=['\"][^'\"]*DZSIDd[^'\"]*['\"][^>]*>(.*?)</span>",
            )
        )
        reply_text = self._strip_html_markup(
            self._extract_first_html_fragment(
                after_marker,
                r"<span[^>]*class=['\"][^'\"]*wiI7pd[^'\"]*['\"][^>]*>(.*?)</span>",
            )
        )
        if not reply_text:
            return None

        return {"text": reply_text, "relative_time": reply_time or ""}

    def _parse_rating(self, value: str | None) -> float | None:
        if not value:
            return None

        cleaned = self._normalize_text(value)
        match = re.search(r"(\d+(?:[.,]\d+)?)", cleaned)
        if not match:
            return None

        number = match.group(1).replace(",", ".")
        try:
            rating = float(number)
        except ValueError:
            return None

        if 0.0 <= rating <= 5.0:
            return rating

        return None

    def _parse_total_reviews(self, value: str | None) -> int | None:
        if not value:
            return None

        candidates = re.findall(r"\d[\d.,\s]*", value)
        if not candidates:
            return None

        numbers: list[int] = []
        for candidate in candidates:
            digits = re.sub(r"\D", "", candidate)
            if not digits:
                continue
            try:
                numbers.append(int(digits))
            except ValueError:
                continue

        if not numbers:
            return None

        high_confidence = [number for number in numbers if number >= 10]
        if high_confidence:
            return max(high_confidence)

        return max(numbers)

    def _normalize_listing_categories(self, values: list[str]) -> list[str]:
        results: list[str] = []
        seen: set[str] = set()
        for raw_value in values:
            text = self._clean_text(raw_value) or ""
            if not text:
                continue
            for candidate in self._split_category_candidates(text):
                if not self._is_probable_category(candidate):
                    continue
                normalized = self._normalize_text(candidate)
                if normalized in seen:
                    continue
                seen.add(normalized)
                results.append(candidate)
                if len(results) >= 8:
                    return results
        return results

    def _split_category_candidates(self, value: str) -> list[str]:
        text = re.sub(r"[\uE000-\uF8FF]", " ", str(value or ""))
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return []

        parts: list[str] = []
        for chunk in re.split(r"[,\u00b7|\u2022]+", text):
            cleaned = self._clean_text(chunk)
            if not cleaned:
                continue
            parts.append(cleaned)
        return parts

    def _is_probable_category(self, value: str) -> bool:
        normalized = self._normalize_text(value)
        if not normalized:
            return False

        if len(normalized) > 60:
            return False

        if re.search(r"\d", normalized):
            return False

        if not re.search(r"[a-záéíóúüñ]", normalized):
            return False

        blocked_terms = {
            "copiar",
            "guardar",
            "compartir",
            "como llegar",
            "escribir una reseña",
            "resenas",
            "informacion",
            "vista general",
            "carta",
            "ordenar",
            "buscar reseñas",
            "reviews",
            "sugerir nuevo horario",
            "mas",
            "me gusta",
            "anadir precio",
            "ver mas",
            "enviar al telefono",
            "cercano",
            "reservar una mesa",
            "accesible con silla de ruedas",
            "no accesible con silla de ruedas",
            "abierto",
        }
        if normalized in blocked_terms:
            return False

        blocked_fragments = (
            "sugerir",
            "horario",
            "anadir",
            "copiar",
            "compartir",
            "me gusta",
            "google maps",
        )
        return not any(token in normalized for token in blocked_fragments)

    def _is_owner_reply_label(self, value: str) -> bool:
        normalized = self._normalize_text(value)
        keywords = (
            "respuesta del propietario",
            "owner response",
            "response from the owner",
        )
        return any(keyword in normalized for keyword in keywords)

    def _is_cookie_accept_label(self, value: str) -> bool:
        normalized = self._normalize_text(value)
        if not normalized:
            return False
        keywords = (
            "aceptar",
            "accept all",
            "i agree",
            "estoy de acuerdo",
            "agree",
        )
        return any(keyword in normalized for keyword in keywords)

    def _is_more_reviews_label(self, value: str | None) -> bool:
        normalized = self._normalize_text(value or "")
        if not normalized:
            return False

        has_phrase = (
            "mas resenas" in normalized
            or "more reviews" in normalized
            or "more review" in normalized
        )
        if not has_phrase:
            return False

        # Typical format: "Más reseñas (12.030)".
        return bool(re.search(r"\d", normalized)) or normalized.startswith("mas resenas")

    def _is_review_entrypoint_text(self, value: str | None) -> bool:
        normalized = self._normalize_text(value or "")
        if not normalized:
            return False

        if "rese" not in normalized and "review" not in normalized:
            return False

        blocked_tokens = (
            "aviso legal",
            "avisos legales",
            "mas informacion sobre los avisos legales",
            "publicas en google maps",
            "public reviews",
            "escribir una resena",
            "write a review",
            "resumen de resenas",
            "review summary",
            "acciones en la resena",
            "compartir resena",
            "share review",
        )
        return not any(token in normalized for token in blocked_tokens)

    def _clean_text(self, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = re.sub(r"\s+", " ", value).strip()
        return cleaned or None

    def _normalize_text(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized
