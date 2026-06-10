from __future__ import annotations

import re

from playwright.async_api import Locator


class TripadvisorBrowserReviewOwnerReplyFacet:
    async def _extract_owner_reply(self, card: Locator, *, reviewer_author_name: str = "") -> dict[str, str] | None:
        # Fast path: Tripadvisor currently wraps owner reply in `div.mahws` inside each review card.
        block = card.locator("div.mahws").first
        try:
            if await block.count() == 0:
                block = card.locator("div[data-test-target='owner-reply']").first
        except Exception:
            block = card.locator("div.mahws").first
        if await block.count() == 0:
            return None

        author_name = await self._extract_profile_display_name(
            scope=block,
            exclude_names=[reviewer_author_name],
        )
        written_date = await self._owner_reply_written_date(block)
        reply_text = await self._owner_reply_text(block, author_name=author_name, written_date=written_date)
        if not reply_text:
            return None

        return {
            "text": reply_text,
            "relative_time": written_date or "",
            "written_date": written_date or "",
            "author_name": author_name or "",
        }

    async def _owner_reply_marker_in_scope(self, scope: Locator) -> Locator | None:
        marker_selectors = (
            "div:has-text('Esta respuesta es la opinión subjetiva del representante de la dirección')",
            "div:has-text('Esta respuesta es la opinion subjetiva del representante de la direccion')",
            "div:has-text('This response is the subjective opinion of the management representative')",
        )
        for selector in marker_selectors:
            markers = scope.locator(selector)
            try:
                total = await markers.count()
            except Exception:
                continue
            for idx in range(min(total, 6)):
                marker = markers.nth(idx)
                text = await self._safe_locator_inner_text(marker)
                if self._is_owner_reply_disclaimer(text):
                    return marker
        return None

    async def _owner_reply_written_date(self, block: Locator) -> str:
        matches: list[str] = []
        selectors = (
            "div:has-text('Escrita el')",
            "div:has-text('Escrito el')",
            "div:has-text('Responded')",
            "div:has-text('Written')",
        )
        for selector in selectors:
            candidates = block.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, 6)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if not text:
                    continue
                extracted = self._extract_written_date_line_from_text(text)
                if extracted:
                    matches.append(extracted)

        lines = await self._locator_text_lines(block)
        for line in lines:
            extracted = self._extract_written_date_line_from_text(line)
            if extracted:
                matches.append(extracted)
        return matches[-1] if matches else ""

    def _extract_written_date_line_from_text(self, text: str) -> str:
        cleaned = self._clean_text(text)
        if not cleaned:
            return ""
        patterns = (
            r"(Escrita el .*?)(?=\s+Esta respuesta|\s+This response|$)",
            r"(Escrito el .*?)(?=\s+Esta respuesta|\s+This response|$)",
            r"(Responded .*?)(?=\s+This response|$)",
            r"(Written .*?)(?=\s+This response|$)",
        )
        for pattern in patterns:
            match = re.search(pattern, cleaned, flags=re.IGNORECASE)
            if match:
                return self._clean_text(match.group(1))
        return cleaned if self._looks_like_written_date_text(cleaned) else ""

    async def _owner_reply_text(
        self,
        block: Locator,
        *,
        author_name: str,
        written_date: str,
    ) -> str:
        candidate_selectors = (
            "div._T.FKffI span.JguWG",
            "div._T.FKffI div.biGQs._P.VImYz.AWdfh",
            "span.JguWG",
            "div.biGQs._P.VImYz.AWdfh",
        )
        best = ""
        for selector in candidate_selectors:
            candidates = block.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, 8)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if not text:
                    continue
                if self._owner_reply_text_is_noise(text, author_name=author_name, written_date=written_date):
                    continue
                if len(text) > len(best):
                    best = text
        if best:
            return best

        lines = await self._locator_text_lines(block)
        cleaned_lines: list[str] = []
        for line in lines:
            if self._owner_reply_text_is_noise(line, author_name=author_name, written_date=written_date):
                continue
            if len(line) < 12:
                continue
            cleaned_lines.append(line)
        return " ".join(cleaned_lines).strip()

    async def _locator_text_lines(self, locator: Locator) -> list[str]:
        try:
            if await locator.count() == 0:
                return []
            raw = await locator.inner_text()
        except Exception:
            return []
        lines = [self._clean_text(line) for line in re.split(r"\n+", raw or "") if self._clean_text(line)]
        return lines

    def _owner_reply_text_is_noise(self, text: str, *, author_name: str, written_date: str) -> bool:
        normalized = self._normalize_text(text)
        if not normalized:
            return True
        if self._is_owner_reply_disclaimer(text):
            return True
        if self._looks_like_written_date_text(text):
            return True
        if normalized in {"leer mas", "leer menos", "read more", "read less"}:
            return True
        if author_name and normalized == self._normalize_text(author_name):
            return True
        if written_date and normalized == self._normalize_text(written_date):
            return True
        return False

    def _is_owner_reply_disclaimer(self, text: str) -> bool:
        normalized = self._normalize_text(text)
        return (
            "respuesta es la opinion subjetiva del representante de la direccion" in normalized
            or "response is the subjective opinion of the management representative" in normalized
        )

    def _looks_like_written_date_text(self, text: str) -> bool:
        normalized = self._normalize_text(text)
        return any(
            token in normalized
            for token in (
                "escrita el",
                "escrito el",
                "written",
                "responded",
            )
        )
