from __future__ import annotations

from typing import Any

from playwright.async_api import Locator


class GoogleMapsBrowserReviewCardFacet:

    async def _extract_owner_reply(self, card: Locator) -> dict[str, str] | None:
        block = await self._find_owner_reply_block(card)
        if block is None:
            return None

        reply_time = await self._text_from_descendant_patterns(block, "OWNER_REPLY_TIME")
        reply_text = await self._text_from_descendant_patterns(block, "OWNER_REPLY_TEXT")

        if not reply_text:
            raw_block_text = await self._text_from_locator(block)
            if raw_block_text:
                lines = [line.strip() for line in re.split(r"\n+", raw_block_text) if line.strip()]
                cleaned_lines: list[str] = []
                for line in lines:
                    if self._is_owner_reply_label(line):
                        continue
                    if reply_time and self._normalize_text(line) == self._normalize_text(reply_time):
                        continue
                    cleaned_lines.append(line)
                if cleaned_lines:
                    reply_text = " ".join(cleaned_lines)

        if not reply_text:
            return None

        return {"text": reply_text, "relative_time": reply_time or ""}

    async def _find_owner_reply_block(self, card: Locator) -> Locator | None:
        for selector in SELECTOR_PATTERNS["OWNER_REPLY_BLOCK"]:
            candidates = card.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue

            if total <= 0:
                continue

            # Owner reply usually appears at the end of the review block.
            for idx in range(min(total - 1, 5), -1, -1):
                candidate = candidates.nth(idx)
                if await self._looks_like_owner_reply_block(candidate):
                    return candidate

        return None

    async def _looks_like_owner_reply_block(self, block: Locator) -> bool:
        label = await self._text_from_descendant_patterns(block, "OWNER_REPLY_LABEL")
        reply_text = await self._text_from_descendant_patterns(block, "OWNER_REPLY_TEXT")

        if label and self._is_owner_reply_label(label):
            return True

        block_text = await self._text_from_locator(block)
        if block_text and self._is_owner_reply_label(block_text):
            return True

        if reply_text:
            try:
                child_divs = block.locator(":scope > div")
                first_child_span_count = await child_divs.nth(0).locator("span").count()
                child_count = await child_divs.count()
                if child_count >= 2 and first_child_span_count > 0:
                    return True
            except Exception:
                pass

        return False

    async def _extract_review_photo_urls(self, card: Locator) -> list[str]:
        buttons = card.locator("button[data-photo-index][data-review-id]")
        urls: list[str] = []
        seen: set[str] = set()

        try:
            total = await buttons.count()
        except Exception:
            return urls

        for idx in range(total):
            style = await buttons.nth(idx).get_attribute("style")
            for url in self._extract_urls_from_style(style):
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)

        return urls
