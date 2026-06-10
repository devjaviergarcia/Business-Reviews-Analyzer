from __future__ import annotations

import re
from typing import Any

from playwright.async_api import Locator


class TripadvisorBrowserReviewDomFacet:
    async def _extract_reviews_from_dom(
        self,
        *,
        include_owner_reply: bool,
        include_image_urls: bool,
    ) -> list[dict[str, Any]]:
        page = self._require_page()
        try:
            raw_items = await page.evaluate(
                """
                ({ includeOwnerReply, includeImageUrls }) => {
                  const clean = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
                  const hasBodyNode = (root) => !!(
                    root?.querySelector("[data-test-target='review-body']")
                    || root?.querySelector("div._T.FKffI")
                    || root?.querySelector("span.JguWG")
                  );
                  const parseRating = (value) => {
                    const normalized = clean(value).replace(',', '.');
                    const match = normalized.match(/(\\d+(?:\\.\\d+)?)/);
                    if (!match) return null;
                    const parsed = Number(match[1]);
                    if (!Number.isFinite(parsed)) return null;
                    if (parsed < 0 || parsed > 5) return null;
                    return parsed;
                  };
                  const pickWrittenDate = (root) => {
                    const candidates = Array.from(root.querySelectorAll('div.biGQs._P.VImYz.ncFvv.navcl, div.biGQs._P.VImYz.navcl'));
                    for (const el of candidates) {
                      const text = clean(el.textContent);
                      if (!text) continue;
                      if (/escrita el|escrito el|written|responded/i.test(text)) return text;
                    }
                    return '';
                  };
                  let cards = Array.from(document.querySelectorAll("div[data-automation='reviewCard'], div[data-test-target='HR_CC_CARD']"));
                  if (!cards.length) {
                    const roots = [];
                    const seen = new Set();
                    const titles = Array.from(document.querySelectorAll("[data-test-target='review-title'] a[href], [data-test-target='review-title']")).slice(0, 48);
                    for (const node of titles) {
                      let root = node;
                      let selectedRoot = null;
                      for (let depth = 0; depth < 10 && root; depth += 1) {
                        const hasTitle = !!root.querySelector("[data-test-target='review-title']");
                        const hasBody = hasBodyNode(root);
                        if (hasTitle && hasBody) {
                          selectedRoot = root;
                          const hasAuthorProfile = !!root.querySelector("a[href*='/Profile/']");
                          if (hasAuthorProfile) break;
                        }
                        root = root.parentElement;
                      }
                      root = selectedRoot;
                      if (!root) continue;
                      if (seen.has(root)) continue;
                      seen.add(root);
                      roots.push(root);
                    }
                    cards = roots;
                  }
                  cards = cards.slice(0, 32);
                  const items = [];
                  for (const card of cards) {
                    const titleAnchor = card.querySelector("[data-test-target='review-title'] a[href]") || card.querySelector("h3 a[href]");
                    const titleNode = card.querySelector("[data-test-target='review-title']");
                    const title = clean(titleAnchor?.textContent || titleNode?.textContent);
                    const titleHref = clean(titleAnchor?.getAttribute('href'));
                    const authorAnchor = card.querySelector("a[href*='/Profile/'].ukgoS") || card.querySelector("span.biGQs._P.ezezH a[href*='/Profile/']");
                    const authorName = clean(authorAnchor?.textContent);
                    const relativeTime = clean(
                      (
                        card.querySelector('div.VufqL.o.W')
                        || card.querySelector('div.VufqL')
                        || card.querySelector('div.ZRBpD div.biGQs._P.VImYz.AWdfh')
                        || card.querySelector('div.biGQs._P.VImYz.AWdfh')
                      )?.textContent
                    );
                    const writtenDate = pickWrittenDate(card);
                    const text = clean(
                      (card.querySelector("div[data-test-target='review-body'] span.JguWG div.biGQs._P.VImYz.AWdfh")
                        || card.querySelector("div[data-test-target='review-body'] span.JguWG")
                        || card.querySelector("div[data-test-target='review-body'] div.biGQs._P.VImYz.AWdfh")
                        || card.querySelector("div[data-test-target='review-body']")
                        || card.querySelector("div._c div._T.FKffI span.JguWG div.biGQs._P.VImYz.AWdfh")
                        || card.querySelector("div._c div._T.FKffI span.JguWG")
                        || card.querySelector("div._c div._T.FKffI")
                        || card.querySelector("div._T.FKffI"))?.textContent
                    ).slice(0, 6000);
                    const rating = parseRating(
                      clean(card.querySelector("svg[data-automation='bubbleRatingImage'] title")?.textContent)
                      || clean(card.querySelector("title[id*='_lithium']")?.textContent)
                    );
                    const item = {
                      title_href: titleHref,
                      review_title: title,
                      author_name: authorName,
                      relative_time: relativeTime,
                      written_date: writtenDate,
                      text,
                      rating,
                      raw_card_html: String(card.outerHTML || '').slice(0, 50000),
                    };
                    if (includeImageUrls) {
                      const images = Array.from(card.querySelectorAll("button img, picture img"))
                        .map((img) => clean(img.currentSrc || img.getAttribute('src')))
                        .filter((url) => !!url && !/default-avatar/i.test(url));
                      item.image_urls = Array.from(new Set(images)).slice(0, 12);
                    }
                    if (includeOwnerReply) {
                      const replyRoot = card.querySelector("div.mahws");
                      if (replyRoot) {
                        const replyText = clean(
                          (replyRoot.querySelector("div._T.FKffI span.JguWG")
                            || replyRoot.querySelector("div._T.FKffI div.biGQs._P.VImYz.AWdfh")
                            || replyRoot.querySelector("span.JguWG"))?.textContent
                        ).slice(0, 3000);
                        const replyAuthor = clean(
                          (replyRoot.querySelector("a[href*='/Profile/'].ukgoS")
                            || replyRoot.querySelector("span.biGQs._P.ezezH"))?.textContent
                        );
                        const replyWrittenDate = pickWrittenDate(replyRoot);
                        if (replyText) {
                          item.owner_reply = {
                            text: replyText,
                            relative_time: replyWrittenDate,
                            written_date: replyWrittenDate,
                            author_name: replyAuthor,
                          };
                        }
                      }
                    }
                    if (
                      item.review_title ||
                      item.author_name ||
                      item.relative_time ||
                      item.written_date ||
                      item.text ||
                      item.title_href
                    ) {
                      items.push(item);
                    }
                  }
                  return items;
                }
                """,
                {
                    "includeOwnerReply": bool(include_owner_reply),
                    "includeImageUrls": bool(include_image_urls),
                },
            )
        except Exception:
            return []

        if not isinstance(raw_items, list):
            return []

        normalized_items: list[dict[str, Any]] = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            title_href = self._clean_text(str(raw.get("title_href", "") or ""))
            review_id = self._extract_review_id_from_href(title_href)
            item: dict[str, Any] = {
                "source": "tripadvisor",
                "review_id": review_id,
                "author_name": self._clean_text(str(raw.get("author_name", "") or "")),
                "rating": self._parse_rating(raw.get("rating")) or 0.0,
                "relative_time": self._clean_text(str(raw.get("relative_time", "") or "")),
                "text": self._clean_text(str(raw.get("text", "") or "")),
                "review_title": self._clean_text(str(raw.get("review_title", "") or "")),
                "written_date": self._extract_written_date_line_from_text(str(raw.get("written_date", "") or "")),
            }
            raw_card_html = str(raw.get("raw_card_html", "") or "").strip()
            if raw_card_html:
                item["raw_card_html"] = raw_card_html[:50_000]
            if include_image_urls:
                image_urls = raw.get("image_urls")
                if isinstance(image_urls, list):
                    item["image_urls"] = [
                        self._clean_text(str(url or ""))
                        for url in image_urls
                        if self._clean_text(str(url or ""))
                    ]
            if include_owner_reply:
                owner_reply = raw.get("owner_reply")
                if isinstance(owner_reply, dict):
                    owner_reply_text = self._clean_text(str(owner_reply.get("text", "") or ""))
                    if owner_reply_text:
                        owner_written = self._extract_written_date_line_from_text(
                            str(owner_reply.get("written_date", "") or owner_reply.get("relative_time", "") or "")
                        )
                        item["owner_reply"] = {
                            "text": owner_reply_text,
                            "relative_time": owner_written,
                        }
                        owner_author = self._clean_text(str(owner_reply.get("author_name", "") or ""))
                        if owner_author:
                            item["owner_reply_author_name"] = owner_author
                        if owner_written:
                            item["owner_reply_written_date"] = owner_written

            normalized_items.append(item)
        return normalized_items

    async def _extract_review_image_urls(self, card: Locator) -> list[str]:
        images = card.locator("button[aria-label*='imagen' i] img")
        total = await images.count()
        collected: list[str] = []
        seen: set[str] = set()

        for idx in range(min(total, 12)):
            image = images.nth(idx)
            src = self._clean_text(await image.get_attribute("src"))
            srcset = self._clean_text(await image.get_attribute("srcset"))
            url = src or self._first_url_from_srcset(srcset)
            if not url:
                continue
            if "default-avatar" in url.lower():
                continue
            if url in seen:
                continue
            seen.add(url)
            collected.append(url)
        return collected

    async def _extract_review_card_rating(self, card: Locator) -> float | None:
        svg = card.locator("svg[data-automation='bubbleRatingImage']").first
        rating_from_paths = await self._extract_bubble_rating_from_svg(svg)
        if rating_from_paths is not None:
            return rating_from_paths

        rating_label = await self._safe_locator_inner_text(svg.locator("title").first)
        if not rating_label:
            rating_label = await self._safe_locator_inner_text(card.locator("title[id*='_lithium']").first)
        return self._parse_rating(rating_label)

    async def _extract_bubble_rating_from_svg(self, svg: Locator) -> float | None:
        try:
            if await svg.count() == 0:
                return None
        except Exception:
            return None

        paths = svg.locator("path")
        try:
            total_paths = await paths.count()
        except Exception:
            return None
        if total_paths <= 0:
            return None

        first_d = await self._safe_locator_attribute(paths.nth(0), "d")
        normalized_first = self._normalize_svg_path_d(first_d)
        if not normalized_first:
            return None

        filled = 0
        for idx in range(min(total_paths, 5)):
            d_value = await self._safe_locator_attribute(paths.nth(idx), "d")
            if self._normalize_svg_path_d(d_value) == normalized_first:
                filled += 1

        if 1 <= filled <= 5:
            return float(filled)
        return None

    def _normalize_svg_path_d(self, value: str) -> str:
        cleaned = self._clean_text(value)
        if not cleaned:
            return ""
        return re.sub(r"\s+", "", cleaned).lower()

    async def _extract_review_author_name(self, card: Locator) -> str:
        return await self._extract_profile_display_name(
            scope=card,
            exclude_names=[],
        )

    async def _extract_profile_display_name(
        self,
        *,
        scope: Locator,
        exclude_names: list[str],
    ) -> str:
        excluded = {self._normalize_text(value) for value in exclude_names if self._clean_text(value)}
        selectors = (
            "div.QIHsu span.biGQs._P.ezezH a[href*='/Profile/']",
            "a[href*='/Profile/'].ukgoS",
            "div.QIHsu span.biGQs._P.ezezH",
            "span.biGQs._P.ezezH",
        )
        for selector in selectors:
            candidates = scope.locator(selector)
            try:
                total = await candidates.count()
            except Exception:
                continue
            for idx in range(min(total, 10)):
                text = await self._safe_locator_inner_text(candidates.nth(idx))
                if not text:
                    continue
                normalized = self._normalize_text(text)
                if not normalized:
                    continue
                if normalized in excluded:
                    continue
                if "contribuciones" in normalized:
                    continue
                if normalized in {"leer mas", "leer menos", "read more", "read less"}:
                    continue
                return text
        return ""
