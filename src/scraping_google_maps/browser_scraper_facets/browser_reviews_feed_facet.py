from __future__ import annotations

from typing import Any

from src.scraping_google_maps.selectors import SELECTOR_PATTERNS


class GoogleMapsBrowserReviewsFeedFacet:

    async def _reviews_feed_state(
        self,
        *,
        step_px: int | None,
        capture_html: bool,
    ) -> dict[str, Any]:
        page = self._require_page()
        panel_selectors = list(SELECTOR_PATTERNS["REVIEWS_PANEL_READY"])
        card_selectors = list(SELECTOR_PATTERNS["REVIEW_CARDS"])
        normalized_step = max(200, step_px) if step_px is not None else None

        result = await page.evaluate(
            """
            (payload) => {
                const panelSelectors = payload.panelSelectors || [];
                const cardSelectors = payload.cardSelectors || [];
                const requestedStep = payload.stepPx;
                const captureHtml = Boolean(payload.captureHtml);

                const normalizeText = (value) => {
                    if (typeof value !== "string") return "";
                    return value.trim().toLowerCase();
                };

                const normalizeLoose = (value) => {
                    const normalized = normalizeText(value);
                    try {
                        return normalized.normalize("NFD").replace(/[\\u0300-\\u036f]/g, "");
                    } catch (_) {
                        return normalized;
                    }
                };

                const hasReviewKeyword = (value) => {
                    const normalized = normalizeLoose(value);
                    return normalized.includes("rese") || normalized.includes("review");
                };

                const isVisible = (el) => {
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    if (style.display === "none" || style.visibility === "hidden") return false;
                    const rect = el.getBoundingClientRect();
                    return rect.width > 0 && rect.height > 0;
                };

                const safeQueryAll = (root, selector) => {
                    try {
                        return root.querySelectorAll(selector);
                    } catch (_) {
                        return [];
                    }
                };

                const countVisibleMatches = (root, selectors) => {
                    let count = 0;
                    for (const selector of selectors) {
                        const nodes = safeQueryAll(root, selector);
                        for (const node of nodes) {
                            if (isVisible(node)) count += 1;
                        }
                    }
                    return count;
                };

                const hasSearchCue = (root) => {
                    const inputs = root.querySelectorAll("input, textarea");
                    for (const input of inputs) {
                        if (!isVisible(input)) continue;
                        const ariaLabel = input.getAttribute("aria-label") || "";
                        const placeholder = input.getAttribute("placeholder") || "";
                        const labelledBy = (input.getAttribute("aria-labelledby") || "").trim();
                        let labelsText = "";
                        if (labelledBy) {
                            for (const id of labelledBy.split(/\\s+/)) {
                                if (!id) continue;
                                const labelNode = document.getElementById(id);
                                if (!labelNode) continue;
                                labelsText += ` ${labelNode.textContent || ""}`;
                            }
                        }
                        if (
                            hasReviewKeyword(ariaLabel) ||
                            hasReviewKeyword(placeholder) ||
                            hasReviewKeyword(labelsText)
                        ) {
                            return true;
                        }
                    }
                    return false;
                };

                const hasFilterCue = (root) => {
                    let reviewLabeledButtons = 0;
                    const buttons = root.querySelectorAll("button[aria-label]");
                    for (const button of buttons) {
                        if (!isVisible(button)) continue;
                        const label = button.getAttribute("aria-label") || "";
                        const normalized = normalizeLoose(label);
                        if (
                            normalized.includes("todas las rese") ||
                            normalized.includes("all reviews") ||
                            normalized.includes("mas utiles") ||
                            normalized.includes("most relevant") ||
                            normalized.includes("mas recientes") ||
                            normalized.includes("newest")
                        ) {
                            return true;
                        }
                        if (hasReviewKeyword(normalized)) {
                            reviewLabeledButtons += 1;
                        }
                    }
                    if (reviewLabeledButtons >= 2) {
                        return true;
                    }

                    const groups = root.querySelectorAll("[role='radiogroup'], [role='tablist']");
                    for (const group of groups) {
                        const label = group.getAttribute("aria-label") || "";
                        if (hasReviewKeyword(label)) {
                            return true;
                        }
                    }
                    return false;
                };

                const collectCards = (root) => {
                    const byReviewId = new Map();
                    const withoutId = [];
                    for (const selector of cardSelectors) {
                        const nodes = safeQueryAll(root, selector);
                        for (const node of nodes) {
                            const reviewId = (node.getAttribute("data-review-id") || "").trim();
                            if (reviewId) {
                                if (!byReviewId.has(reviewId)) {
                                    byReviewId.set(reviewId, node);
                                }
                            } else {
                                withoutId.push(node);
                            }
                        }
                    }
                    return [...byReviewId.values(), ...withoutId];
                };

                const findScrollableParent = (node, stopRoot) => {
                    let parent = node?.parentElement || null;
                    while (parent) {
                        const style = window.getComputedStyle(parent);
                        const overflowY = style.overflowY;
                        const canScroll = parent.scrollHeight > parent.clientHeight + 20;
                        if ((overflowY === "auto" || overflowY === "scroll" || overflowY === "overlay") && canScroll) {
                            return parent;
                        }
                        if (parent === stopRoot || parent === document.body) break;
                        parent = parent.parentElement;
                    }
                    return null;
                };

                const markers = [];
                for (const selector of panelSelectors) {
                    const nodes = safeQueryAll(document, selector);
                    for (const node of nodes) {
                        if (isVisible(node)) markers.push(node);
                    }
                }

                const roots = [];
                const addRoot = (candidate) => {
                    if (!candidate) return;
                    if (!roots.includes(candidate)) {
                        roots.push(candidate);
                    }
                };

                for (const marker of markers) {
                    const root =
                        marker.closest("[role='main']") ||
                        marker.closest("div.m6QErb") ||
                        document.body;
                    addRoot(root);
                }

                for (const selector of cardSelectors) {
                    const cards = safeQueryAll(document, selector);
                    for (const card of cards) {
                        if (!isVisible(card)) continue;
                        // Keep multiple candidate ancestors, from narrow to broad.
                        // Some layouts render cards inside a non-scrollable wrapper while the
                        // actual feed scroll container lives in a higher ancestor.
                        addRoot(card.closest("div.m6QErb.XiKgde"));
                        addRoot(card.closest("div.m6QErb"));
                        addRoot(card.closest("[role='main']"));
                        addRoot(document.body);
                    }
                }

                if (roots.length === 0) {
                    addRoot(document.querySelector("div[role='main']"));
                    addRoot(document.body);
                }

                let best = null;
                for (const root of roots) {
                    const cards = collectCards(root);
                    if (cards.length === 0) continue;

                    let feed = null;
                    for (const card of cards) {
                        feed = findScrollableParent(card, root);
                        if (feed) break;
                    }

                    if (!feed) {
                        const divs = root.querySelectorAll("div");
                        for (const div of divs) {
                            if (!div.querySelector("[data-review-id]")) continue;
                            const style = window.getComputedStyle(div);
                            const overflowY = style.overflowY;
                            const canScroll = div.scrollHeight > div.clientHeight + 20;
                            if (
                                (overflowY === "auto" || overflowY === "scroll" || overflowY === "overlay") &&
                                canScroll
                            ) {
                                feed = div;
                                break;
                            }
                        }
                    }

                    const markerCount = countVisibleMatches(root, panelSelectors);
                    const searchCue = hasSearchCue(root);
                    const filterCue = hasFilterCue(root);
                    const searchFilterReady =
                        (searchCue && filterCue && cards.length >= 1) ||
                        ((searchCue || filterCue) && cards.length >= 5);
                    const panelReady = markerCount > 0 || searchFilterReady;
                    const variant = markerCount > 0
                        ? "classic_controls"
                        : searchFilterReady
                            ? "search_filter_controls"
                            : "cards_only";

                    const score =
                        cards.length * 100000 +
                        (feed ? (feed.scrollHeight - feed.clientHeight) : 0) +
                        (panelReady ? 50000000 : 0) +
                        markerCount * 50000 +
                        (searchCue ? 10000 : 0) +
                        (filterCue ? 10000 : 0);
                    if (!best || score > best.score) {
                        best = {
                            root,
                            cards,
                            feed,
                            score,
                            panelReady,
                            markerCount,
                            searchCue,
                            filterCue,
                            variant,
                        };
                    }
                }

                if (!best) {
                    return {
                        panel_ready: false,
                        found: false,
                        scrolled: false,
                        at_bottom: true,
                        review_count: 0,
                        scroll_top: 0,
                        scroll_height: 0,
                        client_height: 0,
                        html: "",
                        section_variant: "none",
                        marker_count: 0,
                        search_cue: false,
                        filter_cue: false,
                    };
                }

                if (!best.feed) {
                    const fallbackHtml = captureHtml
                        ? `<div data-review-feed-fallback="true">${best.cards.map((node) => node.outerHTML).join("")}</div>`
                        : "";
                    return {
                        panel_ready: Boolean(best.panelReady),
                        found: false,
                        scrolled: false,
                        at_bottom: true,
                        review_count: best.cards.length,
                        scroll_top: 0,
                        scroll_height: 0,
                        client_height: 0,
                        html: fallbackHtml,
                        section_variant: best.variant || "cards_only",
                        marker_count: Number(best.markerCount || 0),
                        search_cue: Boolean(best.searchCue),
                        filter_cue: Boolean(best.filterCue),
                    };
                }

                const feed = best.feed;
                const before = feed.scrollTop;
                if (requestedStep !== null && requestedStep !== undefined) {
                    const step = requestedStep > 0 ? requestedStep : Math.max(420, feed.clientHeight * 0.9);
                    feed.scrollBy(0, step);
                    if (feed.scrollTop === before) {
                        feed.scrollTop = Math.min(feed.scrollTop + step, feed.scrollHeight);
                    }
                }
                const after = feed.scrollTop;
                const atBottom = after + feed.clientHeight >= feed.scrollHeight - 4;

                return {
                    panel_ready: Boolean(best.panelReady),
                    found: true,
                    scrolled: after > before,
                    at_bottom: atBottom,
                    review_count: best.cards.length,
                    scroll_top: Math.round(after),
                    scroll_height: Math.round(feed.scrollHeight),
                    client_height: Math.round(feed.clientHeight),
                    html: captureHtml ? feed.outerHTML : "",
                    section_variant: best.variant || "cards_only",
                    marker_count: Number(best.markerCount || 0),
                    search_cue: Boolean(best.searchCue),
                    filter_cue: Boolean(best.filterCue),
                };
            }
            """,
            {
                "panelSelectors": panel_selectors,
                "cardSelectors": card_selectors,
                "stepPx": normalized_step,
                "captureHtml": bool(capture_html),
            },
        )
        if not isinstance(result, dict):
            return {
                "panel_ready": False,
                "found": False,
                "scrolled": False,
                "at_bottom": True,
                "review_count": 0,
                "scroll_top": 0,
                "scroll_height": 0,
                "client_height": 0,
                "html": "",
                "section_variant": "none",
                "marker_count": 0,
                "search_cue": False,
                "filter_cue": False,
            }
        return result

    async def _scroll_reviews_feed_step(self, step_px: int | None = None) -> dict[str, Any]:
        state = await self._reviews_feed_state(step_px=step_px, capture_html=False)
        return {
            "panel_ready": bool(state.get("panel_ready")),
            "found": bool(state.get("found")),
            "scrolled": bool(state.get("scrolled")),
            "at_bottom": bool(state.get("at_bottom")),
            "review_count": int(state.get("review_count", 0)),
            "scroll_top": int(state.get("scroll_top", 0)),
            "scroll_height": int(state.get("scroll_height", 0)),
            "client_height": int(state.get("client_height", 0)),
        }

    async def _capture_reviews_feed_html(self) -> str:
        state = await self._reviews_feed_state(step_px=None, capture_html=True)
        return str(state.get("html", "") or "")
