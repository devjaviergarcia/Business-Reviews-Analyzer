from __future__ import annotations

import json
from datetime import datetime, timezone
from time import monotonic
from typing import Any, Awaitable, Callable


class TripadvisorBrowserSearchEntryFacet:
    async def search_business(
        self,
        name: str,
        *,
        progress_callback: Callable[[dict[str, Any]], Awaitable[None] | None] | None = None,
    ) -> None:
        query = self._clean_text(name)
        if not query:
            raise ValueError("Business query is empty.")

        page = await self.start()
        await self._install_tripadvisor_graphql_review_capture()
        started_at = monotonic()
        direct_listing_url = self._resolve_direct_listing_target_url(query)
        search_input_selectors = (
            "form[role='search'] input[type='search'][name='q']",
            "form[action='/Search'] input[type='search'][name='q']",
            "input[role='searchbox'][name='q']",
            "input[type='search'][name='q'][aria-label*='Buscar' i]",
            "input[type='search'][name='q'][title='Buscar']",
            "input[type='search'][name='q']",
            "input[name='q'][type='search']",
        )
        open_search_button_selectors = (
            "form[role='search'] button[type='submit'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
            "button[type='submit'][title='Buscar'][aria-label*='Buscar' i]",
            "button[type='submit'][aria-label*='Buscar' i]",
        )
        submit_button_selectors = (
            "form[role='search'] button[type='submit'][aria-label*='Buscar' i]",
            "div.bOfFT button[type='submit'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
            "button[type='submit'][title='Buscar'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search']",
            "form[role='search'] button[type='submit']",
        )

        async def _emit_search_progress(event: str, *, step: str, step_started_at: float) -> None:
            await self._emit_progress(
                progress_callback,
                {
                    "event": event,
                    "source": "tripadvisor",
                    "step": step,
                    "elapsed_step_s": round(monotonic() - step_started_at, 3),
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                },
            )

        try:
            if direct_listing_url:
                open_direct_started_at = monotonic()
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "tripadvisor_search_direct_url_detected",
                        "source": "tripadvisor",
                        "input": query,
                        "target_url": direct_listing_url,
                        "elapsed_total_s": round(monotonic() - started_at, 3),
                        "page_url": page.url,
                    },
                )
                await page.goto(direct_listing_url, wait_until="domcontentloaded")
                await self._wait_after_navigation()
                await self._accept_cookies_if_present()
                await self._dismiss_consent_if_present()
                await self._dismiss_location_prompt_if_present()
                await _emit_search_progress(
                    "tripadvisor_search_listing_opened",
                    step="open_direct_url",
                    step_started_at=open_direct_started_at,
                )
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "tripadvisor_search_completed",
                        "source": "tripadvisor",
                        "elapsed_total_s": round(monotonic() - started_at, 3),
                        "page_url": page.url,
                    },
                )
                return

            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()

            typing_started_at = monotonic()
            search_input = await self._find_first_optional_visible(
                search_input_selectors,
                timeout_ms=7000,
            )
            if search_input is None:
                open_search_button = await self._find_first_optional_visible(
                    open_search_button_selectors,
                    timeout_ms=3500,
                )
                if open_search_button is not None:
                    try:
                        await self._human_click(open_search_button)
                        await page.wait_for_timeout(self._rng.randint(180, 460))
                    except Exception:
                        pass
                search_input = await self._find_first_optional_visible(
                    search_input_selectors,
                    timeout_ms=7000,
                )
            if search_input is None:
                page_title = ""
                try:
                    page_title = self._clean_text(await page.title())
                except Exception:
                    page_title = ""
                raise RuntimeError(
                    "Tripadvisor search input not found after retries. "
                    f"url={self._clean_text(page.url)} title={page_title!r}"
                )
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_input_ready",
                    "source": "tripadvisor",
                    "query": query,
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                    "debug_state": await self._collect_search_debug_state(
                        search_input_selectors=search_input_selectors,
                        submit_button_selectors=submit_button_selectors,
                    ),
                },
            )
            await self._human_click(search_input)
            await self._human_type(search_input, query)
            await page.wait_for_timeout(self._rng.randint(250, 700))
            await _emit_search_progress(
                "tripadvisor_search_query_typed",
                step="type_query",
                step_started_at=typing_started_at,
            )

            typeahead_started_at = monotonic()
            opened_from_typeahead = await self._open_exact_typeahead_result(query)
            if opened_from_typeahead:
                await _emit_search_progress(
                    "tripadvisor_search_typeahead_exact_match_opened",
                    step="open_typeahead_exact",
                    step_started_at=typeahead_started_at,
                )
                await _emit_search_progress(
                    "tripadvisor_search_listing_opened",
                    step="open_listing",
                    step_started_at=typeahead_started_at,
                )
                await self._emit_progress(
                    progress_callback,
                    {
                        "event": "tripadvisor_search_completed",
                        "source": "tripadvisor",
                        "elapsed_total_s": round(monotonic() - started_at, 3),
                        "page_url": page.url,
                    },
                )
                return

            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_typeahead_miss",
                    "source": "tripadvisor",
                    "query": query,
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                    "debug_state": await self._collect_search_debug_state(
                        search_input_selectors=search_input_selectors,
                        submit_button_selectors=submit_button_selectors,
                    ),
                },
            )

            submit_started_at = monotonic()
            submit_button = await self._find_first_visible(
                submit_button_selectors,
                timeout_ms=6000,
            )
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_submit_ready",
                    "source": "tripadvisor",
                    "query": query,
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                    "debug_state": await self._collect_search_debug_state(
                        search_input_selectors=search_input_selectors,
                        submit_button_selectors=submit_button_selectors,
                    ),
                },
            )
            await self._human_click(submit_button)
            await _emit_search_progress(
                "tripadvisor_search_submitted",
                step="submit_query",
                step_started_at=submit_started_at,
            )

            results_ready_started_at = monotonic()
            await self._wait_after_navigation()
            await self._accept_cookies_if_present()
            await self._dismiss_consent_if_present()
            await self._dismiss_location_prompt_if_present()
            await _emit_search_progress(
                "tripadvisor_search_results_ready",
                step="results_ready",
                step_started_at=results_ready_started_at,
            )
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_results_candidates_detected",
                    "source": "tripadvisor",
                    "query": query,
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                    "debug_state": await self._collect_search_debug_state(
                        search_input_selectors=search_input_selectors,
                        submit_button_selectors=submit_button_selectors,
                    ),
                },
            )

            open_listing_started_at = monotonic()
            await self._open_best_search_result(query)
            await _emit_search_progress(
                "tripadvisor_search_listing_opened",
                step="open_listing",
                step_started_at=open_listing_started_at,
            )
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_completed",
                    "source": "tripadvisor",
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                },
            )
        except Exception as exc:
            debug_artifact_payload = await self._write_search_debug_artifacts(
                query=query,
                error=str(exc),
                debug_state=await self._collect_search_debug_state(
                    search_input_selectors=search_input_selectors,
                    submit_button_selectors=submit_button_selectors,
                ),
            )
            await self._emit_progress(
                progress_callback,
                {
                    "event": "tripadvisor_search_failed",
                    "source": "tripadvisor",
                    "query": query,
                    "error": self._clean_text(str(exc)),
                    "elapsed_total_s": round(monotonic() - started_at, 3),
                    "page_url": page.url,
                    **debug_artifact_payload,
                },
            )
            raise

    async def _go_to_home(self) -> None:
        page = self._require_page()
        await page.goto(self._tripadvisor_url, wait_until="domcontentloaded")
        await self._accept_cookies_if_present()
        await self._dismiss_consent_if_present()
        await self._dismiss_location_prompt_if_present()
        search_input_selectors = (
            "form[role='search'] input[type='search'][name='q']",
            "form[action='/Search'] input[type='search'][name='q']",
            "input[role='searchbox'][name='q']",
            "input[type='search'][name='q'][aria-label*='Buscar' i]",
            "input[type='search'][name='q']",
        )
        open_search_button_selectors = (
            "form[role='search'] button[type='submit'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
            "button[type='submit'][formaction='/Search']",
        )
        try:
            search_input = await self._find_first_optional_visible(
                search_input_selectors,
                timeout_ms=12000,
            )
            if search_input is None:
                open_search_button = await self._find_first_optional_visible(
                    open_search_button_selectors,
                    timeout_ms=3000,
                )
                if open_search_button is not None:
                    try:
                        await self._human_click(open_search_button)
                        await page.wait_for_timeout(self._rng.randint(160, 420))
                    except Exception:
                        pass
                    await self._find_first_optional_visible(
                        search_input_selectors,
                        timeout_ms=4000,
                    )
        except Exception:
            # Do not fail startup here; search stage handles retries/selectors.
            return

    async def _collect_search_debug_state(
        self,
        *,
        search_input_selectors: tuple[str, ...],
        submit_button_selectors: tuple[str, ...],
    ) -> dict[str, Any]:
        page = self._require_page()
        page_title = ""
        try:
            page_title = self._clean_text(await page.title())
        except Exception:
            page_title = ""
        input_state = await self._collect_selector_probe(
            selectors=search_input_selectors,
            max_visible_samples=2,
        )
        submit_state = await self._collect_selector_probe(
            selectors=submit_button_selectors,
            max_visible_samples=2,
        )
        typeahead_state = await self._collect_selector_probe(
            selectors=(
                "#typeahead_results a[role='option'][href]",
                "[data-test-attribute='typeahead-results'] a[role='option'][href]",
                "[role='listbox'] a[role='option'][href]",
                "a[role='option'][href*='_Review-']",
            ),
            max_visible_samples=3,
        )
        results_state = await self._collect_selector_probe(
            selectors=(
                "[data-test-attribute='top-results-card']",
                "[data-test-attribute='location-results-card']",
                "[data-test-attribute$='results-card']",
                "[data-test-attribute*='results-card']",
            ),
            max_visible_samples=3,
        )
        return {
            "page_title": page_title,
            "page_url": self._clean_text(page.url),
            "search_input_state": input_state,
            "submit_button_state": submit_state,
            "typeahead_state": typeahead_state,
            "results_state": results_state,
        }

    async def _collect_selector_probe(
        self,
        *,
        selectors: tuple[str, ...],
        max_visible_samples: int = 2,
    ) -> dict[str, Any]:
        page = self._require_page()
        first_visible_selector = ""
        total_matches = 0
        visible_matches = 0
        visible_samples: list[str] = []

        for selector in selectors:
            try:
                candidates = page.locator(selector)
                total = await candidates.count()
            except Exception:
                continue
            if total <= 0:
                continue
            total_matches += int(total)
            for idx in range(min(total, 8)):
                locator = candidates.nth(idx)
                try:
                    if not await locator.is_visible():
                        continue
                except Exception:
                    continue
                visible_matches += 1
                if not first_visible_selector:
                    first_visible_selector = selector
                if len(visible_samples) < max_visible_samples:
                    text = await self._safe_locator_inner_text(locator)
                    href = await self._safe_locator_attribute(locator, "href")
                    value = await self._safe_locator_attribute(locator, "value")
                    sample_bits = [item for item in (text, href, value) if item]
                    if sample_bits:
                        visible_samples.append(" | ".join(sample_bits[:2]))
            if visible_matches >= max_visible_samples and first_visible_selector:
                break

        return {
            "selectors_checked": list(selectors),
            "total_matches": total_matches,
            "visible_matches": visible_matches,
            "first_visible_selector": first_visible_selector,
            "visible_samples": visible_samples,
        }

    async def _write_search_debug_artifacts(
        self,
        *,
        query: str,
        error: str,
        debug_state: dict[str, Any],
    ) -> dict[str, Any]:
        page = self._require_page()
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        slug = self._normalize_text(query).replace(" ", "_")[:80] or "query"
        artifact_dir = self._project_root / "artifacts" / "tripadvisor_search_debug"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        screenshot_path = artifact_dir / f"{timestamp}_{slug}.png"
        summary_path = artifact_dir / f"{timestamp}_{slug}.json"
        artifact_errors: list[str] = []

        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
        except Exception as exc:
            artifact_errors.append(f"screenshot_failed: {exc}")

        summary_payload = {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "query": query,
            "error": self._clean_text(error),
            "debug_state": debug_state,
            "artifact_errors": artifact_errors,
        }
        try:
            summary_path.write_text(
                json.dumps(summary_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            artifact_errors.append(f"summary_write_failed: {exc}")

        return {
            "debug_state": debug_state,
            "debug_artifacts": {
                "screenshot_path": str(screenshot_path) if screenshot_path.exists() else "",
                "summary_path": str(summary_path),
                "artifact_errors": artifact_errors,
            },
        }
