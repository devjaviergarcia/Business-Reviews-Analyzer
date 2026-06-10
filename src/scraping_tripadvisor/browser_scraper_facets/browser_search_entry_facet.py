from __future__ import annotations

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
        started_at = monotonic()
        direct_listing_url = self._resolve_direct_listing_target_url(query)

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

        submit_started_at = monotonic()
        submit_button = await self._find_first_visible(
            (
                "form[role='search'] button[type='submit'][aria-label*='Buscar' i]",
                "div.bOfFT button[type='submit'][aria-label*='Buscar' i]",
                "button[type='submit'][formaction='/Search'][aria-label*='Buscar' i]",
                "button[type='submit'][title='Buscar'][aria-label*='Buscar' i]",
                "button[type='submit'][formaction='/Search']",
                "form[role='search'] button[type='submit']",
            ),
            timeout_ms=6000,
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
