# Scrapers

This document explains one thing only:

how to read the real scraping pipeline in this repository without getting lost.

## Main rule

If you want to understand the scraper flow for a source, **do not start with** `browser_scraper.py`.

Start with:

- `src/scraping_google_maps/google_maps_business_page_scraper.py`
- `src/scraping_tripadvisor/tripadvisor_business_page_scraper.py`

Those files are the high-level orchestration entrypoints.

The `browser_scraper.py` files are composition roots. They assemble browser capabilities, but they do not tell the full operational story by themselves.

## Pipeline map at a glance

The real path is:

`job runtime -> source adapter -> business service / business catalog -> business page scraper -> browser scraper -> facet modules`

More concretely:

1. A browser-driven job is claimed by the local browser runtime.
2. The source adapter translates the job payload into a source-specific scrape call.
3. The business-facing orchestration layer delegates to the source business-page scraper.
4. The business-page scraper runs the high-level pipeline stages.
5. The source `browser_scraper.py` provides browser primitives.
6. Facet modules implement each concrete browser behavior.

## Runtime context

All real browser execution is designed to happen on the **Linux host machine**.

Important runtime pieces:

- `src/browser_runtime/local_browser_runtime_worker.py`
- `src/browser_runtime/browser_job_live_display_runtime.py`
- `scripts/tripadvisor_local_worker_bridge.py`
- `scripts/tripadvisor_ctl.sh`

Runtime flags:

- `execution_mode`
  - `automatic`
    - background / headless execution
  - `live`
    - headed execution

- `live_display_mode`
  - only used when `execution_mode=live`

Live display modes:

- `native`
  - visible on the real display
- `xvfb`
  - headed browser in a virtual X display

This matters especially for Tripadvisor, where the real scrape path is currently tied to the live replay flow.

## Google Maps

### Where to read the pipeline

Primary file:

- `src/scraping_google_maps/google_maps_business_page_scraper.py`

Main sequence:

1. resolve effective scrape limits and strategy
2. emit `scraper_starting`
3. `self._scraper.start()`
4. `self._scraper.search_business(...)`
5. `self._scraper.extract_listing()`
6. `self._scraper.extract_reviews(...)`
7. `self._scraper.close()` in `finally`

In short:

`start -> search -> listing -> reviews -> close`

### Google review collection strategies

The review extractor supports two modes:

- `scroll_copy`
  - recommended mode
  - scrolls the review feed, captures HTML, and parses from the captured feed
- `interactive`
  - legacy mode
  - relies more directly on interactive DOM traversal

In practice, Google usually runs best with `scroll_copy`.

Operational runtime note:

- Google can run in `automatic` or `live`
- in code today, `automatic` means the local browser runtime flips the scraper into headless mode
- `live` means headed mode, and then `native` vs `xvfb` decides where that headed browser lives

### Meaning of the important Google tuning fields

- `html_scroll_max_rounds`
  - hard cap on feed scroll rounds
- `html_stable_rounds`
  - number of consecutive non-growing rounds before the feed is considered stable and the scroll loop stops
- `interactive_max_rounds`
  - max rounds for the legacy `interactive` strategy only

Operational recommendation:

- keep `scroll_copy`
- change `html_scroll_max_rounds` when you need more depth
- leave `html_stable_rounds` near default unless the feed stops too early or wastes too many empty loops

### Where each concern lives

Browser root:

- `src/scraping_google_maps/browser_scraper.py`

Important facets:

- `browser_lifecycle_facet.py`
  - open / close browser, context, and page
- `browser_navigation_facet.py`
  - search and initial navigation in Maps
- `browser_listing_facet.py`
  - business listing extraction
- `browser_reviews_open_facet.py`
  - opening the review panel
- `browser_reviews_feed_facet.py`
  - feed control and scroll behavior
- `browser_reviews_collection_facet.py`
  - review extraction orchestration
- `browser_review_card_facet.py`
  - parsing one review card
- `browser_parsing_facet.py`
  - text / parsing helpers

### What not to read first

- `src/scraping_google_maps/google_maps_browser_adapter.py`

That file is not the browser pipeline. It is only the runtime bridge from job payload to business orchestration.

## Tripadvisor

### Where to read the pipeline

Primary file:

- `src/scraping_tripadvisor/tripadvisor_business_page_scraper.py`

Main sequence:

1. validate that the Tripadvisor session is available
2. build the runtime limits and per-stage timing constraints
3. emit `scraper_starting`
4. execute `start`
5. optionally wait for `start_delay`
6. execute `search`
7. execute `listing`
8. execute `reviews`
9. close browser in `finally`
10. persist diagnostics and classify antibot / needs-human conditions if the run fails

In short:

`session check -> start -> optional delay -> search -> listing -> reviews -> diagnostics / close`

### Operational difference vs Google

Tripadvisor has a denser orchestration layer because it must also manage:

- session validation
- antibot detection
- needs-human classification
- replay and live capture support
- diagnostics persistence

That is why `tripadvisor_business_page_scraper.py` is expected to be more operationally dense than the Google equivalent.

Runtime note:

- Tripadvisor scrape jobs are currently forced into `execution_mode=live`
- so for Tripadvisor the practical operator choice is not `automatic` vs `live`
- it is `native` vs `xvfb` for the live replay / needs-human path

### Tripadvisor review collection model

Primary review orchestration lives in:

- `src/scraping_tripadvisor/browser_scraper_facets/browser_reviews_orchestration_facet.py`

The current model is page-based rather than a Google-style feed stabilization loop.

Important controls:

- `max_pages`
  - hard cap on how many review pages to traverse
- `max_pages_percent`
  - relative cap derived from known page count
- `max_duration_seconds`
  - soft overall time bound for the review collection phase

### GraphQL support

There is also source-specific review GraphQL support in:

- `src/scraping_tripadvisor/browser_scraper_facets/browser_reviews_graphql_facet.py`

This is part of the Tripadvisor scraping context and should be read as a supporting extraction path, not as the primary high-level business pipeline entrypoint.

### Where each concern lives

Browser root:

- `src/scraping_tripadvisor/browser_scraper.py`

Important facets:

- `browser_lifecycle_facet.py`
  - open / close browser
- `browser_search_entry_facet.py`
  - enter and submit the search
- `browser_search_typeahead_facet.py`
  - exact-match attempt from typeahead
- `browser_search_results_facet.py`
  - open the best search result
- `browser_search_matching_facet.py`
  - result-title / href matching heuristics
- `browser_listing_facet.py`
  - listing extraction
- `browser_reviews_orchestration_facet.py`
  - top-level review capture flow
- `browser_reviews_page_collection_facet.py`
  - collect reviews inside the current page
- `browser_reviews_navigation_facet.py`
  - go to next reviews page
- `browser_reviews_pagination_state_facet.py`
  - pagination snapshots
- `browser_reviews_graphql_facet.py`
  - GraphQL review capture helpers
- `browser_review_dom_facet.py`
  - DOM review extraction
- `browser_review_owner_reply_facet.py`
  - owner-reply extraction
- `browser_review_identity_facet.py`
  - review identity and lightweight parsing
- `browser_page_support_facet.py`
  - cookies, prompts, consent, waits
- `browser_text_facet.py`
  - text helpers

### Tripadvisor needs-human and live replay

The live operational flow is split across:

- `scripts/tripadvisor_local_worker_bridge.py`
- `scripts/tripadvisor_ctl.sh`
- manager UI job drawer

This flow supports:

- opening a needs-human session for a specific job
- running it in `native` or `xvfb`
- reading live-session state
- reading live-session log tail
- stopping the live session from the manager

So if you are debugging a Tripadvisor human-assisted scrape, the scraper code alone is not enough. You also need to read the bridge and the manager-side control flow.

## Browser adapters

Files:

- `src/scraping_google_maps/google_maps_browser_adapter.py`
- `src/scraping_tripadvisor/tripadvisor_browser_adapter.py`

Real responsibility:

- they do not contain browser scraping logic
- they do not describe the browser pipeline
- they only translate `AnalyzeBusinessTaskPayload` into the business-facing scrape call

Think of them as runtime adapters, not scraper cores.

## What to edit depending on what you want to change

### Change the stage sequence

Edit:

- `google_maps_business_page_scraper.py`
- `tripadvisor_business_page_scraper.py`

### Change how a business is searched

Edit:

- Google Maps: `browser_navigation_facet.py`
- Tripadvisor:
  - `browser_search_entry_facet.py`
  - `browser_search_typeahead_facet.py`
  - `browser_search_results_facet.py`
  - `browser_search_matching_facet.py`

### Change how the review area is opened or traversed

Edit:

- Google Maps:
  - `browser_reviews_open_facet.py`
  - `browser_reviews_feed_facet.py`
  - `browser_reviews_collection_facet.py`
- Tripadvisor:
  - `browser_reviews_orchestration_facet.py`
  - `browser_reviews_page_collection_facet.py`
  - `browser_reviews_navigation_facet.py`
  - `browser_reviews_pagination_state_facet.py`

### Change review parsing

Edit:

- Google Maps:
  - `browser_review_card_facet.py`
- Tripadvisor:
  - `browser_review_dom_facet.py`
  - `browser_review_owner_reply_facet.py`
  - `browser_review_identity_facet.py`

### Change needs-human / live operational behavior

Edit:

- `scripts/tripadvisor_local_worker_bridge.py`
- `scripts/tripadvisor_ctl.sh`
- `src/browser_runtime/browser_job_live_display_runtime.py`
- manager UI job controls

## Short summary

If you want the scraper pipeline story, read the `*business_page_scraper.py` files.

If you want the browser implementation details, read `browser_scraper.py` and the facet modules.

If you want the runtime and live-control behavior, read the source adapters, local browser runtime, Tripadvisor bridge, and manager UI together.
