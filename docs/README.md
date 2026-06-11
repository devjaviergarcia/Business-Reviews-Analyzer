# Repiq Documentation

This is the long-form documentation entrypoint for the repository.

Use:

- [`README.md`](../README.md) for setup, launch, and day-one operation
- [`SCRAPERS.md`](../SCRAPERS.md) for scraper pipeline reading
- this file for the broader architectural map

## What the system does

Repiq is not only a review scraper.

It is a local-first operational system for:

- scraping public local-business data from Google Maps and Tripadvisor
- normalizing business profiles, listings, and reviews
- running LLM-assisted analysis pipelines
- generating lead reports, advanced reports, public studies, and benchmark outputs
- supporting CRM, report-request, and geogrid workflows

## Architecture in one sentence

Mongo stores operational state, FastAPI exposes the application surface, classic workers process queued jobs, and browser-driven jobs are executed on the Linux host through an explicit local browser runtime.

## Operating model

The current operating model is:

- Linux host machine is the canonical runtime for browser automation
- API and Mongo can run locally or in Docker
- browser-driven jobs target the `local_browser` runtime
- the manager UI is the operational control surface
- Tripadvisor live / needs-human sessions are mediated by a local bridge

## Execution modes

Browser-driven jobs currently use two runtime flags:

- `execution_mode`
  - `automatic`
    - background / headless execution
    - currently relevant mainly for Google Maps
  - `live`
    - headed execution
    - used for visible or operator-assisted capture

- `live_display_mode`
  - only meaningful when `execution_mode=live`

Live jobs also support a display mode:

- `native`
  - visible browser on the real display
- `xvfb`
  - headed browser inside a virtual display

Important source-specific note:

- Google Maps supports both execution modes
- Tripadvisor scrape jobs are currently forced into the `live` / replay-headfull path, so the practical choice there is `native` vs `xvfb`

## Recommended reading order

1. `src/main.py`
2. `src/platform/application_root.py`
3. `src/routers/`
4. `src/job_runtime/`
5. `src/browser_runtime/`
6. `src/scraping_google_maps/`
7. `src/scraping_tripadvisor/`
8. `src/business_catalog/`
9. `src/crm/`
10. `src/pipeline/` and `src/pipeline/report_rendering/`
11. `scripts/`

## Top-level code map

### `src/main.py`

FastAPI entrypoint.

Responsibilities:

- create the app
- open and close Mongo
- register routers

### `src/platform/`

Infrastructure-level platform wiring.

Most important file:

- `src/platform/application_root.py`

This is the composition root. It wires repositories, services, use cases, runtimes, and adapters explicitly.

### `src/dependencies.py`

FastAPI bridge layer.

Responsibilities:

- expose dependency helpers for routers
- forward resolution to the application root

### `src/job_runtime/`

Shared job contracts and routing metadata.

Responsibilities:

- define job metadata such as `source`, `execution_mode`, `runtime_target`, and `fallback_policy`
- represent browser-driven work consistently
- support local browser job claiming

### `src/browser_runtime/`

Local browser execution runtime.

Responsibilities:

- heartbeat for the local runtime worker
- claim browser-driven jobs
- execute source adapters locally
- support `automatic`, `live native`, and `live xvfb`
- persist progress and state back into Mongo

This is the boundary that keeps real Playwright execution on the host machine instead of inside a container.

### `src/scraping_google_maps/`

Google Maps scraping context.

Key files:

- `browser_scraper.py`
- `browser_scraper_facets/*`
- `google_maps_business_page_scraper.py`
- `google_maps_browser_adapter.py`

Reading rule:

- `google_maps_business_page_scraper.py` tells the pipeline story
- `browser_scraper.py` assembles the browser primitives
- facets implement the detailed browser behavior

### `src/scraping_tripadvisor/`

Tripadvisor scraping context.

Key files:

- `browser_scraper.py`
- `browser_scraper_facets/*`
- `tripadvisor_business_page_scraper.py`
- `tripadvisor_browser_adapter.py`
- `tripadvisor_scrape_diagnostics.py`

Reading rule:

- `tripadvisor_business_page_scraper.py` tells the orchestration story
- `browser_scraper.py` assembles the browser primitives
- facets implement search, listing, review collection, pagination, parsing, and diagnostics support

### `src/scraping_shared/`

Shared browser scraping abstractions.

Responsibilities:

- common browser scrape adapter contract
- shared scrape error taxonomy

### `src/scraper/`

Compatibility layer.

This is not the primary implementation anymore. It mostly exists to support older import paths and transitional wrappers.

### `src/business_catalog/`

Business scraping and persistence context.

Responsibilities:

- orchestrate scraping by source
- enqueue and relaunch browser-driven scrape jobs
- persist businesses, source profiles, datasets, reviews, and scrape runs
- hand off to analysis and reporting

### `src/crm/`

CRM context.

Main subcontexts:

- `leads/`
- `report_requests/`
- `studies/`
- `campaigns/`
- `discovery/`
- `repositories/`
- `benchmark/`

### `src/services/business_service.py`

Business-facing facade over the business catalog and analysis flow.

It still matters as a visible entrypoint, but responsibilities have been moved into more contextual modules.

### `src/services/crm_service.py`

CRM-facing facade.

Same idea: still an entrypoint, but backed by clearer contextual modules than before.

### `src/services/business_query_service.py`

Read/query side for manager and API use cases.

Responsibilities:

- fetch businesses, reviews, analyses, jobs, and report artifacts
- serve query-oriented routes and manager views

### `src/services/analysis_job_service.py`

Analysis job orchestration.

Responsibilities:

- enqueue analysis jobs
- persist job state and progress
- support worker execution and relaunch logic

### `src/pipeline/`

Analysis payload preparation.

Responsibilities:

- preprocess reviews
- run LLM analysis
- assemble structured report payloads

### `src/pipeline/report_rendering/`

Final report rendering.

Responsibilities:

- generate HTML, PDF, annexes, and CSV outputs
- split rendering concerns into sections, charts, layout, and exports

### `src/routers/`

HTTP boundary.

Responsibilities:

- validate requests and responses
- invoke use cases or facades
- map runtime and domain errors to HTTP

### `src/workers/`

Classic queue workers.

Responsibilities:

- process analysis, report, CRM, and scrape jobs
- coexist with the newer local browser runtime model

### `apps/manager/`

Local operational UI.

Responsibilities:

- launch scrape and analysis flows
- inspect pipeline state
- operate Tripadvisor needs-human flows
- read Tripadvisor live-session state and log tail
- inspect CRM state and operational progress

### `scripts/`

Operational entrypoints and support tooling.

Important scripts:

- `scripts/local_workers.sh`
- `scripts/run_local_browser_runtime_worker.py`
- `scripts/tripadvisor_local_worker_bridge.py`
- `scripts/tripadvisor_ctl.sh`

## Main flows

### 1. Browser-driven scrape flow

1. A scrape job is created or relaunched.
2. The job is stored in Mongo with `runtime_target=local_browser`.
3. The local browser runtime claims the job.
4. The source adapter runs on the Linux host.
5. `business_catalog` persists listings, reviews, datasets, and scrape runs.
6. The flow hands off to analysis and then reporting if required.

### 2. Tripadvisor needs-human flow

1. A Tripadvisor scrape reaches a blocked or human-required state.
2. The operator opens the needs-human session from the manager UI.
3. The local Tripadvisor bridge launches a live session in `native` or `xvfb` mode.
4. The manager UI can read the live-session status and log tail.
5. Once the session is resolved, the job can continue or be replayed.

### 3. CRM lead and report-request flow

1. A lead or report request enters the system.
2. The CRM context persists the request state.
3. The queue / pull workers process pending work.
4. Benchmark, scraping, analysis, or reporting can be triggered from that state.

### 4. Geogrid flow

1. A geogrid run is created.
2. The city and grid points are resolved.
3. Ranking capture runs for the selected provider mode.
4. Results are aggregated into visibility metrics and can feed studies or reports.

## Additional entrypoints

- [`docs/context/README.md`](context/README.md)
- [`docs/context/architecture/README.md`](context/architecture/README.md)
- [`docs/process/task_workflow.md`](process/task_workflow.md)
