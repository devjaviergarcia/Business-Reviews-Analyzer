# Repiq Documentation

This is the main documentation entrypoint for the repository.

Use the root [`README.md`](../README.md) for quick setup.
Use this document for the longer architectural map.
Use [`SCRAPERS.md`](../SCRAPERS.md) when you want to understand the scraper pipelines specifically.

## What the project does

Repiq is not just a review scraper.

It is a local-first system for:

- scraping public local-business data from Google Maps and Tripadvisor
- normalizing business profiles and reviews
- analyzing reputation with LLM-assisted pipelines
- generating lead reports, paid reports, and public studies
- running discovery, benchmarking, geogrids, and CRM workflows

## Current architecture in one sentence

Mongo stores the operational state, FastAPI exposes the application surface, workers process queued jobs, and browser-driven scraping runs on the local machine through an explicit local browser runtime.

## Operating model

The project is designed for Linux-based development and operations.

Practical assumptions:

- API and Mongo can run locally or in Docker
- real Playwright execution runs on the host machine
- browser-driven jobs can run in `automatic` or `live` mode
- the manager UI is the local operational panel

## Recommended reading order

1. `src/main.py`
2. `src/platform/application_root.py`
3. `src/dependencies.py`
4. `src/routers/`
5. `src/job_runtime/`
6. `src/browser_runtime/`
7. `src/business_catalog/`
8. `src/crm/`
9. `src/scraping_google_maps/` and `src/scraping_tripadvisor/`
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

This is the actual composition root of the project. It wires repositories, services, use cases, runtimes, and adapters.

### `src/dependencies.py`

Thin bridge between FastAPI and the application root.

Responsibilities:

- resolve dependencies for routers and some workers
- expose explicit `create_*` functions backed by the root wiring

### `src/job_runtime/`

Shared job contracts for browser-driven work.

Key responsibilities:

- define job metadata such as `source`, `execution_mode`, `runtime_target`, and `fallback_policy`
- identify which jobs belong to the local browser runtime
- coordinate job claiming for the local runtime worker

### `src/browser_runtime/`

Local browser runtime.

Key responsibilities:

- heartbeat for the local worker
- claim browser-driven jobs
- execute Google Maps or Tripadvisor scraping locally
- support `automatic` and `live` execution
- report progress and status back into Mongo

This is the key piece that keeps Playwright execution on the machine instead of inside a container.

### `src/scraping_google_maps/`

Real Google Maps scraping implementation.

Key files:

- `browser_scraper.py`
- `browser_scraper_facets/*`
- `google_maps_business_page_scraper.py`
- `google_maps_browser_adapter.py`
- `selectors.py`

Current structure:

- `browser_scraper.py` acts as the scraper composition root
- detailed logic is split into facets for lifecycle, navigation, listing extraction, review extraction, and parsing

### `src/scraping_tripadvisor/`

Real Tripadvisor scraping implementation.

Key files:

- `browser_scraper.py`
- `browser_scraper_facets/*`
- `browser_scraper_types.py`
- `tripadvisor_business_page_scraper.py`
- `tripadvisor_browser_adapter.py`
- `tripadvisor_scrape_diagnostics.py`

Current structure:

- `browser_scraper.py` is the root
- inner logic is split into explicit subcontexts such as search submission, result matching, listing opening, review collection, pagination state, DOM extraction, owner replies, and review parsing

### `src/scraping_shared/`

Shared browser-scraping abstractions.

Responsibilities:

- common browser scrape adapter contract
- shared scrape error taxonomy

### `src/scraper/`

Compatibility layer, not the primary implementation.

Today it mainly contains wrappers that re-export the new scrapers for older scripts or tests.

### `src/business_catalog/`

Business catalog and persisted scraping pipeline.

Responsibilities:

- orchestrate scraping by source
- persist business profiles, reviews, and datasets
- create scrape snapshots and runs
- enqueue and relaunch browser scrape jobs

### `src/crm/`

CRM context, already split into business-facing subcontexts.

Main subpackages:

- `leads/`
- `report_requests/`
- `studies/`
- `campaigns/`
- `discovery/`
- `repositories/`
- `benchmark/`

### `src/services/business_service.py`

Business analysis facade.

It still matters, but it is no longer a single giant mixed implementation. It now behaves more like a visible composition root over more specific components.

### `src/services/crm_service.py`

CRM facade.

Same story: still important, but much more traceable than before because responsibilities have been pushed into contextual modules.

### `src/services/business_query_service.py`

Read/query context.

Responsibilities:

- read businesses, reviews, analyses, jobs, and artifacts
- serve query-oriented routes and manager views

### `src/services/analysis_job_service.py`

Analysis job context.

Responsibilities:

- enqueue analysis jobs
- persist progress, events, and status
- support worker execution

### `src/pipeline/`

Analysis and report-payload assembly.

Key responsibilities:

- preprocess reviews
- run LLM analysis
- assemble structured report payloads
- prepare data for rendering

### `src/pipeline/report_rendering/`

Final report rendering context.

Responsibilities:

- generate HTML, PDF, annexes, and CSVs
- separate layout, sections, charts, and export logic

### `src/routers/`

HTTP boundary.

Responsibilities:

- validate request and response payloads
- invoke use cases or composed services
- map domain or runtime errors into HTTP responses

### `src/workers/`

Traditional queue workers.

Responsibilities:

- process analysis, report, CRM, and scrape jobs
- coexist with the newer local browser runtime model

### `apps/manager/`

Local operations UI.

Responsibilities:

- inspect jobs
- inspect CRM state
- launch pipelines and studies
- operate the system manually

### `scripts/`

Operational tooling and lab entrypoints.

These scripts are useful, but they are not the domain model. They are wrappers around operations, debugging, and local execution.

## Main flows

### 1. Business scraping and analysis flow

1. A scrape job is created or relaunched.
2. The job is persisted in Mongo.
3. If it is browser-driven and targets `local_browser`, the local runtime claims it.
4. The Google Maps or Tripadvisor adapter runs Playwright locally.
5. `business_catalog` persists listings, reviews, datasets, and scrape runs.
6. The pipeline preprocesses and analyzes the content.
7. The report renderer generates the final artifacts.

### 2. `automatic` vs `live`

#### `automatic`

- default mode
- silent execution
- intended for throughput and retryability

#### `live`

- explicit opt-in
- visible browser session
- intended for antibot handling, manual validation, or human intervention

### 3. CRM lead discovery flow

1. A discovery run is created.
2. A CRM discovery job is queued.
3. If it needs a real browser, the local runtime claims it.
4. Google Maps discovery extracts candidate businesses.
5. Leads, steps, and events are persisted.
6. The CRM pipeline may continue into benchmark or campaign flows.

### 4. Report request flow

1. A report request enters the system.
2. The request and its context are stored.
3. Benchmark or geogrid work is triggered if needed.
4. The system generates a lead report, paid report, or public study.
5. Feedback can be stored afterwards.

### 5. Geogrid flow

1. A geogrid run is created.
2. The city, grid, and provider mode are resolved.
3. The study runs.
4. Ranking points and aggregate stats are calculated.
5. The result feeds reporting or a public study.

### 6. CRM campaign flow

1. A campaign is created.
2. Eligible leads are selected.
3. Dispatch jobs are queued.
4. Messages or emails are sent.
5. Resend events and webhooks come back.
6. Campaign state, messages, and events are persisted.

## What is actively in use

These are the current live architectural pieces:

- `src/platform/application_root.py`
- `src/dependencies.py`
- `src/main.py`
- `src/browser_runtime/*`
- `src/job_runtime/*`
- `src/scraping_google_maps/*`
- `src/scraping_tripadvisor/*`
- `src/scraping_shared/*`
- `src/business_catalog/*`
- `src/crm/*`
- `src/pipeline/*`
- `src/pipeline/report_rendering/*`
- `src/services/analysis_job_service.py`
- `src/services/business_service.py`
- `src/services/crm_service.py`
- `src/services/business_query_service.py`
- `src/routers/*`
- `src/workers/*` except the RabbitMQ placeholder broker

## Compatibility and transitional code

Still used, but not the target architecture:

- `src/scraper/*`
- `src/business_catalog/legacy_review_dataset_packager.py`
- `src/crm/*/legacy_*.py`
- `src/services/tripadvisor_local_worker_control_service.py`
- `scripts/tripadvisor_local_worker_bridge.py`

## Known heavy files still worth splitting further

The biggest remaining candidates are:

- `src/services/analysis_job_service.py`
- `src/crm/repositories/mongo.py`
- `src/services/business_query_service.py`
- `src/business_catalog/business_scrape_pipeline_runner.py`
- `src/crm/discovery/google_maps_live_discovery_runtime.py`
- `src/platform/application_root.py`
- `src/workers/scraper_worker.py`
- `src/workers/contracts.py`
- `src/routers/business.py`
- `src/routers/crm.py`
- `src/scraping_tripadvisor/tripadvisor_scrape_diagnostics.py`

## What is runtime noise, not architecture

These paths should not be mentally treated as architecture:

- `artifacts/`
- `playwright-data*/`
- `.venv-uv/`
- `__pycache__/`
- `.pytest_cache/`
- `apps/manager/node_modules/`
- `apps/manager/dist/`

Important nuance:

- `playwright-data*/` may contain useful session/runtime state
- `artifacts/` may contain useful report output
- so they should not be deleted blindly even if they are not part of the code architecture

## Other documentation areas

- `docs/backlogs/`: planning, tickets, and epics
- `docs/context/`: context notes and supporting material
- `docs/editorial/`: editorial assets and publication notes
- `docs/ops/`: manual operating checklists
- `docs/process/`: internal process notes
- `docs/product/`: product-specific notes
- `docs/seo/`: SEO notes and research material

## Practical maintenance rules

1. If it is real domain or runtime logic, it belongs in `src/` with a contextual name.
2. If it exists only for compatibility, it should say `legacy` or clearly read as a wrapper.
3. If it is operational glue or debugging support, it belongs in `scripts/`.
4. If it is local output or runtime state, keep it out of the architecture mental model.
5. If multiple docs disagree, this file is the long-form architectural source of truth.
