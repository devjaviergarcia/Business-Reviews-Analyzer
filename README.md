# Business Review Analyzer

Google Maps reviews scraper and LLM-oriented processing pipeline.

This repository is the Phase 1 foundation for an end-to-end workflow:

`Google Maps Scraping -> MongoDB -> Preprocessing -> LLM Analysis -> FastAPI`

## Current Status

- Implemented:
  - Google Maps scraper with resilient selectors and human-like interaction pacing.
  - Listing extraction (name, address, phone, website, rating, categories).
  - Reviews extraction pipeline (with owner-reply support when available).
  - MongoDB connection lifecycle.
  - Health endpoint: `GET /health`.
  - End-to-end business analysis endpoint with persistence in MongoDB.
  - Read endpoints for businesses, reviews, jobs, and analysis (page/page_size pagination).

## Tech Stack

- Python + FastAPI
- Playwright (browser automation)
- MongoDB (Motor/PyMongo)
- Gemini API (`google-genai`)
- `uv` for Python dependency and environment management
- Docker Compose for local orchestration

## Project Structure

```text
src/
  main.py
  config.py
  database.py
  models/
  routers/
  scraper/
  pipeline/
  services/
scripts/
  bootstrap_google_maps_login.py
  incognito_scroll_all_reviews.py
  manual_chromium_session.py
  smoke_test_google_maps_incognito.py
  smoke_test_gemini_flash.py
  smoke_test_google_maps_search.py
docs/
tests/
```

## Quickstart

### 1) Create environment and install dependencies

```bash
uv venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

uv pip install -r requirements.txt
```

### 2) Configure environment

```bash
copy .env.example .env
```

Important scraper vars:

- `SCRAPER_USER_DATA_DIR`: persistent Playwright profile directory.
- `SCRAPER_BROWSER_CHANNEL`: optional browser channel (`msedge`, `chrome`, empty = bundled Chromium).
- `SCRAPER_INCOGNITO`: use non-persistent incognito context in official scraper flow.
- `SCRAPER_MIN_CLICK_DELAY_MS` / `SCRAPER_MAX_CLICK_DELAY_MS`: click spacing.
- `SCRAPER_MIN_KEY_DELAY_MS` / `SCRAPER_MAX_KEY_DELAY_MS`: per-key typing delay.
- `SCRAPER_STEALTH_MODE`: injects anti-detection JS patches (`navigator.webdriver`, etc.).
- `SCRAPER_HARDEN_HEADLESS`: applies hardened Chromium args when `SCRAPER_HEADLESS=true`.
- `SCRAPER_EXTRA_CHROMIUM_ARGS`: extra comma-separated Chromium launch args.
- `SCRAPER_REVIEWS_STRATEGY`: `interactive` or `scroll_copy`.
  - `POST /business/analyze` defaults to `scroll_copy`, and can be overridden per request with payload `strategy`.
- `SCRAPER_INTERACTIVE_MAX_ROUNDS`: rounds for interactive extraction.
- `SCRAPER_HTML_SCROLL_MAX_ROUNDS` / `SCRAPER_HTML_STABLE_ROUNDS`: limits for `scroll_copy` (`0` rounds means auto-scroll until end with a high safety cap).
- `SCRAPER_HTML_SCROLL_MIN_INTERVAL_S` / `SCRAPER_HTML_SCROLL_MAX_INTERVAL_S`: random wait range between scroll iterations in `scroll_copy` (supports decimals, e.g. `1.0` to `2.0`).
- `SCRAPER_TRIPADVISOR_STAGE_TIMEOUT_SECONDS`: max seconds per Tripadvisor stage (`start`, `search`, `listing`, `reviews`). On timeout, the job fails and stores a diagnostic in Mongo (`scrape_diagnostics`) with page HTML snapshot and `bot` keyword snippets.
- `ANALYSIS_REANALYZE_DEFAULT_BATCHERS`: batchers used when reanalyzing saved reviews.
- `ANALYSIS_REANALYZE_BATCH_SIZE`: reviews per batch for reanalysis.
- `ANALYSIS_REANALYZE_POOL_SIZE`: max stored reviews loaded for reanalysis.

Important worker vars:

- `WORKER_POLL_SECONDS`: polling interval used by queue workers.
- `WORKER_IDLE_LOG_SECONDS`: idle heartbeat interval in worker logs when no jobs are found.
- `WORKER_BROKER_BACKEND`: queue broker backend for workers (`mongo` by default). `rabbitmq` is currently deferred and not enabled.

Important LLM vars:

- `GEMINI_API_KEY`: API key for Gemini.
- `GEMINI_MODEL`: preferred model (default `gemini-1.5-flash`; automatic fallback to available Flash models).

### 3) Install Playwright browser

If you use bundled Chromium:

```bash
uv run playwright install chromium
```

If you use Chrome channel:

```bash
uv run playwright install chrome
```

### 4) Run API

Local:

```bash
uv run uvicorn src.main:app --host 0.0.0.0 --port 8000
```

Docker/dev:

```bash
docker compose up --build
```

Docker/dev with optional scraper worker:

```bash
docker compose --profile worker up --build
```

Worker profile now includes:
- `scraper-google-worker` (Google Maps scrape stage)
- `analysis-worker` (analysis stage)
- `report-worker` (PDF/HTML/JSON report generation stage)

Operational mode used in this project:
- TripAdvisor worker runs on host (local, with xvfb), not in Docker.

### Centralized Operations (No aliases required)

Use:

```bash
./scripts/tripadvisor_ctl.sh <command>
```

Quick command reference (copy/paste):

```bash
# Show help
./scripts/tripadvisor_ctl.sh help

# Start all required services (Mongo + API + Google worker + Analysis worker + Report worker)
# and start TripAdvisor worker locally on host (detached, xvfb)
./scripts/tripadvisor_ctl.sh up

# Same as above but forcing rebuild of Docker images
./scripts/tripadvisor_ctl.sh up --build
# or
./scripts/tripadvisor_ctl.sh rebuild

# Show status: Docker services, local TA worker, API health, TA session state, recent jobs
./scripts/tripadvisor_ctl.sh status

# Follow logs (choose target)
./scripts/tripadvisor_ctl.sh logs local-ta 200
./scripts/tripadvisor_ctl.sh logs app 200
./scripts/tripadvisor_ctl.sh logs google 200
./scripts/tripadvisor_ctl.sh logs analysis 200
./scripts/tripadvisor_ctl.sh logs report 200

# Queue one scrape with different names per source
./scripts/tripadvisor_ctl.sh scrape \
  --google "Gamberra Smash burger" \
  --tripadvisor "Gamberra Burger & Chicken" \
  --max-pages 2 \
  --pages-percent 1 \
  --force true

# Inspect one job
./scripts/tripadvisor_ctl.sh job <job_id>

# Trace one job in real time (SSE)
./scripts/tripadvisor_ctl.sh trace <job_id> 0

# Fetch latest structured report for a business (includes intro context + artifacts paths)
curl -fsS "http://localhost:8000/business/<business_id>/report" | jq .

# Relaunch one job (auto-detect scrape/analyze)
./scripts/tripadvisor_ctl.sh relaunch <job_id>
# Force relaunch even if the original job is active (creates a new queued clone)
./scripts/tripadvisor_ctl.sh relaunch <job_id> --force
# Relaunch from scratch (force rescrape + strict_rescrape, overwrites the previous scrape for that job/source)
./scripts/tripadvisor_ctl.sh relaunch <job_id> --from-zero

# Human recovery flow (automatic confirm + validation)
./scripts/tripadvisor_ctl.sh human

# Optional: manual confirm endpoint (normally not needed because `human` already does it)
./scripts/tripadvisor_ctl.sh session-confirm playwright-data-tripadvisor-worker-docker true
# same without relaunch:
./scripts/tripadvisor_ctl.sh session-confirm playwright-data-tripadvisor-worker-docker false

# Stop services (keeps Mongo by default)
./scripts/tripadvisor_ctl.sh down

# Stop services including Mongo
./scripts/tripadvisor_ctl.sh down --with-mongo
```

Recommended day-to-day flow:

```bash
./scripts/tripadvisor_ctl.sh up --build
./scripts/tripadvisor_ctl.sh status
./scripts/tripadvisor_ctl.sh scrape --google "<name_google>" --tripadvisor "<name_tripadvisor>"
./scripts/tripadvisor_ctl.sh trace <job_id> 0
```

If a TripAdvisor job goes to `needs_human`:

```bash
./scripts/tripadvisor_ctl.sh human
./scripts/tripadvisor_ctl.sh relaunch <job_id>
# if UI/API says the job is already active:
./scripts/tripadvisor_ctl.sh relaunch <job_id> --force
# if you want to force a full rescrape (no cache hit / strict rescrape):
./scripts/tripadvisor_ctl.sh relaunch <job_id> --from-zero
./scripts/tripadvisor_ctl.sh trace <job_id> 0
```

Notes:
- `relaunch` now refuses TripAdvisor relaunch if session is not available (`availability_now=false`) and prints the recovery flow.
- Operational mode in this project is: TripAdvisor worker local on host, not Docker.

Infra-only (Mongo in Docker, API local):

```powershell
.\scripts\dev_infra.ps1 up
# then
.\scripts\run_local_api.ps1
```

API docs:

- Swagger UI: `http://localhost:8000/docs`

### 4.1) Local manager UI (TypeScript, fastest setup)

```bash
cd apps/manager
npm install
npm run dev
```

- Default UI URL: `http://localhost:5173`
- Set API base in the UI header (default: `http://localhost:8000`)
- Features:
  - queue analysis jobs
  - search businesses
  - list jobs
  - inspect job detail
  - live SSE event stream per job

### 5) Local vs Docker Profile

- Local profile (recommended for login/manual checks): `SCRAPER_HEADLESS=false`
- Incognito in official API flow (no persisted login cookies): `SCRAPER_INCOGNITO=true`
- Docker/dev profile (recommended for server stability): `SCRAPER_HEADLESS_DOCKER=true`
- Docker uses its own Playwright profile dir by default: `SCRAPER_USER_DATA_DIR_DOCKER=playwright-data-docker`
- Google worker profile dir by default: `SCRAPER_USER_DATA_DIR_GOOGLE_WORKER_DOCKER=playwright-data-google-worker-docker`
- Optional Docker virtual display mode: `SCRAPER_USE_XVFB_DOCKER=true` with `SCRAPER_HEADLESS_DOCKER=false`
- Optional worker mode in Docker profile: `docker compose --profile worker up -d`
- TripAdvisor scraping is host-local only (no Docker TripAdvisor worker service).
- Local API expects `MONGO_URI=mongodb://localhost:27017`
- Docker API uses `MONGO_URI_DOCKER=mongodb://mongodb:27017`
- On Windows, avoid `uvicorn --reload` when using Playwright endpoints (it may break subprocess support).

## Scraper Session Setup (Important)

Google Maps may show a limited UI (`Limited view detected: True`) if the browser profile is not properly initialized/signed in.

Initialize login once in the same persistent profile:

```bash
uv run python scripts/bootstrap_google_maps_login.py
```

Open a manual Chromium session (incognito by default) and keep it alive until you close it:

```bash
uv run python scripts/manual_chromium_session.py
```

Open manual session with persistent profile (to keep login cookies):

```bash
uv run python scripts/manual_chromium_session.py --persistent
```

Use a specific browser binary (for example Brave):

```bash
uv run python scripts/manual_chromium_session.py --executable-path "C:\\Program Files\\BraveSoftware\\Brave-Browser\\Application\\brave.exe"
```

If you want Docker to reuse that logged session, run bootstrap with the same profile dir used in Docker:

```powershell
$env:SCRAPER_USER_DATA_DIR="playwright-data-docker"
uv run python scripts/bootstrap_google_maps_login.py
```

If you use worker flow and want the worker to share the same session, set:

```env
SCRAPER_USER_DATA_DIR_GOOGLE_WORKER_DOCKER=playwright-data-docker
```

Then run the smoke test:

```bash
uv run python scripts/smoke_test_google_maps_search.py "Restaurante Casa Pepe Madrid"
```

Choose review extraction strategy:

```bash
uv run python scripts/smoke_test_google_maps_search.py "Restaurante Casa Pepe Madrid" --strategy interactive
uv run python scripts/smoke_test_google_maps_search.py "Restaurante Casa Pepe Madrid" --strategy scroll_copy
```

Smoke test flow in Chromium incognito:

```bash
uv run python scripts/smoke_test_google_maps_incognito.py "Restaurante Casa Pepe Madrid"
```

Incognito deterministic full-scroll flow (scroll every 1s, then print all loaded reviews):

```bash
uv run python scripts/incognito_scroll_all_reviews.py "Restaurante Casa Pepe Madrid" --interval-ms 1000 --output output/reviews.json
```

The script prints a count summary:
- listing total reviews
- DOM loaded review cards
- extracted raw reviews
- extracted unique reviews
- coverage percentage vs listing total

Gemini connectivity smoke test:

```bash
uv run python scripts/smoke_test_gemini_flash.py --model gemini-1.5-flash
```

Expected output includes:

- business page URL
- listing payload
- reviews extracted count
- limited-view diagnostic

## API Endpoints

- `GET /health`: service + MongoDB health.
- `POST /business/analyze`: implemented (scrape -> preprocess -> LLM -> MongoDB).
  - Payload supports optional `strategy`: `scroll_copy` (default) or `interactive`.
- `POST /business/scrape/jobs`: enqueue scrape jobs by source (202 Accepted).
- `GET /business/scrape/jobs`: list scrape jobs with `page` and `page_size`.
- `GET /business/scrape/jobs/{job_id}`: read scrape job status/result.
- `GET /business/scrape/jobs/{job_id}/comments`: list canonical comments stored for that scrape job (`source_job_id`) with optional `source`.
- `POST /business/scrape/jobs/{job_id}/stop`: stop a running scrape job (Google/TripAdvisor); optionally enqueue analysis continuation when stopping Google.
- `GET /business/scrape/jobs/{job_id}/events`: SSE stream with real-time progress events.
- `POST /business/analyze/jobs`: enqueue analyze-only job from stored reviews.
- `GET /business/analyze/jobs`: list analyze-only jobs.
- `GET /business/analyze/jobs/{job_id}`: read analyze-only job status/result.
- `GET /business/analyze/jobs/{job_id}/events`: SSE stream with real-time progress events.

Example (SSE progress stream):

```bash
curl -N "http://localhost:8000/business/scrape/jobs/{job_id}/events"
```
- `POST /business/{business_id}/reanalyze`: rerun LLM analysis from stored reviews using multi-batcher strategy.
- `GET /business/`: list businesses with `page` and `page_size`.
- `GET /business/{business_id}`: implemented.
- `GET /business/{business_id}/reviews`: implemented with `page` and `page_size`.
- `GET /business/{business_id}/analysis`: implemented.
- `GET /business/{business_id}/analyses`: analysis history with `page` and `page_size`.

## Tests

```bash
uv run pytest
```

Reanalyze from stored reviews (without new scrape):

```bash
curl -X POST "http://localhost:8000/business/{business_id}/reanalyze" \
  -H "Content-Type: application/json" \
  -d '{"batchers":["latest_text","balanced_rating","low_rating_focus"],"batch_size":30,"max_reviews_pool":250}'
```

Docker E2E scrape test:

```powershell
.\scripts\e2e_docker_scrape.ps1 -Query "Baños Árabes de Córdoba"
```

Docker E2E scrape test with hidden virtual display (`xvfb`):

```powershell
.\scripts\e2e_docker_scrape.ps1 -UseXvfb -Query "Baños Árabes de Córdoba"
```

Docker E2E scrape test using worker queue flow:

```powershell
.\scripts\e2e_docker_scrape.ps1 -UseWorker -Query "Baños Árabes de Córdoba"
```

Build behavior in e2e script:

- Reuses existing image by default (no rebuild).
- Use `-ForceBuild` to rebuild.
- Use `-NoCache` to force full reinstall/rebuild.

## Notes

- This project uses dynamic selectors and fallback strategies to avoid brittle ID-based scraping.
- Use responsibly and always comply with website terms of service and applicable laws.
