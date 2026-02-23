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
- `ANALYSIS_REANALYZE_DEFAULT_BATCHERS`: batchers used when reanalyzing saved reviews.
- `ANALYSIS_REANALYZE_BATCH_SIZE`: reviews per batch for reanalysis.
- `ANALYSIS_REANALYZE_POOL_SIZE`: max stored reviews loaded for reanalysis.

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

Infra-only (Mongo in Docker, API local):

```powershell
.\scripts\dev_infra.ps1 up
# then
.\scripts\run_local_api.ps1
```

API docs:

- Swagger UI: `http://localhost:8000/docs`

### 5) Local vs Docker Profile

- Local profile (recommended for login/manual checks): `SCRAPER_HEADLESS=false`
- Incognito in official API flow (no persisted login cookies): `SCRAPER_INCOGNITO=true`
- Docker/dev profile (recommended for server stability): `SCRAPER_HEADLESS_DOCKER=true`
- Docker uses its own Playwright profile dir by default: `SCRAPER_USER_DATA_DIR_DOCKER=playwright-data-docker`
- Worker has its own profile dir by default: `SCRAPER_USER_DATA_DIR_WORKER_DOCKER=playwright-data-worker-docker`
- Optional Docker virtual display mode: `SCRAPER_USE_XVFB_DOCKER=true` with `SCRAPER_HEADLESS_DOCKER=false`
- Optional worker mode in Docker profile: `docker compose --profile worker up -d`
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
SCRAPER_USER_DATA_DIR_WORKER_DOCKER=playwright-data-docker
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
- `POST /business/analyze/queue`: enqueue analysis job for worker flow (202 Accepted).
  - Accepts optional `strategy` too, persisted in job and used by worker.
- `GET /business/analyze/queue`: list analysis jobs with `page` and `page_size`.
- `GET /business/analyze/queue/{job_id}`: read worker job status/result.
- `GET /business/analyze/queue/{job_id}/events`: SSE stream with real-time progress events.

Example (SSE progress stream):

```bash
curl -N "http://localhost:8000/business/analyze/queue/{job_id}/events"
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
