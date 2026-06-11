# Repiq

Repiq is a local-first reputation intelligence and scraping system for local businesses.

This repository is used to:

1. scrape Google Maps and Tripadvisor with a real browser
2. normalize business profiles, listings, and reviews
3. run LLM-assisted analysis and reporting pipelines
4. support CRM, benchmark, geogrid, lead-report, and editorial workflows

## Platform support

This project is built for **Linux**.

Supported environments:

- Ubuntu / Debian
- Arch / Manjaro
- other Linux distributions with Docker, Python, Node, and Playwright dependencies available

Not supported as the primary runtime:

- native Windows
- PowerShell-first workflows

Notes:

- Some legacy `.ps1` files still exist, but they are not the supported path.
- WSL may work for partial tasks, but interactive Playwright scraping is designed for Linux host execution.

## Repository layout

- `src/`: backend, business catalog, browser runtime, CRM, reporting, and workers
- `apps/manager/`: local operational UI
- `scripts/`: operational entrypoints, worker launchers, debugging tools, and replay helpers
- `docs/`: architecture, process, product, SEO, editorial, and operational documentation
- `SCRAPERS.md`: focused guide for understanding scraper pipelines

## Runtime model

Repiq is **local-first**.

The important distinction is:

- API and Mongo may run locally or in Docker
- real browser automation runs on the **host machine**
- browser-driven jobs are claimed by the **local browser runtime**
- Tripadvisor live / needs-human sessions are exposed through a dedicated local bridge

### Browser execution modes

Browser-driven jobs currently use two runtime flags:

- `execution_mode`
  - `automatic`
    - background / headless execution
    - currently meaningful mainly for Google Maps
  - `live`
    - headed execution
    - used for visible or operator-driven scraping flows

- `live_display_mode`
  - only relevant when `execution_mode=live`

For `live` mode, the UI can choose the display mode:

- `native`
  - visible browser on your real display
- `xvfb`
  - headed browser inside a virtual display, useful for hidden live runs on Linux

Important source-specific note:

- Google Maps supports both `automatic` and `live`
- Tripadvisor is currently forced into the `live` / replay-headfull path, so in practice the meaningful choice there is `native` vs `xvfb`

## Requirements

Recommended local setup:

- Linux
- Python 3.12
- `pip` or `uv`
- Docker + Docker Compose
- Node.js 20+ for `apps/manager/`
- Chromium and Playwright system dependencies

## Environment setup

Create the environment file:

```bash
cp .env.example .env
```

At minimum, configure:

- `MONGO_URI`
- `DB_NAME`
- `GEMINI_API_KEY` if you want real LLM analysis
- CRM / email / queue variables if you want form or funnel flows

## Build and run

### Option A: local API + Mongo in Docker

Start Mongo:

```bash
docker compose up -d mongodb
```

Create the Python environment and install dependencies:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install --with-deps chromium
```

Start the API:

```bash
PYTHONPATH=. uvicorn src.main:app --host 0.0.0.0 --port 8000
```

### Option B: Docker build for the API

Build:

```bash
docker compose build app
```

Start API + Mongo:

```bash
docker compose up -d mongodb app
```

## Local worker launcher

The main operational entrypoint is:

```bash
scripts/local_workers.sh
```

Usage:

```bash
scripts/local_workers.sh start
scripts/local_workers.sh start-build
scripts/local_workers.sh stop
scripts/local_workers.sh restart
scripts/local_workers.sh status
scripts/local_workers.sh logs front
```

Selectors:

- `front` -> `manager-ui`
- `host` -> `local-browser-runtime`, `tripadvisor-live-bridge`, `supabase-queue`, `report-requests`
- `queue` -> `analysis`, `report`, `crm`, `scraper`
- `all` -> every managed process

Managed processes:

- `manager-ui`
- `local-browser-runtime`
- `tripadvisor-live-bridge`
- `supabase-queue`
- `report-requests`
- `analysis`
- `report`
- `crm`
- `scraper`

Notes:

- `start` is idempotent and will not spawn duplicate processes.
- `start-build` stops the selected processes first, runs required builds, and starts them again.
- `status` prints the local URL for `manager-ui` and `tripadvisor-live-bridge`.

## Manager UI

Build the UI:

```bash
cd apps/manager
npm install
npm run build
```

Run it in development mode:

```bash
cd apps/manager
npm run dev
```

Default preview / local-worker URL:

- manager UI: `http://127.0.0.1:4173`
- Tripadvisor live bridge: `http://127.0.0.1:8765`

The manager UI is the main operational panel for:

- launching scrape pipelines
- selecting source scope
- choosing `automatic`, `live native`, or `live xvfb`
- relaunching jobs
- opening Tripadvisor needs-human sessions
- reading Tripadvisor live-session state and log tail

## Classic workers

The repository still contains classic queue workers:

```bash
PYTHONPATH=. python -m src.workers.analysis_worker
PYTHONPATH=. python -m src.workers.report_worker
PYTHONPATH=. python -m src.workers.crm_worker
PYTHONPATH=. python -m src.workers.scraper_worker
```

For browser-driven scraping, the important runtime is the **local browser runtime**, not a Docker-contained Playwright worker.

## Tests

```bash
PYTHONPATH=. pytest -q
```

## Documentation

Primary entrypoints:

- [`docs/README.md`](docs/README.md)
- [`SCRAPERS.md`](SCRAPERS.md)

Recommended reading order:

1. `docs/README.md`
2. `SCRAPERS.md`
3. `src/platform/application_root.py`
4. `src/browser_runtime/`
5. `src/scraping_google_maps/`
6. `src/scraping_tripadvisor/`
7. `src/business_catalog/`
8. `src/crm/`
9. `src/pipeline/` and `src/pipeline/report_rendering/`
