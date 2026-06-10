# Repiq

Repiq is a local-first reputation intelligence platform for local businesses.

This repository exists for four main jobs:

1. Scrape Google Maps and Tripadvisor with a real browser.
2. Normalize and analyze business listings and reviews.
3. Generate advanced reports, previews, and public studies.
4. Orchestrate discovery, CRM, benchmarks, and geogrid workflows on top of Mongo and workers.

## Operating system support

This project is built for **Linux**.

Supported development and runtime environments:

- Ubuntu / Debian
- Arch / Manjaro
- other Linux distributions with Docker, Python, and Playwright dependencies available

Not currently supported as a primary environment:

- native Windows
- PowerShell as the main shell

Notes:

- There are legacy `.ps1` scripts in the repo, but the supported workflow is Linux.
- WSL may work for some parts, but it is not the target environment for interactive Playwright scraping.

## Repository layout

- `src/`: backend, scrapers, workers, CRM, business catalog, and reporting pipeline
- `apps/manager/`: local manager UI built with Vite
- `scripts/`: operational entrypoints and support scripts
- `docs/`: full project documentation, architecture, ops, backlog, editorial, and SEO notes
- `SCRAPERS.md`: scraper pipeline reading guide

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
- CRM / email variables if you want campaigns or form flows

## Build and run locally

### Option A: local backend + Mongo in Docker

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

### Option B: build with Docker Compose

Build the main application image:

```bash
docker compose build app
```

Start API + Mongo:

```bash
docker compose up -d mongodb app
```

## Workers

Depending on the flow you want to test:

```bash
PYTHONPATH=. python -m src.workers.analysis_worker
PYTHONPATH=. python -m src.workers.report_worker
PYTHONPATH=. python -m src.workers.crm_worker
PYTHONPATH=. python -m src.workers.scraper_worker
```

For browser-driven scraping, the important runtime is the **local browser runtime**, not Docker running Playwright directly.

## Local worker launcher

The repository now includes an idempotent shell launcher for host-side workers:

```bash
scripts/local_workers.sh start
scripts/local_workers.sh start-build
scripts/local_workers.sh status
scripts/local_workers.sh logs front
```

Default profile:

- `all` -> frontend + host workers + classic queue workers

Other selectors:

- `front` -> `manager-ui`
- `host` -> `local-browser-runtime`, `supabase-queue`, `report-requests`
- `queue` -> `analysis`, `report`, `crm`, `scraper`

`start-build` stops the selected processes first, runs required build steps, and starts them again.

The launcher is idempotent: if a process is already running, it will not spawn a duplicate one.

## Local manager UI

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

## Tests

```bash
PYTHONPATH=. pytest -q
```

## Documentation

Start here for the full project map:

- `docs/README.md`
- `SCRAPERS.md`

## Recommended reading order

1. `docs/README.md`
2. `SCRAPERS.md`
3. `src/platform/application_root.py`
4. `src/browser_runtime/`
5. `src/scraping_google_maps/` and `src/scraping_tripadvisor/`
6. `src/business_catalog/`
7. `src/crm/`
8. `src/pipeline/` and `src/pipeline/report_rendering/`
