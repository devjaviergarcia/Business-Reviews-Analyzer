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
- In progress:
  - Business and analysis endpoints are scaffolded but return `501 Not Implemented`.

## Tech Stack

- Python + FastAPI
- Playwright (browser automation)
- MongoDB (Motor/PyMongo)
- OpenAI SDK (LLM stage scaffolded)
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
- `SCRAPER_MIN_CLICK_DELAY_MS` / `SCRAPER_MAX_CLICK_DELAY_MS`: click spacing.
- `SCRAPER_MIN_KEY_DELAY_MS` / `SCRAPER_MAX_KEY_DELAY_MS`: per-key typing delay.

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
uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

Or with Docker Compose:

```bash
docker compose up --build
```

API docs:

- Swagger UI: `http://localhost:8000/docs`

## Scraper Session Setup (Important)

Google Maps may show a limited UI (`Limited view detected: True`) if the browser profile is not properly initialized/signed in.

Initialize login once in the same persistent profile:

```bash
uv run python scripts/bootstrap_google_maps_login.py
```

Then run the smoke test:

```bash
uv run python scripts/smoke_test_google_maps_search.py "Restaurante Casa Pepe Madrid"
```

Expected output includes:

- business page URL
- listing payload
- reviews extracted count
- limited-view diagnostic

## API Endpoints

- `GET /health`: service + MongoDB health.
- `POST /business/analyze`: scaffold (501).
- `GET /business/{business_id}`: scaffold (501).
- `GET /business/{business_id}/reviews`: scaffold (501).
- `GET /business/{business_id}/analysis`: scaffold (501).

## Tests

```bash
uv run pytest
```

## Notes

- This project uses dynamic selectors and fallback strategies to avoid brittle ID-based scraping.
- Use responsibly and always comply with website terms of service and applicable laws.

