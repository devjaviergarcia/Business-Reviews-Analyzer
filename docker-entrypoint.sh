#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
RELOAD="${API_RELOAD_DOCKER:-false}"
USE_XVFB="${SCRAPER_USE_XVFB_DOCKER:-false}"
XVFB_SCREEN="${XVFB_SCREEN:-1920x1080x24}"

cmd=(uvicorn src.main:app --host "$HOST" --port "$PORT")

if [[ "${RELOAD,,}" == "true" ]]; then
  cmd+=(--reload)
fi

if [[ "${USE_XVFB,,}" == "true" ]]; then
  exec xvfb-run --auto-servernum --server-args="-screen 0 ${XVFB_SCREEN} -ac +extension RANDR" "${cmd[@]}"
fi

exec "${cmd[@]}"
