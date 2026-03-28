#!/usr/bin/env bash
set -euo pipefail

QUERY="Baños Árabes de Córdoba"
TRIPADVISOR_MAX_PAGES=3
TAIL_LINES=200
FORCE_BUILD=true
NO_BUILD=false
ENQUEUE_JOB=true
HEALTH_TIMEOUT_SECONDS=180
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8000}"
IN_CONTAINER_API_BASE_URL="http://127.0.0.1:8000"
HEALTH_FAILED=false
DOWN_ON_EXIT=true
CLEANUP_DONE=false
REMOVE_GOOGLE_JOB=true
SHOW_MONGODB=false

usage() {
  cat <<'EOF'
Usage:
  scripts/run_tripadvisor_worker_xvfb_logs.sh [options]

Options:
  --query "TEXT"                 Business query for queue job.
  --tripadvisor-max-pages N      Tripadvisor max pages (default: 3).
  --tail N                       Number of log lines to tail initially (default: 200).
  --health-timeout-seconds N     Max wait for API health (default: 180).
  --api-base-url URL             API base URL from host (default: http://127.0.0.1:8000).
  --force-build                  Force build before up (default behavior).
  --no-build                     Skip build and reuse existing image.
  --no-enqueue                   Do not enqueue a test job.
  --keep-google-job             Keep the Google queued job created by /scrape/jobs.
  --show-mongodb                Include mongodb logs in live stream (disabled by default).
  --leave-up                     Do not run docker compose down on exit.
  -h, --help                     Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --query)
      QUERY="${2:-}"
      shift 2
      ;;
    --tripadvisor-max-pages)
      TRIPADVISOR_MAX_PAGES="${2:-}"
      shift 2
      ;;
    --tail)
      TAIL_LINES="${2:-}"
      shift 2
      ;;
    --health-timeout-seconds)
      HEALTH_TIMEOUT_SECONDS="${2:-}"
      shift 2
      ;;
    --api-base-url)
      API_BASE_URL="${2:-}"
      shift 2
      ;;
    --force-build)
      FORCE_BUILD=true
      NO_BUILD=false
      shift
      ;;
    --no-build)
      NO_BUILD=true
      FORCE_BUILD=false
      shift
      ;;
    --no-enqueue)
      ENQUEUE_JOB=false
      shift
      ;;
    --keep-google-job)
      REMOVE_GOOGLE_JOB=false
      shift
      ;;
    --show-mongodb)
      SHOW_MONGODB=true
      shift
      ;;
    --leave-up)
      DOWN_ON_EXIT=false
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found." >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl command not found." >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "Cannot access Docker daemon (permission denied or daemon down)." >&2
  echo "Try: newgrp docker  (or log out/in), and ensure Docker service is running." >&2
  exit 1
fi

cleanup() {
  if [[ "$CLEANUP_DONE" == "true" ]]; then
    return
  fi
  CLEANUP_DONE=true
  set +e
  if [[ -n "${HEALTH_ERR_FILE:-}" ]]; then
    rm -f "$HEALTH_ERR_FILE" >/dev/null 2>&1 || true
  fi
  if [[ -n "${COMPOSE_LOGS_PID:-}" ]]; then
    pkill -P "$COMPOSE_LOGS_PID" >/dev/null 2>&1 || true
    kill "$COMPOSE_LOGS_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TRIP_LOGS_PID:-}" ]]; then
    pkill -P "$TRIP_LOGS_PID" >/dev/null 2>&1 || true
    kill "$TRIP_LOGS_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${JOB_MONITOR_PID:-}" ]]; then
    pkill -P "$JOB_MONITOR_PID" >/dev/null 2>&1 || true
    kill "$JOB_MONITOR_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${COMPOSE_LOGS_PID:-}" ]]; then
    wait "$COMPOSE_LOGS_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TRIP_LOGS_PID:-}" ]]; then
    wait "$TRIP_LOGS_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${JOB_MONITOR_PID:-}" ]]; then
    wait "$JOB_MONITOR_PID" >/dev/null 2>&1 || true
  fi
  docker rm -f bra-scraper-tripadvisor-worker-xvfb >/dev/null 2>&1 || true
  if [[ "$DOWN_ON_EXIT" == "true" ]]; then
    docker compose --profile worker down >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

api_request() {
  local method="$1"
  local path="$2"
  local data="${3:-}"
  local host_url="${API_BASE_URL}${path}"
  local container_url="${IN_CONTAINER_API_BASE_URL}${path}"

  if [[ "$method" == "GET" ]]; then
    if curl --noproxy "*" -sS -m 8 "$host_url"; then
      return 0
    fi
    docker compose --profile worker exec -T app curl -sS -m 8 "$container_url"
    return $?
  fi

  if [[ -n "$data" ]]; then
    if curl --noproxy "*" -sS -m 8 -X "$method" "$host_url" \
      -H "Content-Type: application/json" \
      -d "$data"; then
      return 0
    fi
    docker compose --profile worker exec -T app curl -sS -m 8 -X "$method" "$container_url" \
      -H "Content-Type: application/json" \
      -d "$data"
    return $?
  fi

  if curl --noproxy "*" -sS -m 8 -X "$method" "$host_url"; then
    return 0
  fi
  docker compose --profile worker exec -T app curl -sS -m 8 -X "$method" "$container_url"
}

echo "[1/5] Starting Docker services (Tripadvisor-only)..."
UP_ARGS=(compose --profile worker up -d mongodb app)
if [[ "$NO_BUILD" != "true" ]] && [[ "$FORCE_BUILD" == "true" ]]; then
  UP_ARGS=(compose --profile worker up -d --build mongodb app)
fi
docker "${UP_ARGS[@]}"

echo "[2/5] Restarting Tripadvisor worker in xvfb mode..."
docker rm -f bra-scraper-tripadvisor-worker-xvfb >/dev/null 2>&1 || true
docker compose --profile worker run -d \
  --name bra-scraper-tripadvisor-worker-xvfb \
  -e WORKER_SCRAPE_QUEUE=scrape_tripadvisor \
  -e WORKER_SCRAPE_SOURCE=tripadvisor \
  -e WORKER_POLL_SECONDS=2 \
  -e WORKER_IDLE_LOG_SECONDS=10 \
  -e WORKER_JOB_HEARTBEAT_SECONDS=10 \
  -e LOG_LEVEL=INFO \
  -e SCRAPER_HEADLESS=false \
  -e SCRAPER_INCOGNITO=false \
  -e SCRAPER_TRIPADVISOR_USER_DATA_DIR=playwright-data-tripadvisor-worker-docker \
  -e SCRAPER_USER_DATA_DIR=playwright-data-tripadvisor-worker-docker \
  scraper-tripadvisor-worker \
  bash -lc 'Xvfb :99 -screen 0 1920x1080x24 -ac +extension RANDR -nolisten tcp >/tmp/xvfb.log 2>&1 & export DISPLAY=:99; sleep 1; python -m src.workers.scraper_worker'

sleep 2
TRIP_STATE="$(docker inspect -f '{{.State.Status}}' bra-scraper-tripadvisor-worker-xvfb 2>/dev/null || true)"
if [[ "$TRIP_STATE" != "running" ]]; then
  echo "Tripadvisor worker container is not running (state=${TRIP_STATE:-unknown})." >&2
  echo "Container info:" >&2
  docker ps -a --filter "name=bra-scraper-tripadvisor-worker-xvfb" >&2 || true
  echo "Tripadvisor worker logs (tail 120):" >&2
  docker logs --tail 120 bra-scraper-tripadvisor-worker-xvfb >&2 || true
  exit 1
fi
echo "Tripadvisor worker is running."
echo "Tripadvisor worker runtime env:"
docker exec bra-scraper-tripadvisor-worker-xvfb bash -lc \
  'printf "  WORKER_SCRAPE_QUEUE=%s\n  WORKER_SCRAPE_SOURCE=%s\n  WORKER_BROKER_BACKEND=%s\n  MONGO_URI=%s\n  SCRAPER_TRIPADVISOR_USER_DATA_DIR=%s\n" "${WORKER_SCRAPE_QUEUE:-}" "${WORKER_SCRAPE_SOURCE:-}" "${WORKER_BROKER_BACKEND:-}" "${MONGO_URI:-}" "${SCRAPER_TRIPADVISOR_USER_DATA_DIR:-}"' \
  || true

echo "[3/5] Starting live logs (background)..."
docker compose --profile worker logs -f --tail "$TAIL_LINES" mongodb app &
COMPOSE_LOGS_PID=$!
docker logs -f --tail "$TAIL_LINES" bra-scraper-tripadvisor-worker-xvfb &
TRIP_LOGS_PID=$!

echo "[4/5] Waiting for API health..."
HEALTH_OK=false
ATTEMPTS=$(( HEALTH_TIMEOUT_SECONDS / 2 ))
if (( ATTEMPTS < 1 )); then
  ATTEMPTS=1
fi
HEALTH_ERR_FILE="$(mktemp)"
for ATTEMPT in $(seq 1 "$ATTEMPTS"); do
  HEALTH_BODY=""
  CURL_ERR=""
  if HEALTH_BODY="$(api_request GET "/health" "" 2>"$HEALTH_ERR_FILE")"; then
    :
  else
    CURL_ERR="$(cat "$HEALTH_ERR_FILE" 2>/dev/null || true)"
  fi
  if [[ -n "$HEALTH_BODY" ]] && [[ "$HEALTH_BODY" == *'"status":"ok"'* ]]; then
    HEALTH_OK=true
    break
  fi
  if [[ -n "$CURL_ERR" ]]; then
    echo "  healthcheck attempt $ATTEMPT/$ATTEMPTS: curl_error=$CURL_ERR"
  else
    BODY_SNIPPET="$(printf '%s' "$HEALTH_BODY" | tr '\n' ' ' | cut -c1-260)"
    echo "  healthcheck attempt $ATTEMPT/$ATTEMPTS: body=${BODY_SNIPPET:-<empty>}"
  fi
  if (( ATTEMPT % 5 == 0 )); then
    echo "  ---- docker compose ps ----"
    docker compose --profile worker ps || true
    echo "  ---- app logs (tail 40) ----"
    docker compose --profile worker logs --tail 40 app || true
  fi
  sleep 2
done
if [[ "$HEALTH_OK" != "true" ]]; then
  HEALTH_FAILED=true
  ENQUEUE_JOB=false
  echo "API healthcheck failed after ${HEALTH_TIMEOUT_SECONDS}s (${API_BASE_URL}/health)." >&2
  echo "Continuing in log-only mode (no job enqueue)." >&2
  echo "Current compose status:" >&2
  docker compose --profile worker ps >&2 || true
fi

if [[ "$ENQUEUE_JOB" == "true" ]]; then
  echo "[5/5] Enqueueing test job..."
  QUEUE_PAYLOAD=$(cat <<EOF
{"name":"$QUERY","force":true,"strategy":"scroll_copy","scraper_params":{"scraper_tripadvisor_max_pages":$TRIPADVISOR_MAX_PAGES}}
EOF
)
  QUEUE_RESPONSE="$(api_request POST "/business/scrape/jobs" "$QUEUE_PAYLOAD")"

  echo "Queue response:"
  echo "$QUEUE_RESPONSE"

  COMPACT_RESPONSE="$(printf '%s' "$QUEUE_RESPONSE" | tr -d '\n\r[:space:]')"
  TRIP_JOB_ID="$(printf '%s' "$COMPACT_RESPONSE" | sed -n 's/.*"tripadvisor":{"job_id":"\([^"]*\)".*/\1/p')"
  GOOGLE_JOB_ID="$(printf '%s' "$COMPACT_RESPONSE" | sed -n 's/.*"google_maps":{"job_id":"\([^"]*\)".*/\1/p')"
  if [[ -n "$TRIP_JOB_ID" ]]; then
    echo "Tripadvisor job id: $TRIP_JOB_ID"
    echo "Tripadvisor state:   ${API_BASE_URL}/business/scrape/jobs/$TRIP_JOB_ID"
    echo "Tripadvisor SSE:     ${API_BASE_URL}/business/scrape/jobs/$TRIP_JOB_ID/events"
  fi
  if [[ -n "$GOOGLE_JOB_ID" ]]; then
    echo "Google job id:       $GOOGLE_JOB_ID"
    if [[ "$REMOVE_GOOGLE_JOB" == "true" ]]; then
      DELETE_RESPONSE="$(api_request DELETE "/business/scrape/jobs/$GOOGLE_JOB_ID" "")"
      echo "Google queued job removed (tripadvisor-only mode)."
      echo "$DELETE_RESPONSE"
    fi
  fi
else
  if [[ "$HEALTH_FAILED" == "true" ]]; then
    echo "[5/5] Skipping queue job because API health failed."
  else
    echo "[5/5] Skipping queue job (--no-enqueue)."
  fi
fi

echo "Streaming logs (Ctrl+C to stop log view)..."
echo "Services: mongodb, app, tripadvisor-xvfb-worker"

wait "$COMPOSE_LOGS_PID" "$TRIP_LOGS_PID"
