#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
DEFAULT_PROFILE_DIR="${SCRAPER_TRIPADVISOR_USER_DATA_DIR_LOCAL:-playwright-data-tripadvisor-worker-docker}"
LOCAL_WORKER_PID_FILE="${LOCAL_WORKER_PID_FILE:-$REPO_ROOT/.tripadvisor_local_worker.pid}"
LOCAL_WORKER_LOG_FILE="${LOCAL_WORKER_LOG_FILE:-$REPO_ROOT/artifacts/tripadvisor_local_worker.log}"
BRIDGE_PID_FILE="${BRIDGE_PID_FILE:-$REPO_ROOT/.tripadvisor_local_worker_bridge.pid}"
BRIDGE_LOG_FILE="${BRIDGE_LOG_FILE:-$REPO_ROOT/artifacts/tripadvisor_local_worker_bridge.log}"
BRIDGE_HOST="${TRIPADVISOR_LOCAL_WORKER_BRIDGE_HOST:-0.0.0.0}"
BRIDGE_PORT="${TRIPADVISOR_LOCAL_WORKER_BRIDGE_PORT:-8765}"

usage() {
  cat <<'EOF'
TripAdvisor operations (real mode):
- Docker: mongodb + app + scraper-google-worker + analysis-worker + report-worker
- Host local: scraper-tripadvisor worker (xvfb, detached)

Usage:
  scripts/tripadvisor_ctl.sh <command> [args]

Commands:
  bridge-up
      Start local TripAdvisor bridge API (detached) for bra-api.

  bridge-down
      Stop local TripAdvisor bridge API.

  bridge-status
      Show local TripAdvisor bridge status and health endpoint.

  local-worker-start [--xvfb]
      Start local TripAdvisor worker only (detached).

  local-worker-stop
      Stop local TripAdvisor worker only.

  up [--build]
      Start docker core services (detached) + local TripAdvisor worker (detached).

  rebuild
      Same as: up --build

  down [--with-mongo]
      Stop local TripAdvisor worker and docker core services.
      By default keeps mongodb running. Use --with-mongo to stop it too.

  status
      Show docker status, local worker status, API health, TA session-state, recent jobs.

  logs [app|google|analysis|report|local-ta] [tail_lines]
      Follow logs. Default target=local-ta, tail_lines=120.

  scrape --google "<name>" --tripadvisor "<name>" [--max-pages N] [--pages-percent P] [--force true|false]
      Queue one scrape request with both sources and different source names.

  job <job_id>
      Print compact summary of a job (auto-detect scrape/analyze).

  payload <job_id>
      Print full payload/context for a specific job (auto-detect scrape/analyze).

  trace <job_id> [from_index]
      Stream SSE events for a specific job (auto-detect scrape/analyze).

  relaunch <job_id> [--force] [--from-zero]
      Relaunch one job by id (auto-detect scrape/analyze).

  replay-headfull <tripadvisor_scrape_job_id> [manual_session_extra_args...]
      Replay one Tripadvisor scrape job locally in headed mode (visible browser),
      using the original source_name/name + tripadvisor_max_pages from that job.
      It temporarily stops local TA worker to avoid profile lock and restores it after exit.

  human [manual_session_extra_args...]
      Full human flow:
      1) stop local TA worker
      2) open manual Chromium session with worker profile
      3) on close, refresh storage_state.json from profile
      4) auto confirm in API + relaunch pending TA jobs + verify session is valid
      5) if valid, start local TA worker again

  session-confirm [profile_dir] [relaunch_pending]
      Confirm manual TA session in API state.
      relaunch_pending: true|false (default false)

  help
      Show this help.
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
}

resolve_python_bin() {
  if [[ -x "$REPO_ROOT/.venv-uv/bin/python" ]]; then
    printf '%s\n' "$REPO_ROOT/.venv-uv/bin/python"
    return 0
  fi
  if [[ -x "$REPO_ROOT/.venv-linux/bin/python" ]]; then
    printf '%s\n' "$REPO_ROOT/.venv-linux/bin/python"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  echo "python not found (.venv-uv/.venv-linux/python3)." >&2
  exit 1
}

sync_storage_state_from_profile() {
  local python_bin="$1"
  local profile_dir="$2"
  local profile_abs="$REPO_ROOT/$profile_dir"
  local storage_state_path="$profile_abs/storage_state.json"

  mkdir -p "$profile_abs"
  find "$profile_abs" -maxdepth 1 -name 'Singleton*' -delete >/dev/null 2>&1 || true
  PROFILE_DIR="$profile_abs" STORAGE_STATE_PATH="$storage_state_path" "$python_bin" - <<'PY'
import asyncio
import os
from playwright.async_api import async_playwright


async def _main() -> None:
    profile_dir = os.environ["PROFILE_DIR"]
    storage_state_path = os.environ["STORAGE_STATE_PATH"]
    async with async_playwright() as playwright:
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            headless=True,
            locale="es-ES",
            timezone_id="Europe/Madrid",
        )
        try:
            await context.storage_state(path=storage_state_path)
        finally:
            await context.close()


asyncio.run(_main())
print(f"storage_state synced: {os.environ['STORAGE_STATE_PATH']}")
PY
}

resolve_profile_dir_abs() {
  local profile_dir="$1"
  if [[ "$profile_dir" = /* ]]; then
    printf '%s\n' "$profile_dir"
    return 0
  fi
  printf '%s\n' "$REPO_ROOT/$profile_dir"
}

terminate_playwright_sessions_for_profile() {
  local profile_dir="$1"
  local profile_abs
  profile_abs="$(resolve_profile_dir_abs "$profile_dir")"

  local -a pids=()
  while IFS= read -r line; do
    local pid args
    pid="${line%% *}"
    args="${line#* }"

    if [[ -z "$pid" || "$pid" == "$$" ]]; then
      continue
    fi

    if [[ "$args" == *"--user-data-dir=$profile_abs"* ]]; then
      pids+=("$pid")
      continue
    fi

    if [[ "$args" == *"manual_chromium_session.py"* ]]; then
      if [[ "$args" == *"--profile-dir $profile_dir"* || "$args" == *"--profile-dir $profile_abs"* ]]; then
        pids+=("$pid")
      fi
    fi
  done < <(ps -eo pid=,args=)

  if [[ "${#pids[@]}" -gt 0 ]]; then
    echo "Closing existing Playwright/Chromium sessions for profile '$profile_dir' (pids: ${pids[*]})."
    for pid in "${pids[@]}"; do
      kill "$pid" >/dev/null 2>&1 || true
    done
    for _ in $(seq 1 25); do
      local any_alive="false"
      for pid in "${pids[@]}"; do
        if kill -0 "$pid" >/dev/null 2>&1; then
          any_alive="true"
          break
        fi
      done
      if [[ "$any_alive" == "false" ]]; then
        break
      fi
      sleep 0.2
    done
    for pid in "${pids[@]}"; do
      if kill -0 "$pid" >/dev/null 2>&1; then
        kill -9 "$pid" >/dev/null 2>&1 || true
      fi
    done
  fi

  find "$profile_abs" -maxdepth 1 -name 'Singleton*' -delete >/dev/null 2>&1 || true
}

is_local_worker_running() {
  if [[ ! -f "$LOCAL_WORKER_PID_FILE" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$LOCAL_WORKER_PID_FILE" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  if kill -0 "$pid" >/dev/null 2>&1; then
    return 0
  fi
  rm -f "$LOCAL_WORKER_PID_FILE" >/dev/null 2>&1 || true
  return 1
}

is_bridge_running() {
  _pid_is_bridge_process() {
    local probe_pid="$1"
    ps -p "$probe_pid" -o args= 2>/dev/null | grep -q "tripadvisor_local_worker_bridge.py"
  }

  if [[ ! -f "$BRIDGE_PID_FILE" ]]; then
    local discovered_pid
    discovered_pid="$(pgrep -f "[t]ripadvisor_local_worker_bridge.py" | head -n 1 || true)"
    if [[ -n "$discovered_pid" ]] && kill -0 "$discovered_pid" >/dev/null 2>&1 && _pid_is_bridge_process "$discovered_pid"; then
      echo "$discovered_pid" > "$BRIDGE_PID_FILE"
      return 0
    fi
    return 1
  fi
  local pid
  pid="$(cat "$BRIDGE_PID_FILE" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    return 1
  fi
  if kill -0 "$pid" >/dev/null 2>&1 && _pid_is_bridge_process "$pid"; then
    return 0
  fi
  rm -f "$BRIDGE_PID_FILE" >/dev/null 2>&1 || true
  return 1
}

start_bridge_detached() {
  local python_bin
  python_bin="$(resolve_python_bin)"
  cd "$REPO_ROOT"
  mkdir -p "$(dirname "$BRIDGE_LOG_FILE")"
  touch "$BRIDGE_LOG_FILE"

  if is_bridge_running; then
    local pid
    pid="$(cat "$BRIDGE_PID_FILE")"
    echo "Local TripAdvisor bridge already running (pid=$pid)."
    return 0
  fi

  nohup "$python_bin" scripts/tripadvisor_local_worker_bridge.py --host "$BRIDGE_HOST" --port "$BRIDGE_PORT" >>"$BRIDGE_LOG_FILE" 2>&1 &
  local pid=$!
  echo "$pid" > "$BRIDGE_PID_FILE"
  sleep 1

  if ! kill -0 "$pid" >/dev/null 2>&1; then
    echo "Failed to start bridge. Check logs: $BRIDGE_LOG_FILE" >&2
    tail -n 80 "$BRIDGE_LOG_FILE" >&2 || true
    rm -f "$BRIDGE_PID_FILE"
    exit 1
  fi
  echo "Local TripAdvisor bridge started (pid=$pid, ${BRIDGE_HOST}:${BRIDGE_PORT})."
}

stop_bridge() {
  if [[ ! -f "$BRIDGE_PID_FILE" ]]; then
    echo "Local TripAdvisor bridge is not running."
    return 0
  fi
  local pid
  pid="$(cat "$BRIDGE_PID_FILE" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$BRIDGE_PID_FILE"
    echo "Local TripAdvisor bridge pid file was empty."
    return 0
  fi
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    rm -f "$BRIDGE_PID_FILE"
    echo "Local TripAdvisor bridge was not running (stale pid file removed)."
    return 0
  fi

  kill "$pid" >/dev/null 2>&1 || true
  for _ in $(seq 1 20); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$BRIDGE_PID_FILE"
  echo "Local TripAdvisor bridge stopped."
}

cmd_bridge_up() {
  if [[ $# -gt 0 ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh bridge-up" >&2
    exit 1
  fi
  start_bridge_detached
  cmd_bridge_status
}

cmd_bridge_down() {
  if [[ $# -gt 0 ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh bridge-down" >&2
    exit 1
  fi
  stop_bridge
}

cmd_bridge_status() {
  if [[ $# -gt 0 ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh bridge-status" >&2
    exit 1
  fi
  echo "== Local bridge process =="
  if is_bridge_running; then
    echo "running (pid=$(cat "$BRIDGE_PID_FILE"))"
  else
    echo "stopped"
  fi
  echo "pid_file=$BRIDGE_PID_FILE"
  echo "log_file=$BRIDGE_LOG_FILE"
  echo "health_url=http://127.0.0.1:${BRIDGE_PORT}/health"
  if command -v curl >/dev/null 2>&1; then
    echo
    curl -fsS "http://127.0.0.1:${BRIDGE_PORT}/health" | jq . || true
  fi
}

start_local_worker_detached() {
  local use_xvfb="${1:-true}"
  require_cmd docker
  if [[ "$use_xvfb" == "true" ]]; then
    require_cmd xvfb-run
  fi

  cd "$REPO_ROOT"
  mkdir -p "$(dirname "$LOCAL_WORKER_LOG_FILE")"
  touch "$LOCAL_WORKER_LOG_FILE"

  if is_local_worker_running; then
    local pid
    pid="$(cat "$LOCAL_WORKER_PID_FILE")"
    echo "Local TripAdvisor worker already running (pid=$pid)."
    return 0
  fi

  local python_bin
  python_bin="$(resolve_python_bin)"

  local launch_command
  if [[ "$use_xvfb" == "true" ]]; then
    launch_command="exec xvfb-run -a --server-args=\"-screen 0 1920x1080x24 -ac +extension RANDR\" \"$python_bin\" -m src.workers.scraper_worker"
  else
    launch_command="exec \"$python_bin\" -m src.workers.scraper_worker"
  fi

  local run_cmd
  run_cmd="$(cat <<EOF
cd "$REPO_ROOT"
export PYTHONPATH="."
export MONGO_URI="\${MONGO_URI:-mongodb://localhost:27017}"
export WORKER_BROKER_BACKEND="\${WORKER_BROKER_BACKEND:-mongo}"
export WORKER_SCRAPE_QUEUE="scrape_tripadvisor"
export WORKER_SCRAPE_SOURCE="tripadvisor"
export SCRAPER_HEADLESS="\${SCRAPER_HEADLESS_LOCAL:-false}"
export SCRAPER_INCOGNITO="\${SCRAPER_INCOGNITO_LOCAL:-false}"
export SCRAPER_TRIPADVISOR_USER_DATA_DIR="\${SCRAPER_TRIPADVISOR_USER_DATA_DIR_LOCAL:-$DEFAULT_PROFILE_DIR}"
export SCRAPER_USER_DATA_DIR="\${SCRAPER_TRIPADVISOR_USER_DATA_DIR_LOCAL:-$DEFAULT_PROFILE_DIR}"
$launch_command
EOF
)"
  nohup bash -lc "$run_cmd" >>"$LOCAL_WORKER_LOG_FILE" 2>&1 &
  local pid=$!
  echo "$pid" > "$LOCAL_WORKER_PID_FILE"
  sleep 1

  if ! kill -0 "$pid" >/dev/null 2>&1; then
    echo "Failed to start local TripAdvisor worker. Check logs: $LOCAL_WORKER_LOG_FILE" >&2
    tail -n 120 "$LOCAL_WORKER_LOG_FILE" >&2 || true
    rm -f "$LOCAL_WORKER_PID_FILE"
    exit 1
  fi
  echo "Local TripAdvisor worker started (pid=$pid)."
}

cmd_local_worker_start() {
  local use_xvfb="false"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --xvfb)
        use_xvfb="true"
        shift
        ;;
      *)
        echo "Usage: scripts/tripadvisor_ctl.sh local-worker-start [--xvfb]" >&2
        exit 1
        ;;
    esac
  done
  start_local_worker_detached "$use_xvfb"
}

cmd_local_worker_stop() {
  if [[ $# -gt 0 ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh local-worker-stop" >&2
    exit 1
  fi
  stop_local_worker
}

stop_local_worker() {
  if [[ ! -f "$LOCAL_WORKER_PID_FILE" ]]; then
    echo "Local TripAdvisor worker is not running."
    return 0
  fi

  local pid
  pid="$(cat "$LOCAL_WORKER_PID_FILE" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$LOCAL_WORKER_PID_FILE"
    echo "Local TripAdvisor worker pid file was empty."
    return 0
  fi
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    rm -f "$LOCAL_WORKER_PID_FILE"
    echo "Local TripAdvisor worker was not running (stale pid file removed)."
    return 0
  fi

  kill "$pid" >/dev/null 2>&1 || true
  for _ in $(seq 1 20); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$LOCAL_WORKER_PID_FILE"
  echo "Local TripAdvisor worker stopped."
}

detect_job_kind() {
  local job_id="$1"
  if curl -fsS "$API_BASE_URL/business/scrape/jobs/$job_id" >/dev/null 2>&1; then
    echo "scrape"
    return 0
  fi
  if curl -fsS "$API_BASE_URL/business/analyze/jobs/$job_id" >/dev/null 2>&1; then
    echo "analyze"
    return 0
  fi
  return 1
}

cmd_up() {
  require_cmd docker
  local build_arg=""
  if [[ "${1:-}" == "--build" ]]; then
    build_arg="--build"
    shift
  fi
  if [[ $# -gt 0 ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh up [--build]" >&2
    exit 1
  fi

  cd "$REPO_ROOT"
  start_bridge_detached
  docker compose --profile worker up -d $build_arg mongodb app scraper-google-worker analysis-worker report-worker
  start_local_worker_detached
  cmd_status
}

cmd_rebuild() {
  if [[ $# -gt 0 ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh rebuild" >&2
    exit 1
  fi
  cmd_up --build
}

cmd_down() {
  require_cmd docker
  local with_mongo="false"
  if [[ "${1:-}" == "--with-mongo" ]]; then
    with_mongo="true"
    shift
  fi
  if [[ $# -gt 0 ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh down [--with-mongo]" >&2
    exit 1
  fi

  stop_local_worker
  stop_bridge
  cd "$REPO_ROOT"
  docker compose --profile worker stop app scraper-google-worker analysis-worker report-worker >/dev/null 2>&1 || true
  if [[ "$with_mongo" == "true" ]]; then
    docker compose --profile worker stop mongodb >/dev/null 2>&1 || true
  fi
  cmd_status
}

cmd_status() {
  require_cmd docker
  require_cmd curl
  require_cmd jq
  cd "$REPO_ROOT"

  echo "== Docker services =="
  docker compose --profile worker ps mongodb app scraper-google-worker analysis-worker report-worker || true

  echo
  echo "== Local TripAdvisor worker =="
  if is_local_worker_running; then
    echo "running (pid=$(cat "$LOCAL_WORKER_PID_FILE"))"
  else
    echo "stopped"
  fi
  echo "pid_file=$LOCAL_WORKER_PID_FILE"
  echo "log_file=$LOCAL_WORKER_LOG_FILE"

  echo
  echo "== Local TripAdvisor bridge =="
  if is_bridge_running; then
    echo "running (pid=$(cat "$BRIDGE_PID_FILE"))"
  else
    echo "stopped"
  fi
  echo "pid_file=$BRIDGE_PID_FILE"
  echo "log_file=$BRIDGE_LOG_FILE"
  echo "health_url=http://127.0.0.1:${BRIDGE_PORT}/health"
  curl -fsS "http://127.0.0.1:${BRIDGE_PORT}/health" | jq . || true

  echo
  echo "== API health =="
  curl -fsS "$API_BASE_URL/health" | jq .

  echo
  echo "== TripAdvisor session-state =="
  curl -fsS "$API_BASE_URL/tripadvisor/session-state" \
    | jq '{
      session_state,
      availability_now,
      last_validation_result,
      session_cookie_expires_at,
      last_human_intervention_at,
      last_error,
      playwright_profile_path,
      playwright_storage_state_path,
      bot_detected_count,
      worker_singleton: (
        .worker_singleton // null
      )
    }' || true

  echo
  echo "== Recent scrape jobs =="
  curl -fsS "$API_BASE_URL/business/scrape/jobs?page=1&page_size=8" \
    | jq '[.items[] | {job_id,status,queue_name,name,updated_at,stage:.progress.stage,message:.progress.message}]'
}

cmd_logs() {
  require_cmd docker
  local target="${1:-local-ta}"
  local tail_lines="${2:-120}"
  cd "$REPO_ROOT"

  case "$target" in
    local-ta)
      mkdir -p "$(dirname "$LOCAL_WORKER_LOG_FILE")"
      touch "$LOCAL_WORKER_LOG_FILE"
      tail -n "$tail_lines" -f "$LOCAL_WORKER_LOG_FILE"
      ;;
    app)
      docker compose --profile worker logs -f --tail "$tail_lines" app
      ;;
    google)
      docker compose --profile worker logs -f --tail "$tail_lines" scraper-google-worker
      ;;
    analysis)
      docker compose --profile worker logs -f --tail "$tail_lines" analysis-worker
      ;;
    report)
      docker compose --profile worker logs -f --tail "$tail_lines" report-worker
      ;;
    *)
      echo "Unknown logs target: $target. Allowed: app|google|analysis|report|local-ta" >&2
      exit 1
      ;;
  esac
}

cmd_scrape() {
  require_cmd curl
  require_cmd jq
  local google_name=""
  local tripadvisor_name=""
  local max_pages=""
  local pages_percent=""
  local force_raw="true"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --google)
        google_name="${2:-}"
        shift 2
        ;;
      --tripadvisor)
        tripadvisor_name="${2:-}"
        shift 2
        ;;
      --max-pages)
        max_pages="${2:-}"
        shift 2
        ;;
      --pages-percent)
        pages_percent="${2:-}"
        shift 2
        ;;
      --force)
        force_raw="${2:-}"
        shift 2
        ;;
      *)
        echo "Unknown arg for scrape: $1" >&2
        exit 1
        ;;
    esac
  done

  if [[ -z "$google_name" || -z "$tripadvisor_name" ]]; then
    echo "Usage: scripts/tripadvisor_ctl.sh scrape --google \"...\" --tripadvisor \"...\" [--max-pages N] [--pages-percent P] [--force true|false]" >&2
    exit 1
  fi

  local force_json
  case "${force_raw,,}" in
    true|1|yes|y) force_json="true" ;;
    false|0|no|n) force_json="false" ;;
    *) echo "--force must be true|false"; exit 1 ;;
  esac

  local max_pages_json="null"
  if [[ -n "$max_pages" ]]; then
    max_pages_json="$(jq -n --arg v "$max_pages" '$v|tonumber')"
  fi
  local pages_percent_json="null"
  if [[ -n "$pages_percent" ]]; then
    pages_percent_json="$(jq -n --arg v "$pages_percent" '$v|tonumber')"
  fi

  local payload
  payload="$(jq -n \
    --arg name "$google_name" \
    --arg google_maps_name "$google_name" \
    --arg tripadvisor_name "$tripadvisor_name" \
    --argjson force "$force_json" \
    --argjson max_pages "$max_pages_json" \
    --argjson pages_percent "$pages_percent_json" \
    '{
      name: $name,
      force: $force,
      sources: ["google_maps", "tripadvisor"],
      google_maps_name: $google_maps_name,
      tripadvisor_name: $tripadvisor_name,
      scraper_params: (
        {}
        + (if $max_pages != null then {scraper_tripadvisor_max_pages: $max_pages} else {} end)
        + (if $pages_percent != null then {scraper_tripadvisor_pages_percent: $pages_percent} else {} end)
      )
    }'
  )"

  curl -fsS -X POST "$API_BASE_URL/business/scrape/jobs" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -d "$payload" \
    | jq '{job_id,primary_source,sources_requested,source_names,jobs_by_source}'
}

cmd_job() {
  require_cmd curl
  require_cmd jq
  local job_id="${1:-}"
  if [[ -z "$job_id" ]]; then
    echo "Missing job_id. Usage: scripts/tripadvisor_ctl.sh job <job_id>" >&2
    exit 1
  fi

  local kind
  if ! kind="$(detect_job_kind "$job_id")"; then
    echo "Job not found in scrape/analyze: $job_id" >&2
    exit 1
  fi

  curl -fsS "$API_BASE_URL/business/$kind/jobs/$job_id" \
    | jq '{
      job_id,
      status,
      queue_name,
      job_type,
      attempts,
      created_at,
      updated_at,
      started_at,
      finished_at,
      progress,
      result_summary: {
        business_id: (.result.business_id // null),
        source_job_id: (.result.source_job_id // null),
        analysis_handoff_job_id: (.result.analysis_handoff.analysis_job_id // null),
        dataset_id: (.result.dataset_id // null),
        review_count: (.result.review_count // null)
      }
    }'
}

cmd_payload() {
  require_cmd curl
  require_cmd jq
  local job_id="${1:-}"
  if [[ -z "$job_id" ]]; then
    echo "Missing job_id. Usage: scripts/tripadvisor_ctl.sh payload <job_id>" >&2
    exit 1
  fi
  local kind
  if ! kind="$(detect_job_kind "$job_id")"; then
    echo "Job not found in scrape/analyze: $job_id" >&2
    exit 1
  fi

  curl -fsS "$API_BASE_URL/business/$kind/jobs/$job_id" \
    | jq '{
      kind: "'"$kind"'",
      job_id,
      status,
      queue_name,
      job_type,
      name,
      canonical_name,
      source_name,
      root_business_id,
      attempts,
      progress,
      payload,
      created_at,
      updated_at,
      started_at,
      finished_at,
      error,
      result
    }'
}

cmd_replay_headfull() {
  require_cmd curl
  require_cmd jq
  local job_id="${1:-}"
  shift || true
  if [[ -z "$job_id" ]]; then
    echo "Missing job_id. Usage: scripts/tripadvisor_ctl.sh replay-headfull <tripadvisor_scrape_job_id> [manual_session_extra_args...]" >&2
    exit 1
  fi

  local kind
  if ! kind="$(detect_job_kind "$job_id")"; then
    echo "Job not found in scrape/analyze: $job_id" >&2
    exit 1
  fi
  if [[ "$kind" != "scrape" ]]; then
    echo "Only scrape jobs are supported for replay-headfull. Received kind=$kind" >&2
    exit 1
  fi

  local job_json queue_name query_name max_pages pages_percent live_pages_percent
  job_json="$(curl -fsS "$API_BASE_URL/business/scrape/jobs/$job_id")"
  queue_name="$(printf '%s' "$job_json" | jq -r '.queue_name // ""')"
  if [[ "$queue_name" != "scrape_tripadvisor" ]]; then
    echo "replay-headfull currently supports only scrape_tripadvisor jobs. queue_name=$queue_name" >&2
    exit 1
  fi

  query_name="$(printf '%s' "$job_json" | jq -r '.source_name // .payload.source_name // .name // .payload.name // ""')"
  if [[ -z "$query_name" ]]; then
    echo "Could not resolve query name from job payload." >&2
    exit 1
  fi
  max_pages="$(printf '%s' "$job_json" | jq -r '
    if (.tripadvisor_max_pages | type) == "number" then (.tripadvisor_max_pages | floor)
    elif (.payload.tripadvisor_max_pages | type) == "number" then (.payload.tripadvisor_max_pages | floor)
    elif (.payload.interactive_max_rounds | type) == "number" and .payload.interactive_max_rounds > 0 then (.payload.interactive_max_rounds | floor)
    else 10
    end
  ')"
  pages_percent="$(printf '%s' "$job_json" | jq -r '
    if (.tripadvisor_pages_percent | type) == "number" then .tripadvisor_pages_percent
    elif (.payload.tripadvisor_pages_percent | type) == "number" then .payload.tripadvisor_pages_percent
    else empty
    end
  ')"
  if ! [[ "$max_pages" =~ ^[0-9]+$ ]] || [[ "$max_pages" -lt 1 ]]; then
    max_pages="10"
  fi
  live_pages_percent="${SCRAPER_TRIPADVISOR_LIVE_PAGES_PERCENT:-100}"
  if ! [[ "$live_pages_percent" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    live_pages_percent="100"
  fi
  if ! awk "BEGIN { exit !($live_pages_percent > 0 && $live_pages_percent <= 100) }"; then
    live_pages_percent="100"
  fi

  local python_bin
  python_bin="$(resolve_python_bin)"

  local was_running="false"
  if is_local_worker_running; then
    was_running="true"
    stop_local_worker
  fi

  terminate_playwright_sessions_for_profile "$DEFAULT_PROFILE_DIR"

  local delay_fixed
  delay_fixed="${SCRAPER_TRIPADVISOR_REPLAY_START_DELAY_SECONDS:-0}"
  local live_capture_dir live_capture_ts live_capture_json
  live_capture_dir="$REPO_ROOT/artifacts/tripadvisor_live_capture"
  mkdir -p "$live_capture_dir"
  live_capture_ts="$(date +%Y%m%d_%H%M%S)"
  live_capture_json="$live_capture_dir/live_capture_${job_id}_${live_capture_ts}.json"

  local -a cmd
  cmd=(
    "$python_bin"
    -u
    scripts/manual_chromium_session.py
    --url "https://www.tripadvisor.es"
    --persistent
    --profile-dir "$DEFAULT_PROFILE_DIR"
    --tripadvisor-query "$query_name"
    --tripadvisor-trigger auto
    --exit-after-tripadvisor-flow
    --max-pages "0"
    --tripadvisor-pages-percent "$live_pages_percent"
    --tripadvisor-start-delay-seconds "$delay_fixed"
    --tripadvisor-output-json "$live_capture_json"
  )

  echo "Replaying Tripadvisor job in headed mode:"
  echo "  job_id=$job_id"
  echo "  query=$query_name"
  echo "  max_pages=all (live)"
  echo "  pages_percent=$live_pages_percent"
  echo "  profile_dir=$DEFAULT_PROFILE_DIR"
  echo "  queue_name=$queue_name"
  echo
  printf 'Command: '
  printf '%q ' "${cmd[@]}" "$@"
  echo
  echo

  cd "$REPO_ROOT"
  local replay_exit=0
  set +e
  "${cmd[@]}" "$@"
  replay_exit=$?
  set -e

  if [[ "$was_running" == "true" ]]; then
    echo
    echo "Restoring local TripAdvisor worker..."
    start_local_worker_detached
  elif ! is_local_worker_running; then
    echo
    echo "Starting local TripAdvisor worker..."
    start_local_worker_detached
  fi

  if [[ "$replay_exit" -eq 0 ]]; then
    local live_commit_done="false"
    local live_capture_success="false"
    if [[ -f "$live_capture_json" ]]; then
      live_capture_success="$(jq -r '.success // false' "$live_capture_json" 2>/dev/null || printf 'false')"
      if [[ "$live_capture_success" == "true" ]]; then
        echo
        echo "Committing live captured reviews directly into DB for job: $job_id"
        local live_commit_payload live_commit_tmp live_commit_http_status live_commit_output
        live_commit_payload="$(jq -c \
          --arg capture_path "$live_capture_json" \
          '{
            listing: (.listing // {}),
            reviews: (.reviews // []),
            commit_reason: "live_session_capture",
            metadata: {
              capture_json_path: $capture_path,
              capture_success: (.success // false),
              capture_review_count: (.review_count // ((.reviews // []) | length))
            }
          }' "$live_capture_json" 2>/dev/null || true)"
        if [[ -n "$live_commit_payload" ]]; then
          live_commit_tmp="$(mktemp)"
          live_commit_http_status="$(curl -sS -o "$live_commit_tmp" -w '%{http_code}' -X POST \
            "$API_BASE_URL/business/scrape/jobs/$job_id/commit-live" \
            -H "accept: application/json" \
            -H "Content-Type: application/json" \
            -d "$live_commit_payload" || true)"
          live_commit_output="$(cat "$live_commit_tmp")"
          rm -f "$live_commit_tmp"
          if [[ "$live_commit_http_status" == "200" ]]; then
            live_commit_done="true"
            printf '%s\n' "$live_commit_output" | jq '{job_id,status,already_done,result_summary:{business_id:(.result.business_id // null),review_count:(.result.review_count // null),dataset_id:(.result.dataset_id // null),analysis_dataset_id:(.result.analysis_dataset_id // null)}}' 2>/dev/null || printf '%s\n' "$live_commit_output"
          else
            echo "Warning: live commit failed (HTTP $live_commit_http_status). Fallback relaunch will be attempted." >&2
            printf '%s\n' "$live_commit_output" >&2
          fi
        else
          echo "Warning: could not build live commit payload from $live_capture_json. Fallback relaunch will be attempted." >&2
        fi
      else
        echo "Live capture JSON exists but flow was not successful (success=false). Fallback relaunch will be attempted."
      fi
    else
      echo "Warning: live capture JSON not found at $live_capture_json. Fallback relaunch will be attempted." >&2
    fi

    echo
    echo "Syncing storage_state.json from profile after replay..."
    sync_storage_state_from_profile "$python_bin" "$DEFAULT_PROFILE_DIR" || {
      echo "Warning: storage_state sync failed after replay." >&2
    }

    echo "Confirming TripAdvisor session after replay..."
    local replay_confirm_payload replay_confirm_payload_legacy replay_confirm_output replay_confirm_http_status replay_confirm_tmp
    replay_confirm_payload="$(jq -n \
      --arg profile_dir "$DEFAULT_PROFILE_DIR" \
      '{profile_dir:$profile_dir,relaunch_pending_tripadvisor_jobs:false,force_relaunch_if_session_unavailable:true,relaunch_limit:1}')"
    replay_confirm_payload_legacy="$(jq -n \
      --arg profile_dir "$DEFAULT_PROFILE_DIR" \
      '{profile_dir:$profile_dir,relaunch_pending_tripadvisor_jobs:false,relaunch_limit:1}')"
    replay_confirm_tmp="$(mktemp)"
    replay_confirm_http_status="$(curl -sS -o "$replay_confirm_tmp" -w '%{http_code}' -X POST "$API_BASE_URL/tripadvisor/session-state/manual-confirm" \
      -H "accept: application/json" \
      -H "Content-Type: application/json" \
      -d "$replay_confirm_payload" || true)"
    replay_confirm_output="$(cat "$replay_confirm_tmp")"
    rm -f "$replay_confirm_tmp"
    if [[ "$replay_confirm_http_status" == "422" ]] && printf '%s' "$replay_confirm_output" | grep -q 'force_relaunch_if_session_unavailable'; then
      replay_confirm_output="$(curl -fsS -X POST "$API_BASE_URL/tripadvisor/session-state/manual-confirm" \
        -H "accept: application/json" \
        -H "Content-Type: application/json" \
        -d "$replay_confirm_payload_legacy" || true)"
    elif [[ "$replay_confirm_http_status" != "200" ]]; then
      echo "Warning: session confirm after replay failed (HTTP $replay_confirm_http_status)." >&2
      printf '%s\n' "$replay_confirm_output" >&2
      replay_confirm_output=""
    fi

    local replay_available replay_session_state replay_validation_result
    replay_available="$(printf '%s' "$replay_confirm_output" | jq -r '.session_state.availability_now // false' 2>/dev/null || printf 'false')"
    replay_session_state="$(printf '%s' "$replay_confirm_output" | jq -r '.session_state.session_state // "unknown"' 2>/dev/null || printf 'unknown')"
    replay_validation_result="$(printf '%s' "$replay_confirm_output" | jq -r '.session_state.last_validation_result // "unknown"' 2>/dev/null || printf 'unknown')"
    echo "Replay session state: session_state=$replay_session_state availability_now=$replay_available last_validation_result=$replay_validation_result"

    if [[ "$live_commit_done" == "true" ]]; then
      echo "Live commit completed successfully. Skipping automatic relaunch for job: $job_id"
    elif [[ "$replay_available" == "true" && "$replay_session_state" == "valid" ]]; then
      echo "Relaunching original job after replay: $job_id"
      local relaunch_payload replay_relaunch_http_status replay_relaunch_output replay_relaunch_tmp
      relaunch_payload='{"force":true,"restart_from_zero":true}'
      replay_relaunch_tmp="$(mktemp)"
      replay_relaunch_http_status="$(curl -sS -o "$replay_relaunch_tmp" -w '%{http_code}' -X POST "$API_BASE_URL/business/scrape/jobs/$job_id/relaunch" \
        -H "accept: application/json" \
        -H "Content-Type: application/json" \
        -d "$relaunch_payload" || true)"
      replay_relaunch_output="$(cat "$replay_relaunch_tmp")"
      rm -f "$replay_relaunch_tmp"
      if [[ "$replay_relaunch_http_status" == "200" ]]; then
        printf '%s\n' "$replay_relaunch_output" | jq '{job_id,status,queue_name,attempts,last_event:(.events[-1] // null)}'
      else
        echo "Warning: automatic relaunch after replay failed (HTTP $replay_relaunch_http_status)." >&2
        printf '%s\n' "$replay_relaunch_output" >&2
      fi
    else
      echo "Skipping automatic relaunch after replay because session is not valid/available." >&2
    fi
  fi

  if [[ "$replay_exit" -ne 0 ]]; then
    echo "Replay finished with error (exit_code=$replay_exit)." >&2
  fi
  return "$replay_exit"
}

cmd_trace() {
  require_cmd curl
  local job_id="${1:-}"
  local from_index="${2:-0}"
  if [[ -z "$job_id" ]]; then
    echo "Missing job_id. Usage: scripts/tripadvisor_ctl.sh trace <job_id> [from_index]" >&2
    exit 1
  fi
  local kind
  if ! kind="$(detect_job_kind "$job_id")"; then
    echo "Job not found in scrape/analyze: $job_id" >&2
    exit 1
  fi
  echo "Tracing $kind job: $job_id"
  curl -sS -N "$API_BASE_URL/business/$kind/jobs/$job_id/events?from_index=$from_index"
}

cmd_relaunch() {
  require_cmd curl
  require_cmd jq
  local job_id=""
  local force_relaunch="false"
  local restart_from_zero="false"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --force|-f)
        force_relaunch="true"
        shift
        ;;
      --from-zero|--zero|-z)
        restart_from_zero="true"
        force_relaunch="true"
        shift
        ;;
      *)
        if [[ -n "$job_id" ]]; then
          echo "Unexpected argument: $1" >&2
          echo "Usage: scripts/tripadvisor_ctl.sh relaunch <job_id> [--force] [--from-zero]" >&2
          exit 1
        fi
        job_id="$1"
        shift
        ;;
    esac
  done
  if [[ -z "$job_id" ]]; then
    echo "Missing job_id. Usage: scripts/tripadvisor_ctl.sh relaunch <job_id> [--force] [--from-zero]" >&2
    exit 1
  fi
  local kind
  if ! kind="$(detect_job_kind "$job_id")"; then
    echo "Job not found in scrape/analyze: $job_id" >&2
    exit 1
  fi

  if [[ "$kind" == "scrape" ]]; then
    local scrape_job_json
    scrape_job_json="$(curl -fsS "$API_BASE_URL/business/scrape/jobs/$job_id")"
    local queue_name
    queue_name="$(printf '%s' "$scrape_job_json" | jq -r '.queue_name // ""')"
    if [[ "$queue_name" == "scrape_tripadvisor" ]]; then
      local ta_state_json ta_available ta_session_state ta_last_validation
      ta_state_json="$(curl -fsS "$API_BASE_URL/tripadvisor/session-state")"
      ta_available="$(printf '%s' "$ta_state_json" | jq -r '.availability_now // false')"
      ta_session_state="$(printf '%s' "$ta_state_json" | jq -r '.session_state // "invalid"')"
      ta_last_validation="$(printf '%s' "$ta_state_json" | jq -r '.last_validation_result // "unknown"')"
      if [[ "$ta_available" != "true" ]]; then
        echo "Refusing relaunch: TripAdvisor session is not available." >&2
        echo "session_state=$ta_session_state last_validation_result=$ta_last_validation" >&2
        echo "Run recovery first:" >&2
        echo "  1) ./scripts/tripadvisor_ctl.sh human" >&2
        echo "  2) ./scripts/tripadvisor_ctl.sh session-confirm $DEFAULT_PROFILE_DIR true" >&2
        exit 2
      fi
    fi
  fi

  curl -fsS -X POST "$API_BASE_URL/business/$kind/jobs/$job_id/relaunch" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -d "$(jq -n \
      --argjson force "$force_relaunch" \
      --argjson restart_from_zero "$restart_from_zero" \
      '{force:$force,restart_from_zero:$restart_from_zero}')" \
    | jq '{job_id,status,queue_name,job_type,attempts,updated_at,last_event:(.events[-1] // null)}'
}

cmd_human() {
  require_cmd curl
  require_cmd jq
  local python_bin
  python_bin="$(resolve_python_bin)"
  stop_local_worker

  local profile_path="$REPO_ROOT/$DEFAULT_PROFILE_DIR"
  mkdir -p "$profile_path"
  if command -v sudo >/dev/null 2>&1; then
    if sudo -n true >/dev/null 2>&1; then
      sudo chown -R "$USER:$USER" "$profile_path" || true
    else
      chown -R "$USER:$USER" "$profile_path" || true
    fi
  else
    chown -R "$USER:$USER" "$profile_path" || true
  fi
  find "$profile_path" -maxdepth 1 -name 'Singleton*' -delete || true

  cd "$REPO_ROOT"
  "$python_bin" scripts/manual_chromium_session.py \
    --url "https://www.tripadvisor.es" \
    --persistent \
    --profile-dir "$DEFAULT_PROFILE_DIR" \
    --no-tripadvisor-flow \
    "$@"

  echo
  echo "Manual session closed. Syncing storage_state.json from profile..."
  sync_storage_state_from_profile "$python_bin" "$DEFAULT_PROFILE_DIR"

  echo
  echo "Confirming TripAdvisor session in API..."
  local confirm_payload confirm_payload_legacy confirm_output confirm_http_status confirm_tmp
  confirm_payload="$(jq -n \
    --arg profile_dir "$DEFAULT_PROFILE_DIR" \
    '{profile_dir:$profile_dir,relaunch_pending_tripadvisor_jobs:true,force_relaunch_if_session_unavailable:true,relaunch_limit:500}')"
  confirm_payload_legacy="$(jq -n \
    --arg profile_dir "$DEFAULT_PROFILE_DIR" \
    '{profile_dir:$profile_dir,relaunch_pending_tripadvisor_jobs:true,relaunch_limit:500}')"
  confirm_tmp="$(mktemp)"
  confirm_http_status="$(curl -sS -o "$confirm_tmp" -w '%{http_code}' -X POST "$API_BASE_URL/tripadvisor/session-state/manual-confirm" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -d "$confirm_payload" || true)"
  confirm_output="$(cat "$confirm_tmp")"
  rm -f "$confirm_tmp"
  if [[ "$confirm_http_status" == "422" ]] && printf '%s' "$confirm_output" | grep -q 'force_relaunch_if_session_unavailable'; then
    echo "API running legacy manual-confirm schema (422 on force_relaunch_if_session_unavailable). Retrying with legacy payload..."
    confirm_output="$(curl -fsS -X POST "$API_BASE_URL/tripadvisor/session-state/manual-confirm" \
      -H "accept: application/json" \
      -H "Content-Type: application/json" \
      -d "$confirm_payload_legacy")"
  elif [[ "$confirm_http_status" != "200" ]]; then
    echo "manual-confirm failed (HTTP $confirm_http_status)." >&2
    printf '%s\n' "$confirm_output" >&2
    return 2
  fi
  printf '%s\n' "$confirm_output" | jq .

  local availability session_state validation_result confirm_matched confirm_relaunched
  availability="$(printf '%s' "$confirm_output" | jq -r '.session_state.availability_now // false')"
  session_state="$(printf '%s' "$confirm_output" | jq -r '.session_state.session_state // "invalid"')"
  validation_result="$(printf '%s' "$confirm_output" | jq -r '.session_state.last_validation_result // "unknown"')"
  confirm_matched="$(printf '%s' "$confirm_output" | jq -r '.relaunch.matched_jobs // 0')"
  confirm_relaunched="$(printf '%s' "$confirm_output" | jq -r '(.relaunch.relaunched_jobs // []) | length')"

  echo "Restarting local TripAdvisor worker..."
  start_local_worker_detached
  echo "Initial relaunch after confirm (needs_human): matched=$confirm_matched relaunched=$confirm_relaunched"

  if [[ "$availability" == "true" && "$session_state" == "valid" ]]; then
    echo "TripAdvisor session is VALID (last_validation_result=$validation_result)."
    echo
    echo "Ensuring pending TripAdvisor jobs are relaunched (failed/needs_human)..."
    local relaunch_payload relaunch_payload_legacy relaunch_output relaunch_matched relaunch_count relaunch_errors relaunch_http_status relaunch_tmp
    relaunch_payload="$(jq -n \
      --arg reason "Automatic relaunch after human manual session." \
      '{limit:200,status_filter:"failed_or_needs_human",reason:$reason}')"
    relaunch_payload_legacy='{"limit":200}'
    relaunch_tmp="$(mktemp)"
    relaunch_http_status="$(curl -sS -o "$relaunch_tmp" -w '%{http_code}' -X POST "$API_BASE_URL/business/scrape/jobs/tripadvisor/antibot/relaunch" \
      -H "accept: application/json" \
      -H "Content-Type: application/json" \
      -d "$relaunch_payload" || true)"
    relaunch_output="$(cat "$relaunch_tmp")"
    rm -f "$relaunch_tmp"
    if [[ "$relaunch_http_status" == "422" ]] && printf '%s' "$relaunch_output" | grep -Eq 'status_filter|reason'; then
      echo "API running legacy antibot relaunch schema (422 on status_filter/reason). Retrying with legacy payload..."
      relaunch_output="$(curl -fsS -X POST "$API_BASE_URL/business/scrape/jobs/tripadvisor/antibot/relaunch" \
        -H "accept: application/json" \
        -H "Content-Type: application/json" \
        -d "$relaunch_payload_legacy")"
    elif [[ "$relaunch_http_status" != "200" ]]; then
      echo "Warning: antibot bulk relaunch failed (HTTP $relaunch_http_status)." >&2
      printf '%s\n' "$relaunch_output" >&2
      echo "Manual session is already confirmed; you can continue and relaunch jobs from UI/API." >&2
      return 0
    fi
    printf '%s\n' "$relaunch_output" | jq .
    relaunch_matched="$(printf '%s' "$relaunch_output" | jq -r '.matched_jobs // 0')"
    relaunch_count="$(printf '%s' "$relaunch_output" | jq -r '(.relaunched_jobs // []) | length')"
    relaunch_errors="$(printf '%s' "$relaunch_output" | jq -r '(.errors // []) | length')"
    echo "Final relaunch summary: matched=$relaunch_matched relaunched=$relaunch_count errors=$relaunch_errors"
    return 0
  fi

  echo "TripAdvisor session is NOT valid after manual flow." >&2
  echo "session_state=$session_state availability_now=$availability last_validation_result=$validation_result" >&2
  echo "Only needs_human jobs were relaunched (forced confirm). Failed jobs were not auto-relaunched." >&2
  echo "Run status for details: ./scripts/tripadvisor_ctl.sh status" >&2
  return 0
}

cmd_session_confirm() {
  require_cmd curl
  require_cmd jq
  local profile_dir="${1:-$DEFAULT_PROFILE_DIR}"
  local relaunch_pending="${2:-false}"
  if [[ "$relaunch_pending" != "true" && "$relaunch_pending" != "false" ]]; then
    echo "relaunch_pending must be true|false (received: $relaunch_pending)" >&2
    exit 1
  fi
  local payload
  payload="$(jq -n \
    --arg profile_dir "$profile_dir" \
    --argjson relaunch_pending_tripadvisor_jobs "$relaunch_pending" \
    '{profile_dir:$profile_dir,relaunch_pending_tripadvisor_jobs:$relaunch_pending_tripadvisor_jobs}')"
  curl -fsS -X POST "$API_BASE_URL/tripadvisor/session-state/manual-confirm" \
    -H "accept: application/json" \
    -H "Content-Type: application/json" \
    -d "$payload" \
    | jq .
}

main() {
  local cmd="${1:-status}"
  shift || true
  case "$cmd" in
    bridge-up) cmd_bridge_up "$@" ;;
    bridge-down) cmd_bridge_down "$@" ;;
    bridge-status) cmd_bridge_status "$@" ;;
    local-worker-start) cmd_local_worker_start "$@" ;;
    local-worker-stop) cmd_local_worker_stop "$@" ;;
    up) cmd_up "$@" ;;
    rebuild) cmd_rebuild "$@" ;;
    down) cmd_down "$@" ;;
    status) cmd_status "$@" ;;
    logs) cmd_logs "$@" ;;
    scrape) cmd_scrape "$@" ;;
    job) cmd_job "$@" ;;
    payload) cmd_payload "$@" ;;
    replay-headfull) cmd_replay_headfull "$@" ;;
    trace) cmd_trace "$@" ;;
    relaunch) cmd_relaunch "$@" ;;
    human) cmd_human "$@" ;;
    session-confirm) cmd_session_confirm "$@" ;;
    help|-h|--help) usage ;;
    *)
      echo "Unknown command: $cmd" >&2
      echo >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
