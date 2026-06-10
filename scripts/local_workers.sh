#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR="${REPIQ_LOCAL_WORKERS_DIR:-$REPO_ROOT/artifacts/local-workers}"
PID_DIR="$WORK_DIR/pids"
LOG_DIR="$WORK_DIR/logs"
DEFAULT_PROFILE="${REPIQ_LOCAL_WORKERS_DEFAULT_PROFILE:-all}"
PYTHON_BIN="${REPIQ_PYTHON_BIN:-}"
MANAGER_DIR="${REPIQ_MANAGER_DIR:-$REPO_ROOT/apps/manager}"
MANAGER_HOST="${REPIQ_MANAGER_HOST:-0.0.0.0}"
MANAGER_PORT="${REPIQ_MANAGER_PORT:-4173}"

mkdir -p "$PID_DIR" "$LOG_DIR"

usage() {
  cat <<'USAGE'
Usage:
  scripts/local_workers.sh <command> [selector ...]

Commands:
  list
      Show available worker names and profiles.

  start [selector ...]
      Start selected processes if they are not already running.

  start-build [selector ...]
      Stop selected processes, rebuild what needs a build step, and start again.

  stop [selector ...]
      Stop selected processes.

  restart [selector ...]
      Stop and start selected processes.

  status [selector ...]
      Show running/stopped status for selected processes.

  logs <worker-name-or-single-selector> [tail_lines]
      Tail one managed log file.

Selectors:
  front   frontend only
  host    host-side local workers
  queue   classic queue workers
  all     every managed process

Managed processes:
  manager-ui
  local-browser-runtime
  supabase-queue
  report-requests
  analysis
  report
  crm
  scraper

Examples:
  scripts/local_workers.sh start
  scripts/local_workers.sh start front
  scripts/local_workers.sh start queue
  scripts/local_workers.sh start-build
  scripts/local_workers.sh stop
  scripts/local_workers.sh status all
  scripts/local_workers.sh logs front
USAGE
}

resolve_python_bin() {
  if [[ -n "$PYTHON_BIN" && -x "$PYTHON_BIN" ]]; then
    printf '%s\n' "$PYTHON_BIN"
    return 0
  fi
  if [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
    printf '%s\n' "$REPO_ROOT/.venv/bin/python"
    return 0
  fi
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
  echo "python3 not found and no virtualenv python available." >&2
  exit 1
}

worker_names() {
  cat <<'EOFN'
manager-ui
local-browser-runtime
supabase-queue
report-requests
analysis
report
crm
scraper
EOFN
}

is_worker_name() {
  local candidate="$1"
  while IFS= read -r name; do
    [[ "$candidate" == "$name" ]] && return 0
  done < <(worker_names)
  return 1
}

worker_description() {
  case "$1" in
    manager-ui) echo "Frontend preview server for apps/manager" ;;
    local-browser-runtime) echo "Local Playwright runtime for browser-driven jobs" ;;
    supabase-queue) echo "Supabase form/job pull worker" ;;
    report-requests) echo "Pending report-request processor" ;;
    analysis) echo "Classic analysis queue worker" ;;
    report) echo "Classic report queue worker" ;;
    crm) echo "Classic CRM queue worker" ;;
    scraper) echo "Classic scraper queue worker" ;;
    *) return 1 ;;
  esac
}

worker_endpoint() {
  case "$1" in
    manager-ui) printf 'http://127.0.0.1:%s\n' "$MANAGER_PORT" ;;
    *) return 1 ;;
  esac
}

worker_patterns() {
  case "$1" in
    manager-ui)
      printf '%s\n' \
        "vite/bin/vite.js preview --host $MANAGER_HOST --port $MANAGER_PORT" \
        "vite preview --host $MANAGER_HOST --port $MANAGER_PORT"
      ;;
    local-browser-runtime) printf '%s\n' "scripts/run_local_browser_runtime_worker.py" ;;
    supabase-queue) printf '%s\n' "scripts/run_supabase_queue_worker.py" ;;
    report-requests) printf '%s\n' "scripts/run_report_requests_worker.py" ;;
    analysis) printf '%s\n' "src.workers.analysis_worker" ;;
    report) printf '%s\n' "src.workers.report_worker" ;;
    crm) printf '%s\n' "src.workers.crm_worker" ;;
    scraper) printf '%s\n' "src.workers.scraper_worker" ;;
    *) return 1 ;;
  esac
}

worker_command() {
  local python_bin="$1"
  local name="$2"
  case "$name" in
    manager-ui)
      printf 'cd %q && exec node ./node_modules/vite/bin/vite.js preview --host %q --port %q' "$MANAGER_DIR" "$MANAGER_HOST" "$MANAGER_PORT"
      ;;
    local-browser-runtime)
      printf 'cd %q && export PYTHONPATH=%q${PYTHONPATH:+:$PYTHONPATH} && exec %q -u scripts/run_local_browser_runtime_worker.py' "$REPO_ROOT" "$REPO_ROOT" "$python_bin"
      ;;
    supabase-queue)
      printf 'cd %q && export PYTHONPATH=%q${PYTHONPATH:+:$PYTHONPATH} && exec %q -u scripts/run_supabase_queue_worker.py' "$REPO_ROOT" "$REPO_ROOT" "$python_bin"
      ;;
    report-requests)
      printf 'cd %q && export PYTHONPATH=%q${PYTHONPATH:+:$PYTHONPATH} && exec %q -u scripts/run_report_requests_worker.py' "$REPO_ROOT" "$REPO_ROOT" "$python_bin"
      ;;
    analysis)
      printf 'cd %q && export PYTHONPATH=%q${PYTHONPATH:+:$PYTHONPATH} && exec %q -u -m src.workers.analysis_worker' "$REPO_ROOT" "$REPO_ROOT" "$python_bin"
      ;;
    report)
      printf 'cd %q && export PYTHONPATH=%q${PYTHONPATH:+:$PYTHONPATH} && exec %q -u -m src.workers.report_worker' "$REPO_ROOT" "$REPO_ROOT" "$python_bin"
      ;;
    crm)
      printf 'cd %q && export PYTHONPATH=%q${PYTHONPATH:+:$PYTHONPATH} && exec %q -u -m src.workers.crm_worker' "$REPO_ROOT" "$REPO_ROOT" "$python_bin"
      ;;
    scraper)
      printf 'cd %q && export PYTHONPATH=%q${PYTHONPATH:+:$PYTHONPATH} && exec %q -u -m src.workers.scraper_worker' "$REPO_ROOT" "$REPO_ROOT" "$python_bin"
      ;;
    *) return 1 ;;
  esac
}

build_worker() {
  local name="$1"
  case "$name" in
    manager-ui)
      if [[ ! -d "$MANAGER_DIR/node_modules" ]]; then
        echo "[build] manager-ui installing dependencies"
        (cd "$MANAGER_DIR" && npm install)
      fi
      echo "[build] manager-ui npm run build"
      (cd "$MANAGER_DIR" && npm run build)
      ;;
  esac
}

pid_file() {
  printf '%s/%s.pid\n' "$PID_DIR" "$1"
}

log_file() {
  printf '%s/%s.log\n' "$LOG_DIR" "$1"
}

args_match_worker() {
  local name="$1"
  local args="$2"
  local pattern
  while IFS= read -r pattern; do
    [[ -n "$pattern" && "$args" == *"$pattern"* ]] && return 0
  done < <(worker_patterns "$name")
  return 1
}

process_matches_pid() {
  local name="$1"
  local pid="$2"
  local args
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    return 1
  fi
  args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ -n "$args" ]] || return 1
  args_match_worker "$name" "$args"
}

find_running_pid() {
  local name="$1"
  local file pid found pattern
  file="$(pid_file "$name")"
  if [[ -f "$file" ]]; then
    pid="$(tr -d '[:space:]' < "$file" || true)"
    if [[ -n "$pid" ]] && process_matches_pid "$name" "$pid"; then
      printf '%s\n' "$pid"
      return 0
    fi
    rm -f "$file"
  fi

  while IFS= read -r pattern; do
    [[ -n "$pattern" ]] || continue
    found="$(pgrep -f "$pattern" | head -n 1 || true)"
    if [[ -n "$found" ]] && process_matches_pid "$name" "$found"; then
      printf '%s\n' "$found" > "$file"
      printf '%s\n' "$found"
      return 0
    fi
  done < <(worker_patterns "$name")
  return 1
}

matching_pids() {
  local name="$1"
  local pattern
  while IFS= read -r pattern; do
    [[ -n "$pattern" ]] || continue
    pgrep -f "$pattern" || true
  done < <(worker_patterns "$name")
}

expand_selectors() {
  if [[ "$#" -eq 0 ]]; then
    set -- "$DEFAULT_PROFILE"
  fi

  declare -A seen=()
  local selector name
  for selector in "$@"; do
    case "$selector" in
      front)
        seen["manager-ui"]=1
        ;;
      host)
        for name in local-browser-runtime supabase-queue report-requests; do
          seen["$name"]=1
        done
        ;;
      queue)
        for name in analysis report crm scraper; do
          seen["$name"]=1
        done
        ;;
      all)
        while IFS= read -r name; do
          seen["$name"]=1
        done < <(worker_names)
        ;;
      *)
        if is_worker_name "$selector"; then
          seen["$selector"]=1
        else
          echo "Unknown selector or worker: $selector" >&2
          exit 1
        fi
        ;;
    esac
  done

  while IFS= read -r name; do
    if [[ -n "${seen[$name]:-}" ]]; then
      printf '%s\n' "$name"
    fi
  done < <(worker_names)
}

resolve_single_log_target() {
  local target="$1"
  local expanded=()
  local name

  if is_worker_name "$target"; then
    printf '%s\n' "$target"
    return 0
  fi

  while IFS= read -r name; do
    expanded+=("$name")
  done < <(expand_selectors "$target")

  if [[ "${#expanded[@]}" -ne 1 ]]; then
    echo "logs requires a single worker or a selector that expands to one worker." >&2
    exit 1
  fi

  printf '%s\n' "${expanded[0]}"
}

start_worker() {
  local name="$1"
  local python_bin command file log pid endpoint
  if pid="$(find_running_pid "$name")"; then
    endpoint="$(worker_endpoint "$name" 2>/dev/null || true)"
    if [[ -n "$endpoint" ]]; then
      echo "[skip] $name already running (pid=$pid, url=$endpoint)"
    else
      echo "[skip] $name already running (pid=$pid)"
    fi
    return 0
  fi

  python_bin="$(resolve_python_bin)"
  command="$(worker_command "$python_bin" "$name")"
  file="$(pid_file "$name")"
  log="$(log_file "$name")"
  touch "$log"

  setsid bash -lc "$command" >>"$log" 2>&1 &
  pid="$!"
  printf '%s\n' "$pid" > "$file"

  for _ in $(seq 1 40); do
    if process_matches_pid "$name" "$pid"; then
      endpoint="$(worker_endpoint "$name" 2>/dev/null || true)"
      if [[ -n "$endpoint" ]]; then
        echo "[ok] $name started (pid=$pid, url=$endpoint, log=$log)"
      else
        echo "[ok] $name started (pid=$pid, log=$log)"
      fi
      return 0
    fi
    if discovered_pid="$(find_running_pid "$name" 2>/dev/null || true)" && [[ -n "$discovered_pid" ]]; then
      printf '%s\n' "$discovered_pid" > "$file"
      endpoint="$(worker_endpoint "$name" 2>/dev/null || true)"
      if [[ -n "$endpoint" ]]; then
        echo "[ok] $name started (pid=$discovered_pid, url=$endpoint, log=$log)"
      else
        echo "[ok] $name started (pid=$discovered_pid, log=$log)"
      fi
      return 0
    fi
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done

  rm -f "$file"
  echo "[error] failed to start $name. Check log: $log" >&2
  return 1
}

stop_worker() {
  local name="$1"
  local pid file extra_pid
  file="$(pid_file "$name")"
  if ! pid="$(find_running_pid "$name")"; then
    echo "[skip] $name not running"
    rm -f "$file"
    return 0
  fi

  kill -- "-$pid" >/dev/null 2>&1 || kill "$pid" >/dev/null 2>&1 || true
  for _ in $(seq 1 40); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      rm -f "$file"
      echo "[ok] $name stopped"
      return 0
    fi
    sleep 0.25
  done

  while IFS= read -r extra_pid; do
    [[ -n "$extra_pid" ]] || continue
    kill -- "-$extra_pid" >/dev/null 2>&1 || kill "$extra_pid" >/dev/null 2>&1 || true
  done < <(matching_pids "$name" | sort -u)
  sleep 0.5

  while IFS= read -r extra_pid; do
    [[ -n "$extra_pid" ]] || continue
    kill -9 -- "-$extra_pid" >/dev/null 2>&1 || kill -9 "$extra_pid" >/dev/null 2>&1 || true
  done < <(matching_pids "$name" | sort -u)

  kill -9 -- "-$pid" >/dev/null 2>&1 || kill -9 "$pid" >/dev/null 2>&1 || true
  rm -f "$file"
  echo "[ok] $name killed"
}

status_worker() {
  local name="$1"
  local pid log endpoint
  log="$(log_file "$name")"
  endpoint="$(worker_endpoint "$name" 2>/dev/null || true)"
  if pid="$(find_running_pid "$name")"; then
    if [[ -n "$endpoint" ]]; then
      echo "[running] $name pid=$pid url=$endpoint log=$log"
    else
      echo "[running] $name pid=$pid log=$log"
    fi
  else
    if [[ -n "$endpoint" ]]; then
      echo "[stopped] $name url=$endpoint log=$log"
    else
      echo "[stopped] $name log=$log"
    fi
  fi
}

cmd_list() {
  echo "Profiles:"
  echo "  front -> manager-ui"
  echo "  host  -> local-browser-runtime, supabase-queue, report-requests"
  echo "  queue -> analysis, report, crm, scraper"
  echo "  all   -> every managed process"
  echo
  echo "Managed processes:"
  while IFS= read -r name; do
    if endpoint="$(worker_endpoint "$name" 2>/dev/null || true)" && [[ -n "$endpoint" ]]; then
      printf '  %-22s %s [%s]\n' "$name" "$(worker_description "$name")" "$endpoint"
    else
      printf '  %-22s %s\n' "$name" "$(worker_description "$name")"
    fi
  done < <(worker_names)
}

cmd_start() {
  local name failures=0
  while IFS= read -r name; do
    start_worker "$name" || failures=$((failures + 1))
  done < <(expand_selectors "$@")
  return "$failures"
}

cmd_start_build() {
  local selected=()
  local name failures=0
  while IFS= read -r name; do
    selected+=("$name")
  done < <(expand_selectors "$@")

  for name in "${selected[@]}"; do
    stop_worker "$name"
  done
  for name in "${selected[@]}"; do
    build_worker "$name"
  done
  for name in "${selected[@]}"; do
    start_worker "$name" || failures=$((failures + 1))
  done
  return "$failures"
}

cmd_stop() {
  local name
  while IFS= read -r name; do
    stop_worker "$name"
  done < <(expand_selectors "$@")
}

cmd_restart() {
  cmd_stop "$@"
  cmd_start "$@"
}

cmd_status() {
  local name
  while IFS= read -r name; do
    status_worker "$name"
  done < <(expand_selectors "$@")
}

cmd_logs() {
  local raw_target="${1:-}"
  local tail_lines="${2:-120}"
  local name log
  if [[ -z "$raw_target" ]]; then
    echo "Usage: scripts/local_workers.sh logs <worker-name-or-single-selector> [tail_lines]" >&2
    exit 1
  fi
  name="$(resolve_single_log_target "$raw_target")"
  log="$(log_file "$name")"
  mkdir -p "$(dirname "$log")"
  touch "$log"
  tail -n "$tail_lines" -f "$log"
}

main() {
  local command="${1:-}"
  shift || true
  case "$command" in
    list) cmd_list ;;
    start) cmd_start "$@" ;;
    start-build|build-start) cmd_start_build "$@" ;;
    stop) cmd_stop "$@" ;;
    restart) cmd_restart "$@" ;;
    status) cmd_status "$@" ;;
    logs) cmd_logs "$@" ;;
    help|-h|--help|"") usage ;;
    *)
      echo "Unknown command: $command" >&2
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
