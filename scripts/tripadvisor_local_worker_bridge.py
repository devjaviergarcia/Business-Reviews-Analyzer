from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

REPO_ROOT = Path(__file__).resolve().parents[1]
TRIPADVISOR_CTL_SCRIPT = REPO_ROOT / "scripts" / "tripadvisor_ctl.sh"
LOCAL_WORKER_PID_FILE = Path(
    str(os.getenv("LOCAL_WORKER_PID_FILE", REPO_ROOT / ".tripadvisor_local_worker.pid"))
).expanduser()
LOCAL_WORKER_LOG_FILE = Path(
    str(os.getenv("LOCAL_WORKER_LOG_FILE", REPO_ROOT / "artifacts/tripadvisor_local_worker.log"))
).expanduser()
LIVE_SESSION_PID_FILE = Path(
    str(os.getenv("LIVE_SESSION_PID_FILE", REPO_ROOT / ".tripadvisor_live_session.pid"))
).expanduser()
LIVE_SESSION_LOG_FILE = Path(
    str(os.getenv("LIVE_SESSION_LOG_FILE", REPO_ROOT / "artifacts/tripadvisor_live_session.log"))
).expanduser()
LIVE_SESSION_STATE_FILE = Path(
    str(os.getenv("LIVE_SESSION_STATE_FILE", REPO_ROOT / ".tripadvisor_live_session_state.json"))
).expanduser()
LIVE_SESSION_DEFAULT_DISPLAY = str(
    os.getenv("TRIPADVISOR_LIVE_SESSION_DISPLAY", os.getenv("DISPLAY", ":0"))
).strip() or ":0"

app = FastAPI(
    title="Tripadvisor Local Worker Bridge",
    version="0.1.0",
    docs_url="/docs",
    redoc_url=None,
)


class EnsureStartedRequest(BaseModel):
    use_xvfb: bool = True
    reason: str = Field(default="api_enqueue")
    timeout_seconds: float = Field(default=120.0, ge=5.0, le=600.0)

    model_config = ConfigDict(extra="forbid")


class StopWorkerRequest(BaseModel):
    timeout_seconds: float = Field(default=60.0, ge=2.0, le=300.0)

    model_config = ConfigDict(extra="forbid")


class LaunchLiveSessionRequest(BaseModel):
    reason: str = Field(default="needs_human_live")
    display: str | None = None
    profile_dir: str | None = None
    job_id: str | None = None

    model_config = ConfigDict(extra="forbid")


def _read_pid_file() -> int | None:
    if not LOCAL_WORKER_PID_FILE.exists():
        return None
    raw_pid = LOCAL_WORKER_PID_FILE.read_text(encoding="utf-8").strip()
    if not raw_pid:
        return None
    try:
        return int(raw_pid)
    except ValueError:
        return None


def _read_pid_file_path(pid_file: Path) -> int | None:
    if not pid_file.exists():
        return None
    raw_pid = pid_file.read_text(encoding="utf-8").strip()
    if not raw_pid:
        return None
    try:
        return int(raw_pid)
    except ValueError:
        return None


def _pid_is_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        try:
            proc = subprocess.run(
                ["ps", "-o", "stat=", "-p", str(pid)],
                capture_output=True,
                text=True,
                check=False,
            )
            state = str(proc.stdout or "").strip()
            if state.startswith("Z"):
                return False
        except Exception:
            pass
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _pid_cmdline(pid: int | None) -> str:
    if pid is None or pid <= 0:
        return ""
    proc_cmdline = Path(f"/proc/{pid}/cmdline")
    try:
        raw = proc_cmdline.read_bytes()
        text = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
        if text:
            return text
    except Exception:
        pass
    try:
        proc = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=False,
        )
        return str(proc.stdout or "").strip()
    except Exception:
        return ""


def _pid_looks_like_live_session(pid: int | None) -> bool:
    cmdline = _pid_cmdline(pid).lower()
    if not cmdline:
        return False
    markers = (
        "tripadvisor_ctl.sh replay-headfull",
        "tripadvisor_ctl.sh human",
        "manual_chromium_session.py",
    )
    return any(marker in cmdline for marker in markers)


def _current_worker_status() -> dict[str, Any]:
    pid = _read_pid_file()
    running = _pid_is_alive(pid)
    if pid is not None and not running:
        try:
            LOCAL_WORKER_PID_FILE.unlink(missing_ok=True)
        except Exception:
            pass
    return {
        "running": bool(running),
        "pid": pid if running else None,
        "pid_file": LOCAL_WORKER_PID_FILE.as_posix(),
        "log_file": LOCAL_WORKER_LOG_FILE.as_posix(),
    }


def _current_live_session_status() -> dict[str, Any]:
    now_ts = time.time()
    pid = _read_pid_file_path(LIVE_SESSION_PID_FILE)
    running = _pid_is_alive(pid)
    stale_pid_reused = False
    if running and not _pid_looks_like_live_session(pid):
        running = False
        stale_pid_reused = True
    state_snapshot = _read_live_session_state()
    if pid is not None and not running:
        try:
            LIVE_SESSION_PID_FILE.unlink(missing_ok=True)
        except Exception:
            pass

    if running:
        state = "running"
        finished_reason: str | None = None
        if state_snapshot.get("state") != "running" or int(state_snapshot.get("pid") or 0) != int(pid or 0):
            _write_live_session_state(
                {
                    "state": "running",
                    "pid": int(pid or 0),
                    "updated_at_ts": now_ts,
                    "finished_reason": None,
                }
            )
    else:
        state = str(state_snapshot.get("state") or "finished")
        if state == "running":
            state = "finished"
        finished_reason = (
            ("stale_pid_reused" if stale_pid_reused else None)
            or _extract_last_live_exit_reason()
            or str(state_snapshot.get("finished_reason") or "").strip()
            or None
        )
        _write_live_session_state(
            {
                "state": "finished",
                "pid": None,
                "updated_at_ts": now_ts,
                "finished_reason": finished_reason,
            }
        )
    return {
        "running": bool(running),
        "pid": pid if running else None,
        "pid_file": LIVE_SESSION_PID_FILE.as_posix(),
        "log_file": LIVE_SESSION_LOG_FILE.as_posix(),
        "state": state,
        "finished_reason": finished_reason,
    }


def _run_tripadvisor_ctl(command_args: list[str], *, timeout_seconds: float) -> dict[str, Any]:
    if not TRIPADVISOR_CTL_SCRIPT.exists():
        raise RuntimeError(f"Script not found: {TRIPADVISOR_CTL_SCRIPT}")
    cmd = ["bash", str(TRIPADVISOR_CTL_SCRIPT), *command_args]
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        timeout=max(1.0, float(timeout_seconds)),
        check=False,
    )
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    if proc.returncode != 0:
        detail = {
            "command": cmd,
            "exit_code": proc.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }
        raise RuntimeError(f"tripadvisor_ctl failed: {detail}")
    return {"stdout": stdout, "stderr": stderr}


def _tail_file(path: Path, *, max_chars: int = 4000) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    if len(content) <= max_chars:
        return content
    return content[-max_chars:]


def _read_live_session_state() -> dict[str, Any]:
    if not LIVE_SESSION_STATE_FILE.exists():
        return {}
    try:
        import json

        raw = LIVE_SESSION_STATE_FILE.read_text(encoding="utf-8")
        parsed = json.loads(raw)
    except Exception:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _write_live_session_state(payload: dict[str, Any]) -> None:
    try:
        import json

        LIVE_SESSION_STATE_FILE.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def _extract_last_live_exit_reason() -> str | None:
    marker = "[live-session-exit] reason="
    tail = _tail_file(LIVE_SESSION_LOG_FILE, max_chars=24000)
    if not tail:
        return None
    for line in reversed(tail.splitlines()):
        if marker not in line:
            continue
        raw_reason = line.split(marker, 1)[1].strip()
        reason = raw_reason.split()[0] if raw_reason else ""
        if reason:
            return reason
    return None


@app.get("/health")
def bridge_health() -> dict[str, Any]:
    return {
        "ok": True,
        "bridge": "tripadvisor-local-worker",
        "repo_root": REPO_ROOT.as_posix(),
        "worker": _current_worker_status(),
        "live_session": _current_live_session_status(),
    }


@app.get("/worker/status")
def worker_status() -> dict[str, Any]:
    return {
        "ok": True,
        "worker": _current_worker_status(),
    }


@app.post("/worker/start")
def worker_start(payload: EnsureStartedRequest) -> dict[str, Any]:
    worker = _current_worker_status()
    if worker["running"]:
        return {
            "ok": True,
            "already_running": True,
            "worker": worker,
        }
    args = ["local-worker-start"]
    if payload.use_xvfb:
        args.append("--xvfb")
    try:
        run_result = _run_tripadvisor_ctl(args, timeout_seconds=payload.timeout_seconds)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    worker_after = _current_worker_status()
    if not worker_after["running"]:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Tripadvisor local worker did not start successfully.",
                "worker": worker_after,
                "run_result": run_result,
            },
        )
    return {
        "ok": True,
        "already_running": False,
        "reason": payload.reason,
        "worker": worker_after,
        "run_result": run_result,
    }


@app.post("/worker/ensure-started")
def worker_ensure_started(payload: EnsureStartedRequest) -> dict[str, Any]:
    return worker_start(payload)


@app.post("/worker/stop")
def worker_stop(payload: StopWorkerRequest | None = None) -> dict[str, Any]:
    effective_payload = payload or StopWorkerRequest()
    try:
        run_result = _run_tripadvisor_ctl(
            ["local-worker-stop"],
            timeout_seconds=effective_payload.timeout_seconds,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {
        "ok": True,
        "worker": _current_worker_status(),
        "run_result": run_result,
    }


@app.get("/live-session/status")
def live_session_status() -> dict[str, Any]:
    return {
        "ok": True,
        "live_session": _current_live_session_status(),
    }


@app.get("/live-session/log-tail")
def live_session_log_tail(max_chars: int = 6000) -> dict[str, Any]:
    safe_max = max(200, min(int(max_chars), 50000))
    return {
        "ok": True,
        "live_session": _current_live_session_status(),
        "log_file": LIVE_SESSION_LOG_FILE.as_posix(),
        "log_tail": _tail_file(LIVE_SESSION_LOG_FILE, max_chars=safe_max),
    }


@app.post("/live-session/launch")
def live_session_launch(payload: LaunchLiveSessionRequest) -> dict[str, Any]:
    current = _current_live_session_status()
    if current["running"]:
        return {
            "ok": True,
            "already_running": True,
            "live_session": current,
        }

    if not TRIPADVISOR_CTL_SCRIPT.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Script not found: {TRIPADVISOR_CTL_SCRIPT}",
        )

    LIVE_SESSION_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    selected_display = (str(payload.display).strip() if payload.display else "") or LIVE_SESSION_DEFAULT_DISPLAY
    if selected_display:
        env["DISPLAY"] = selected_display
    if payload.profile_dir:
        env["SCRAPER_TRIPADVISOR_USER_DATA_DIR_LOCAL"] = str(payload.profile_dir).strip()

    requested_job_id = str(payload.job_id or "").strip()
    if requested_job_id:
        mode = "replay_headfull"
        cmd = ["bash", str(TRIPADVISOR_CTL_SCRIPT), "replay-headfull", requested_job_id]
    else:
        mode = "human_session"
        cmd = ["bash", str(TRIPADVISOR_CTL_SCRIPT), "human"]
    try:
        with LIVE_SESSION_LOG_FILE.open("ab") as log_stream:
            log_stream.write(
                (
                    "\n"
                    f"[live-session] launch mode={mode} reason={payload.reason!r} "
                    f"display={env.get('DISPLAY')!r} job_id={requested_job_id or '-'}\n"
                ).encode("utf-8", errors="replace")
            )
            proc = subprocess.Popen(  # noqa: S603
                cmd,
                cwd=REPO_ROOT,
                stdout=log_stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                env=env,
            )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to launch live session: {exc}") from exc

    try:
        LIVE_SESSION_PID_FILE.write_text(str(proc.pid), encoding="utf-8")
    except Exception as exc:
        try:
            os.kill(proc.pid, signal.SIGTERM)
        except Exception:
            pass
        raise HTTPException(status_code=503, detail=f"Failed to persist live-session pid: {exc}") from exc

    _write_live_session_state(
        {
            "state": "running",
            "pid": int(proc.pid),
            "updated_at_ts": time.time(),
            "finished_reason": None,
        }
    )

    # Quick boot check: process should still be alive shortly after spawn.
    time.sleep(1.2)
    if proc.poll() is not None:
        LIVE_SESSION_PID_FILE.unlink(missing_ok=True)
        tail = _tail_file(LIVE_SESSION_LOG_FILE)
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Live session process exited immediately.",
                "exit_code": proc.returncode,
                "log_tail": tail,
            },
        )

    return {
        "ok": True,
        "already_running": False,
        "reason": payload.reason,
        "mode": mode,
        "job_id": requested_job_id or None,
        "live_session": _current_live_session_status(),
        "display": env.get("DISPLAY"),
        "profile_dir": env.get("SCRAPER_TRIPADVISOR_USER_DATA_DIR_LOCAL"),
        "log_file": LIVE_SESSION_LOG_FILE.as_posix(),
        "command": cmd,
    }


@app.post("/live-session/stop")
def live_session_stop() -> dict[str, Any]:
    pid = _read_pid_file_path(LIVE_SESSION_PID_FILE)
    if not _pid_is_alive(pid):
        LIVE_SESSION_PID_FILE.unlink(missing_ok=True)
        return {
            "ok": True,
            "already_stopped": True,
            "live_session": _current_live_session_status(),
        }

    assert pid is not None
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Failed to stop live session pid={pid}: {exc}") from exc
    _write_live_session_state(
        {
            "state": "stopping",
            "pid": int(pid),
            "updated_at_ts": time.time(),
            "finished_reason": "api-stop",
        }
    )
    return {
        "ok": True,
        "already_stopped": False,
        "live_session": _current_live_session_status(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run local bridge to control TripAdvisor worker from API."
    )
    parser.add_argument(
        "--host",
        default=os.getenv("TRIPADVISOR_LOCAL_WORKER_BRIDGE_HOST", "127.0.0.1"),
        help="Bind host (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("TRIPADVISOR_LOCAL_WORKER_BRIDGE_PORT", "8765")),
        help="Bind port (default: 8765).",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable autoreload for development.",
    )
    args = parser.parse_args()
    if args.reload:
        raise RuntimeError(
            "--reload is not supported for this entrypoint. "
            "Run without --reload."
        )
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info",
    )


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    main()
