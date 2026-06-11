from __future__ import annotations

import contextlib
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Iterator

from src.job_runtime.browser_job_contracts import (
    DEFAULT_BROWSER_EXECUTION_MODE,
    DEFAULT_BROWSER_LIVE_DISPLAY_MODE,
)


class BrowserJobLiveDisplayRuntime:
    """Prepares the display environment for one live browser job."""

    def __init__(
        self,
        *,
        display_start: int = 91,
        display_end: int = 110,
        screen_geometry: str = "1920x1080x24",
    ) -> None:
        self._display_start = max(1, int(display_start))
        self._display_end = max(self._display_start, int(display_end))
        self._screen_geometry = str(screen_geometry).strip() or "1920x1080x24"

    @contextlib.contextmanager
    def activate_for_job(
        self,
        *,
        execution_mode: str,
        live_display_mode: str | None,
    ) -> Iterator[str | None]:
        normalized_execution_mode = (
            str(execution_mode or DEFAULT_BROWSER_EXECUTION_MODE).strip().lower()
            or DEFAULT_BROWSER_EXECUTION_MODE
        )
        normalized_live_display_mode = (
            str(live_display_mode or DEFAULT_BROWSER_LIVE_DISPLAY_MODE).strip().lower()
            or DEFAULT_BROWSER_LIVE_DISPLAY_MODE
        )
        if normalized_execution_mode != "live" or normalized_live_display_mode != "xvfb":
            yield os.environ.get("DISPLAY")
            return

        xvfb_binary = shutil.which("Xvfb")
        if not xvfb_binary:
            raise RuntimeError("Xvfb is required for live_display_mode='xvfb', but it is not installed.")

        previous_display = os.environ.get("DISPLAY")
        previous_wayland_display = os.environ.get("WAYLAND_DISPLAY")
        process: subprocess.Popen[bytes] | None = None
        active_display: str | None = None

        try:
            for display_number in range(self._display_start, self._display_end + 1):
                socket_path = Path(f"/tmp/.X11-unix/X{display_number}")
                if socket_path.exists():
                    continue
                candidate_display = f":{display_number}"
                process = subprocess.Popen(  # noqa: S603
                    [
                        xvfb_binary,
                        candidate_display,
                        "-screen",
                        "0",
                        self._screen_geometry,
                        "-ac",
                        "+extension",
                        "RANDR",
                        "-nolisten",
                        "tcp",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                if self._wait_until_display_is_ready(process=process, socket_path=socket_path):
                    active_display = candidate_display
                    break
                self._stop_xvfb_process(process)
                process = None

            if not process or not active_display:
                raise RuntimeError("Could not allocate a temporary Xvfb display for the live browser job.")

            os.environ["DISPLAY"] = active_display
            os.environ.pop("WAYLAND_DISPLAY", None)
            yield active_display
        finally:
            if previous_display:
                os.environ["DISPLAY"] = previous_display
            else:
                os.environ.pop("DISPLAY", None)
            if previous_wayland_display:
                os.environ["WAYLAND_DISPLAY"] = previous_wayland_display
            else:
                os.environ.pop("WAYLAND_DISPLAY", None)
            if process is not None:
                self._stop_xvfb_process(process)

    def _wait_until_display_is_ready(
        self,
        *,
        process: subprocess.Popen[bytes],
        socket_path: Path,
        timeout_seconds: float = 3.0,
    ) -> bool:
        deadline = time.monotonic() + max(0.2, float(timeout_seconds))
        while time.monotonic() < deadline:
            if process.poll() is not None:
                return False
            if socket_path.exists():
                return True
            time.sleep(0.1)
        return process.poll() is None and socket_path.exists()

    def _stop_xvfb_process(self, process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=3.0)
            return
        except subprocess.TimeoutExpired:
            pass
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        with contextlib.suppress(Exception):
            process.wait(timeout=1.0)
