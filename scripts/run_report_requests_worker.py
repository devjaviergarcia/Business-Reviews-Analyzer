#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

import sys

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.database import close_mongo_connection, connect_to_mongo
from src.services.crm_service import CRMService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Procesa solicitudes de informe pendientes al arrancar el PC.")
    parser.add_argument("--limit", type=int, default=50, help="Maximo de solicitudes por pasada.")
    parser.add_argument("--interval-seconds", type=float, default=30.0, help="Intervalo entre pasadas en modo daemon.")
    parser.add_argument("--once", action="store_true", help="Ejecuta una sola pasada y termina.")
    parser.add_argument("--json-output", type=Path, default=None, help="Ruta opcional para guardar el ultimo resultado.")
    return parser.parse_args()


async def run_once(*, service: CRMService, limit: int) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    result = await service.process_pending_report_requests(limit=limit)
    return {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "result": result,
    }


async def main_async() -> int:
    args = parse_args()
    stop_event = asyncio.Event()

    def _stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    await connect_to_mongo()
    try:
        service = CRMService()
        while not stop_event.is_set():
            payload = await run_once(service=service, limit=args.limit)
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            if args.json_output:
                args.json_output.parent.mkdir(parents=True, exist_ok=True)
                args.json_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            if args.once:
                return 0
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=max(1.0, float(args.interval_seconds)))
            except asyncio.TimeoutError:
                continue
    finally:
        await close_mongo_connection()
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
