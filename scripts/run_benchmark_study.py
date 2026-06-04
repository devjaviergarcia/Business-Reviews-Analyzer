#!/usr/bin/env python3
"""
Run a local benchmark study from the terminal.

Examples:
  python3 scripts/run_benchmark_study.py --query "merienda cordoba" --limit 100
  python3 scripts/run_benchmark_study.py --query "restaurantes cordoba" --city Cordoba --enqueue
  python3 scripts/run_benchmark_study.py --query "cafeterias cordoba" --xvfb
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.database import close_mongo_connection, connect_to_mongo
from src.services.crm_service import CRMService
from src.workers.contracts import BenchmarkLocalStudyTaskPayload

_XVFB_ENV_MARKER = "BENCHMARK_STUDY_XVFB"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lanza un benchmark local usando discovery live de Google Maps.")
    parser.add_argument("--query", required=True, help="Query del estudio, por ejemplo 'merienda cordoba'.")
    parser.add_argument("--city", default="", help="Ciudad opcional.")
    parser.add_argument("--category", default="", help="Categoria opcional.")
    parser.add_argument("--limit", type=int, default=100, help="Limite de negocios (default: 100).")
    parser.add_argument("--source", default="auto_live_google_maps", help="Source discovery (default: auto_live_google_maps).")
    parser.add_argument("--title", default="", help="Titulo legible del estudio.")
    parser.add_argument(
        "--profile-dir",
        default=settings.scraper_user_data_dir,
        help=f"Perfil Playwright persistente (default: {settings.scraper_user_data_dir}).",
    )
    parser.add_argument(
        "--enqueue",
        action="store_true",
        help="Solo encola el job benchmark_local_study para que lo ejecute el CRM worker.",
    )
    parser.add_argument(
        "--job-id",
        default="benchmark-study-debug",
        help="job_id logico cuando se ejecuta directo (default: benchmark-study-debug).",
    )
    parser.add_argument("--output", default="", help="Ruta JSON de salida.")
    parser.add_argument("--xvfb", action="store_true", help="Relanza el script dentro de xvfb-run.")
    parser.add_argument("--xvfb-screen", default="1920x1080x24", help="Geometria XVFB (default: 1920x1080x24).")
    return parser.parse_args()


def _slugify(value: str) -> str:
    cleaned = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    cleaned = "".join(ch for ch in cleaned if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return cleaned or "benchmark"


def _resolve_output_path(raw_output: str, query: str) -> Path:
    raw = str(raw_output or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path.resolve()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "artifacts" / f"benchmark_study_{_slugify(query)}_{ts}.json"


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _maybe_reexec_with_xvfb(args: argparse.Namespace) -> None:
    if not args.xvfb or os.environ.get(_XVFB_ENV_MARKER) == "1":
        return
    xvfb_run = shutil.which("xvfb-run")
    if not xvfb_run:
        raise RuntimeError("--xvfb solicitado, pero xvfb-run no esta instalado o no esta en PATH.")

    forwarded_args: list[str] = []
    skip_next = False
    for raw_arg in sys.argv[1:]:
        if skip_next:
            skip_next = False
            continue
        if raw_arg == "--xvfb":
            continue
        forwarded_args.append(raw_arg)

    env = dict(os.environ)
    env[_XVFB_ENV_MARKER] = "1"
    cmd = [
        xvfb_run,
        "-a",
        "-s",
        f"-screen 0 {args.xvfb_screen} -ac +extension RANDR -nolisten tcp",
        sys.executable,
        str(Path(__file__).resolve()),
        *forwarded_args,
    ]
    raise SystemExit(subprocess.call(cmd, cwd=str(PROJECT_ROOT), env=env))


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    # El flujo de Google Maps debe correr headed. Si se necesita pantalla virtual, usar --xvfb o el worker Docker.
    started_at = datetime.now(timezone.utc)
    settings.scraper_headless = False
    settings.scraper_incognito = False
    settings.scraper_user_data_dir = str(args.profile_dir or settings.scraper_user_data_dir)

    await connect_to_mongo()
    try:
        service = CRMService()
        if args.enqueue:
            result = await service.enqueue_benchmark_study_job(
                query=args.query,
                city=args.city or None,
                category=args.category or None,
                limit=args.limit,
                source=args.source,
                title=args.title or None,
            )
            mode = "enqueue"
        else:
            payload = BenchmarkLocalStudyTaskPayload(
                query=args.query,
                city=args.city or None,
                category=args.category or None,
                limit=args.limit,
                source=args.source,
                title=args.title or None,
            )
            result = await service.process_benchmark_study_task(task_payload=payload, job_id=args.job_id)
            mode = "direct"

        return {
            "started_at": started_at.isoformat(),
            "mode": mode,
            "runtime": {
                "query": args.query,
                "city": args.city or None,
                "category": args.category or None,
                "limit": args.limit,
                "source": args.source,
                "profile_dir": settings.scraper_user_data_dir,
                "headless": settings.scraper_headless,
                "incognito": settings.scraper_incognito,
            },
            "result": result,
        }
    finally:
        await close_mongo_connection()


def main() -> None:
    args = _parse_args()
    _maybe_reexec_with_xvfb(args)
    output_path = _resolve_output_path(args.output, args.query)
    payload = asyncio.run(_run(args))
    payload["finished_at"] = datetime.now(timezone.utc).isoformat()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    print(json.dumps({"output": str(output_path), "result": payload.get("result")}, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    main()
