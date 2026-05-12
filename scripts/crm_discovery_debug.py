#!/usr/bin/env python3
"""
Run CRM discovery from terminal with optional XVFB and manual session bootstrap.

Examples:
  python scripts/crm_discovery_debug.py \
    --query "restaurantes sevilla" \
    --source auto_live_google_maps \
    --dry-run \
    --print-candidates \
    --xvfb

  python scripts/crm_discovery_debug.py \
    --query "restaurantes cordoba" \
    --city "Cordoba" \
    --source auto_live_google_maps \
    --manual-first
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
from src.workers.contracts import CRMLeadDiscoveryTaskPayload

_XVFB_ENV_MARKER = "CRM_DISCOVERY_DEBUG_XVFB"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ejecuta el flujo de discovery CRM en local para debug. "
            "Soporta XVFB, sesión manual previa y salida de artefactos JSON."
        )
    )
    parser.add_argument("--query", required=True, help="Query de discovery (ej: 'restaurantes sevilla').")
    parser.add_argument("--city", default="", help="Ciudad opcional para filtrar contexto.")
    parser.add_argument("--category", default="", help="Categoría opcional.")
    parser.add_argument("--limit", type=int, default=100, help="Límite de candidatos (default: 100).")
    parser.add_argument(
        "--source",
        default="auto_live_google_maps",
        help=(
            "Source para discovery (default: auto_live_google_maps). "
            "Ej: live_google_maps, auto_live_google_maps, auto."
        ),
    )
    parser.add_argument(
        "--profile-dir",
        default=settings.scraper_user_data_dir,
        help=f"Directorio de perfil Playwright (default: {settings.scraper_user_data_dir}).",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override para SCRAPER_HEADLESS solo durante esta ejecución.",
    )
    parser.add_argument(
        "--incognito",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override para SCRAPER_INCOGNITO solo durante esta ejecución.",
    )
    parser.add_argument(
        "--manual-first",
        action="store_true",
        help=(
            "Abre scripts/manual_chromium_session.py antes de discovery para login/captcha. "
            "La sesión queda guardada si usas modo persistent."
        ),
    )
    parser.add_argument(
        "--manual-url",
        default=settings.scraper_maps_url,
        help=f"URL inicial para sesión manual (default: {settings.scraper_maps_url}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Solo ejecuta extracción de candidatos (sin persistir leads ni escribir eventos/run en Mongo). "
            "Útil para depurar selectors/flujo."
        ),
    )
    parser.add_argument(
        "--print-candidates",
        action="store_true",
        help="Imprime candidatos en consola (con truncado de campos largos).",
    )
    parser.add_argument(
        "--save-candidates-limit",
        type=int,
        default=200,
        help="Máximo de candidatos a guardar en artefacto JSON (default: 200).",
    )
    parser.add_argument(
        "--job-id",
        default="crm-discovery-debug",
        help="job_id lógico para modo persistente (default: crm-discovery-debug).",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta JSON de salida (default: artifacts/crm_discovery_debug_<timestamp>.json).",
    )
    parser.add_argument(
        "--xvfb",
        action="store_true",
        help="Relanza este script dentro de xvfb-run automáticamente.",
    )
    parser.add_argument(
        "--xvfb-screen",
        default="1920x1080x24",
        help="Geometría XVFB para -screen 0 (default: 1920x1080x24).",
    )
    parser.add_argument(
        "--xvfb-extra-args",
        default="-ac +extension RANDR -nolisten tcp",
        help="Argumentos extra XVFB junto a -screen (default: '-ac +extension RANDR -nolisten tcp').",
    )
    return parser.parse_args()


def _slugify(value: str) -> str:
    cleaned = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    cleaned = "".join(ch for ch in cleaned if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return cleaned or "query"


def _default_output_path(query: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    slug = _slugify(query)
    return PROJECT_ROOT / "artifacts" / f"crm_discovery_debug_{slug}_{ts}.json"


def _resolve_output_path(output: str, query: str) -> Path:
    raw = str(output or "").strip()
    if not raw:
        return _default_output_path(query=query)
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _apply_runtime_scraper_overrides(args: argparse.Namespace) -> dict[str, Any]:
    previous = {
        "scraper_user_data_dir": settings.scraper_user_data_dir,
        "scraper_headless": settings.scraper_headless,
        "scraper_incognito": settings.scraper_incognito,
    }

    settings.scraper_user_data_dir = str(args.profile_dir or settings.scraper_user_data_dir)
    if args.headless is not None:
        settings.scraper_headless = bool(args.headless)
    if args.incognito is not None:
        settings.scraper_incognito = bool(args.incognito)
    return previous


def _restore_runtime_scraper_overrides(previous: dict[str, Any]) -> None:
    settings.scraper_user_data_dir = str(previous.get("scraper_user_data_dir") or settings.scraper_user_data_dir)
    settings.scraper_headless = bool(previous.get("scraper_headless"))
    settings.scraper_incognito = bool(previous.get("scraper_incognito"))


def _print_candidates(candidates: list[dict[str, Any]]) -> None:
    print(f"Candidatos extraidos: {len(candidates)}")
    for index, candidate in enumerate(candidates, start=1):
        name = str(candidate.get("business_name") or "").strip()
        rating = candidate.get("rating")
        review_count = candidate.get("review_count")
        address = str(candidate.get("address") or "").strip()
        source_ref = candidate.get("source_ref") if isinstance(candidate.get("source_ref"), dict) else {}
        maps_url = str(source_ref.get("maps_url") or source_ref.get("maps_url_canonical") or "").strip()
        print(
            f"[{index:03d}] {name or '(sin nombre)'} | rating={rating} reviews={review_count} | "
            f"address={address or '-'} | maps={maps_url or '-'}"
        )


def _run_manual_session(*, profile_dir: str, manual_url: str, incognito: bool) -> None:
    manual_script = PROJECT_ROOT / "scripts" / "manual_chromium_session.py"
    if not manual_script.exists():
        raise RuntimeError(f"No existe script manual: {manual_script}")

    cmd = [
        sys.executable,
        str(manual_script),
        "--url",
        str(manual_url),
        "--profile-dir",
        str(profile_dir),
        "--no-tripadvisor-flow",
    ]
    if incognito:
        cmd.append("--incognito")
    else:
        cmd.append("--persistent")

    print("Abriendo sesion manual. Cierra el navegador para continuar con discovery...")
    subprocess.run(cmd, check=True)


def _reexec_in_xvfb(args: argparse.Namespace) -> int:
    xvfb_run = shutil.which("xvfb-run")
    if not xvfb_run:
        raise RuntimeError("No se encontro 'xvfb-run' en PATH.")

    if os.environ.get(_XVFB_ENV_MARKER) == "1":
        return 0

    filtered_argv: list[str] = []
    for item in sys.argv[1:]:
        if item == "--xvfb":
            continue
        filtered_argv.append(item)

    server_args = f"-screen 0 {args.xvfb_screen} {args.xvfb_extra_args}".strip()
    cmd = [
        xvfb_run,
        "-a",
        "-s",
        server_args,
        sys.executable,
        str(Path(__file__).resolve()),
        *filtered_argv,
    ]

    env = os.environ.copy()
    env[_XVFB_ENV_MARKER] = "1"
    print("Relanzando bajo XVFB:")
    print(" ".join(cmd))
    completed = subprocess.run(cmd, env=env)
    return int(completed.returncode)


async def _run_discovery(args: argparse.Namespace) -> dict[str, Any]:
    service = CRMService()
    payload = CRMLeadDiscoveryTaskPayload(
        query=args.query,
        city=args.city or None,
        category=args.category or None,
        limit=args.limit,
        source=args.source,
    )

    if args.dry_run:
        # Intended debug path: run extract/scroll/enrich without DB writes.
        candidates = await service._discover_candidates(task_payload=payload)  # type: ignore[attr-defined]
        result = {
            "mode": "dry_run",
            "payload": payload.model_dump(mode="python"),
            "candidates_count": len(candidates),
            "candidates": candidates,
        }
        return result

    await connect_to_mongo()
    try:
        result = await service.process_discovery_task(task_payload=payload, job_id=args.job_id)
        discovery_run_id = str(result.get("discovery_run_id") or "").strip() or None
        run_doc: dict[str, Any] | None = None
        if discovery_run_id:
            try:
                run_doc = await service.get_discovery_run(discovery_run_id=discovery_run_id)
            except Exception:
                run_doc = None
        return {
            "mode": "persist",
            "payload": payload.model_dump(mode="python"),
            "result": result,
            "discovery_run": run_doc,
        }
    finally:
        await close_mongo_connection()


def _build_artifact_payload(
    *,
    args: argparse.Namespace,
    discovery_output: dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, Any]:
    mode = str(discovery_output.get("mode") or "unknown")
    payload: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "mode": mode,
        "xvfb_active": os.environ.get(_XVFB_ENV_MARKER) == "1",
        "runtime": {
            "query": args.query,
            "city": args.city or None,
            "category": args.category or None,
            "limit": args.limit,
            "source": args.source,
            "profile_dir": settings.scraper_user_data_dir,
            "headless": settings.scraper_headless,
            "incognito": settings.scraper_incognito,
            "manual_first": bool(args.manual_first),
        },
        "discovery": discovery_output,
    }

    if mode == "dry_run":
        candidates = discovery_output.get("candidates")
        if isinstance(candidates, list):
            max_items = max(1, int(args.save_candidates_limit))
            payload["discovery"]["candidates"] = candidates[:max_items]
            payload["discovery"]["candidates_saved"] = min(len(candidates), max_items)
            payload["discovery"]["candidates_truncated"] = max(0, len(candidates) - max_items)
    return payload


async def _async_main(args: argparse.Namespace) -> int:
    previous_settings = _apply_runtime_scraper_overrides(args)
    started_at = datetime.now(timezone.utc)
    try:
        if args.manual_first:
            _run_manual_session(
                profile_dir=settings.scraper_user_data_dir,
                manual_url=args.manual_url,
                incognito=settings.scraper_incognito,
            )

        discovery_output = await _run_discovery(args)
        finished_at = datetime.now(timezone.utc)
        artifact = _build_artifact_payload(
            args=args,
            discovery_output=discovery_output,
            started_at=started_at,
            finished_at=finished_at,
        )

        output_path = _resolve_output_path(args.output, args.query)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(artifact, ensure_ascii=False, indent=2, default=_json_default),
            encoding="utf-8",
        )

        mode = str(discovery_output.get("mode") or "unknown")
        if mode == "dry_run":
            candidates = discovery_output.get("candidates")
            count = len(candidates) if isinstance(candidates, list) else 0
            print(f"Discovery dry-run completado. candidates={count}")
            if args.print_candidates and isinstance(candidates, list):
                _print_candidates(candidates)
        else:
            result = discovery_output.get("result") if isinstance(discovery_output.get("result"), dict) else {}
            print(
                "Discovery persistente completado. "
                f"status={result.get('status')} candidates={result.get('candidates')} "
                f"inserted={result.get('inserted')} updated={result.get('updated')} skipped={result.get('skipped')}"
            )
            if args.print_candidates:
                print("Nota: --print-candidates solo imprime candidatos en --dry-run.")

        print(f"Artefacto guardado en: {output_path}")
        return 0
    finally:
        _restore_runtime_scraper_overrides(previous_settings)


def main() -> int:
    args = _parse_args()
    if args.xvfb:
        rc = _reexec_in_xvfb(args)
        if rc != 0:
            return rc
        if os.environ.get(_XVFB_ENV_MARKER) != "1":
            return 0

    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())

