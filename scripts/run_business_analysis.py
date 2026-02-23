import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.database import close_mongo_connection, connect_to_mongo
from src.services.business_service import BusinessService


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run business analysis pipeline without API server.")
    parser.add_argument("name", nargs="+", help="Business name/query to analyze.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-scraping and re-analysis even if cached analysis exists.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print compact JSON output (single line).",
    )
    return parser.parse_args()


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


async def _run() -> None:
    args = _parse_args()
    business_name = " ".join(args.name).strip()
    if not business_name:
        raise RuntimeError("Business name is required.")

    await connect_to_mongo()
    try:
        service = BusinessService()
        result = await service.analyze_business(name=business_name, force=args.force)
    finally:
        await close_mongo_connection()

    if args.compact:
        print(json.dumps(result, ensure_ascii=False, default=_json_default))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    asyncio.run(_run())
