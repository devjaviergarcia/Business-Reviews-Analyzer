from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed geo cities from data/geo_points/*.json using the Mongo repository.")
    parser.add_argument("--mongo-uri", default="mongodb://127.0.0.1:27017")
    parser.add_argument("--db-name", default="business_reviews_analyzer")
    return parser.parse_args()


async def _run() -> dict[str, Any]:
    args = _parse_args()
    os.environ["MONGO_URI"] = str(args.mongo_uri)
    os.environ["DB_NAME"] = str(args.db_name)

    from src.crm.repositories import CRMRepositoryBootstrap, MongoGeoCityRepository
    from src.database import close_mongo_connection, connect_to_mongo

    await connect_to_mongo()
    try:
        await CRMRepositoryBootstrap().ensure_indexes()
        return await MongoGeoCityRepository().seed_default_cities()
    finally:
        await close_mongo_connection()


def main() -> None:
    result = asyncio.run(_run())
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
