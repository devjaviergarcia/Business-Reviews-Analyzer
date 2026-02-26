from __future__ import annotations

from typing import Any

from bson import ObjectId
from bson.errors import InvalidId


def parse_mongo_object_id(value: str, *, field_name: str) -> ObjectId:
    try:
        return ObjectId(str(value))
    except (InvalidId, TypeError) as exc:
        raise ValueError(f"Invalid {field_name}. Expected a Mongo ObjectId string.") from exc


async def ensure_business_exists(*, businesses_collection: Any, business_id: str) -> ObjectId:
    parsed_business_id = parse_mongo_object_id(business_id, field_name="business_id")
    business_exists = await businesses_collection.count_documents({"_id": parsed_business_id}, limit=1)
    if business_exists == 0:
        raise LookupError(f"Business '{business_id}' not found.")
    return parsed_business_id
