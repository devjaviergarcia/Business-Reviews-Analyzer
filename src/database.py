from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from src.config import settings

_client: Optional[AsyncIOMotorClient] = None
_database: Optional[AsyncIOMotorDatabase] = None


async def connect_to_mongo() -> None:
    global _client, _database

    if _client is not None:
        return

    _client = AsyncIOMotorClient(settings.mongo_uri)
    await _client.admin.command("ping")
    _database = _client[settings.db_name]


async def close_mongo_connection() -> None:
    global _client, _database

    if _client is not None:
        _client.close()

    _client = None
    _database = None


async def ping_mongo_detailed() -> tuple[bool, str | None]:
    if _client is None:
        return False, "MongoDB client is not initialized."

    try:
        await _client.admin.command("ping")
    except Exception as exc:
        return False, str(exc)

    return True, None


async def ping_mongo() -> bool:
    mongo_ok, _ = await ping_mongo_detailed()
    return mongo_ok


def get_database() -> AsyncIOMotorDatabase:
    if _database is None:
        raise RuntimeError("MongoDB connection has not been initialized.")
    return _database
