from __future__ import annotations

from fastapi import APIRouter

from src.platform.application_root import get_application_root

router = APIRouter(prefix="/browser-runtime", tags=["Browser Runtime"])


@router.get("/workers")
async def list_local_browser_workers() -> dict[str, object]:
    items = await get_application_root().local_browser_registry.list_workers()
    return {
        "items": items,
        "total": len(items),
    }
