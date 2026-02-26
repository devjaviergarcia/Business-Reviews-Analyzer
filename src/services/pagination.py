from __future__ import annotations

from typing import Any


def coerce_pagination(*, page: int, page_size: int, max_page_size: int) -> tuple[int, int]:
    try:
        page_value = int(page)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid page. It must be an integer >= 1.") from exc
    try:
        page_size_value = int(page_size)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid page_size. It must be an integer >= 1.") from exc

    if page_value < 1:
        raise ValueError("Invalid page. It must be >= 1.")
    if page_size_value < 1:
        raise ValueError("Invalid page_size. It must be >= 1.")

    return page_value, min(page_size_value, max_page_size)


def build_pagination_payload(
    *,
    items: list[dict[str, Any]],
    page: int,
    page_size: int,
    total: int,
) -> dict[str, Any]:
    total_value = max(0, int(total))
    total_pages = ((total_value + page_size - 1) // page_size) if total_value else 0
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": total_value,
        "total_pages": total_pages,
        "has_next": bool(total_pages and page < total_pages),
        "has_prev": bool(total_pages and page > 1),
    }
