from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

router = APIRouter(tags=["Editorial"])

_CONTENT_DIR = Path(__file__).resolve().parents[1] / "content" / "editorial"

_PAGE_MAP: dict[str, str] = {
    "index": "insights_index.html",
    "estado-resenas-restaurantes-cordoba": "estado-resenas-restaurantes-cordoba.html",
    "como-responder-resenas-negativas-restaurantes": "como-responder-resenas-negativas-restaurantes.html",
    "que-significa-tener-4-4-estrellas-y-muchas-resenas": "que-significa-tener-4-4-estrellas-y-muchas-resenas.html",
    "errores-frecuentes-google-maps-restaurantes": "errores-frecuentes-google-maps-restaurantes.html",
    "como-mejorar-ficha-google-business-profile-restaurante": "como-mejorar-ficha-google-business-profile-restaurante.html",
    "como-comparar-tu-restaurante-con-competidores-locales": "como-comparar-tu-restaurante-con-competidores-locales.html",
}


def get_editorial_page_html(page_slug: str) -> str:
    """Load a static editorial HTML page by slug."""
    normalized = str(page_slug or "").strip().lower()
    filename = _PAGE_MAP.get(normalized)
    if not filename:
        raise LookupError(f"Editorial page '{page_slug}' not found.")

    path = _CONTENT_DIR / filename
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Missing editorial page file: {path}")

    return path.read_text(encoding="utf-8")


@router.get("/insights", response_class=HTMLResponse, include_in_schema=False)
async def insights_index() -> HTMLResponse:
    html = get_editorial_page_html("index")
    return HTMLResponse(content=html)


@router.get("/insights/{page_slug}", response_class=HTMLResponse, include_in_schema=False)
async def insight_page(page_slug: str) -> HTMLResponse:
    try:
        html = get_editorial_page_html(page_slug)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return HTMLResponse(content=html)
