from __future__ import annotations

import html
from typing import Any


def render_review_rows_table(renderer: Any, payload: Any) -> str:
    if not isinstance(payload, list) or not payload:
        return ""
    rows = payload[:2000]
    headers = [
        "Índice",
        "Fuente",
        "Autor",
        "Valoración",
        "Sentimiento",
        "Brecha de expectativas",
        "Satisfacción",
        "Tema principal",
        "Tiene respuesta del negocio",
        "Resumen de reseña",
    ]
    map_header = {
        "Índice": "review_index",
        "Fuente": "source",
        "Autor": "author_name",
        "Valoración": "rating",
        "Sentimiento": "sentiment",
        "Brecha de expectativas": "expectation_gap",
        "Satisfacción": "satisfaction",
        "Tema principal": "dominant_problem",
        "Tiene respuesta del negocio": "has_owner_reply",
        "Resumen de reseña": "review_excerpt",
    }
    head_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_html_rows = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        tds = []
        for header in headers:
            value = item.get(map_header[header])
            tds.append(f"<td>{html.escape(str(value if value is not None else ''))}</td>")
        body_html_rows.append(f"<tr>{''.join(tds)}</tr>")
    return f"<table><thead><tr>{head_html}</tr></thead><tbody>{''.join(body_html_rows)}</tbody></table>"
