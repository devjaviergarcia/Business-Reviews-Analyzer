from __future__ import annotations

import html
from typing import Any


def build_final_report_intro_block(
    *,
    renderer: Any,
    intro_context_text: str,
    total_reviews: int,
    fuentes_label: str,
    generated_human: str,
) -> str:
    intro_text = renderer._clean_narrative_text(str(intro_context_text or "").strip())
    parts: list[str] = []
    parts.append("<section class='intro context-banner'>")
    parts.append("<div class='context-row'>")
    parts.append(
        f"<span class='context-item'>{renderer._icon_slot('reviews')}<strong>{total_reviews}</strong> opiniones analizadas</span>"
    )
    if fuentes_label:
        parts.append(
            f"<span class='context-item'>{renderer._icon_slot('sources')}Fuentes: <strong>{html.escape(fuentes_label)}</strong></span>"
        )
    parts.append(
        f"<span class='context-item'>{renderer._icon_slot('updated')}Actualizado: <strong>{html.escape(generated_human)}</strong></span>"
    )
    parts.append("</div>")
    if intro_text:
        parts.append(f"<p class='muted'>{html.escape(intro_text)}</p>")
    parts.append("</section>")
    return "".join(parts)
