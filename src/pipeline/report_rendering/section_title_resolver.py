from __future__ import annotations

import re


def humanize_section_key(key: str) -> str:
    mapped_titles = {
        "1_resumen_ejecutivo": "Diagnóstico temprano",
        "2_score_reputacion": "Puntuación de tu reputación",
        "3_quien_es_tu_cliente_y_que_le_preocupa": "Quién es tu cliente y qué le preocupa",
        "4_lectura_fuente_google_maps": "Lectura por fuente: Google Maps",
        "5_lectura_fuente_tripadvisor": "Lectura por fuente: Tripadvisor",
        "4_plan_de_accion": "Plan de acción",
        "7_comparativa_fuentes": "Comparativa entre fuentes",
        "5_anexos_resumen": "Anexo resumen",
    }
    if key in mapped_titles:
        return mapped_titles[key]
    clean = str(key or "").strip()
    clean = re.sub(r"^\d+[_\-.]?", "", clean)
    clean = clean.replace("_", " ").replace("-", " ").strip()
    if not clean:
        return "Sección"
    return clean[:1].upper() + clean[1:]
