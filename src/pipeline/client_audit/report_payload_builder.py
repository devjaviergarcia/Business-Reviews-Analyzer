from __future__ import annotations

from typing import Any


def build_client_audit_report_payload(
    *,
    business_doc: dict[str, Any],
    analysis_doc: dict[str, Any],
    advanced_report: dict[str, Any],
    source_availability: dict[str, Any],
    source_mode: str,
    sources_included: list[str],
    source_counts: dict[str, int],
    report_profile: str,
    report_complexity: str,
    report_cadence: str,
    include_competitors: bool,
    include_geogrid: bool,
    preparation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    listing = business_doc.get("listing") if isinstance(business_doc.get("listing"), dict) else {}
    hydration_context = (
        preparation.get("hydration_context")
        if isinstance(preparation, dict) and isinstance(preparation.get("hydration_context"), dict)
        else {}
    )
    benchmark_context = (
        hydration_context.get("benchmark")
        if isinstance(hydration_context.get("benchmark"), dict)
        else {}
    )
    geogrid_context = (
        hydration_context.get("geogrid")
        if isinstance(hydration_context.get("geogrid"), dict)
        else {}
    )
    deep_study_snapshot = (
        benchmark_context.get("deep_study_snapshot")
        if isinstance(benchmark_context.get("deep_study_snapshot"), dict)
        else None
    )
    benchmark_run = (
        benchmark_context.get("benchmark_run")
        if isinstance(benchmark_context.get("benchmark_run"), dict)
        else None
    )
    benchmark_business = (
        benchmark_context.get("benchmark_business")
        if isinstance(benchmark_context.get("benchmark_business"), dict)
        else None
    )
    competitors = (
        benchmark_context.get("competitors")
        if isinstance(benchmark_context.get("competitors"), list)
        else []
    )
    geo_grid_run = (
        geogrid_context.get("geo_grid_run") if isinstance(geogrid_context.get("geo_grid_run"), dict) else None
    )
    geo_grid_stats = (
        geogrid_context.get("geo_grid_stats") if isinstance(geogrid_context.get("geo_grid_stats"), dict) else None
    )
    geo_grid_business = (
        geogrid_context.get("geo_grid_business")
        if isinstance(geogrid_context.get("geo_grid_business"), dict)
        else None
    )
    benchmark_presence_state = str(
        benchmark_context.get("presence_state") or (preparation or {}).get("business_presence_state") or ""
    ).strip()
    geogrid_presence_state = str(
        geogrid_context.get("presence_state") or (preparation or {}).get("business_presence_state") or ""
    ).strip()
    report_metadata = (
        advanced_report.get("report_metadata")
        if isinstance(advanced_report.get("report_metadata"), dict)
        else {}
    )
    study_scope = hydration_context.get("scope") if isinstance(hydration_context.get("scope"), dict) else {}

    return {
        "report_profile": report_profile,
        "report_complexity": report_complexity,
        "report_cadence": report_cadence,
        "business_name": str(business_doc.get("name", "") or listing.get("business_name") or "Negocio").strip()
        or "Negocio",
        "generated_at": analysis_doc.get("report_generated_at") or analysis_doc.get("created_at"),
        "analysis_id": str(analysis_doc.get("_id") or ""),
        "business_id": str(business_doc.get("_id") or ""),
        "report_version": advanced_report.get("report_version"),
        "annexes": advanced_report.get("annexes") if isinstance(advanced_report.get("annexes"), dict) else {},
        "advanced_report": advanced_report,
        "listing_readiness": _build_listing_readiness(listing=listing),
        "study_scope": study_scope,
        "study_hydration": {
            "status": str((preparation or {}).get("hydration_status") or "skipped"),
            "business_presence_state": str(
                (preparation or {}).get("business_presence_state") or "study_scope_unresolved"
            ),
            "notes": list((preparation or {}).get("notes") or []),
            "scope": study_scope,
            "benchmark": {
                "presence_state": benchmark_presence_state,
                "benchmark_run": benchmark_run,
                "benchmark_business": benchmark_business,
                "competitors": competitors,
                "deep_study_snapshot": deep_study_snapshot,
            },
            "geogrid": {
                "presence_state": geogrid_presence_state,
                "geo_grid_run": geo_grid_run,
                "geo_grid_stats": geo_grid_stats,
                "geo_grid_business": geo_grid_business,
            },
        },
        "report_metadata": {
            **report_metadata,
            "source_availability": source_availability,
            "report_source_mode": source_mode,
            "report_sources_included": list(sources_included),
            "source_counts": dict(source_counts),
            "include_competitors": bool(include_competitors),
            "include_geogrid": bool(include_geogrid),
        },
    }


def _build_listing_readiness(*, listing: dict[str, Any]) -> dict[str, Any]:
    categories = listing.get("categories") if isinstance(listing.get("categories"), list) else []
    service_options = (
        listing.get("service_options") if isinstance(listing.get("service_options"), list) else []
    )
    primary_category = str(
        listing.get("category") or (categories[0] if categories else "") or ""
    ).strip() or None
    items = [
        _check_item(
            label="Categoría principal",
            value=primary_category,
            positive_note="Google entiende mejor la intención local.",
            negative_note="Falta categoría clara para posicionar la ficha.",
        ),
        _check_item(
            label="Dirección visible",
            value=str(listing.get("address") or "").strip() or None,
            positive_note="Reduce fricción para visitas y confianza.",
            negative_note="Sin dirección visible la ficha convierte peor.",
        ),
        _check_item(
            label="Teléfono visible",
            value=str(listing.get("phone") or "").strip() or None,
            positive_note="Facilita reservas o contacto directo.",
            negative_note="Se pierde conversión inmediata desde Maps.",
        ),
        _check_item(
            label="Web visible",
            value=str(listing.get("website") or "").strip() or None,
            positive_note="Permite capturar tráfico con intención alta.",
            negative_note="Sin web visible se corta el siguiente paso.",
        ),
        _check_item(
            label="Carta o menú",
            value=str(listing.get("menu_url") or "").strip() or None,
            positive_note="Ayuda a decidir antes de entrar.",
            negative_note="No hay carta enlazada desde la ficha.",
        ),
        _check_item(
            label="Reserva",
            value=str(listing.get("reservation_url") or "").strip() or None,
            positive_note="Reduce fricción de conversión.",
            negative_note="No aparece vía directa de reserva.",
        ),
        _check_item(
            label="Precio visible",
            value=str(listing.get("price_per_person") or "").strip() or None,
            positive_note="Alinea expectativas antes de visitar.",
            negative_note="Sin referencia de precio aumenta la duda.",
        ),
        _check_item(
            label="Opciones de servicio",
            value=", ".join(str(item).strip() for item in service_options if str(item).strip()) or None,
            positive_note="La ficha comunica bien cómo se puede consumir.",
            negative_note="No se ven opciones de consumo claras.",
        ),
    ]
    completed = sum(1 for item in items if item.get("status") == "ok")
    return {
        "items": items,
        "completed": completed,
        "total": len(items),
    }


def _check_item(
    *,
    label: str,
    value: str | None,
    positive_note: str,
    negative_note: str,
) -> dict[str, str | None]:
    return {
        "label": label,
        "value": value,
        "status": "ok" if value else "missing",
        "note": positive_note if value else negative_note,
    }
