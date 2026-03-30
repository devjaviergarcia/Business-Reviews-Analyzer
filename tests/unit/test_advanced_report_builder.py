from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from src.pipeline.advanced_report_builder import AdvancedBusinessReportBuilder


class _FakeCursor:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = list(docs)

    def sort(self, *_args, **_kwargs) -> "_FakeCursor":
        return self

    def limit(self, n: int) -> "_FakeCursor":
        self._docs = self._docs[: int(n)]
        return self

    async def to_list(self, length: int | None = None) -> list[dict[str, Any]]:
        if length is None:
            return list(self._docs)
        return list(self._docs[: int(length)])


class _FakeCollection:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self._docs = list(docs)

    def find(self, query: dict[str, Any] | None = None, *_args, **_kwargs) -> _FakeCursor:
        query = query or {}
        docs = list(self._docs)

        id_filter = query.get("_id")
        if isinstance(id_filter, dict) and "$ne" in id_filter:
            docs = [doc for doc in docs if doc.get("_id") != id_filter["$ne"]]

        categories_filter = query.get("listing.categories")
        if isinstance(categories_filter, dict) and "$in" in categories_filter:
            allowed = {str(item) for item in categories_filter.get("$in", [])}
            filtered = []
            for doc in docs:
                listing = doc.get("listing")
                categories = listing.get("categories") if isinstance(listing, dict) else []
                if not isinstance(categories, list):
                    continue
                if any(str(category) in allowed for category in categories):
                    filtered.append(doc)
            docs = filtered

        business_id = query.get("business_id")
        if business_id is not None:
            docs = [doc for doc in docs if str(doc.get("business_id")) == str(business_id)]

        return _FakeCursor(docs)


def _sample_reviews() -> list[dict[str, Any]]:
    return [
        {
            "review_id": "r1",
            "source": "google_maps",
            "author_name": "Ana",
            "rating": 5,
            "relative_time_bucket": "recent",
            "text": "Excelente servicio, personal muy amable y comida increíble.",
            "owner_reply": "Gracias por venir.",
            "has_owner_reply": True,
        },
        {
            "review_id": "r2",
            "source": "google_maps",
            "author_name": "Luis",
            "rating": 2,
            "relative_time_bucket": "recent",
            "text": "Muy lento, caro para lo que ofrecen. Esperaba mucho más.",
            "owner_reply": "",
            "has_owner_reply": False,
        },
        {
            "review_id": "r3",
            "source": "tripadvisor",
            "author_name": "Marta",
            "rating": 4,
            "relative_time_bucket": "medium",
            "text": "Buen ambiente y buena comida, aunque podrían mejorar la espera.",
            "owner_reply": "Tomamos nota.",
            "has_owner_reply": True,
        },
        {
            "review_id": "r4",
            "source": "tripadvisor",
            "author_name": "Pedro",
            "rating": 1,
            "relative_time_bucket": "old",
            "text": "Pésimo servicio!! Inaceptable, no volveré jamás.",
            "owner_reply": "",
            "has_owner_reply": False,
        },
    ]


def test_advanced_report_builder_builds_5_sections_redesigned() -> None:
    builder = AdvancedBusinessReportBuilder()
    business_id = str(ObjectId())
    competitor_docs = [
        {
            "_id": ObjectId(),
            "name": "Competidor A",
            "listing": {"overall_rating": 4.4, "categories": ["restaurante"]},
            "review_count": 120,
        },
        {
            "_id": ObjectId(),
            "name": "Competidor B",
            "listing": {"overall_rating": 4.1, "categories": ["restaurante"]},
            "review_count": 90,
        },
        {
            "_id": ObjectId(),
            "name": "Competidor C",
            "listing": {"overall_rating": 3.9, "categories": ["restaurante"]},
            "review_count": 70,
        },
    ]
    analyses_docs = [
        {
            "_id": ObjectId(),
            "business_id": business_id,
            "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            "overall_sentiment": "mixed",
        },
        {
            "_id": ObjectId(),
            "business_id": business_id,
            "created_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
            "overall_sentiment": "positive",
        },
    ]

    report = asyncio.run(
        builder.build(
            business_id=business_id,
            business_name="Negocio Demo",
            listing={"categories": ["restaurante"], "overall_rating": 4.2},
            stats={"avg_rating": 3.4, "response_rate": 0.32},
            reviews=_sample_reviews(),
            analysis_payload={
                "overall_sentiment": "mixed",
                "main_topics": ["servicio", "comida"],
                "strengths": ["amabilidad"],
                "weaknesses": ["espera"],
            },
            businesses_collection=_FakeCollection(competitor_docs),
            analyses_collection=_FakeCollection(analyses_docs),
        )
    )

    sections = report.get("sections")
    assert isinstance(sections, dict)
    assert len(sections) == 5
    assert "1_resumen_ejecutivo" in sections
    assert "2_score_reputacion" in sections
    assert "3_quien_es_tu_cliente_y_que_le_preocupa" in sections
    assert "4_plan_de_accion" in sections
    assert "5_anexos_resumen" in sections

    score = sections["2_score_reputacion"]
    assert str(score.get("score_display", "")).endswith("/100")
    assert isinstance(score.get("explicacion"), str)

    customer_and_problems = sections["3_quien_es_tu_cliente_y_que_le_preocupa"]
    assert isinstance(customer_and_problems.get("tipologias_cliente_top3"), list)
    assert isinstance(customer_and_problems.get("preocupaciones_top3"), list)
    scatter = customer_and_problems.get("scatter_clientes")
    assert isinstance(scatter, dict)
    assert isinstance(scatter.get("circles"), list)

    action_plan = sections["4_plan_de_accion"]
    assert isinstance(action_plan.get("corto_plazo_0_30_dias"), list)
    assert isinstance(action_plan.get("medio_plazo_30_90_dias"), list)
    assert isinstance(action_plan.get("largo_plazo_90_mas_dias"), list)
    if action_plan.get("corto_plazo_0_30_dias"):
        first_action = action_plan["corto_plazo_0_30_dias"][0]
        assert isinstance(first_action.get("tipo"), str)
        assert "herramienta_si_aplica" in first_action

    resumen = sections["1_resumen_ejecutivo"]
    assert isinstance(resumen.get("aciertos_estructurados"), list)
    clientes = sections["3_quien_es_tu_cliente_y_que_le_preocupa"]
    assert isinstance(clientes.get("fortalezas_debilidades"), dict)

    annexes = report.get("annexes")
    assert isinstance(annexes, dict)
    full_data = annexes.get("full_data")
    assert isinstance(full_data, dict)
    assert isinstance(full_data.get("dataset_summary"), dict)
    assert isinstance(full_data.get("review_rows"), list)

    evolution = sections["2_score_reputacion"].get("evolucion") or {}
    history = evolution.get("analyses_history") if isinstance(evolution, dict) else []
    if isinstance(history, list) and history:
        assert "analysis_id" not in history[0]


def test_advanced_report_builder_handles_empty_reviews() -> None:
    builder = AdvancedBusinessReportBuilder()
    business_id = str(ObjectId())

    report = asyncio.run(
        builder.build(
            business_id=business_id,
            business_name="Sin Reseñas",
            listing={"categories": ["hotel"], "overall_rating": 0},
            stats={"avg_rating": 0, "response_rate": 0},
            reviews=[],
            analysis_payload={"overall_sentiment": "mixed", "main_topics": []},
            businesses_collection=_FakeCollection([]),
            analyses_collection=_FakeCollection([]),
        )
    )

    sections = report.get("sections") or {}
    assert "2_score_reputacion" in sections
    merged = sections["3_quien_es_tu_cliente_y_que_le_preocupa"]
    scatter = merged.get("scatter_clientes") if isinstance(merged, dict) else {}
    assert isinstance(scatter, dict)
    assert isinstance(scatter.get("circles"), list)

    annexes = report.get("annexes") or {}
    full_data = annexes.get("full_data") if isinstance(annexes, dict) else {}
    assert isinstance(full_data, dict)
    assert isinstance(full_data.get("review_rows"), list)
    assert len(full_data.get("review_rows") or []) == 0


def test_advanced_report_builder_builds_preview_report_from_full_report() -> None:
    builder = AdvancedBusinessReportBuilder()
    business_id = str(ObjectId())

    full_report = asyncio.run(
        builder.build(
            business_id=business_id,
            business_name="Negocio Preview",
            listing={"categories": ["restaurante"], "overall_rating": 4.1},
            stats={"avg_rating": 3.6, "response_rate": 0.41},
            reviews=_sample_reviews(),
            analysis_payload={"overall_sentiment": "mixed", "main_topics": ["servicio"]},
            businesses_collection=_FakeCollection([]),
            analyses_collection=_FakeCollection([]),
        )
    )

    preview = builder.build_preview_report(
        advanced_report=full_report,
        business_name="Negocio Preview",
        max_comments=3,
    )

    assert isinstance(preview, dict)
    assert preview.get("preview_version") == "2026.1"
    sections = preview.get("sections")
    assert isinstance(sections, dict)
    assert "1_resumen_ejecutivo_preview" in sections
    assert "2_tipos_cliente_y_comentarios_relevantes" in sections
    assert "3_llamada_a_la_accion" in sections

    tipos_section = sections["2_tipos_cliente_y_comentarios_relevantes"]
    assert isinstance(tipos_section, dict)
    comments = tipos_section.get("comentarios_relevantes")
    assert isinstance(comments, list)
    assert len(comments) <= 3
    if comments:
        assert isinstance(comments[0].get("quote"), str)
        assert comments[0].get("quote")

    cta = sections["3_llamada_a_la_accion"]
    assert isinstance(cta, dict)
    assert "formulario" in str(cta.get("texto", "")).lower()


def test_dominant_problem_is_always_categorized_without_sin_categoria() -> None:
    builder = AdvancedBusinessReportBuilder()
    forbidden = {"sin_categoria", "sin categoria", "sin categoría", ""}

    rows = [
        {"rating": 5, "text": "", "expected": builder._NO_COMMENT_HIGH_PROBLEM},
        {"rating": 3, "text": "", "expected": builder._NO_COMMENT_MEDIUM_PROBLEM},
        {"rating": 1, "text": "", "expected": builder._NO_COMMENT_LOW_PROBLEM},
        {
            "rating": 5,
            "text": "blorb snarp frindle",
            "expected": builder._POSITIVE_COMMENT_PROBLEM,
        },
        {
            "rating": 1,
            "text": "blorb snarp frindle",
            "expected": builder._NEGATIVE_COMMENT_PROBLEM,
        },
        {
            "rating": 3,
            "text": "blorb snarp frindle",
            "expected": builder._GENERIC_COMMENT_PROBLEM,
        },
    ]

    for idx, row in enumerate(rows):
        scored = builder._score_review_dimensions(
            index=idx,
            review={
                "review_id": f"r_{idx}",
                "source": "tripadvisor",
                "author_name": "Cliente",
                "rating": row["rating"],
                "text": row["text"],
            },
        )
        dominant_problem = str(scored.get("dominant_problem", "") or "").strip().lower()
        assert dominant_problem not in forbidden
        assert scored.get("dominant_problem") == row["expected"]


def test_sanitize_llm_text_reduces_generation_artifacts() -> None:
    builder = AdvancedBusinessReportBuilder()
    raw = "El serviiicio tiene impactoooo directo..  **Texto** conEspacios"
    cleaned = builder._sanitize_llm_text(raw)
    assert "impactoooo" not in cleaned
    assert "**" not in cleaned
    assert ".." not in cleaned
