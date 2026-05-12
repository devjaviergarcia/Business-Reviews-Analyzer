from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bson import ObjectId

from src.pipeline.advanced_report_builder import AdvancedBusinessReportBuilder
from src.pipeline.report_renderer import StructuredReportRenderer
from src.workers.contracts import ReportGenerateTaskPayload


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
    def __init__(self, docs: list[dict[str, Any]] | None = None) -> None:
        self._docs = list(docs or [])

    def find(self, query: dict[str, Any] | None = None, *_args, **_kwargs) -> _FakeCursor:
        query = query or {}
        docs = list(self._docs)
        business_id = query.get("business_id")
        if business_id is not None:
            docs = [doc for doc in docs if str(doc.get("business_id")) == str(business_id)]
        return _FakeCursor(docs)


def _fixture_path(filename: str) -> Path:
    return Path(__file__).resolve().parents[1] / "fixtures" / filename


def _load_reviews(filename: str) -> list[dict[str, Any]]:
    return json.loads(_fixture_path(filename).read_text(encoding="utf-8"))


def _build_report_from_reviews(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    builder = AdvancedBusinessReportBuilder(enable_llm=False)
    business_id = str(ObjectId())
    report = asyncio.run(
        builder.build(
            business_id=business_id,
            business_name="Negocio Multi Fuente",
            listing={"categories": ["restaurante"], "overall_rating": 4.1},
            stats={"avg_rating": 3.8, "response_rate": 0.2},
            reviews=reviews,
            analysis_payload={
                "overall_sentiment": "mixed",
                "main_topics": ["servicio", "calidad_comida", "precio_valor"],
            },
            businesses_collection=_FakeCollection([]),
            analyses_collection=_FakeCollection(
                [
                    {
                        "_id": ObjectId(),
                        "business_id": business_id,
                        "created_at": datetime(2026, 3, 1, tzinfo=timezone.utc),
                        "overall_sentiment": "mixed",
                    }
                ]
            ),
        )
    )
    source_counts = Counter(str(item.get("source") or "").strip().lower() for item in reviews)
    report["report_metadata"] = {
        "report_source_mode": "auto",
        "report_sources_included": [k for k, v in source_counts.items() if v > 0],
        "source_counts": dict(source_counts),
    }
    return report


def _render_html(tmp_path: Path, report_payload: dict[str, Any]) -> str:
    renderer = StructuredReportRenderer(artifacts_root=tmp_path)
    artifacts = asyncio.run(
        renderer.render(
            report_payload=report_payload,
            intro_context_text="Contexto de prueba",
            business_id=str(report_payload.get("business_id") or ObjectId()),
            analysis_id=str(ObjectId()),
            output_format="html",
        )
    )
    html_path = Path(str((artifacts.get("html") or {}).get("path")))
    return html_path.read_text(encoding="utf-8")


def _assert_no_empty_sections(html_content: str) -> None:
    assert not re.search(r"<section[^>]*>\s*</section>", html_content, flags=re.IGNORECASE | re.DOTALL)


def test_case_a_solo_google(tmp_path: Path) -> None:
    report = _build_report_from_reviews(_load_reviews("reviews_google_only.json"))

    source_reports = report.get("source_reports")
    assert isinstance(source_reports, dict)
    assert "google_maps" in source_reports
    assert "tripadvisor" not in source_reports
    assert report.get("source_comparison") is None

    html_content = _render_html(tmp_path, report)
    assert "Lectura por fuente: Google Maps" not in html_content
    assert "Lectura por fuente: Tripadvisor" not in html_content
    assert "Comparativa entre fuentes" not in html_content
    assert "Google Maps" in html_content
    _assert_no_empty_sections(html_content)


def test_case_b_solo_tripadvisor(tmp_path: Path) -> None:
    report = _build_report_from_reviews(_load_reviews("reviews_tripadvisor_only.json"))

    source_reports = report.get("source_reports")
    assert isinstance(source_reports, dict)
    assert "tripadvisor" in source_reports
    assert "google_maps" not in source_reports
    assert report.get("source_comparison") is None

    html_content = _render_html(tmp_path, report)
    assert "Lectura por fuente: Google Maps" not in html_content
    assert "Comparativa entre fuentes" not in html_content
    assert "Tripadvisor" in html_content
    assert "Google Maps (" not in html_content
    _assert_no_empty_sections(html_content)


def test_case_c_combined(tmp_path: Path) -> None:
    report = _build_report_from_reviews(_load_reviews("reviews_combined.json"))

    source_reports = report.get("source_reports")
    assert isinstance(source_reports, dict)
    assert "google_maps" in source_reports
    assert "tripadvisor" in source_reports
    source_comparison = report.get("source_comparison")
    assert isinstance(source_comparison, dict)
    assert "narrativa_comparacion" in source_comparison
    assert "recomendaciones" in source_comparison

    html_content = _render_html(tmp_path, report)
    assert "Lectura por fuente: Google Maps" in html_content
    assert "Lectura por fuente: Tripadvisor" in html_content
    assert "Comparativa entre fuentes" in html_content
    assert "Google Maps" in html_content
    assert "Tripadvisor" in html_content
    _assert_no_empty_sections(html_content)


def test_legacy_payload_sin_campos_nuevos() -> None:
    payload = ReportGenerateTaskPayload(
        business_id=str(ObjectId()),
        analysis_id=str(ObjectId()),
    )
    assert payload.source_mode == "auto"
    assert payload.selected_source is None


def test_builder_output_backward_compatible() -> None:
    report = _build_report_from_reviews(_load_reviews("reviews_google_only.json"))
    sections = report.get("sections")
    assert isinstance(sections, dict)
    assert "1_resumen_ejecutivo" in sections
    assert "annexes" in report
    assert "source_reports" in report
