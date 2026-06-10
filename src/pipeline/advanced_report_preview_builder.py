from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from src.pipeline.advanced_report_payload_assembly import build_advanced_report_preview_payload


class AdvancedReportPreviewBuilder:
    def __init__(
        self,
        *,
        compress_text: Callable[[str], str],
        safe_float: Callable[[Any], float],
        normalize_text: Callable[[str], str],
    ) -> None:
        self._compress_text = compress_text
        self._safe_float = safe_float
        self._normalize_text = normalize_text

    def build_preview_report(
        self,
        *,
        advanced_report: dict[str, Any],
        business_name: str | None = None,
        max_comments: int = 3,
    ) -> dict[str, Any]:
        if not isinstance(advanced_report, dict):
            advanced_report = {}

        sections = advanced_report.get("sections")
        if not isinstance(sections, dict):
            sections = {}

        section_resumen = sections.get("1_resumen_ejecutivo")
        if not isinstance(section_resumen, dict):
            section_resumen = {}
        section_score = sections.get("2_score_reputacion")
        if not isinstance(section_score, dict):
            section_score = {}
        section_customer = sections.get("3_quien_es_tu_cliente_y_que_le_preocupa")
        if not isinstance(section_customer, dict):
            section_customer = {}

        tipologias = section_customer.get("tipologias_cliente_top3")
        if not isinstance(tipologias, list):
            tipologias = []

        annexes = advanced_report.get("annexes")
        if not isinstance(annexes, dict):
            annexes = {}
        full_data = annexes.get("full_data")
        if not isinstance(full_data, dict):
            full_data = {}
        review_rows = full_data.get("review_rows")
        if not isinstance(review_rows, list):
            review_rows = []

        selected_comments = self._select_preview_comments(
            tipologias=tipologias,
            review_rows=review_rows,
            max_comments=max_comments,
        )
        preview_types = self._compose_preview_types(
            tipologias=tipologias,
            selected_comments=selected_comments,
            max_items=max_comments,
        )
        summary_source = str(section_resumen.get("diagnostico", "") or "").strip()
        score_display = str(section_score.get("score_display", "") or "").strip()
        score_label = str(section_score.get("nivel_reputacion", "") or "").strip()
        resolved_name = str(
            business_name
            or advanced_report.get("business_name")
            or "Negocio"
        ).strip() or "Negocio"

        summary_preview = self._compress_text(summary_source, max_chars=540)
        if not summary_preview:
            summary_preview = (
                f"{resolved_name} mantiene señales positivas, pero convive con segmentos de cliente "
                "que muestran expectativas no cumplidas en puntos críticos de experiencia."
            )

        return build_advanced_report_preview_payload(
            resolved_name=resolved_name,
            source_report_version=str(advanced_report.get("report_version", "") or "").strip() or None,
            score_display=score_display or None,
            score_label=score_label or None,
            summary_preview=summary_preview,
            selected_comments=selected_comments,
            preview_types=preview_types,
            max_comments=max_comments,
        )

    def _select_preview_comments(
        self,
        *,
        tipologias: list[dict[str, Any]],
        review_rows: list[dict[str, Any]],
        max_comments: int = 3,
    ) -> list[dict[str, Any]]:
        normalized_rows = [row for row in review_rows if isinstance(row, dict)]
        if not normalized_rows:
            return []

        by_cluster: dict[int, list[dict[str, Any]]] = defaultdict(list)
        fallback_rows: list[dict[str, Any]] = []
        for row in normalized_rows:
            text = str(row.get("review_excerpt", "") or "").strip()
            if not text:
                continue
            cluster_id_raw = row.get("cluster_id")
            try:
                cluster_id = int(cluster_id_raw)
            except (TypeError, ValueError):
                cluster_id = None
            if cluster_id is None:
                fallback_rows.append(row)
            else:
                by_cluster[cluster_id].append(row)
                fallback_rows.append(row)

        for cluster_rows in by_cluster.values():
            cluster_rows.sort(key=self._preview_comment_score, reverse=True)
        fallback_rows.sort(key=self._preview_comment_score, reverse=True)

        selected: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for tip in tipologias:
            if len(selected) >= max(1, int(max_comments)):
                break
            if not isinstance(tip, dict):
                continue
            try:
                cluster_id = int(tip.get("cluster_id"))
            except (TypeError, ValueError):
                continue
            candidates = by_cluster.get(cluster_id) or []
            if not candidates:
                continue
            row = candidates[0]
            row_id = self._preview_row_identity(row)
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            selected.append(self._serialize_preview_comment(row=row, tipologia=tip))

        for row in fallback_rows:
            if len(selected) >= max(1, int(max_comments)):
                break
            row_id = self._preview_row_identity(row)
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            selected.append(self._serialize_preview_comment(row=row, tipologia=None))

        return selected[: max(1, int(max_comments))]

    def _compose_preview_types(
        self,
        *,
        tipologias: list[dict[str, Any]],
        selected_comments: list[dict[str, Any]],
        max_items: int = 3,
    ) -> list[dict[str, Any]]:
        max_items = max(1, int(max_items))
        comments_by_cluster: dict[int, dict[str, Any]] = {}
        for comment in selected_comments:
            if not isinstance(comment, dict):
                continue
            cluster_id_raw = comment.get("cluster_id")
            try:
                cluster_id = int(cluster_id_raw)
            except (TypeError, ValueError):
                continue
            comments_by_cluster[cluster_id] = comment

        output: list[dict[str, Any]] = []
        for tip in tipologias:
            if len(output) >= max_items:
                break
            if not isinstance(tip, dict):
                continue
            cluster_id = tip.get("cluster_id")
            representative_comment = None
            try:
                representative_comment = comments_by_cluster.get(int(cluster_id))
            except (TypeError, ValueError):
                representative_comment = None
            output.append(
                {
                    "cluster_id": cluster_id,
                    "label": tip.get("label"),
                    "estado_emocional": tip.get("estado_emocional"),
                    "intencion_detectada": tip.get("intencion_detectada"),
                    "expectativas": tip.get("expectativas"),
                    "comentario_representativo": representative_comment,
                }
            )

        if output:
            return output

        for comment in selected_comments[:max_items]:
            output.append(
                {
                    "cluster_id": comment.get("cluster_id"),
                    "label": comment.get("cluster_label") or "Cliente detectado",
                    "estado_emocional": "Sin clasificación disponible",
                    "intencion_detectada": "Sin clasificación disponible",
                    "expectativas": "Sin clasificación disponible",
                    "comentario_representativo": comment,
                }
            )
        return output

    def _preview_comment_score(self, row: dict[str, Any]) -> float:
        text = str(row.get("review_excerpt", "") or "").strip()
        sentiment = abs(self._safe_float(row.get("sentiment")))
        expectation_gap = self._safe_float(row.get("expectation_gap"))
        improvement_intent = self._safe_float(row.get("improvement_intent"))
        rating = self._safe_float(row.get("rating"))
        rating_impact = abs((rating - 3.0) / 2.0)
        length_score = min(1.0, len(text) / 300.0)
        return (
            (sentiment * 1.6)
            + (expectation_gap * 1.2)
            + (improvement_intent * 0.8)
            + (rating_impact * 0.6)
            + (length_score * 0.4)
        )

    def _preview_row_identity(self, row: dict[str, Any]) -> str:
        review_index = str(row.get("review_index", "") or "").strip()
        author_name = str(row.get("author_name", "") or "").strip()
        excerpt = str(row.get("review_excerpt", "") or "").strip()[:80]
        return f"{review_index}|{author_name}|{excerpt}"

    def _serialize_preview_comment(self, *, row: dict[str, Any], tipologia: dict[str, Any] | None) -> dict[str, Any]:
        quote = self._compress_text(str(row.get("review_excerpt", "") or "").strip(), max_chars=320)
        sentiment = self._safe_float(row.get("sentiment"))
        expectation_gap = self._safe_float(row.get("expectation_gap"))
        improvement_intent = self._safe_float(row.get("improvement_intent"))

        if sentiment <= -0.2 and expectation_gap >= 0.3:
            relevance_reason = "Muestra una expectativa no cumplida con impacto emocional claro."
        elif sentiment >= 0.2:
            relevance_reason = "Refuerza los factores que más valoran los clientes satisfechos."
        elif improvement_intent >= 0.35:
            relevance_reason = "Aporta señales concretas de mejora priorizable."
        else:
            relevance_reason = "Resume una experiencia representativa del segmento detectado."

        cluster_label = str(row.get("cluster_label", "") or "").strip()
        if not cluster_label and isinstance(tipologia, dict):
            cluster_label = str(tipologia.get("label", "") or "").strip()

        return {
            "cluster_id": row.get("cluster_id"),
            "cluster_label": cluster_label or None,
            "author_name": str(row.get("author_name", "") or "").strip() or "Cliente anónimo",
            "source": str(row.get("source", "") or "").strip() or "unknown",
            "rating": round(self._safe_float(row.get("rating")), 2),
            "quote": quote,
            "relevance_reason": relevance_reason,
        }
