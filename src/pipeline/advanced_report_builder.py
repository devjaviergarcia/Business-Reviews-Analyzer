from __future__ import annotations

import asyncio
import json
import math
import re
import statistics
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId

from src.config import settings

try:
    from google import genai
    from google.genai import errors as genai_errors
except Exception:  # pragma: no cover - optional dependency at runtime.
    genai = None
    genai_errors = None


class AdvancedBusinessReportBuilder:
    """Build a structured multi-section reputation report from analyzed reviews."""

    _POSITIVE_TOKENS = (
        "excelente",
        "genial",
        "perfecto",
        "perfecta",
        "bueno",
        "buena",
        "increible",
        "increíble",
        "maravilloso",
        "maravillosa",
        "amable",
        "recomendable",
        "volveremos",
        "great",
        "excellent",
        "amazing",
        "friendly",
        "recommend",
    )
    _NEGATIVE_TOKENS = (
        "malo",
        "mala",
        "fatal",
        "horrible",
        "terrible",
        "pésimo",
        "pesimo",
        "caro",
        "cara",
        "frio",
        "fría",
        "fria",
        "lento",
        "lenta",
        "sucio",
        "sucia",
        "decepcion",
        "decepción",
        "never",
        "bad",
        "awful",
        "slow",
        "dirty",
        "disappoint",
    )
    _EXPECTATION_TOKENS = (
        "esperaba",
        "esperabamos",
        "esperábamos",
        "deberia",
        "debería",
        "por este precio",
        "mejorable",
        "decepcion",
        "decepción",
        "could be better",
        "expected",
    )
    _IMPROVEMENT_TOKENS = (
        "deberian",
        "deberían",
        "podrian",
        "podrían",
        "sugiero",
        "recomiendo mejorar",
        "mejorar",
        "seria mejor",
        "sería mejor",
        "would improve",
        "should",
    )
    _AGGRESSIVE_TOKENS = (
        "vergüenza",
        "verguenza",
        "estafa",
        "nunca",
        "jamas",
        "jamás",
        "horrible",
        "asqueroso",
        "desastre",
        "inaceptable",
        "impresentable",
        "worst",
        "scam",
        "unacceptable",
    )
    _THEME_KEYWORDS: dict[str, tuple[str, ...]] = {
        "servicio": (
            "servicio",
            "camarero",
            "camarera",
            "atencion",
            "atención",
            "staff",
            "trato",
        ),
        "tiempo_espera": (
            "espera",
            "tardar",
            "tardaron",
            "lento",
            "lenta",
            "cola",
            "wait",
        ),
        "precio_valor": (
            "precio",
            "caro",
            "cara",
            "calidad precio",
            "quality price",
            "coste",
            "cost",
        ),
        "calidad_comida": (
            "comida",
            "plato",
            "sabor",
            "frio",
            "fría",
            "fria",
            "caliente",
            "food",
            "taste",
        ),
        "limpieza": (
            "limpio",
            "limpia",
            "sucio",
            "sucia",
            "higiene",
            "baño",
            "bano",
            "clean",
            "dirty",
        ),
        "ambiente_ruido": (
            "ruido",
            "ruidoso",
            "ruidosa",
            "ambiente",
            "musica",
            "música",
            "noise",
            "loud",
        ),
        "gestion_reservas": (
            "reserva",
            "booking",
            "cancelacion",
            "cancelación",
            "mesa",
            "confirmacion",
            "confirmación",
        ),
    }
    _GENERIC_COMMENT_PROBLEM = "Comentario general no específico"
    _POSITIVE_COMMENT_PROBLEM = "Comentario positivo no específico"
    _NEGATIVE_COMMENT_PROBLEM = "Comentario negativo no específico"
    _NO_COMMENT_HIGH_PROBLEM = "Valoración alta sin comentarios"
    _NO_COMMENT_MEDIUM_PROBLEM = "Valoración media sin comentarios"
    _NO_COMMENT_LOW_PROBLEM = "Valoración baja sin comentarios"
    _STOPWORDS = {
        "de",
        "la",
        "el",
        "y",
        "a",
        "en",
        "que",
        "por",
        "con",
        "para",
        "los",
        "las",
        "un",
        "una",
        "muy",
        "del",
        "al",
        "se",
        "es",
        "lo",
        "le",
        "me",
        "mi",
        "no",
        "si",
        "sin",
        "como",
        "todo",
        "esta",
        "está",
        "the",
        "and",
        "for",
        "with",
        "was",
        "were",
        "this",
        "that",
        "from",
        "our",
        "very",
    }

    def __init__(self, *, model_name: str | None = None) -> None:
        self.model_name = str(model_name or settings.gemini_model or "gemini-2.5-flash").strip()
        self.fallback_models = ["gemini-2.5-flash", "gemini-flash-latest"]
        if genai is not None and settings.gemini_api_key:
            try:
                self.client = genai.Client(api_key=settings.gemini_api_key)
            except Exception:
                self.client = None
        else:
            self.client = None

    async def build(
        self,
        *,
        business_id: str,
        business_name: str,
        listing: dict[str, Any] | None,
        stats: dict[str, Any],
        reviews: list[dict[str, Any]],
        analysis_payload: dict[str, Any],
        businesses_collection,
        analyses_collection,
    ) -> dict[str, Any]:
        review_metrics = [self._score_review_dimensions(index=idx, review=review) for idx, review in enumerate(reviews)]

        customer_clusters = self._build_customer_clusters(review_metrics=review_metrics)
        problem_clusters = self._build_problem_clusters(review_metrics=review_metrics)
        business_context = self._build_business_context(
            business_name=business_name,
            listing=listing if isinstance(listing, dict) else {},
            stats=stats,
        )

        benchmarking = await self._build_benchmarking(
            business_id=business_id,
            business_name=business_name,
            listing=listing,
            stats=stats,
            review_metrics=review_metrics,
            businesses_collection=businesses_collection,
        )
        score_and_evolution = await self._build_score_and_evolution(
            business_id=business_id,
            stats=stats,
            review_metrics=review_metrics,
            analyses_collection=analyses_collection,
        )

        voice_of_customer = self._build_voice_of_customer(review_metrics=review_metrics)
        action_plan = self._build_action_plan(problem_clusters=problem_clusters, customer_clusters=customer_clusters)
        quick_wins = self._build_quick_wins(stats=stats, problem_clusters=problem_clusters, action_plan=action_plan)
        invisible_and_opportunities = self._build_invisible_and_opportunities(
            stats=stats,
            review_metrics=review_metrics,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
        )
        llm_clustering_insights = await self._build_llm_clustering_insights(
            business_name=business_name,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            quick_wins=quick_wins,
        )
        llm_section_narratives = await self._build_llm_section_narratives(
            business_name=business_name,
            business_context=business_context,
            score_and_evolution=score_and_evolution,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            invisible_and_opportunities=invisible_and_opportunities,
            action_plan=action_plan,
            quick_wins=quick_wins,
        )
        full_data_annex = self._build_full_data_annex(
            stats=stats,
            review_metrics=review_metrics,
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            analysis_payload=analysis_payload,
        )

        score_value = self._safe_float(score_and_evolution.get("reputation_score"))
        score_label = self._score_label(score_value)
        customer_clusters_top = self._summarize_customer_clusters(customer_clusters=customer_clusters, limit=3)
        problem_clusters_top = self._summarize_problem_clusters(problem_clusters=problem_clusters, limit=3)
        benchmarking_summary = {
            "target_rank": benchmarking.get("target_rank"),
            "total_competitors_compared": benchmarking.get("total_competitors_compared"),
            "target_reputation_score": benchmarking.get("reputation_score"),
            "top_competitors": (benchmarking.get("ranking") or [])[:3],
        }

        sections = {
            "1_resumen_ejecutivo": {
                "diagnostico": llm_section_narratives["resumen_ejecutivo"],
                "estado_actual": {
                    "score_reputacion": score_value,
                    "nivel_reputacion": score_label,
                    "cluster_count": customer_clusters.get("cluster_count"),
                    "problemas_principales": [
                        str(item.get("problem", "") or "") for item in problem_clusters_top
                    ],
                },
                "aciertos_notorios": [
                    str(item.get("quote", "") or "")
                    for item in (voice_of_customer.get("positive_quotes") or [])[:3]
                ],
            },
            "2_score_reputacion": {
                "score_display": f"{round(score_value, 1)}/100",
                "score_value": score_value,
                "nivel_reputacion": score_label,
                "explicacion": llm_section_narratives["score"],
                "componentes_numericos": score_and_evolution.get("components"),
                "evolucion": score_and_evolution.get("evolution"),
            },
            "3_quien_es_tu_cliente_y_que_le_preocupa": {
                "lectura_ejecutiva": llm_section_narratives["cliente_y_preocupaciones"],
                "tipologias_cliente_top3": customer_clusters_top,
                "preocupaciones_top3": problem_clusters_top,
                "scatter_clientes": customer_clusters.get("scatter"),
            },
            "4_plan_de_accion": {
                "lectura_ejecutiva": llm_section_narratives["plan_accion"],
                "problemas_invisibles": invisible_and_opportunities.get("invisible_problems"),
                "corto_plazo_0_30_dias": action_plan.get("inmediato_0_30_dias"),
                "medio_plazo_30_90_dias": action_plan.get("medio_30_90_dias"),
                "largo_plazo_90_mas_dias": action_plan.get("largo_90_mas_dias"),
                "quick_wins_esta_semana": quick_wins.get("items"),
            },
            "5_anexos_resumen": {
                "nota": (
                    "Los anexos completos se entregan fuera del PDF principal en archivos separados "
                    "(CSV y PDF de anexos)."
                ),
                "resumen_dataset": full_data_annex.get("dataset_summary"),
                "benchmarking_resumen": benchmarking_summary,
                "voz_literal_muestra": voice_of_customer,
            },
        }

        annexes = {
            "full_data": full_data_annex,
            "benchmarking_full": benchmarking,
            "voice_of_customer": voice_of_customer,
            "customer_clusters_full": customer_clusters,
            "problem_clusters_full": problem_clusters,
        }

        return {
            "report_version": "2026.2",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "business_id": business_id,
            "business_name": business_name,
            "business_context": business_context,
            "section_order": list(sections.keys()),
            "sections": sections,
            "llm_clustering_insights": llm_clustering_insights,
            "llm_section_narratives": llm_section_narratives,
            "annexes": annexes,
        }

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

        cta_text = (
            "Este documento es un aperitivo del diagnóstico. "
            "Si quieres recibir el plan de acción completo, el análisis integral de clústeres y "
            "la priorización detallada, rellena el formulario para solicitar el reporte completo."
        )

        return {
            "preview_version": "2026.1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "business_name": resolved_name,
            "source_report_version": str(advanced_report.get("report_version", "") or "").strip() or None,
            "sections": {
                "1_resumen_ejecutivo_preview": {
                    "texto": summary_preview,
                    "score": score_display or None,
                    "nivel_reputacion": score_label or None,
                    "nota": "Resumen parcial basado en el informe principal.",
                },
                "2_tipos_cliente_y_comentarios_relevantes": {
                    "tipos_cliente": preview_types,
                    "comentarios_relevantes": selected_comments[: max(1, int(max_comments))],
                },
                "3_llamada_a_la_accion": {
                    "texto": cta_text,
                    "accion_recomendada": "Completar formulario para recibir el reporte completo.",
                },
            },
        }

    def _score_review_dimensions(self, *, index: int, review: dict[str, Any]) -> dict[str, Any]:
        text = str(review.get("text", "") or "").strip()
        author_name = str(review.get("author_name", "") or "").strip()
        rating = self._safe_rating(review.get("rating"))
        text_norm = self._normalize_text(text)

        pos_hits = self._count_keyword_hits(text_norm, self._POSITIVE_TOKENS)
        neg_hits = self._count_keyword_hits(text_norm, self._NEGATIVE_TOKENS)
        exp_hits = self._count_keyword_hits(text_norm, self._EXPECTATION_TOKENS)
        imp_hits = self._count_keyword_hits(text_norm, self._IMPROVEMENT_TOKENS)
        aggr_hits = self._count_keyword_hits(text_norm, self._AGGRESSIVE_TOKENS)

        lexical_sentiment = (pos_hits - neg_hits) / max(1, pos_hits + neg_hits)
        rating_sentiment = (rating - 3.0) / 2.0
        sentiment = self._clamp(rating_sentiment * 0.75 + lexical_sentiment * 0.25, -1.0, 1.0)

        expectation_gap = self._clamp01(
            (max(0.0, 3.0 - rating) / 3.0) * 0.55 + min(1.0, exp_hits / 3.0) * 0.45
        )
        satisfaction = self._clamp01((rating / 5.0) * 0.8 + max(0.0, lexical_sentiment) * 0.2)

        punctuation_aggr = 1.0 if "!!" in text else 0.0
        upper_ratio = self._upper_ratio(text)
        upper_aggr = 1.0 if upper_ratio >= 0.35 and len(text) >= 20 else 0.0
        aggr_score = self._clamp01((aggr_hits + punctuation_aggr + upper_aggr) / 4.0)
        tranquility_aggressiveness = self._clamp(1.0 - (2.0 * aggr_score), -1.0, 1.0)

        improvement_intent = self._clamp01(min(1.0, imp_hits / 3.0) * 0.7 + expectation_gap * 0.3)

        theme_scores = self._theme_scores(text_norm)
        dominant_problem = self._resolve_dominant_problem(
            rating=rating,
            text_norm=text_norm,
            sentiment=sentiment,
            theme_scores=theme_scores,
        )

        customer_key = self._normalize_text(author_name)
        if not customer_key:
            review_id = str(review.get("review_id") or "").strip()
            customer_key = review_id or f"anon_{index}"

        return {
            "index": index,
            "customer_key": customer_key,
            "author_name": author_name or "Cliente anónimo",
            "rating": rating,
            "source": str(review.get("source", "") or "").strip() or "unknown",
            "text": text,
            "relative_time_bucket": str(review.get("relative_time_bucket", "unknown") or "unknown"),
            "has_owner_reply": bool(review.get("has_owner_reply")),
            "owner_reply": str(review.get("owner_reply", "") or "").strip(),
            "dimensions": {
                "sentiment": round(sentiment, 4),
                "expectation_gap": round(expectation_gap, 4),
                "satisfaction": round(satisfaction, 4),
                "tranquility_aggressiveness": round(tranquility_aggressiveness, 4),
                "improvement_intent": round(improvement_intent, 4),
            },
            "theme_scores": theme_scores,
            "dominant_problem": dominant_problem,
        }

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

        # fallback if there are no explicit customer types available
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
        return (sentiment * 1.6) + (expectation_gap * 1.2) + (improvement_intent * 0.8) + (rating_impact * 0.6) + (length_score * 0.4)

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

    async def _build_benchmarking(
        self,
        *,
        business_id: str,
        business_name: str,
        listing: dict[str, Any] | None,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        businesses_collection,
    ) -> dict[str, Any]:
        categories_raw = (listing or {}).get("categories")
        categories = [str(item).strip() for item in categories_raw] if isinstance(categories_raw, list) else []
        categories = [item for item in categories if item][:3]

        target_query_id = None
        try:
            target_query_id = ObjectId(str(business_id))
        except (InvalidId, TypeError):
            target_query_id = None

        base_query: dict[str, Any] = {"listing.overall_rating": {"$gt": 0}}
        if target_query_id is not None:
            base_query["_id"] = {"$ne": target_query_id}
        if categories:
            base_query["listing.categories"] = {"$in": categories}

        competitor_docs = await businesses_collection.find(base_query).limit(120).to_list(length=120)
        if len(competitor_docs) < 5 and categories:
            fallback_query = {"listing.overall_rating": {"$gt": 0}}
            if target_query_id is not None:
                fallback_query["_id"] = {"$ne": target_query_id}
            competitor_docs = await businesses_collection.find(fallback_query).limit(120).to_list(length=120)

        target_avg_rating = self._safe_float((stats or {}).get("avg_rating"))
        target_response_rate = self._safe_float((stats or {}).get("response_rate"))
        target_neg_ratio = self._negative_ratio(review_metrics=review_metrics)
        target_score = self._compute_reputation_score(
            avg_rating=target_avg_rating,
            response_rate=target_response_rate,
            negative_ratio=target_neg_ratio,
            sentiment_avg=self._average_dimension(review_metrics, "sentiment"),
            tranquility_avg=self._average_dimension(review_metrics, "tranquility_aggressiveness"),
        )

        target_record = {
            "business_id": business_id,
            "name": business_name,
            "avg_rating": round(target_avg_rating, 3),
            "review_count": len(review_metrics),
            "reputation_score": target_score,
            "is_target": True,
        }
        competitors: list[dict[str, Any]] = []
        for doc in competitor_docs:
            listing_payload = doc.get("listing")
            listing_dict = listing_payload if isinstance(listing_payload, dict) else {}
            avg_rating = self._safe_float(
                listing_dict.get("overall_rating")
                if listing_dict.get("overall_rating") is not None
                else (doc.get("stats") or {}).get("avg_rating")
            )
            review_count = self._safe_int(doc.get("review_count") or 0)
            if avg_rating <= 0:
                continue
            score = round((avg_rating / 5.0) * 80.0 + min(20.0, math.log1p(max(0, review_count)) * 4.5), 2)
            competitors.append(
                {
                    "business_id": str(doc.get("_id")),
                    "name": str(doc.get("name", "") or ""),
                    "avg_rating": round(avg_rating, 3),
                    "review_count": review_count,
                    "reputation_score": score,
                    "is_target": False,
                }
            )

        ranking = [target_record, *competitors]
        ranking.sort(key=lambda item: (float(item.get("reputation_score", 0.0)), float(item.get("avg_rating", 0.0))), reverse=True)
        target_rank = 1
        for idx, item in enumerate(ranking, start=1):
            if item.get("is_target"):
                target_rank = idx
                break

        top_competitors = [item for item in ranking if not item.get("is_target")][:8]
        nearest_competitors = sorted(
            top_competitors,
            key=lambda item: abs(float(item.get("avg_rating", 0.0)) - target_avg_rating),
        )[:5]

        return {
            "target": target_record,
            "target_rank": target_rank,
            "total_competitors_compared": len(competitors),
            "top_competitors": top_competitors,
            "nearest_by_rating": nearest_competitors,
            "comparison_note": (
                "Benchmark calculado sobre negocios con rating público en la base de datos."
            ),
        }

    async def _build_score_and_evolution(
        self,
        *,
        business_id: str,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        analyses_collection,
    ) -> dict[str, Any]:
        avg_rating = self._safe_float((stats or {}).get("avg_rating"))
        response_rate = self._safe_float((stats or {}).get("response_rate"))
        negative_ratio = self._negative_ratio(review_metrics=review_metrics)
        sentiment_avg = self._average_dimension(review_metrics, "sentiment")
        tranquility_avg = self._average_dimension(review_metrics, "tranquility_aggressiveness")
        score = self._compute_reputation_score(
            avg_rating=avg_rating,
            response_rate=response_rate,
            negative_ratio=negative_ratio,
            sentiment_avg=sentiment_avg,
            tranquility_avg=tranquility_avg,
        )

        analysis_docs = (
            await analyses_collection.find({"business_id": business_id})
            .sort([("created_at", -1), ("_id", -1)])
            .limit(12)
            .to_list(length=12)
        )
        history = []
        for doc in reversed(analysis_docs):
            sentiment_value = str(doc.get("overall_sentiment", "") or "").strip().lower()
            sentiment_score = {"positive": 1.0, "mixed": 0.0, "negative": -1.0}.get(sentiment_value, 0.0)
            history.append(
                {
                    "analysis_id": str(doc.get("_id")),
                    "created_at": doc.get("created_at"),
                    "overall_sentiment": sentiment_value or "mixed",
                    "sentiment_score": sentiment_score,
                }
            )

        slope = self._linear_slope([float(item.get("sentiment_score", 0.0)) for item in history])
        trend = "estable"
        if slope >= 0.07:
            trend = "al_alza"
        elif slope <= -0.07:
            trend = "a_la_baja"

        buckets = defaultdict(list)
        for item in review_metrics:
            bucket = str(item.get("relative_time_bucket", "unknown") or "unknown")
            buckets[bucket].append(float(item.get("dimensions", {}).get("satisfaction", 0.0)))

        bucket_summary = {
            key: round(statistics.mean(values), 4) if values else 0.0
            for key, values in buckets.items()
        }

        return {
            "reputation_score": score,
            "score_scale": "0-100",
            "components": {
                "avg_rating": round(avg_rating, 3),
                "response_rate": round(response_rate, 4),
                "negative_ratio": round(negative_ratio, 4),
                "sentiment_avg": round(sentiment_avg, 4),
                "tranquility_avg": round(tranquility_avg, 4),
            },
            "evolution": {
                "trend": trend,
                "trend_slope": round(slope, 5),
                "analyses_history": history,
                "satisfaccion_por_antiguedad_resena": bucket_summary,
            },
        }

    def _build_customer_clusters(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        profiles: dict[str, dict[str, Any]] = {}
        for item in review_metrics:
            key = str(item.get("customer_key") or "").strip() or f"anon_{item.get('index', 0)}"
            profile = profiles.get(key)
            if profile is None:
                profile = {
                    "customer_id": key,
                    "display_name": str(item.get("author_name", "Cliente anónimo") or "Cliente anónimo"),
                    "review_count": 0,
                    "ratings": [],
                    "sentiment": [],
                    "expectation_gap": [],
                    "satisfaction": [],
                    "tranquility_aggressiveness": [],
                    "improvement_intent": [],
                }
                profiles[key] = profile

            dims = item.get("dimensions") or {}
            profile["review_count"] += 1
            profile["ratings"].append(self._safe_float(item.get("rating")))
            profile["sentiment"].append(self._safe_float(dims.get("sentiment")))
            profile["expectation_gap"].append(self._safe_float(dims.get("expectation_gap")))
            profile["satisfaction"].append(self._safe_float(dims.get("satisfaction")))
            profile["tranquility_aggressiveness"].append(self._safe_float(dims.get("tranquility_aggressiveness")))
            profile["improvement_intent"].append(self._safe_float(dims.get("improvement_intent")))

        customers = []
        for profile in profiles.values():
            customer = {
                "customer_id": profile["customer_id"],
                "display_name": profile["display_name"],
                "review_count": int(profile["review_count"]),
                "avg_rating": round(statistics.mean(profile["ratings"]) if profile["ratings"] else 0.0, 4),
                "sentiment": round(statistics.mean(profile["sentiment"]) if profile["sentiment"] else 0.0, 4),
                "expectation_gap": round(
                    statistics.mean(profile["expectation_gap"]) if profile["expectation_gap"] else 0.0, 4
                ),
                "satisfaction": round(
                    statistics.mean(profile["satisfaction"]) if profile["satisfaction"] else 0.0, 4
                ),
                "tranquility_aggressiveness": round(
                    statistics.mean(profile["tranquility_aggressiveness"])
                    if profile["tranquility_aggressiveness"]
                    else 0.0,
                    4,
                ),
                "improvement_intent": round(
                    statistics.mean(profile["improvement_intent"]) if profile["improvement_intent"] else 0.0, 4
                ),
            }
            customers.append(customer)

        if not customers:
            return {
                "cluster_count": 0,
                "clusters": [],
                "scatter": {"axes": {"x": "expectation_gap", "y": "satisfaction"}, "circles": [], "points": []},
            }

        features = []
        for customer in customers:
            features.append(
                [
                    float(customer["sentiment"]),
                    float(customer["expectation_gap"]),
                    float(customer["satisfaction"]),
                    float(customer["tranquility_aggressiveness"]),
                    float(customer["improvement_intent"]),
                    float((customer["avg_rating"] - 3.0) / 2.0),
                ]
            )

        k = 1
        total_customers = len(customers)
        if total_customers >= 20:
            k = 4
        elif total_customers >= 9:
            k = 3
        elif total_customers >= 4:
            k = 2

        labels, centroids = self._kmeans(features=features, k=k, max_iter=30)
        for index, customer in enumerate(customers):
            customer["cluster_id"] = int(labels[index])
            customer["x"] = round(float(customer["expectation_gap"]) * 100.0, 3)
            customer["y"] = round(float(customer["satisfaction"]) * 100.0, 3)
            customer["size"] = max(1.0, float(customer["review_count"]))

        clusters_map: dict[int, dict[str, Any]] = {}
        for customer in customers:
            cluster_id = int(customer["cluster_id"])
            cluster = clusters_map.get(cluster_id)
            if cluster is None:
                cluster = {"cluster_id": cluster_id, "customers": []}
                clusters_map[cluster_id] = cluster
            cluster["customers"].append(customer)

        clusters = []
        circles = []
        for cluster_id in sorted(clusters_map.keys()):
            customers_in_cluster = clusters_map[cluster_id]["customers"]
            centroid = centroids[cluster_id]
            label, description = self._label_customer_cluster(centroid)
            center_x = statistics.mean(float(item["x"]) for item in customers_in_cluster)
            center_y = statistics.mean(float(item["y"]) for item in customers_in_cluster)
            max_distance = 0.0
            for item in customers_in_cluster:
                distance = math.dist((center_x, center_y), (float(item["x"]), float(item["y"])))
                if distance > max_distance:
                    max_distance = distance

            radius = round(max_distance + 4.0, 3)
            circles.append(
                {
                    "cluster_id": cluster_id,
                    "label": label,
                    "center": {"x": round(center_x, 3), "y": round(center_y, 3)},
                    "radius": radius,
                    "count": len(customers_in_cluster),
                }
            )

            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "label": label,
                    "description": description,
                    "count_customers": len(customers_in_cluster),
                    "count_reviews": int(sum(float(item["review_count"]) for item in customers_in_cluster)),
                    "centroid": {
                        "sentiment": round(float(centroid[0]), 4),
                        "expectation_gap": round(float(centroid[1]), 4),
                        "satisfaction": round(float(centroid[2]), 4),
                        "tranquility_aggressiveness": round(float(centroid[3]), 4),
                        "improvement_intent": round(float(centroid[4]), 4),
                    },
                    "sample_customers": [
                        {
                            "display_name": item["display_name"],
                            "review_count": item["review_count"],
                            "avg_rating": item["avg_rating"],
                        }
                        for item in sorted(
                            customers_in_cluster,
                            key=lambda item: (float(item["review_count"]), float(item["avg_rating"])),
                            reverse=True,
                        )[:5]
                    ],
                }
            )

        return {
            "cluster_count": len(clusters),
            "clusters": clusters,
            "scatter": {
                "axes": {
                    "x": "expectation_gap",
                    "x_label": "Brecha de expectativa",
                    "y": "satisfaction",
                    "y_label": "Satisfacción",
                    "size": "review_count",
                },
                "circles": circles,
                "points": [
                    {
                        "customer_id": item["customer_id"],
                        "display_name": item["display_name"],
                        "cluster_id": item["cluster_id"],
                        "x": item["x"],
                        "y": item["y"],
                        "size": item["size"],
                        "review_count": item["review_count"],
                        "avg_rating": item["avg_rating"],
                    }
                    for item in customers
                ],
            },
        }

    def _build_problem_clusters(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        relevant_reviews = []
        for review in review_metrics:
            rating = self._safe_float(review.get("rating"))
            sentiment = self._safe_float((review.get("dimensions") or {}).get("sentiment"))
            if rating <= 3.0 or sentiment < 0:
                relevant_reviews.append(review)

        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for review in relevant_reviews:
            groups[
                str(review.get("dominant_problem", self._GENERIC_COMMENT_PROBLEM) or self._GENERIC_COMMENT_PROBLEM)
            ].append(review)

        clusters = []
        circles = []
        total = max(1, len(relevant_reviews))
        for cluster_index, (problem, items) in enumerate(sorted(groups.items(), key=lambda item: len(item[1]), reverse=True)):
            count = len(items)
            avg_rating = statistics.mean(self._safe_float(item.get("rating")) for item in items) if items else 0.0
            avg_sentiment = (
                statistics.mean(self._safe_float((item.get("dimensions") or {}).get("sentiment")) for item in items)
                if items
                else 0.0
            )
            avg_expectation = (
                statistics.mean(
                    self._safe_float((item.get("dimensions") or {}).get("expectation_gap")) for item in items
                )
                if items
                else 0.0
            )
            share = count / total
            severity = self._clamp01(((5.0 - avg_rating) / 4.0) * 0.7 + max(0.0, -avg_sentiment) * 0.3)
            keywords = self._extract_top_keywords(items=items, limit=8)

            x = round(share * 100.0, 3)
            y = round(severity * 100.0, 3)
            radius = round(max(4.0, math.sqrt(count) * 4.0), 3)
            circles.append(
                {
                    "cluster_id": cluster_index,
                    "label": problem,
                    "center": {"x": x, "y": y},
                    "radius": radius,
                    "count": count,
                }
            )

            sample_quotes = []
            for item in items:
                text = str(item.get("text", "") or "").strip()
                if not text:
                    continue
                sample_quotes.append(
                    {
                        "author_name": item.get("author_name"),
                        "rating": item.get("rating"),
                        "quote": text[:280],
                    }
                )
            sample_quotes = sample_quotes[:5]

            clusters.append(
                {
                    "cluster_id": cluster_index,
                    "problem": problem,
                    "count": count,
                    "share": round(share, 4),
                    "avg_rating": round(avg_rating, 4),
                    "avg_sentiment": round(avg_sentiment, 4),
                    "avg_expectation_gap": round(avg_expectation, 4),
                    "severity": round(severity, 4),
                    "keywords": keywords,
                    "sample_quotes": sample_quotes,
                }
            )

        return {
            "cluster_count": len(clusters),
            "clusters": clusters,
            "scatter": {
                "axes": {
                    "x": "frequency_share",
                    "x_label": "Frecuencia del problema",
                    "y": "severity",
                    "y_label": "Severidad",
                    "size": "review_count",
                },
                "circles": circles,
            },
        }

    def _build_voice_of_customer(self, *, review_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        positives = []
        negatives = []
        improvements = []
        owner_replies = []

        for item in review_metrics:
            text = str(item.get("text", "") or "").strip()
            if not text:
                continue
            dims = item.get("dimensions") or {}
            sentiment = self._safe_float(dims.get("sentiment"))
            improvement_intent = self._safe_float(dims.get("improvement_intent"))
            payload = {
                "author_name": str(item.get("author_name", "") or "").strip() or "Cliente anónimo",
                "rating": self._safe_float(item.get("rating")),
                "source": item.get("source"),
                "quote": text[:320],
            }
            if sentiment >= 0.35 and len(positives) < 8:
                positives.append(payload)
            if sentiment <= -0.2 and len(negatives) < 8:
                negatives.append(payload)
            if improvement_intent >= 0.35 and len(improvements) < 8:
                improvements.append(payload)

            owner_reply = str(item.get("owner_reply", "") or "").strip()
            if owner_reply and len(owner_replies) < 8:
                owner_replies.append(
                    {
                        "author_name": payload["author_name"],
                        "rating": payload["rating"],
                        "customer_quote": payload["quote"],
                        "owner_reply": owner_reply[:320],
                    }
                )

        return {
            "positive_quotes": positives[:5],
            "negative_quotes": negatives[:5],
            "improvement_quotes": improvements[:5],
            "owner_reply_examples": owner_replies[:5],
        }

    def _build_action_plan(
        self,
        *,
        problem_clusters: dict[str, Any],
        customer_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        clusters = clusters if isinstance(clusters, list) else []

        top_problems = clusters[:3]
        immediate = []
        medium = []
        long_term = []

        for problem in top_problems:
            label_raw = str(problem.get("problem", self._GENERIC_COMMENT_PROBLEM) or self._GENERIC_COMMENT_PROBLEM)
            label = self._friendly_problem_label(label_raw)
            severity = self._safe_float(problem.get("severity"))
            impact = "alto" if severity >= 0.65 else "medio"
            immediate.append(
                {
                    "action": f"Mejorar de inmediato el punto '{label}' en la operativa diaria.",
                    "accion": f"Mejorar de inmediato el punto '{label}' en la operativa diaria.",
                    "por_que": (
                        "Este tema aparece de forma repetida en reseñas recientes y afecta "
                        "a la satisfacción del cliente."
                    ),
                    "impact": impact,
                    "owner": "Responsable de operación",
                    "encargado": "Encargado de operaciones del local",
                    "horizon_days": 14,
                    "kpi": f"Reducir en un 25% las menciones negativas sobre {label}.",
                    "objetivo": f"Reducir en un 25% las menciones negativas sobre {label}.",
                }
            )
            medium.append(
                {
                    "action": f"Ordenar y estandarizar el proceso para evitar fallos en '{label}'.",
                    "accion": f"Ordenar y estandarizar el proceso para evitar fallos en '{label}'.",
                    "por_que": "Cuando el proceso es claro y repetible, baja la variabilidad del servicio.",
                    "impact": impact,
                    "owner": "Gerencia + Calidad",
                    "encargado": "Gerencia y persona responsable de calidad",
                    "horizon_days": 60,
                    "kpi": f"Subir al menos 0.2 puntos la satisfacción asociada a {label}.",
                    "objetivo": f"Subir al menos 0.2 puntos la satisfacción asociada a {label}.",
                }
            )
            long_term.append(
                {
                    "action": f"Crear un seguimiento continuo para detectar pronto fallos de '{label}'.",
                    "accion": f"Crear un seguimiento continuo para detectar pronto fallos de '{label}'.",
                    "por_que": "Permite anticiparse a quejas repetidas antes de que dañen la reputación.",
                    "impact": "alto",
                    "owner": "Data/Producto",
                    "encargado": "Dirección junto al responsable de mejora continua",
                    "horizon_days": 120,
                    "kpi": "Tener alertas activas para detectar incidencias antes de que escalen.",
                    "objetivo": "Tener alertas activas para detectar incidencias antes de que escalen.",
                }
            )

        if not immediate:
            immediate.append(
                {
                    "action": "Establecer una rutina semanal para revisar reseñas y cerrar acciones de mejora.",
                    "accion": "Establecer una rutina semanal para revisar reseñas y cerrar acciones de mejora.",
                    "por_que": "Sin rutina de seguimiento, los problemas tienden a repetirse.",
                    "impact": "medio",
                    "owner": "Gerencia",
                    "encargado": "Gerencia del negocio",
                    "horizon_days": 14,
                    "kpi": "Revisar el 100% de reseñas críticas en un máximo de 72 horas.",
                    "objetivo": "Revisar el 100% de reseñas críticas en un máximo de 72 horas.",
                }
            )

        cluster_count = self._safe_int(customer_clusters.get("cluster_count"))
        return {
            "inmediato_0_30_dias": immediate[:5],
            "medio_30_90_dias": medium[:5],
            "largo_90_mas_dias": long_term[:5],
            "notes": [
                f"Se detectaron {cluster_count} segmentos de clientes para personalizar acciones.",
            ],
        }

    def _build_quick_wins(
        self,
        *,
        stats: dict[str, Any],
        problem_clusters: dict[str, Any],
        action_plan: dict[str, Any],
    ) -> dict[str, Any]:
        response_rate = self._safe_float((stats or {}).get("response_rate"))
        quick_wins = []
        if response_rate < 0.35:
            quick_wins.append(
                {
                    "title": "Responder reseñas en menos de 24 horas",
                    "why": "Una respuesta rápida transmite cercanía y reduce el impacto de una mala experiencia.",
                    "effort": "bajo",
                    "impact": "alto",
                }
            )

        clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        clusters = clusters if isinstance(clusters, list) else []
        for cluster in clusters[:3]:
            problem = self._friendly_problem_label(
                str(cluster.get("problem", self._GENERIC_COMMENT_PROBLEM) or self._GENERIC_COMMENT_PROBLEM)
            )
            quick_wins.append(
                {
                    "title": f"Atajar ya el tema '{problem}' con una mejora simple",
                    "why": "Este punto se repite en reseñas críticas y tiene impacto directo en la experiencia.",
                    "effort": "medio",
                    "impact": "alto",
                }
            )

        immediate = action_plan.get("inmediato_0_30_dias") if isinstance(action_plan, dict) else []
        if isinstance(immediate, list):
            for item in immediate[:2]:
                action = str((item or {}).get("action", "")).strip()
                if action:
                    quick_wins.append(
                        {
                            "title": action,
                            "why": "Ya priorizado en plan inmediato.",
                            "effort": "medio",
                            "impact": str((item or {}).get("impact", "medio")),
                        }
                    )

        deduped: list[dict[str, Any]] = []
        seen = set()
        for item in quick_wins:
            key = self._normalize_text(str(item.get("title", "") or ""))
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return {"items": deduped[:7]}

    def _build_invisible_and_opportunities(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
    ) -> dict[str, Any]:
        total_reviews = max(1, len(review_metrics))
        without_text = sum(1 for item in review_metrics if not str(item.get("text", "") or "").strip())
        no_text_ratio = without_text / total_reviews
        high_aggressive = sum(
            1
            for item in review_metrics
            if self._safe_float((item.get("dimensions") or {}).get("tranquility_aggressiveness")) <= -0.35
        )
        aggressive_ratio = high_aggressive / total_reviews
        response_rate = self._safe_float((stats or {}).get("response_rate"))

        invisible = []
        if no_text_ratio >= 0.25:
            invisible.append(
                {
                    "risk": "Volumen alto de reseñas sin texto",
                    "detail": "Puede ocultar fricciones no diagnosticadas.",
                    "metric": round(no_text_ratio, 4),
                }
            )
        if aggressive_ratio >= 0.15:
            invisible.append(
                {
                    "risk": "Tono agresivo relevante",
                    "detail": "Existe un subgrupo con experiencia emocionalmente intensa.",
                    "metric": round(aggressive_ratio, 4),
                }
            )
        if response_rate < 0.35:
            invisible.append(
                {
                    "risk": "Baja tasa de respuesta",
                    "detail": "Se pierde oportunidad de recuperación de cliente.",
                    "metric": round(response_rate, 4),
                }
            )

        clusters = customer_clusters.get("clusters") if isinstance(customer_clusters, dict) else []
        clusters = clusters if isinstance(clusters, list) else []
        opportunities = []
        for cluster in clusters[:3]:
            label = str(cluster.get("label", "") or "").strip()
            centroid = cluster.get("centroid") if isinstance(cluster.get("centroid"), dict) else {}
            satisfaction = self._safe_float(centroid.get("satisfaction"))
            improvement = self._safe_float(centroid.get("improvement_intent"))
            if satisfaction >= 0.65:
                opportunities.append(
                    {
                        "opportunity": f"Activar programa de recomendación para cluster '{label}'.",
                        "metric": round(satisfaction, 4),
                    }
                )
            if improvement >= 0.45:
                opportunities.append(
                    {
                        "opportunity": f"Co-crear mejoras con clientes del cluster '{label}'.",
                        "metric": round(improvement, 4),
                    }
                )

        problem_data = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        if isinstance(problem_data, list) and problem_data:
            main_problem = str(problem_data[0].get("problem", "") or "").strip()
            if main_problem:
                opportunities.append(
                    {
                        "opportunity": f"Convertir '{main_problem}' en palanca de diferenciación operativa.",
                        "metric": round(self._safe_float(problem_data[0].get("severity")), 4),
                    }
                )

        return {
            "invisible_problems": invisible[:6],
            "opportunities": opportunities[:6],
        }

    def _build_full_data_annex(
        self,
        *,
        stats: dict[str, Any],
        review_metrics: list[dict[str, Any]],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        analysis_payload: dict[str, Any],
    ) -> dict[str, Any]:
        avg_dims = {
            "sentiment": round(self._average_dimension(review_metrics, "sentiment"), 4),
            "expectation_gap": round(self._average_dimension(review_metrics, "expectation_gap"), 4),
            "satisfaction": round(self._average_dimension(review_metrics, "satisfaction"), 4),
            "tranquility_aggressiveness": round(
                self._average_dimension(review_metrics, "tranquility_aggressiveness"), 4
            ),
            "improvement_intent": round(self._average_dimension(review_metrics, "improvement_intent"), 4),
        }
        by_source = Counter(str(item.get("source", "unknown") or "unknown") for item in review_metrics)
        by_problem = Counter(
            str(item.get("dominant_problem", self._GENERIC_COMMENT_PROBLEM) or self._GENERIC_COMMENT_PROBLEM)
            for item in review_metrics
        )

        customer_points = (
            ((customer_clusters.get("scatter") or {}).get("points") if isinstance(customer_clusters, dict) else [])
            or []
        )
        point_cluster_map: dict[str, int] = {}
        if isinstance(customer_points, list):
            for point in customer_points:
                if not isinstance(point, dict):
                    continue
                customer_id = str(point.get("customer_id", "") or "").strip()
                if not customer_id:
                    continue
                try:
                    point_cluster_map[customer_id] = int(point.get("cluster_id"))
                except (TypeError, ValueError):
                    continue

        cluster_label_map: dict[int, str] = {}
        clusters_full = customer_clusters.get("clusters") if isinstance(customer_clusters.get("clusters"), list) else []
        for cluster in clusters_full:
            if not isinstance(cluster, dict):
                continue
            try:
                cluster_id = int(cluster.get("cluster_id"))
            except (TypeError, ValueError):
                continue
            label = str(cluster.get("label", "") or "").strip()
            if label:
                cluster_label_map[cluster_id] = label

        compact_points = []
        if isinstance(customer_points, list):
            for item in customer_points[:200]:
                if not isinstance(item, dict):
                    continue
                compact_points.append(
                    {
                        "customer_id": item.get("customer_id"),
                        "cluster_id": item.get("cluster_id"),
                        "x": item.get("x"),
                        "y": item.get("y"),
                        "review_count": item.get("review_count"),
                    }
                )

        review_rows = []
        for item in review_metrics:
            dims = item.get("dimensions") if isinstance(item.get("dimensions"), dict) else {}
            customer_key = str(item.get("customer_key", "") or "").strip()
            cluster_id = point_cluster_map.get(customer_key)
            cluster_label = cluster_label_map.get(cluster_id) if cluster_id is not None else None
            review_rows.append(
                {
                    "review_index": self._safe_int(item.get("index")),
                    "customer_key": customer_key or None,
                    "cluster_id": cluster_id,
                    "cluster_label": cluster_label,
                    "source": str(item.get("source", "") or "").strip() or "unknown",
                    "author_name": str(item.get("author_name", "") or "").strip() or "Cliente anónimo",
                    "rating": round(self._safe_float(item.get("rating")), 2),
                    "sentiment": round(self._safe_float(dims.get("sentiment")), 4),
                    "expectation_gap": round(self._safe_float(dims.get("expectation_gap")), 4),
                    "satisfaction": round(self._safe_float(dims.get("satisfaction")), 4),
                    "tranquility_aggressiveness": round(
                        self._safe_float(dims.get("tranquility_aggressiveness")), 4
                    ),
                    "improvement_intent": round(self._safe_float(dims.get("improvement_intent")), 4),
                    "dominant_problem": (
                        str(item.get("dominant_problem", "") or "").strip() or self._GENERIC_COMMENT_PROBLEM
                    ),
                    "has_owner_reply": bool(item.get("has_owner_reply")),
                    "owner_reply_excerpt": str(item.get("owner_reply", "") or "").strip()[:280],
                    "review_excerpt": str(item.get("text", "") or "").strip()[:500],
                }
            )

        dataset_summary = {
            "total_reviews": len(review_metrics),
            "avg_rating": round(self._safe_float((stats or {}).get("avg_rating")), 3),
            "response_rate": round(self._safe_float((stats or {}).get("response_rate")), 4),
            "by_source": dict(by_source),
            "by_problem": dict(by_problem),
            "dimension_averages": avg_dims,
        }

        return {
            "dataset_summary": dataset_summary,
            "stats_snapshot": stats,
            "analysis_topics": {
                "overall_sentiment": analysis_payload.get("overall_sentiment"),
                "main_topics": analysis_payload.get("main_topics"),
                "strengths": analysis_payload.get("strengths"),
                "weaknesses": analysis_payload.get("weaknesses"),
            },
            "dimension_averages": avg_dims,
            "counts": {
                "total_reviews": len(review_metrics),
                "by_source": dict(by_source),
                "by_problem": dict(by_problem),
            },
            "review_rows": review_rows,
            "cluster_assignments_compact": compact_points,
            "problem_clusters_summary": (
                problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
            ),
        }

    def _build_executive_summary(
        self,
        *,
        business_name: str,
        score_and_evolution: dict[str, Any],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        benchmarking: dict[str, Any],
        quick_wins: dict[str, Any],
        llm_clustering_insights: dict[str, Any],
    ) -> dict[str, Any]:
        score = self._safe_float(score_and_evolution.get("reputation_score"))
        trend = str(((score_and_evolution.get("evolution") or {}).get("trend") or "estable")).strip()
        target_rank = self._safe_int(benchmarking.get("target_rank"))
        total_competitors = self._safe_int(benchmarking.get("total_competitors_compared"))
        top_problem = ""
        clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        if isinstance(clusters, list) and clusters:
            top_problem_raw = str(clusters[0].get("problem", "") or "").strip()
            top_problem = self._friendly_problem_label(top_problem_raw)
        cluster_count = self._safe_int(customer_clusters.get("cluster_count"))
        quick_win_items = quick_wins.get("items") if isinstance(quick_wins, dict) else []
        quick_win_count = len(quick_win_items) if isinstance(quick_win_items, list) else 0

        headline = (
            f"{business_name}: reputación {round(score, 1)}/100, tendencia {trend.replace('_', ' ')}, "
            f"{cluster_count} segmentos de cliente detectados."
        )
        bullets = [
            f"Ranking competitivo: posición {target_rank} de {max(1, total_competitors + 1)}.",
            f"Principal foco de fricción: {top_problem or self._GENERIC_COMMENT_PROBLEM}.",
            f"Acciones rápidas identificadas esta semana: {quick_win_count}.",
            "Tenemos tipos de cliente diferenciados para personalizar mejor las decisiones.",
        ]

        llm_text = str(llm_clustering_insights.get("text", "") or "").strip()
        if llm_text:
            bullets.append(f"Lectura del modelo sobre tipos de cliente: {llm_text[:220]}")

        return {
            "headline": headline,
            "bullets": bullets,
            "one_page_takeaway": (
                "Priorizar respuesta rápida a feedback crítico, atacar el problema dominante "
                "y convertir segmentos satisfechos en palanca de recomendación."
            ),
        }

    def _build_business_context(
        self,
        *,
        business_name: str,
        listing: dict[str, Any],
        stats: dict[str, Any],
    ) -> dict[str, Any]:
        categories_raw = listing.get("categories") if isinstance(listing.get("categories"), list) else []
        categories = [str(item or "").strip() for item in categories_raw if str(item or "").strip()]
        normalized_scope = self._normalize_text(" ".join([business_name, *categories]))

        if any(token in normalized_scope for token in ("hotel", "hostal", "hostel", "pension", "hospederia")):
            profile = {
                "tipo_negocio": "alojamiento",
                "cliente_espera": [
                    "limpieza consistente y descanso real",
                    "trato resolutivo en recepción",
                    "buena relación calidad-precio",
                    "check-in/check-out ágiles",
                ],
                "motivacion_de_visita": "descansar, dormir bien y resolver necesidades básicas sin fricción",
                "fricciones_habituales": [
                    "ruido nocturno",
                    "higiene mejorable",
                    "incidencias no resueltas en tiempo",
                ],
            }
        elif any(token in normalized_scope for token in ("restaurante", "foodestablishment", "bar", "burger", "pizza", "cafe")):
            profile = {
                "tipo_negocio": "restauración",
                "cliente_espera": [
                    "comida consistente y sabrosa",
                    "servicio atento y tiempos razonables",
                    "ambiente agradable según ocasión",
                    "precio percibido como justo frente a lo recibido",
                ],
                "motivacion_de_visita": "disfrutar una experiencia gastronómica con buena atención y valor claro",
                "fricciones_habituales": [
                    "demoras en sala",
                    "raciones o calidad percibidas como insuficientes",
                    "desalineación precio-valor",
                ],
            }
        else:
            profile = {
                "tipo_negocio": "servicio local",
                "cliente_espera": [
                    "atención humana clara",
                    "cumplimiento de expectativas básicas",
                    "resolución rápida de incidencias",
                ],
                "motivacion_de_visita": "resolver una necesidad concreta con seguridad y confianza",
                "fricciones_habituales": [
                    "falta de claridad operativa",
                    "tiempos de espera",
                    "experiencia inconsistente",
                ],
            }

        avg_rating = round(self._safe_float((stats or {}).get("avg_rating")), 2)
        profile["rating_medio_observado"] = avg_rating
        profile["categorias_detectadas"] = categories[:8]
        return profile

    def _score_label(self, score_value: float) -> str:
        if score_value >= 85.0:
            return "excelente reputación"
        if score_value >= 70.0:
            return "reputación sólida"
        if score_value >= 55.0:
            return "reputación media mejorable"
        if score_value >= 40.0:
            return "reputación mejorable"
        return "reputación crítica"

    def _summarize_customer_clusters(
        self,
        *,
        customer_clusters: dict[str, Any],
        limit: int = 3,
    ) -> list[dict[str, Any]]:
        clusters = customer_clusters.get("clusters") if isinstance(customer_clusters, dict) else []
        if not isinstance(clusters, list):
            return []
        ranked = sorted(clusters, key=lambda item: self._safe_int(item.get("count_reviews")), reverse=True)
        output: list[dict[str, Any]] = []
        for cluster in ranked[: max(0, int(limit))]:
            centroid = cluster.get("centroid") if isinstance(cluster.get("centroid"), dict) else {}
            satisfaction = self._safe_float(centroid.get("satisfaction"))
            expectation_gap = self._safe_float(centroid.get("expectation_gap"))
            tranquility = self._safe_float(centroid.get("tranquility_aggressiveness"))
            improvement_intent = self._safe_float(centroid.get("improvement_intent"))

            if satisfaction >= 0.75 and tranquility >= 0.4:
                emotional_state = "satisfecho y calmado"
            elif satisfaction <= 0.5 or tranquility <= 0.0:
                emotional_state = "frustrado o tensionado"
            else:
                emotional_state = "neutral-pragmático"

            if improvement_intent >= 0.45:
                intent_state = "alta intención de mejora explícita"
            elif improvement_intent >= 0.2:
                intent_state = "intención de mejora moderada"
            else:
                intent_state = "baja intención de cambio, prioriza continuidad"

            if expectation_gap >= 0.35:
                expectation_state = "expectativas no cubiertas de forma relevante"
            elif expectation_gap >= 0.15:
                expectation_state = "brecha parcial entre expectativa y experiencia"
            else:
                expectation_state = "expectativas mayoritariamente cumplidas"

            output.append(
                {
                    "cluster_id": cluster.get("cluster_id"),
                    "label": cluster.get("label"),
                    "descripcion_segmento": cluster.get("description"),
                    "peso_reseñas": self._safe_int(cluster.get("count_reviews")),
                    "peso_clientes": self._safe_int(cluster.get("count_customers")),
                    "estado_emocional": emotional_state,
                    "intencion_detectada": intent_state,
                    "expectativas": expectation_state,
                }
            )
        return output

    def _summarize_problem_clusters(
        self,
        *,
        problem_clusters: dict[str, Any],
        limit: int = 3,
    ) -> list[dict[str, Any]]:
        clusters = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        if not isinstance(clusters, list):
            return []
        output: list[dict[str, Any]] = []
        for cluster in clusters[: max(0, int(limit))]:
            quotes = cluster.get("sample_quotes") if isinstance(cluster.get("sample_quotes"), list) else []
            first_quote = quotes[0] if quotes else {}
            output.append(
                {
                    "problema": self._friendly_problem_label(
                        str(cluster.get("problem", "") or "").strip() or self._GENERIC_COMMENT_PROBLEM
                    ),
                    "volumen": self._safe_int(cluster.get("count")),
                    "severidad": round(self._safe_float(cluster.get("severity")), 4),
                    "rating_medio_asociado": round(self._safe_float(cluster.get("avg_rating")), 2),
                    "tono_medio": round(self._safe_float(cluster.get("avg_sentiment")), 4),
                    "ejemplo_literal": str(first_quote.get("quote", "") or "").strip()[:280],
                }
            )
        return output

    async def _build_llm_section_narratives(
        self,
        *,
        business_name: str,
        business_context: dict[str, Any],
        score_and_evolution: dict[str, Any],
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        invisible_and_opportunities: dict[str, Any],
        action_plan: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> dict[str, str]:
        score_value = self._safe_float(score_and_evolution.get("reputation_score"))
        score_label = self._score_label(score_value)
        customer_top = self._summarize_customer_clusters(customer_clusters=customer_clusters, limit=3)
        problem_top = self._summarize_problem_clusters(problem_clusters=problem_clusters, limit=3)
        quick_titles = [
            str(item.get("title", "") or "").strip()
            for item in (quick_wins.get("items") if isinstance(quick_wins, dict) else [])[:5]
            if isinstance(item, dict)
        ]
        invisible_items = (
            invisible_and_opportunities.get("invisible_problems")
            if isinstance(invisible_and_opportunities, dict)
            else []
        )
        fallback = {
            "resumen_ejecutivo": (
                f"Ahora mismo {business_name} está en {score_label} ({round(score_value, 1)}/100). "
                "Hay un grupo claro de clientes contentos que sostiene la reputación, "
                "pero también hay otro grupo que se queja de esperas, servicio y relación calidad-precio. "
                "La oportunidad está en arreglar esos fallos repetidos sin perder lo que ya funciona bien."
            ),
            "score": (
                f"La puntuación {round(score_value, 1)}/100 indica {score_label}. "
                "No sale solo de la media de estrellas: también cuenta cómo habla la gente en sus reseñas, "
                "si el negocio responde y si hay muchas opiniones claramente negativas. "
                "En resumen: combina números y sensación real del cliente."
            ),
            "cliente_y_preocupaciones": (
                "Se ven tres tipos de cliente bastante claros: el que sale encantado, "
                "el que ve cosas mejorables y el que acaba frustrado. "
                "Cada uno viene con expectativas distintas, así que conviene ajustar el servicio "
                "a lo que más se repite en sus comentarios."
            ),
            "plan_accion": (
                "El plan tiene que ir por fases: primero arreglos rápidos que se noten ya, "
                "luego cambios de proceso para que no se repitan errores, "
                "y por último mejoras más grandes de fondo. "
                "Todo con tareas concretas y responsables claros."
            ),
        }

        if self.client is None:
            return fallback

        payload = {
            "negocio": business_name,
            "contexto_negocio": business_context,
            "score": {
                "valor": score_value,
                "label": score_label,
                "componentes": score_and_evolution.get("components"),
                "evolucion": score_and_evolution.get("evolution"),
            },
            "segmentos_cliente_top3": customer_top,
            "problemas_top3": problem_top,
            "problemas_invisibles": invisible_items[:4] if isinstance(invisible_items, list) else [],
            "acciones_corto": (action_plan.get("inmediato_0_30_dias") if isinstance(action_plan, dict) else [])[:4],
            "acciones_medio": (action_plan.get("medio_30_90_dias") if isinstance(action_plan, dict) else [])[:4],
            "acciones_largo": (action_plan.get("largo_90_mas_dias") if isinstance(action_plan, dict) else [])[:4],
            "quick_wins": quick_titles,
        }
        prompt = (
            "Eres consultor de reputación para pymes. Escribe en español de España, cercano y fácil de entender, "
            "como si se lo explicaras al dueño de un negocio local sin formación técnica. "
            "Nada de jerga ni anglicismos. Evita palabras como cluster, KPI, owner, insight, benchmark.\n"
            "Debes contextualizar el negocio: qué espera su cliente típico, qué fallos pesan más y cómo se siente la gente.\n"
            "Devuelve SOLO JSON válido con claves exactas:\n"
            "{\n"
            '  "resumen_ejecutivo": "...",\n'
            '  "score": "...",\n'
            '  "cliente_y_preocupaciones": "...",\n'
            '  "plan_accion": "..."\n'
            "}\n"
            "Cada valor: 4-7 frases cortas, directas, útiles para decidir.\n"
            f"Datos: {payload}"
        )
        try:
            text, _model_used = await asyncio.to_thread(self._llm_generate_text, prompt)
            extracted = self._extract_json_object(text)
            parsed = json.loads(extracted)
            if not isinstance(parsed, dict):
                return fallback
            merged: dict[str, str] = {}
            for key, fallback_text in fallback.items():
                value = str(parsed.get(key, "") or "").strip()
                merged[key] = self._plainify_business_text(value or fallback_text)
            return merged
        except Exception:
            return fallback

    def _extract_json_object(self, text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            raise ValueError("Empty LLM response.")
        fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, re.DOTALL | re.IGNORECASE)
        if fenced:
            return fenced.group(1).strip()
        inline = re.search(r"(\{.*\})", raw, re.DOTALL)
        if inline:
            return inline.group(1).strip()
        raise ValueError("Could not extract JSON object from LLM response.")

    async def _build_llm_clustering_insights(
        self,
        *,
        business_name: str,
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> dict[str, Any]:
        fallback_text = self._fallback_clustering_text(
            customer_clusters=customer_clusters,
            problem_clusters=problem_clusters,
            quick_wins=quick_wins,
        )
        if self.client is None:
            return {
                "generated": False,
                "model": None,
                "text": fallback_text,
                "reason": "llm_unavailable",
            }

        payload = {
            "business_name": business_name,
            "customer_clusters": customer_clusters.get("clusters"),
            "problem_clusters": problem_clusters.get("clusters"),
            "quick_wins": quick_wins.get("items"),
        }
        prompt = (
            "Eres analista de experiencia cliente. Con estos datos, escribe SOLO texto plano "
            "en español cercano y muy claro (máximo 8 líneas). "
            "Evita tecnicismos y palabras en inglés. "
            "Explica: 1) tipos de cliente que aparecen, 2) problemas críticos, "
            "3) qué hacer primero en lenguaje sencillo.\n"
            f"Datos: {payload}"
        )
        try:
            text, model_used = await asyncio.to_thread(self._llm_generate_text, prompt)
            clean_text = str(text or "").strip()
            if not clean_text:
                raise RuntimeError("Empty LLM clustering output.")
            return {
                "generated": True,
                "model": model_used,
                "text": self._plainify_business_text(clean_text),
            }
        except Exception:
            return {
                "generated": False,
                "model": None,
                "text": self._plainify_business_text(fallback_text),
                "reason": "llm_failed",
            }

    def _llm_generate_text(self, prompt: str) -> tuple[str, str | None]:
        candidates = list(dict.fromkeys([self.model_name, *self.fallback_models]))
        last_error: Exception | None = None
        for model_name in candidates:
            try:
                response = self.client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                text = self._extract_llm_text(response)
                if text:
                    return text, model_name
            except Exception as exc:
                if genai_errors is not None and isinstance(exc, getattr(genai_errors, "ClientError", Exception)):
                    code = getattr(exc, "code", None)
                    if code == 404:
                        last_error = exc
                        continue
                last_error = exc
                continue
        if last_error:
            raise last_error
        return "", None

    def _extract_llm_text(self, response: object) -> str:
        texts: list[str] = []
        candidates = getattr(response, "candidates", None) or []
        for candidate in candidates:
            content = getattr(candidate, "content", None)
            if content is None:
                continue
            for part in getattr(content, "parts", None) or []:
                text = getattr(part, "text", None)
                if text:
                    texts.append(str(text).strip())
        return "\n".join(item for item in texts if item).strip()

    def _fallback_clustering_text(
        self,
        *,
        customer_clusters: dict[str, Any],
        problem_clusters: dict[str, Any],
        quick_wins: dict[str, Any],
    ) -> str:
        clusters = customer_clusters.get("clusters") if isinstance(customer_clusters, dict) else []
        problems = problem_clusters.get("clusters") if isinstance(problem_clusters, dict) else []
        wins = quick_wins.get("items") if isinstance(quick_wins, dict) else []
        cluster_label = (
            str((clusters or [{}])[0].get("label", "segmentos mixtos")) if isinstance(clusters, list) and clusters else "segmentos mixtos"
        )
        problem_label = (
            str((problems or [{}])[0].get("problem", self._GENERIC_COMMENT_PROBLEM))
            if isinstance(problems, list) and problems
            else self._GENERIC_COMMENT_PROBLEM
        )
        return (
            f"El tipo de cliente que más pesa es '{cluster_label}', y el problema que más se repite es '{problem_label}'. "
            f"Hay {len(wins) if isinstance(wins, list) else 0} acciones rápidas ya detectadas para mejorar en el corto plazo."
        )

    def _plainify_business_text(self, text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        replacements = (
            ("satisfaction by relative time bucket", "satisfacción por antigüedad de reseña"),
            ("cluster", "tipo de cliente"),
            ("clusters", "tipos de cliente"),
            ("kpi", "indicador de seguimiento"),
            ("kpis", "indicadores de seguimiento"),
            ("owner", "encargado"),
            ("impact", "impacto"),
            ("benchmark", "comparativa"),
            ("benchmarking", "comparativa con competidores"),
            ("quick wins", "acciones rápidas"),
            ("insight", "hallazgo"),
            ("roadmap", "plan"),
            ("horizon", "plazo"),
            ("score", "puntuación de reputación"),
            ("trend", "tendencia"),
            ("response rate", "tasa de respuesta a comentarios"),
            ("dataset", "conjunto de reseñas"),
            ("bucket", "tramo temporal"),
            ("checklist", "lista de tareas a realizar"),
            ("checklists", "listas de tareas a realizar"),
            ("<24h", "menos de 24 horas"),
            ("old", "antiguas"),
            ("medium", "intermedias"),
            ("recent", "recientes"),
        )
        lowered = value
        for src, dst in replacements:
            lowered = re.sub(rf"\b{re.escape(src)}\b", dst, lowered, flags=re.IGNORECASE)
        lowered = re.sub(r"\bel tendencia\b", "la tendencia", lowered, flags=re.IGNORECASE)
        lowered = lowered.replace("**", "")
        lowered = re.sub(r"\s+", " ", lowered).strip()
        return lowered

    def _compute_reputation_score(
        self,
        *,
        avg_rating: float,
        response_rate: float,
        negative_ratio: float,
        sentiment_avg: float,
        tranquility_avg: float,
    ) -> float:
        rating_component = self._clamp01(avg_rating / 5.0) * 55.0
        response_component = self._clamp01(response_rate) * 20.0
        sentiment_component = self._clamp01((sentiment_avg + 1.0) / 2.0) * 15.0
        stability_component = self._clamp01((tranquility_avg + 1.0) / 2.0) * 10.0
        penalty = self._clamp01(negative_ratio) * 10.0
        score = rating_component + response_component + sentiment_component + stability_component - penalty
        return round(self._clamp(score, 0.0, 100.0), 2)

    def _negative_ratio(self, *, review_metrics: list[dict[str, Any]]) -> float:
        if not review_metrics:
            return 0.0
        negative_count = 0
        for item in review_metrics:
            rating = self._safe_float(item.get("rating"))
            sentiment = self._safe_float((item.get("dimensions") or {}).get("sentiment"))
            if rating <= 2.0 or sentiment <= -0.25:
                negative_count += 1
        return negative_count / max(1, len(review_metrics))

    def _average_dimension(self, review_metrics: list[dict[str, Any]], key: str) -> float:
        if not review_metrics:
            return 0.0
        values: list[float] = []
        for item in review_metrics:
            dims = item.get("dimensions") if isinstance(item.get("dimensions"), dict) else {}
            values.append(self._safe_float(dims.get(key)))
        return statistics.mean(values) if values else 0.0

    def _label_customer_cluster(self, centroid: list[float]) -> tuple[str, str]:
        sentiment, expectation, satisfaction, tranquility, improvement, _ = centroid
        if satisfaction >= 0.68 and sentiment >= 0.25:
            return (
                "Promotores satisfechos",
                "Valoran positivamente la experiencia y pueden convertirse en embajadores de marca.",
            )
        if sentiment <= -0.1 and tranquility < -0.05:
            return (
                "Críticos intensos",
                "Clientes con fricción emocional alta que requieren recuperación prioritaria.",
            )
        if expectation >= 0.38 and improvement >= 0.42:
            return (
                "Exigentes constructivos",
                "Ven margen de mejora y aportan señales útiles para rediseñar servicio.",
            )
        return (
            "Neutrales pragmáticos",
            "Segmento estable con satisfacción media y sensibilidad a mejoras operativas.",
        )

    def _theme_scores(self, text_norm: str) -> dict[str, int]:
        if not text_norm:
            return {}
        scores: dict[str, int] = {}
        for theme, keywords in self._THEME_KEYWORDS.items():
            hits = self._count_keyword_hits(text_norm, keywords)
            if hits > 0:
                scores[theme] = hits
        return scores

    def _resolve_dominant_problem(
        self,
        *,
        rating: float,
        text_norm: str,
        sentiment: float,
        theme_scores: dict[str, int],
    ) -> str:
        if theme_scores:
            return max(theme_scores.items(), key=lambda item: item[1])[0]

        if not text_norm:
            if rating >= 4.0:
                return self._NO_COMMENT_HIGH_PROBLEM
            if rating <= 2.0:
                return self._NO_COMMENT_LOW_PROBLEM
            return self._NO_COMMENT_MEDIUM_PROBLEM

        if rating <= 2.5 or sentiment <= -0.2:
            return self._NEGATIVE_COMMENT_PROBLEM
        if rating >= 4.0 or sentiment >= 0.25:
            return self._POSITIVE_COMMENT_PROBLEM
        return self._GENERIC_COMMENT_PROBLEM

    def _extract_top_keywords(self, *, items: list[dict[str, Any]], limit: int = 8) -> list[str]:
        token_counter: Counter[str] = Counter()
        for item in items:
            text = self._normalize_text(str(item.get("text", "") or ""))
            tokens = re.findall(r"[a-záéíóúñü]{3,}", text, flags=re.IGNORECASE)
            for token in tokens:
                normalized = self._normalize_text(token)
                if not normalized or normalized in self._STOPWORDS:
                    continue
                token_counter[normalized] += 1
        return [term for term, _ in token_counter.most_common(limit)]

    def _compress_text(self, text: str, *, max_chars: int) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        max_chars = max(40, int(max_chars))
        normalized = re.sub(r"\s+", " ", raw).strip()
        if len(normalized) <= max_chars:
            return normalized
        clipped = normalized[: max_chars - 1].rstrip()
        last_space = clipped.rfind(" ")
        if last_space >= 32:
            clipped = clipped[:last_space]
        return clipped.rstrip(".,;:- ") + "…"

    def _friendly_problem_label(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "experiencia del cliente"
        mapped = {
            self._GENERIC_COMMENT_PROBLEM: "experiencia general",
            self._POSITIVE_COMMENT_PROBLEM: "experiencia general positiva",
            self._NEGATIVE_COMMENT_PROBLEM: "experiencia general negativa",
            self._NO_COMMENT_HIGH_PROBLEM: "valoración alta sin comentario",
            self._NO_COMMENT_MEDIUM_PROBLEM: "valoración media sin comentario",
            self._NO_COMMENT_LOW_PROBLEM: "valoración baja sin comentario",
            "tiempo_espera": "tiempo de espera",
            "precio_valor": "relación calidad-precio",
            "calidad_comida": "calidad de la comida",
            "ambiente_ruido": "ambiente y ruido",
            "gestion_reservas": "gestión de reservas",
        }
        if raw in mapped:
            return mapped[raw]
        normalized = raw.replace("_", " ").strip()
        return normalized

    def _normalize_text(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = normalized.lower()
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _count_keyword_hits(self, text: str, keywords: tuple[str, ...]) -> int:
        if not text:
            return 0
        count = 0
        for keyword in keywords:
            normalized = self._normalize_text(keyword)
            if not normalized:
                continue
            if normalized in text:
                count += 1
        return count

    def _safe_rating(self, value: Any) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = 0.0
        return self._clamp(parsed, 0.0, 5.0)

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    def _upper_ratio(self, text: str) -> float:
        letters = [ch for ch in str(text or "") if ch.isalpha()]
        if not letters:
            return 0.0
        uppercase = [ch for ch in letters if ch.isupper()]
        return len(uppercase) / len(letters)

    def _clamp(self, value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(max_value, value))

    def _clamp01(self, value: float) -> float:
        return self._clamp(value, 0.0, 1.0)

    def _linear_slope(self, values: list[float]) -> float:
        if len(values) < 2:
            return 0.0
        n = len(values)
        x_mean = (n - 1) / 2.0
        y_mean = statistics.mean(values)
        numerator = 0.0
        denominator = 0.0
        for idx, value in enumerate(values):
            dx = idx - x_mean
            numerator += dx * (value - y_mean)
            denominator += dx * dx
        if denominator == 0:
            return 0.0
        return numerator / denominator

    def _kmeans(self, *, features: list[list[float]], k: int, max_iter: int) -> tuple[list[int], list[list[float]]]:
        if not features:
            return [], []
        k = max(1, min(k, len(features)))
        if k == 1:
            centroid = [statistics.mean(values) for values in zip(*features)]
            return [0 for _ in features], [centroid]

        normalized_features, means, stds = self._zscore(features)

        initial_indexes = [0]
        while len(initial_indexes) < k:
            farthest_index = 0
            farthest_distance = -1.0
            for idx, point in enumerate(normalized_features):
                if idx in initial_indexes:
                    continue
                distance = min(
                    math.dist(point, normalized_features[center_idx]) for center_idx in initial_indexes
                )
                if distance > farthest_distance:
                    farthest_distance = distance
                    farthest_index = idx
            initial_indexes.append(farthest_index)

        centroids = [list(normalized_features[idx]) for idx in initial_indexes]
        labels = [0 for _ in normalized_features]

        for _ in range(max(1, max_iter)):
            changed = False
            for idx, point in enumerate(normalized_features):
                best_cluster = 0
                best_distance = float("inf")
                for cluster_id, centroid in enumerate(centroids):
                    distance = math.dist(point, centroid)
                    if distance < best_distance:
                        best_distance = distance
                        best_cluster = cluster_id
                if labels[idx] != best_cluster:
                    labels[idx] = best_cluster
                    changed = True

            cluster_points: dict[int, list[list[float]]] = defaultdict(list)
            for idx, label in enumerate(labels):
                cluster_points[label].append(normalized_features[idx])

            for cluster_id in range(k):
                points = cluster_points.get(cluster_id) or []
                if not points:
                    continue
                centroids[cluster_id] = [statistics.mean(values) for values in zip(*points)]

            if not changed:
                break

        denormalized_centroids: list[list[float]] = []
        for centroid in centroids:
            denormalized = []
            for idx, value in enumerate(centroid):
                denormalized.append((value * stds[idx]) + means[idx])
            denormalized_centroids.append(denormalized)

        return labels, denormalized_centroids

    def _zscore(self, features: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
        dimensions = len(features[0])
        means: list[float] = []
        stds: list[float] = []
        for dim in range(dimensions):
            values = [row[dim] for row in features]
            mean_value = statistics.mean(values) if values else 0.0
            std_value = statistics.pstdev(values) if len(values) >= 2 else 0.0
            means.append(mean_value)
            stds.append(std_value if std_value > 1e-9 else 1.0)

        normalized: list[list[float]] = []
        for row in features:
            normalized.append(
                [((row[idx] - means[idx]) / stds[idx]) for idx in range(dimensions)]
            )
        return normalized, means, stds
