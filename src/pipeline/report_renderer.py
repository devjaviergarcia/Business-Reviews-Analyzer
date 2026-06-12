from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.pipeline.report_rendering import ReportRenderingSectionsMixin
from src.pipeline.report_rendering.annex_csv_export import write_annex_review_rows_csv
from src.pipeline.report_rendering.annex_report_document import build_annex_report_html
from src.pipeline.report_rendering.artifact_layout import (
    build_final_report_artifact_layout,
    build_preview_report_artifact_layout,
)
from src.pipeline.report_rendering.final_report_document import build_final_report_html
from src.pipeline.report_rendering.pdf_export import render_pdf_from_html
from src.pipeline.report_rendering.preview_report_document import build_preview_report_html


class StructuredReportRenderer(ReportRenderingSectionsMixin):
    """Render structured report data to JSON/HTML/PDF artifacts."""

    _PALETTE = (
        "#0A7567",
        "#12B08A",
        "#D4F0E8",
        "#D4950A",
        "#C23B18",
        "#64748B",
    )

    def __init__(self, *, artifacts_root: str | Path = "artifacts/reports") -> None:
        self.artifacts_root = Path(artifacts_root)
        self._resolved_artifacts_root: Path | None = None

    async def render(
        self,
        *,
        report_payload: dict[str, Any],
        intro_context_text: str,
        business_id: str,
        analysis_id: str,
        output_format: str = "pdf",
    ) -> dict[str, Any]:
        normalized_format = str(output_format or "pdf").strip().lower() or "pdf"
        business_name = str(report_payload.get("business_name", "") or "").strip() or "negocio"
        artifacts_root = self._resolve_writable_artifacts_root()
        artifact_layout = build_final_report_artifact_layout(
            artifacts_root=artifacts_root,
            business_name=business_name,
            business_id=str(business_id),
            analysis_id=str(analysis_id),
            safe_name_slug=self._safe_name_slug,
            safe_identifier_slug=self._safe_identifier_slug,
        )

        artifact_layout.json_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2, default=self._json_default),
            encoding="utf-8",
        )

        html_content = build_final_report_html(
            renderer=self,
            report_payload=report_payload,
            intro_context_text=intro_context_text,
        )
        artifact_layout.html_path.write_text(html_content, encoding="utf-8")

        annexes_payload = report_payload.get("annexes")
        if not isinstance(annexes_payload, dict):
            annexes_payload = {}
        write_annex_review_rows_csv(
            annexes_payload=annexes_payload,
            csv_path=artifact_layout.annex_csv_path,
        )
        annex_html = build_annex_report_html(
            renderer=self,
            report_payload=report_payload,
            annexes_payload=annexes_payload,
        )
        artifact_layout.annex_html_path.write_text(annex_html, encoding="utf-8")

        pdf_generated = False
        pdf_error = None
        annex_pdf_generated = False
        annex_pdf_error = None
        if normalized_format == "pdf":
            try:
                await render_pdf_from_html(
                    html_content=html_content,
                    pdf_path=artifact_layout.pdf_path,
                )
                pdf_generated = True
            except Exception as exc:  # noqa: BLE001
                pdf_error = str(exc)
            try:
                await render_pdf_from_html(
                    html_content=annex_html,
                    pdf_path=artifact_layout.annex_pdf_path,
                )
                annex_pdf_generated = True
            except Exception as exc:  # noqa: BLE001
                annex_pdf_error = str(exc)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "output_format": normalized_format,
            "display_name": f"Reporte final - {business_name}",
            "json": {
                "path": str(artifact_layout.json_path.resolve()),
                "filename": artifact_layout.json_path.name,
                "exists": artifact_layout.json_path.exists(),
            },
            "html": {
                "path": str(artifact_layout.html_path.resolve()),
                "filename": artifact_layout.html_path.name,
                "exists": artifact_layout.html_path.exists(),
            },
            "pdf": {
                "path": str(artifact_layout.pdf_path.resolve()),
                "filename": artifact_layout.pdf_path.name,
                "exists": artifact_layout.pdf_path.exists() if normalized_format == "pdf" else False,
                "generated": pdf_generated,
                "error": pdf_error,
            },
            "annex": {
                "csv": {
                    "path": str(artifact_layout.annex_csv_path.resolve()),
                    "filename": artifact_layout.annex_csv_path.name,
                    "exists": artifact_layout.annex_csv_path.exists(),
                },
                "html": {
                    "path": str(artifact_layout.annex_html_path.resolve()),
                    "filename": artifact_layout.annex_html_path.name,
                    "exists": artifact_layout.annex_html_path.exists(),
                },
                "pdf": {
                    "path": str(artifact_layout.annex_pdf_path.resolve()),
                    "filename": artifact_layout.annex_pdf_path.name,
                    "exists": artifact_layout.annex_pdf_path.exists() if normalized_format == "pdf" else False,
                    "generated": annex_pdf_generated,
                    "error": annex_pdf_error,
                },
            },
        }

    async def render_preview(
        self,
        *,
        preview_payload: dict[str, Any],
        business_id: str,
        analysis_id: str,
        output_format: str = "pdf",
    ) -> dict[str, Any]:
        normalized_format = str(output_format or "pdf").strip().lower() or "pdf"
        business_name = str(preview_payload.get("business_name", "") or "").strip() or "negocio"
        artifacts_root = self._resolve_writable_artifacts_root()
        artifact_layout = build_preview_report_artifact_layout(
            artifacts_root=artifacts_root,
            business_name=business_name,
            business_id=str(business_id),
            analysis_id=str(analysis_id),
            safe_name_slug=self._safe_name_slug,
            safe_identifier_slug=self._safe_identifier_slug,
        )

        artifact_layout.json_path.write_text(
            json.dumps(preview_payload, ensure_ascii=False, indent=2, default=self._json_default),
            encoding="utf-8",
        )
        html_content = build_preview_report_html(
            renderer=self,
            preview_payload=preview_payload,
        )
        artifact_layout.html_path.write_text(html_content, encoding="utf-8")

        pdf_generated = False
        pdf_error = None
        if normalized_format == "pdf":
            try:
                await render_pdf_from_html(
                    html_content=html_content,
                    pdf_path=artifact_layout.pdf_path,
                )
                pdf_generated = True
            except Exception as exc:  # noqa: BLE001
                pdf_error = str(exc)

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "output_format": normalized_format,
            "display_name": f"Reporte de bienvenida - {business_name}",
            "json": {
                "path": str(artifact_layout.json_path.resolve()),
                "filename": artifact_layout.json_path.name,
                "exists": artifact_layout.json_path.exists(),
            },
            "html": {
                "path": str(artifact_layout.html_path.resolve()),
                "filename": artifact_layout.html_path.name,
                "exists": artifact_layout.html_path.exists(),
            },
            "pdf": {
                "path": str(artifact_layout.pdf_path.resolve()),
                "filename": artifact_layout.pdf_path.name,
                "exists": artifact_layout.pdf_path.exists() if normalized_format == "pdf" else False,
                "generated": pdf_generated,
                "error": pdf_error,
            },
        }

    def _resolve_writable_artifacts_root(self) -> Path:
        if self._resolved_artifacts_root is not None:
            return self._resolved_artifacts_root

        primary_root = self.artifacts_root
        if self._ensure_directory_is_writable(primary_root):
            self._resolved_artifacts_root = primary_root
            return primary_root

        fallback_root = primary_root.parent / f"{primary_root.name}_local"
        if self._ensure_directory_is_writable(fallback_root):
            self._resolved_artifacts_root = fallback_root
            return fallback_root

        raise PermissionError(
            "No writable reports artifact directory is available. "
            f"Tried '{primary_root}' and fallback '{fallback_root}'."
        )

    def _ensure_directory_is_writable(self, path: Path) -> bool:
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe_path = path / f".write_probe_{os.getpid()}_{uuid.uuid4().hex}"
            probe_path.write_text("ok", encoding="utf-8")
            probe_path.unlink(missing_ok=True)
            return True
        except PermissionError:
            return False
