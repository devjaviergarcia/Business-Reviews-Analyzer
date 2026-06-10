from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class FinalReportArtifactLayout:
    business_dir: Path
    reports_dir: Path
    annexes_dir: Path
    final_report_stem: str
    annex_stem: str
    annex_data_stem: str
    json_path: Path
    html_path: Path
    pdf_path: Path
    annex_csv_path: Path
    annex_html_path: Path
    annex_pdf_path: Path


@dataclass(frozen=True)
class PreviewReportArtifactLayout:
    business_dir: Path
    reports_dir: Path
    welcome_report_stem: str
    json_path: Path
    html_path: Path
    pdf_path: Path


Slugifier = Callable[[str], str]


def build_final_report_artifact_layout(
    *,
    artifacts_root: str | Path,
    business_name: str,
    business_id: str,
    analysis_id: str,
    safe_name_slug: Slugifier,
    safe_identifier_slug: Slugifier,
) -> FinalReportArtifactLayout:
    slug_business_name = safe_name_slug(business_name)
    slug_business_id = safe_identifier_slug(str(business_id))
    slug_analysis = safe_identifier_slug(str(analysis_id))

    business_dir = Path(artifacts_root) / f"{slug_business_name}__{slug_business_id}" / f"analisis_{slug_analysis}"
    reports_dir = business_dir / "reportes"
    annexes_dir = business_dir / "anexos"
    reports_dir.mkdir(parents=True, exist_ok=True)
    annexes_dir.mkdir(parents=True, exist_ok=True)

    final_report_stem = f"reporte_final_{slug_business_name}_{slug_analysis}"
    annex_stem = f"anexo_completo_{slug_business_name}_{slug_analysis}"
    annex_data_stem = f"anexo_datos_{slug_business_name}_{slug_analysis}"

    return FinalReportArtifactLayout(
        business_dir=business_dir,
        reports_dir=reports_dir,
        annexes_dir=annexes_dir,
        final_report_stem=final_report_stem,
        annex_stem=annex_stem,
        annex_data_stem=annex_data_stem,
        json_path=reports_dir / f"{final_report_stem}.json",
        html_path=reports_dir / f"{final_report_stem}.html",
        pdf_path=reports_dir / f"{final_report_stem}.pdf",
        annex_csv_path=annexes_dir / f"{annex_data_stem}.csv",
        annex_html_path=annexes_dir / f"{annex_stem}.html",
        annex_pdf_path=annexes_dir / f"{annex_stem}.pdf",
    )



def build_preview_report_artifact_layout(
    *,
    artifacts_root: str | Path,
    business_name: str,
    business_id: str,
    analysis_id: str,
    safe_name_slug: Slugifier,
    safe_identifier_slug: Slugifier,
) -> PreviewReportArtifactLayout:
    slug_business_name = safe_name_slug(business_name)
    slug_business_id = safe_identifier_slug(str(business_id))
    slug_analysis = safe_identifier_slug(str(analysis_id))

    business_dir = Path(artifacts_root) / f"{slug_business_name}__{slug_business_id}" / f"analisis_{slug_analysis}"
    reports_dir = business_dir / "reportes"
    reports_dir.mkdir(parents=True, exist_ok=True)

    welcome_report_stem = f"reporte_bienvenida_{slug_business_name}_{slug_analysis}"

    return PreviewReportArtifactLayout(
        business_dir=business_dir,
        reports_dir=reports_dir,
        welcome_report_stem=welcome_report_stem,
        json_path=reports_dir / f"{welcome_report_stem}.json",
        html_path=reports_dir / f"{welcome_report_stem}.html",
        pdf_path=reports_dir / f"{welcome_report_stem}.pdf",
    )
