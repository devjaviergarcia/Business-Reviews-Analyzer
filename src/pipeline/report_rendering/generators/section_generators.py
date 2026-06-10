from __future__ import annotations

from typing import Any

from .action_annex_sections import ActionPlanSectionGenerator, AnnexSummarySectionGenerator
from .base_section_generator import _BaseSectionGenerator
from .customer_sections import CustomerProfileSectionGenerator
from .executive_sections import ExecutiveSummarySectionGenerator, ReputationScoreSectionGenerator
from .source_sections import SourceComparisonSectionGenerator, SourceNarrativeSectionGenerator


def build_section_generators(renderer: Any) -> dict[str, _BaseSectionGenerator]:
    return {
        "1_resumen_ejecutivo": ExecutiveSummarySectionGenerator(renderer),
        "2_score_reputacion": ReputationScoreSectionGenerator(renderer),
        "3_quien_es_tu_cliente_y_que_le_preocupa": CustomerProfileSectionGenerator(renderer),
        "4_lectura_fuente_google_maps": SourceNarrativeSectionGenerator(renderer, source_key="google_maps"),
        "5_lectura_fuente_tripadvisor": SourceNarrativeSectionGenerator(renderer, source_key="tripadvisor"),
        "4_plan_de_accion": ActionPlanSectionGenerator(renderer),
        "7_comparativa_fuentes": SourceComparisonSectionGenerator(renderer),
        "5_anexos_resumen": AnnexSummarySectionGenerator(renderer),
    }
