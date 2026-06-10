from __future__ import annotations

from src.config import settings
from src.pipeline.advanced_report_action_plan_builder import AdvancedReportActionPlanBuilder
from src.pipeline.advanced_report_annex_builder import AdvancedReportAnnexBuilder
from src.pipeline.advanced_report_builder_facets import (
    AdvancedReportPipelineFacet,
    AdvancedReportRuntimeSupportFacet,
    AdvancedReportSectionBuildersFacet,
)
from src.pipeline.advanced_report_customer_segments_builder import (
    AdvancedReportCustomerSegmentsBuilder,
)
from src.pipeline.advanced_report_problem_patterns_builder import (
    AdvancedReportProblemPatternsBuilder,
)
from src.pipeline.advanced_report_preview_builder import AdvancedReportPreviewBuilder
from src.pipeline.advanced_report_review_language import (
    AGGRESSIVE_TOKENS,
    EXPECTATION_TOKENS,
    GENERIC_COMMENT_PROBLEM,
    IMPROVEMENT_TOKENS,
    NEGATIVE_COMMENT_PROBLEM,
    NEGATIVE_TOKENS,
    NO_COMMENT_HIGH_PROBLEM,
    NO_COMMENT_LOW_PROBLEM,
    NO_COMMENT_MEDIUM_PROBLEM,
    POSITIVE_COMMENT_PROBLEM,
    POSITIVE_TOKENS,
    STOPWORDS,
    THEME_KEYWORDS,
)
from src.pipeline.advanced_report_section_narrative_builder import (
    AdvancedReportSectionNarrativeBuilder,
)
from src.pipeline.advanced_report_source_insights_builder import AdvancedReportSourceInsightsBuilder
from src.pipeline.preprocessor import ReviewPreprocessor

try:
    from google import genai
    from google.genai import errors as genai_errors
except Exception:  # pragma: no cover - optional dependency at runtime.
    genai = None
    genai_errors = None


class AdvancedBusinessReportBuilder(
    AdvancedReportPipelineFacet,
    AdvancedReportSectionBuildersFacet,
    AdvancedReportRuntimeSupportFacet,
):
    """Build a structured multi-section reputation report from analyzed reviews."""

    _POSITIVE_TOKENS = POSITIVE_TOKENS
    _NEGATIVE_TOKENS = NEGATIVE_TOKENS
    _EXPECTATION_TOKENS = EXPECTATION_TOKENS
    _IMPROVEMENT_TOKENS = IMPROVEMENT_TOKENS
    _AGGRESSIVE_TOKENS = AGGRESSIVE_TOKENS
    _THEME_KEYWORDS = THEME_KEYWORDS
    _GENERIC_COMMENT_PROBLEM = GENERIC_COMMENT_PROBLEM
    _POSITIVE_COMMENT_PROBLEM = POSITIVE_COMMENT_PROBLEM
    _NEGATIVE_COMMENT_PROBLEM = NEGATIVE_COMMENT_PROBLEM
    _NO_COMMENT_HIGH_PROBLEM = NO_COMMENT_HIGH_PROBLEM
    _NO_COMMENT_MEDIUM_PROBLEM = NO_COMMENT_MEDIUM_PROBLEM
    _NO_COMMENT_LOW_PROBLEM = NO_COMMENT_LOW_PROBLEM
    _STOPWORDS = STOPWORDS

    def __init__(self, *, model_name: str | None = None, enable_llm: bool | None = None) -> None:
        self.model_name = str(model_name or settings.gemini_model or "gemini-2.5-flash").strip()
        self.fallback_models = ["gemini-2.5-flash", "gemini-flash-latest"]
        self._source_preprocessor = ReviewPreprocessor()
        self._customer_segments_builder = AdvancedReportCustomerSegmentsBuilder(
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            kmeans=self._kmeans,
            label_customer_cluster=self._label_customer_cluster,
        )
        self._problem_patterns_builder = AdvancedReportProblemPatternsBuilder(
            safe_float=self._safe_float,
            clamp01=self._clamp01,
            extract_top_keywords=self._extract_top_keywords,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
        )
        self._section_narrative_builder = AdvancedReportSectionNarrativeBuilder(
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            score_label=self._score_label,
            summarize_customer_clusters=self._summarize_customer_clusters,
            summarize_problem_clusters=self._summarize_problem_clusters,
            friendly_problem_label=self._friendly_problem_label,
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            extract_json_object=self._extract_json_object,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
        )
        self._source_insights_builder = AdvancedReportSourceInsightsBuilder(
            source_preprocessor=self._source_preprocessor,
            score_review_dimensions=self._score_review_dimensions,
            build_customer_clusters=self._build_customer_clusters,
            build_problem_clusters=self._build_problem_clusters,
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            extract_json_object=self._extract_json_object,
            summarize_problem_clusters=self._summarize_problem_clusters,
            human_label_problem=self._human_label_problem,
            negative_ratio=self._negative_ratio,
            average_dimension=self._average_dimension,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
        )
        self._action_plan_builder = AdvancedReportActionPlanBuilder(
            can_use_llm=self._can_use_llm,
            llm_generate_text=self._llm_generate_text,
            extract_json_object=self._extract_json_object,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            sanitize_llm_text=self._sanitize_llm_text,
            plainify_business_text=self._plainify_business_text,
            normalize_action_type=self._normalize_action_type,
            infer_action_type=self._infer_action_type,
            infer_action_tool=self._infer_action_tool,
            friendly_problem_label=self._friendly_problem_label,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
        )
        self._annex_builder = AdvancedReportAnnexBuilder(
            average_dimension=self._average_dimension,
            safe_float=self._safe_float,
            safe_int=self._safe_int,
            generic_comment_problem=self._GENERIC_COMMENT_PROBLEM,
        )
        self._preview_builder = AdvancedReportPreviewBuilder(
            compress_text=self._compress_text,
            safe_float=self._safe_float,
            normalize_text=self._normalize_text,
        )
        self.llm_enabled = bool(
            settings.report_builder_enable_llm if enable_llm is None else enable_llm
        )
        self._genai_errors = genai_errors
        if self.llm_enabled and genai is not None and settings.gemini_api_key:
            try:
                self.client = genai.Client(api_key=settings.gemini_api_key)
            except Exception:
                self.client = None
        else:
            self.client = None
