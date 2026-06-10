from __future__ import annotations

from typing import Any


async def build_review_analysis_bundle(
    *,
    business_name: str,
    listing: dict[str, Any],
    stats: dict[str, Any],
    reviews: list[dict[str, Any]],
    build_source_reports,
    score_review_dimensions,
    build_customer_clusters,
    build_problem_clusters,
    build_business_context,
    build_source_analysis_bundle,
) -> dict[str, Any]:
    source_reports = build_source_reports(reviews=reviews)
    review_metrics = [score_review_dimensions(index=idx, review=review) for idx, review in enumerate(reviews)]
    customer_clusters = build_customer_clusters(review_metrics=review_metrics)
    problem_clusters = build_problem_clusters(review_metrics=review_metrics)
    business_context = build_business_context(
        business_name=business_name,
        listing=listing,
        stats=stats,
    )
    source_analysis, source_comparison = await build_source_analysis_bundle(
        source_reports=source_reports,
        business_name=business_name,
        business_context=business_context,
    )
    return {
        "source_reports": source_reports,
        "review_metrics": review_metrics,
        "customer_clusters": customer_clusters,
        "problem_clusters": problem_clusters,
        "business_context": business_context,
        "source_analysis": source_analysis,
        "source_comparison": source_comparison,
    }


async def build_report_sections_input_bundle(
    *,
    business_name: str,
    stats: dict[str, Any],
    problem_clusters: dict[str, Any],
    customer_clusters: dict[str, Any],
    business_context: dict[str, Any],
    review_metrics: list[dict[str, Any]],
    analysis_payload: dict[str, Any],
    score_and_evolution: dict[str, Any],
    build_voice_of_customer,
    build_action_plan,
    build_quick_wins,
    build_invisible_and_opportunities,
    build_full_data_annex,
    build_llm_clustering_insights,
    build_llm_section_narratives,
) -> dict[str, Any]:
    voice_of_customer = build_voice_of_customer(review_metrics=review_metrics)
    action_plan = await build_action_plan(
        problem_clusters=problem_clusters,
        customer_clusters=customer_clusters,
        business_name=business_name,
        business_context=business_context,
    )
    quick_wins = build_quick_wins(
        stats=stats,
        problem_clusters=problem_clusters,
        action_plan=action_plan,
    )
    invisible_and_opportunities = build_invisible_and_opportunities(
        stats=stats,
        review_metrics=review_metrics,
        customer_clusters=customer_clusters,
        problem_clusters=problem_clusters,
    )
    full_data_annex = build_full_data_annex(
        stats=stats,
        review_metrics=review_metrics,
        customer_clusters=customer_clusters,
        problem_clusters=problem_clusters,
        analysis_payload=analysis_payload,
    )
    llm_clustering_insights = await build_llm_clustering_insights(
        business_name=business_name,
        customer_clusters=customer_clusters,
        problem_clusters=problem_clusters,
        quick_wins=quick_wins,
    )
    llm_section_narratives = await build_llm_section_narratives(
        business_name=business_name,
        business_context=business_context,
        score_and_evolution=score_and_evolution,
        customer_clusters=customer_clusters,
        problem_clusters=problem_clusters,
        invisible_and_opportunities=invisible_and_opportunities,
        action_plan=action_plan,
        quick_wins=quick_wins,
        stats=stats,
        voice_of_customer=voice_of_customer,
        full_data_annex=full_data_annex,
    )
    return {
        "voice_of_customer": voice_of_customer,
        "action_plan": action_plan,
        "quick_wins": quick_wins,
        "invisible_and_opportunities": invisible_and_opportunities,
        "full_data_annex": full_data_annex,
        "llm_clustering_insights": llm_clustering_insights,
        "llm_section_narratives": llm_section_narratives,
    }
