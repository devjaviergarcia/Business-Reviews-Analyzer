from __future__ import annotations

import asyncio

from src.database import get_database


class CRMRepositoryBootstrap:
    """Ensures CRM Mongo indexes in a single place."""

    def __init__(self) -> None:
        self._ensured = False
        self._lock = asyncio.Lock()

    async def ensure_indexes(self) -> None:
        if self._ensured:
            return
        async with self._lock:
            if self._ensured:
                return
            database = get_database()
            leads = database["crm_leads"]
            campaigns = database["crm_campaigns"]
            cadence = database["crm_cadence_templates"]
            messages = database["crm_messages"]
            events = database["crm_events"]
            suppressions = database["crm_suppressions"]
            discovery_runs = database["crm_discovery_runs"]
            report_requests = database["report_requests"]
            report_feedback = database["report_feedback"]
            benchmark_runs = database["benchmark_runs"]
            benchmark_businesses = database["benchmark_businesses"]
            competitor_sets = database["competitor_sets"]
            lead_reports = database["lead_reports"]
            paid_reports = database["paid_reports"]
            geo_cities = database["geo_cities"]
            geo_grid_runs = database["geo_grid_runs"]
            geo_grid_results = database["geo_grid_results"]

            await leads.create_index(
                [("email_normalized", 1)],
                name="idx_crm_leads_email_partial_unique",
                unique=True,
                partialFilterExpression={"email_normalized": {"$type": "string"}},
            )
            await leads.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_crm_leads_status_updated",
            )
            await leads.create_index(
                [("business_name_normalized", 1), ("address", 1)],
                name="idx_crm_leads_name_address",
            )
            await leads.create_index(
                [("legal.consent_status", 1), ("legal.do_not_contact", 1)],
                name="idx_crm_leads_legal",
            )

            await campaigns.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_crm_campaign_status_updated",
            )

            await cadence.create_index(
                [("key", 1)],
                name="idx_crm_cadence_key_unique",
                unique=True,
            )

            await messages.create_index(
                [("campaign_id", 1), ("scheduled_at", 1), ("status", 1)],
                name="idx_crm_messages_campaign_schedule_status",
            )
            await messages.create_index(
                [("provider_message_id", 1)],
                name="idx_crm_messages_provider_id",
                sparse=True,
            )
            await messages.create_index(
                [("lead_id", 1), ("status", 1)],
                name="idx_crm_messages_lead_status",
            )

            await events.create_index(
                [("lead_id", 1), ("created_at", -1)],
                name="idx_crm_events_lead_created",
            )
            await events.create_index(
                [("campaign_id", 1), ("created_at", -1)],
                name="idx_crm_events_campaign_created",
            )

            await suppressions.create_index(
                [("email_normalized", 1)],
                name="idx_crm_suppressions_email_unique",
                unique=True,
            )

            await discovery_runs.create_index(
                [("created_at", -1), ("_id", -1)],
                name="idx_crm_discovery_runs_created",
            )
            await discovery_runs.create_index(
                [("job_id", 1)],
                name="idx_crm_discovery_runs_job_id",
                sparse=True,
            )

            await report_requests.create_index(
                [("created_at", -1), ("_id", -1)],
                name="idx_report_requests_created",
            )
            await report_requests.create_index(
                [("status", 1), ("created_at", -1)],
                name="idx_report_requests_status_created",
            )
            await report_requests.create_index(
                [("email_normalized", 1), ("created_at", -1)],
                name="idx_report_requests_email_created",
            )
            await report_feedback.create_index(
                [("created_at", -1), ("_id", -1)],
                name="idx_report_feedback_created",
            )
            await report_feedback.create_index(
                [("lead_id", 1), ("created_at", -1)],
                name="idx_report_feedback_lead_created",
                sparse=True,
            )
            await report_feedback.create_index(
                [("report_request_id", 1), ("created_at", -1)],
                name="idx_report_feedback_report_request_created",
                sparse=True,
            )
            await report_feedback.create_index(
                [("lead_report_id", 1), ("created_at", -1)],
                name="idx_report_feedback_lead_report_created",
                sparse=True,
            )

            await benchmark_runs.create_index(
                [("created_at", -1), ("_id", -1)],
                name="idx_benchmark_runs_created",
            )
            await benchmark_runs.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_benchmark_runs_status_updated",
            )
            await benchmark_runs.create_index(
                [("city", 1), ("category", 1), ("created_at", -1)],
                name="idx_benchmark_runs_city_category_created",
            )

            await benchmark_businesses.create_index(
                [("benchmark_id", 1), ("maps_url_canonical", 1)],
                name="idx_benchmark_businesses_benchmark_maps_unique",
                unique=True,
                partialFilterExpression={"maps_url_canonical": {"$type": "string"}},
            )
            await benchmark_businesses.create_index(
                [("benchmark_id", 1), ("business_name_normalized", 1), ("address", 1)],
                name="idx_benchmark_businesses_benchmark_name_address",
            )
            await benchmark_businesses.create_index(
                [("benchmark_id", 1), ("city", 1), ("category", 1)],
                name="idx_benchmark_businesses_benchmark_city_category",
            )
            await benchmark_businesses.create_index(
                [("benchmark_id", 1), ("discovery_rank", 1), ("_id", 1)],
                name="idx_benchmark_businesses_benchmark_discovery_rank",
            )
            await benchmark_businesses.create_index(
                [("benchmark_id", 1), ("rating", -1), ("review_count", -1)],
                name="idx_benchmark_businesses_benchmark_rating_reviews",
            )
            await benchmark_businesses.create_index(
                [("benchmark_id", 1), ("opportunity_score", -1), ("_id", -1)],
                name="idx_benchmark_businesses_benchmark_opportunity",
            )

            await competitor_sets.create_index(
                [("benchmark_id", 1), ("target_business_id", 1)],
                name="idx_competitor_sets_benchmark_target_unique",
                unique=True,
            )
            await competitor_sets.create_index(
                [("benchmark_id", 1), ("updated_at", -1)],
                name="idx_competitor_sets_benchmark_updated",
            )

            await lead_reports.create_index(
                [("benchmark_business_id", 1), ("report_type", 1)],
                name="idx_lead_reports_business_type_unique",
                unique=True,
            )
            await lead_reports.create_index(
                [("benchmark_id", 1), ("created_at", -1)],
                name="idx_lead_reports_benchmark_created",
            )

            await paid_reports.create_index(
                [("benchmark_business_id", 1), ("report_month", 1)],
                name="idx_paid_reports_business_month_unique",
                unique=True,
            )
            await paid_reports.create_index(
                [("benchmark_id", 1), ("report_month", -1)],
                name="idx_paid_reports_benchmark_month",
            )

            await geo_cities.create_index(
                [("city_slug", 1)],
                name="idx_geo_cities_slug_unique",
                unique=True,
            )
            await geo_cities.create_index(
                [("enabled", 1), ("city", 1)],
                name="idx_geo_cities_enabled_city",
            )

            await geo_grid_runs.create_index(
                [("city_slug", 1), ("created_at", -1), ("_id", -1)],
                name="idx_geo_grid_runs_city_created",
            )
            await geo_grid_runs.create_index(
                [("status", 1), ("updated_at", -1)],
                name="idx_geo_grid_runs_status_updated",
            )
            await geo_grid_runs.create_index(
                [("job_id", 1)],
                name="idx_geo_grid_runs_job_id",
                sparse=True,
            )

            await geo_grid_results.create_index(
                [("geo_grid_run_id", 1), ("point_order", 1), ("rank", 1)],
                name="idx_geo_grid_results_run_point_rank_unique",
                unique=True,
            )
            await geo_grid_results.create_index(
                [("geo_grid_run_id", 1), ("business_key", 1)],
                name="idx_geo_grid_results_run_business",
            )
            await geo_grid_results.create_index(
                [("geo_grid_run_id", 1), ("point_order", 1)],
                name="idx_geo_grid_results_run_point",
            )

            self._ensured = True
