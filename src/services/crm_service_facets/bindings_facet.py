from __future__ import annotations

from typing import Any


class CRMServiceBindingsFacet:

    def _reset_crm_queue_use_case_bindings(self) -> None:
        for attr_name in self._USE_CASE_BINDING_NAMES:
            setattr(self, attr_name, None)
            setattr(self, f"_{attr_name}", None)

    def attach_crm_queue_use_cases(
        self,
        *,
        enqueue_crm_lead_discovery_job_use_case: "EnqueueCRMLeadDiscoveryJobUseCase",
        enqueue_crm_lead_pipeline_job_use_case: "EnqueueCRMLeadPipelineJobUseCase",
        enqueue_benchmark_study_job_use_case: "EnqueueBenchmarkStudyJobUseCase",
        create_crm_report_request_use_case: "CreateCRMReportRequestUseCase",
        create_crm_report_feedback_use_case: "CreateCRMReportFeedbackUseCase",
        generate_crm_lead_report_use_case: "GenerateCRMLeadReportUseCase",
        generate_crm_paid_report_use_case: "GenerateCRMPaidReportUseCase",
        generate_crm_public_study_use_case: "GenerateCRMPublicStudyUseCase",
        create_crm_lead_use_case: "CreateCRMLeadUseCase",
        update_crm_lead_use_case: "UpdateCRMLeadUseCase",
        bulk_delete_crm_leads_use_case: "BulkDeleteCRMLeadsUseCase",
        create_crm_campaign_use_case: "CreateCRMCampaignUseCase",
        launch_crm_campaign_use_case: "LaunchCRMCampaignUseCase",
        handle_resend_webhook_use_case: "HandleResendWebhookUseCase",
        list_crm_report_requests_use_case: "ListCRMReportRequestsUseCase",
        list_crm_leads_use_case: "ListCRMLeadsUseCase",
        get_crm_lead_use_case: "GetCRMLeadUseCase",
        list_crm_campaigns_use_case: "ListCRMCampaignsUseCase",
        list_crm_messages_use_case: "ListCRMMessagesUseCase",
        list_crm_events_use_case: "ListCRMEventsUseCase",
        list_crm_discovery_runs_use_case: "ListCRMDiscoveryRunsUseCase",
        get_crm_discovery_run_use_case: "GetCRMDiscoveryRunUseCase",
        list_crm_geo_cities_use_case: "ListCRMGeoCitiesUseCase",
        list_crm_geo_grid_runs_use_case: "ListCRMGeoGridRunsUseCase",
        get_crm_geo_grid_run_use_case: "GetCRMGeoGridRunUseCase",
        list_crm_geo_grid_results_use_case: "ListCRMGeoGridResultsUseCase",
        get_crm_geo_grid_stats_use_case: "GetCRMGeoGridStatsUseCase",
        sync_crm_lead_pipeline_refs_use_case: "SyncCRMLeadPipelineRefsUseCase",
        enqueue_geo_grid_study_job_use_case: "EnqueueGeoGridStudyJobUseCase",
        process_crm_lead_discovery_task_use_case: "ProcessCRMLeadDiscoveryTaskUseCase",
        process_benchmark_study_task_use_case: "ProcessBenchmarkStudyTaskUseCase",
        process_crm_lead_pipeline_task_use_case: "ProcessCRMLeadPipelineTaskUseCase",
        process_geo_grid_study_task_use_case: "ProcessGeoGridStudyTaskUseCase",
        enqueue_due_campaign_dispatch_jobs_use_case: "EnqueueDueCampaignDispatchJobsUseCase",
        process_campaign_dispatch_task_use_case: "ProcessCampaignDispatchTaskUseCase",
        retry_crm_report_request_use_case: "RetryCRMReportRequestUseCase",
        process_pending_crm_report_requests_use_case: "ProcessPendingCRMReportRequestsUseCase",
    ) -> "CRMService":
        self._bind_crm_queue_use_cases(
            bindings={
                "enqueue_crm_lead_discovery_job_use_case": enqueue_crm_lead_discovery_job_use_case,
                "enqueue_crm_lead_pipeline_job_use_case": enqueue_crm_lead_pipeline_job_use_case,
                "enqueue_benchmark_study_job_use_case": enqueue_benchmark_study_job_use_case,
                "create_crm_report_request_use_case": create_crm_report_request_use_case,
                "create_crm_report_feedback_use_case": create_crm_report_feedback_use_case,
                "generate_crm_lead_report_use_case": generate_crm_lead_report_use_case,
                "generate_crm_paid_report_use_case": generate_crm_paid_report_use_case,
                "generate_crm_public_study_use_case": generate_crm_public_study_use_case,
                "create_crm_lead_use_case": create_crm_lead_use_case,
                "update_crm_lead_use_case": update_crm_lead_use_case,
                "bulk_delete_crm_leads_use_case": bulk_delete_crm_leads_use_case,
                "create_crm_campaign_use_case": create_crm_campaign_use_case,
                "launch_crm_campaign_use_case": launch_crm_campaign_use_case,
                "handle_resend_webhook_use_case": handle_resend_webhook_use_case,
                "list_crm_report_requests_use_case": list_crm_report_requests_use_case,
                "list_crm_leads_use_case": list_crm_leads_use_case,
                "get_crm_lead_use_case": get_crm_lead_use_case,
                "list_crm_campaigns_use_case": list_crm_campaigns_use_case,
                "list_crm_messages_use_case": list_crm_messages_use_case,
                "list_crm_events_use_case": list_crm_events_use_case,
                "list_crm_discovery_runs_use_case": list_crm_discovery_runs_use_case,
                "get_crm_discovery_run_use_case": get_crm_discovery_run_use_case,
                "list_crm_geo_cities_use_case": list_crm_geo_cities_use_case,
                "list_crm_geo_grid_runs_use_case": list_crm_geo_grid_runs_use_case,
                "get_crm_geo_grid_run_use_case": get_crm_geo_grid_run_use_case,
                "list_crm_geo_grid_results_use_case": list_crm_geo_grid_results_use_case,
                "get_crm_geo_grid_stats_use_case": get_crm_geo_grid_stats_use_case,
                "sync_crm_lead_pipeline_refs_use_case": sync_crm_lead_pipeline_refs_use_case,
                "enqueue_geo_grid_study_job_use_case": enqueue_geo_grid_study_job_use_case,
                "process_crm_lead_discovery_task_use_case": process_crm_lead_discovery_task_use_case,
                "process_benchmark_study_task_use_case": process_benchmark_study_task_use_case,
                "process_crm_lead_pipeline_task_use_case": process_crm_lead_pipeline_task_use_case,
                "process_geo_grid_study_task_use_case": process_geo_grid_study_task_use_case,
                "enqueue_due_campaign_dispatch_jobs_use_case": enqueue_due_campaign_dispatch_jobs_use_case,
                "process_campaign_dispatch_task_use_case": process_campaign_dispatch_task_use_case,
                "retry_crm_report_request_use_case": retry_crm_report_request_use_case,
                "process_pending_crm_report_requests_use_case": process_pending_crm_report_requests_use_case,
            }
        )
        return self

    def _bind_crm_queue_use_cases(self, *, bindings: dict[str, Any]) -> None:
        for attr_name, use_case in bindings.items():
            setattr(self, attr_name, use_case)
            setattr(self, f"_{attr_name}", use_case)
