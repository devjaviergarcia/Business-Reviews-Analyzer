import "./style.css";

import { AnimationController } from "./animations/controller";
import { createSidebarMenu } from "./components/layout/sidebar-menu";
import { ApiClient } from "./core/api-client";
import { createElement, mustElement } from "./core/dom";
import type { AnalyzeJobItem, MenuKey, PaginatedResponse, ViewModule } from "./core/types";
import { createAnalysisView } from "./views/analysis-view";
import { createApiView } from "./views/api-view";
import { createBusinessView } from "./views/business-view";
import { createJobsView } from "./views/jobs-view";

const ACTIVE_JOB_STATUSES = ["running", "queued", "retrying", "partial", "needs_human"] as const;

const appRoot = mustElement<HTMLDivElement>("#app");
const storedBase = localStorage.getItem("bra_api_base") || "http://localhost:8000";
const apiClient = new ApiClient(storedBase);

const shell = createElement("main", "app-shell");
const sidebarHost = createElement("div", "app-shell__sidebar");
const contentHost = createElement("div", "app-shell__content");
shell.append(sidebarHost, contentHost);
appRoot.append(shell);

const jobsView = createJobsView({ apiClient });
const analysisView = createAnalysisView({
  apiClient,
  onJobQueued: (jobId) => {
    setActiveView("jobs");
    jobsView.selectJob(jobId);
  },
});

const views: ViewModule[] = [analysisView, jobsView, createBusinessView({ apiClient }), createApiView({ apiClient })];

for (const view of views) {
  view.root.classList.add("app-view", "hidden");
  contentHost.append(view.root);
}

let activeKey: MenuKey = "analysis";
let activeJobPollTimer: number | null = null;

const menu = createSidebarMenu({
  items: [
    { key: "analysis", label: "Analisis" },
    { key: "jobs", label: "Pipeline" },
    { key: "business", label: "Negocios" },
    { key: "api", label: "API" },
  ],
  initial: activeKey,
  onSelect: (key) => {
    setActiveView(key);
  },
});
sidebarHost.append(menu.root);

function setActiveView(key: MenuKey): void {
  const previous = views.find((item) => item.key === activeKey);
  if (previous) {
    previous.root.classList.remove("app-view--active");
    previous.root.classList.add("hidden");
    previous.onHide();
  }

  activeKey = key;
  menu.setActive(key);
  const selected = views.find((item) => item.key === key);
  if (!selected) {
    return;
  }
  selected.root.classList.add("app-view--active");
  selected.root.classList.remove("hidden");
  selected.onShow();
  AnimationController.mount(selected.root, "view");
}

function startActiveJobPolling(): void {
  if (activeJobPollTimer !== null) {
    return;
  }
  void refreshActiveJobCard();
  activeJobPollTimer = window.setInterval(() => {
    void refreshActiveJobCard();
  }, 6000);
}

async function refreshActiveJobCard(): Promise<void> {
  try {
    const responses = await Promise.all(
      ACTIVE_JOB_STATUSES.map((statusValue) =>
        apiClient.get<PaginatedResponse<AnalyzeJobItem>>(
          `/business/scrape/jobs?page=1&page_size=25&status=${encodeURIComponent(statusValue)}`
        )
      )
    );

    const jobsById = new Map<string, AnalyzeJobItem>();
    for (const response of responses) {
      const items = Array.isArray(response.items) ? response.items : [];
      for (const item of items) {
        const jobId = String(item.job_id || "").trim();
        if (!jobId || jobsById.has(jobId)) {
          continue;
        }
        jobsById.set(jobId, item);
      }
    }

    const activeJobs = Array.from(jobsById.values()).sort((left, right) => getJobSortTime(right) - getJobSortTime(left));
    const activeJob = activeJobs[0] || null;
    if (!activeJob) {
      menu.setActiveJob(null);
      return;
    }

    const activeJobId = String(activeJob.job_id || "").trim();
    if (!activeJobId) {
      menu.setActiveJob(null);
      return;
    }

    menu.setActiveJob({
      jobId: activeJobId,
      title: resolveActiveJobTitle(activeJob),
      status: String(activeJob.status || "running"),
      progressPercent: estimateActiveProgress(activeJob),
      onClick: () => {
        setActiveView("jobs");
        jobsView.selectJob(activeJobId);
      },
    });
  } catch {
    menu.setActiveJob(null);
  }
}

function getJobSortTime(job: AnalyzeJobItem): number {
  const maybeTimes = [job.updated_at, job.created_at];
  for (const value of maybeTimes) {
    const timestamp = Date.parse(String(value || ""));
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
  }
  return 0;
}

function resolveActiveJobTitle(job: AnalyzeJobItem): string {
  const topLevelName = String(job.name || "").trim();
  if (topLevelName) {
    return topLevelName;
  }
  if (job.payload && typeof job.payload === "object") {
    const payloadName = String((job.payload as Record<string, unknown>).name || "").trim();
    if (payloadName) {
      return payloadName;
    }
  }
  return "Analisis";
}

function estimateActiveProgress(job: AnalyzeJobItem): number {
  const status = String(job.status || "").trim().toLowerCase();
  if (status === "done" || status === "failed") {
    return 100;
  }
  const stage = String(job.progress?.stage || "").trim().toLowerCase();
  if (!stage) {
    return status === "queued" ? 6 : 12;
  }
  if (stage.includes("analysis_worker_summary")) return 99;
  if (stage.includes("analysis_worker_started")) return 97;
  if (stage.includes("handoff_analysis_queued")) return 95;
  if (stage.includes("scraper_reviews_completed")) return 90;
  if (stage.includes("scraper_reviews_progress")) return 74;
  if (stage.includes("scraper_reviews_started")) return 60;
  if (stage.includes("scraper_listing_completed")) return 44;
  if (stage.includes("scraper_search_completed")) return 30;
  if (stage.includes("scraper_search_started")) return 20;
  if (stage.includes("scrape_pipeline_started")) return 10;
  return 14;
}

setActiveView(activeKey);
startActiveJobPolling();
