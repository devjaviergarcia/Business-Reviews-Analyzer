import { AnimationController } from "../animations/controller";
import { createButton } from "../components/atoms/button";
import { ApiClient } from "../core/api-client";
import { clearElement, createElement, formatError } from "../core/dom";
import type { AnalyzeJobItem, JobEventItem, PaginatedResponse, ViewModule } from "../core/types";

type JobsViewDeps = {
  apiClient: ApiClient;
};

type JobFilterMode = "active" | "all";
type WorkerKey = "google_maps" | "tripadvisor";
type WorkerStatus = "idle" | "queued" | "running" | "done" | "failed";

type JobsViewHandle = ViewModule & {
  selectJob: (jobId: string) => void;
  refreshJobs: () => Promise<void>;
};

type WorkerProgressState = {
  key: WorkerKey;
  label: string;
  status: WorkerStatus;
  stage: string;
  message: string;
  percent: number;
  currentPage: number | null;
  totalPages: number | null;
  maxPages: number | null;
  loadedReviews: number | null;
  totalReviews: number | null;
};

type PipelineProgressState = {
  jobStatus: string;
  jobStage: string;
  jobMessage: string;
  analysisStage: string;
  analysisMessage: string;
  google_maps: WorkerProgressState;
  tripadvisor: WorkerProgressState;
};

type WorkerCardHandle = {
  status: HTMLElement;
  stage: HTMLElement;
  metrics: HTMLElement;
  progressFill: HTMLElement;
  percent: HTMLElement;
};

const ACTIVE_STATUSES = new Set(["queued", "running", "retrying", "partial"]);
const STAGE_BASE_PERCENT: Record<string, number> = {
  queued: 2,
  worker_started: 5,
  scrape_pipeline_started: 8,
  scraper_starting: 10,
  scraper_search_started: 20,
  scraper_search_completed: 32,
  scraper_listing_completed: 42,
  scraper_reviews_started: 50,
  scraper_reviews_progress: 58,
  scraper_reviews_completed: 100,
  handoff_analysis_queued: 100,
};

export function createJobsView(deps: JobsViewDeps): JobsViewHandle {
  const root = createElement("section", "view-panel jobs-view");

  const titlePanel = createElement("section", "panel jobs-header-panel");
  titlePanel.append(createElement("h2", "panel__title", "Jobs lanzados"));
  const subtitle = createElement(
    "div",
    "muted",
    "Selecciona un job para ver Google Maps + TripAdvisor en tiempo real."
  );
  titlePanel.append(subtitle);
  root.append(titlePanel);

  const layout = createElement("div", "jobs-layout");
  root.append(layout);

  const listPanel = createElement("section", "panel jobs-list-panel");
  layout.append(listPanel);

  const listHead = createElement("div", "jobs-list-head");
  listPanel.append(listHead);
  listHead.append(createElement("h3", "panel__subtitle", "Listado de jobs"));

  const listActions = createElement("div", "form-actions");
  const activeFilterButton = createButton({ label: "Activos", tone: "turquoise" });
  const allFilterButton = createButton({ label: "Todos", tone: "white" });
  const refreshListButton = createButton({ label: "Recargar", tone: "white" });
  listActions.append(activeFilterButton, allFilterButton, refreshListButton);
  listPanel.append(listActions);

  const listStatus = createElement("div", "muted", "Cargando jobs...");
  listPanel.append(listStatus);

  const jobsList = createElement("div", "jobs-list");
  listPanel.append(jobsList);

  const livePanel = createElement("section", "panel jobs-live-panel");
  layout.append(livePanel);

  const liveTitle = createElement("h3", "panel__subtitle", "Análisis en vivo");
  const liveJobMeta = createElement("div", "muted", "Selecciona un job para comenzar.");
  const liveActions = createElement("div", "form-actions");
  const deleteJobButton = createButton({ label: "Eliminar job", tone: "white" }) as HTMLButtonElement;
  deleteJobButton.disabled = true;
  const deleteStatus = createElement("span", "muted", "");
  liveActions.append(deleteJobButton, deleteStatus);
  livePanel.append(liveTitle, liveJobMeta, liveActions);

  const totalWrap = createElement("div", "jobs-total-card");
  const totalHead = createElement("div", "jobs-total-head");
  const totalLabel = createElement("span", "jobs-total-label", "Progreso total");
  const totalPercent = createElement("span", "jobs-total-percent", "0%");
  totalHead.append(totalLabel, totalPercent);
  totalWrap.append(totalHead);
  const totalTrack = createElement("div", "progress-track");
  const totalFill = createElement("div", "progress-fill");
  totalTrack.append(totalFill);
  totalWrap.append(totalTrack);
  const analysisStatus = createElement("div", "muted", "Pipeline: -");
  totalWrap.append(analysisStatus);
  livePanel.append(totalWrap);

  const workersGrid = createElement("div", "jobs-workers-grid");
  const googleCard = createWorkerCard("Google Maps");
  const tripadvisorCard = createWorkerCard("TripAdvisor");
  workersGrid.append(googleCard.root, tripadvisorCard.root);
  livePanel.append(workersGrid);

  livePanel.append(createElement("h3", "panel__subtitle", "Logs"));
  const logs = createElement("pre", "code-block jobs-log-block", "");
  livePanel.append(logs);

  let filterMode: JobFilterMode = "active";
  let jobs: AnalyzeJobItem[] = [];
  let selectedJobId: string | null = null;
  let selectedJobStream: EventSource | null = null;
  let jobsPollTimer: number | null = null;
  let loadedEventCount = 0;
  let logLines: string[] = [];
  let state = createInitialPipelineState();

  const setFilterMode = (nextMode: JobFilterMode): void => {
    filterMode = nextMode;
    activeFilterButton.classList.toggle("is-selected", filterMode === "active");
    allFilterButton.classList.toggle("is-selected", filterMode === "all");
    renderJobsList();
  };

  activeFilterButton.addEventListener("click", () => setFilterMode("active"));
  allFilterButton.addEventListener("click", () => setFilterMode("all"));
  refreshListButton.addEventListener("click", () => {
    void loadJobsList();
  });
  deleteJobButton.addEventListener("click", () => {
    void deleteSelectedJob();
  });

  async function loadJobsList(): Promise<void> {
    try {
      listStatus.textContent = "Cargando jobs...";
      const response = await deps.apiClient.get<PaginatedResponse<AnalyzeJobItem>>(
        "/business/analyze/queue?page=1&page_size=100"
      );
      jobs = Array.isArray(response.items) ? response.items : [];
      renderJobsList();
      listStatus.textContent = `${jobs.length} jobs cargados`;
      if (!selectedJobId) {
        const preferred = getRenderableJobs()[0];
        if (preferred?.job_id) {
          selectJob(preferred.job_id);
        }
      }
    } catch (error) {
      listStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  function getRenderableJobs(): AnalyzeJobItem[] {
    if (filterMode === "all") {
      return jobs;
    }
    return jobs.filter((item) => isActiveStatus(item.status));
  }

  function renderJobsList(): void {
    const renderable = getRenderableJobs();
    clearElement(jobsList);
    if (renderable.length === 0) {
      jobsList.append(createElement("div", "muted", "No hay jobs para este filtro."));
      return;
    }
    for (const job of renderable) {
      const jobId = String(job.job_id || "").trim();
      if (!jobId) continue;
      const button = createElement("button", "jobs-list-item") as HTMLButtonElement;
      button.type = "button";
      if (jobId === selectedJobId) {
        button.classList.add("jobs-list-item--active");
      }

      const title = createElement("div", "jobs-list-item__title", resolveJobTitle(job));
      const status = createElement(
        "span",
        `jobs-status jobs-status--${normalizeStatusClass(job.status)}`,
        String(job.status || "unknown")
      );
      const head = createElement("div", "jobs-list-item__head");
      head.append(title, status);

      const meta = createElement(
        "div",
        "jobs-list-item__meta muted",
        `ID: ${jobId} | Stage: ${String(job.progress?.stage || "-")}`
      );

      button.append(head, meta);
      button.addEventListener("click", () => {
        selectJob(jobId);
      });
      jobsList.append(button);
    }
  }

  function selectJob(jobId: string): void {
    const normalized = String(jobId || "").trim();
    if (!normalized) return;
    selectedJobId = normalized;
    deleteStatus.textContent = "";
    renderJobsList();
    void loadSelectedJob(normalized);
  }

  function resetLiveState(): void {
    stopJobStream();
    loadedEventCount = 0;
    logLines = [];
    logs.textContent = "";
    state = createInitialPipelineState();
    renderLiveState();
  }

  async function loadSelectedJob(jobId: string): Promise<void> {
    resetLiveState();
    liveJobMeta.textContent = `Cargando job ${jobId}...`;
    try {
      const detail = await deps.apiClient.get<AnalyzeJobItem>(
        `/business/analyze/queue/${encodeURIComponent(jobId)}`
      );
      const events = Array.isArray(detail.events) ? detail.events : [];
      loadedEventCount = events.length;
      state.jobStatus = String(detail.status || "").trim().toLowerCase() || "unknown";
      state.jobStage = String(detail.progress?.stage || "").trim();
      state.jobMessage = String(detail.progress?.message || "").trim();
      for (const event of events) {
        applyJobEvent(event, false);
      }
      if (events.length === 0) {
        appendLogLine(`[snapshot] status=${state.jobStatus} stage=${state.jobStage || "-"}`);
      }
      renderLiveState();
      startJobStream(jobId);
    } catch (error) {
      liveJobMeta.textContent = `ERROR: ${formatError(error)}`;
    }
  }
  async function deleteSelectedJob(): Promise<void> {
    const jobId = String(selectedJobId || "").trim();
    if (!jobId) {
      return;
    }
    const confirmed = window.confirm(`Eliminar job ${jobId}?`);
    if (!confirmed) {
      return;
    }

    deleteJobButton.disabled = true;
    deleteStatus.textContent = "Eliminando job...";
    try {
      await deps.apiClient.delete<Record<string, unknown>>(
        `/business/analyze/queue/${encodeURIComponent(jobId)}?wait_active_stop_seconds=10&poll_seconds=0.5&force_delete_on_timeout=true`
      );
      stopJobStream();
      jobs = jobs.filter((item) => String(item.job_id || "").trim() !== jobId);
      selectedJobId = null;
      resetLiveState();
      liveTitle.textContent = "Análisis en vivo";
      liveJobMeta.textContent = "Selecciona un job para comenzar.";
      deleteStatus.textContent = "Job eliminado.";
      renderJobsList();
      await loadJobsList();
    } catch (error) {
      deleteStatus.textContent = `ERROR: ${formatError(error)}`;
    } finally {
      deleteJobButton.disabled = !selectedJobId;
    }
  }

  function startJobStream(jobId: string): void {
    stopJobStream();
    const stream = deps.apiClient.createEventSource(
      `/business/analyze/queue/${encodeURIComponent(jobId)}/events?from_index=${loadedEventCount}`
    );
    selectedJobStream = stream;

    stream.addEventListener("progress", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      const indexValue = toInteger(payload.index);
      if (indexValue !== null) {
        loadedEventCount = Math.max(loadedEventCount, indexValue);
      } else {
        loadedEventCount += 1;
      }
      applyJobEvent(payload, true);
    });

    stream.addEventListener("done", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (payload && typeof payload.status === "string") {
        state.jobStatus = payload.status.trim().toLowerCase();
      }
      appendLogLine(`[done] status=${state.jobStatus || "done"}`);
      renderLiveState();
      stopJobStream();
      if (selectedJobId) {
        void loadJobsList();
      }
    });

    stream.addEventListener("heartbeat", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      if (typeof payload.status === "string") {
        state.jobStatus = payload.status.trim().toLowerCase();
      }
      renderLiveState();
    });

    stream.addEventListener("error", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (payload?.error) {
        appendLogLine(`[error] ${String(payload.error)}`);
      }
    });

    stream.onerror = () => {
      appendLogLine("[stream] desconectado");
    };
  }

  function stopJobStream(): void {
    if (!selectedJobStream) return;
    selectedJobStream.close();
    selectedJobStream = null;
  }

  function parseEventData(event: MessageEvent<string>): Record<string, unknown> | null {
    try {
      return JSON.parse(event.data) as Record<string, unknown>;
    } catch {
      return null;
    }
  }

  function applyJobEvent(event: JobEventItem | Record<string, unknown>, logEvent: boolean): void {
    const stage = String(event.stage || "").trim();
    const message = String(event.message || "").trim();
    const status = String(event.status || "").trim().toLowerCase();
    const createdAt = String(event.created_at || "").trim();
    const data = isRecord(event.data) ? event.data : {};
    const source = normalizeSource(data.source);

    if (status) {
      state.jobStatus = status;
    }
    if (stage) {
      state.jobStage = stage;
    }
    if (message) {
      state.jobMessage = message;
    }

    if (source) {
      updateWorkerState(state[source], { stage, message, status, data });
    } else {
      applyGlobalStage({ stage, message, status, data });
    }

    if (stage.startsWith("analysis_worker") || stage === "handoff_analysis_queued") {
      state.analysisStage = stage;
      state.analysisMessage = message;
    }

    if (stage === "done" || state.jobStatus === "done") {
      state.google_maps.status = state.google_maps.status === "failed" ? "failed" : "done";
      state.tripadvisor.status = state.tripadvisor.status === "failed" ? "failed" : "done";
      state.google_maps.percent = state.google_maps.status === "failed" ? state.google_maps.percent : 100;
      state.tripadvisor.percent = state.tripadvisor.status === "failed" ? state.tripadvisor.percent : 100;
    }

    if (logEvent) {
      appendLogLine(formatLogLine({ stage, message, data, createdAt }));
    } else if (stage || message) {
      appendLogLine(formatLogLine({ stage, message, data, createdAt }), false);
    }
    renderLiveState();
  }

  function applyGlobalStage(input: {
    stage: string;
    message: string;
    status: string;
    data: Record<string, unknown>;
  }): void {
    const { stage, message, status, data } = input;
    if (stage === "queued") {
      state.google_maps.status = "queued";
      state.tripadvisor.status = "queued";
      state.google_maps.percent = Math.max(state.google_maps.percent, 2);
      state.tripadvisor.percent = Math.max(state.tripadvisor.percent, 2);
      return;
    }
    if (stage === "worker_started" || stage === "scrape_pipeline_started") {
      if (state.google_maps.status === "idle") state.google_maps.status = "running";
      if (state.tripadvisor.status === "idle") state.tripadvisor.status = "running";
      state.google_maps.percent = Math.max(state.google_maps.percent, 8);
      state.tripadvisor.percent = Math.max(state.tripadvisor.percent, 8);
      return;
    }
    if (stage === "handoff_analysis_queued") {
      state.google_maps.status = state.google_maps.status === "failed" ? "failed" : "done";
      state.tripadvisor.status = state.tripadvisor.status === "failed" ? "failed" : "done";
      state.google_maps.percent = state.google_maps.status === "failed" ? state.google_maps.percent : 100;
      state.tripadvisor.percent = state.tripadvisor.status === "failed" ? state.tripadvisor.percent : 100;
      return;
    }
    if (stage === "scrape_source_failed") {
      const source = normalizeSource(data.source);
      if (source) {
        const worker = state[source];
        worker.status = "failed";
        worker.stage = stage;
        worker.message = message || "Source failed.";
        worker.percent = Math.max(worker.percent, 100);
      }
      return;
    }
    if (status === "failed" || stage === "failed") {
      if (state.google_maps.status !== "done") state.google_maps.status = "failed";
      if (state.tripadvisor.status !== "done") state.tripadvisor.status = "failed";
    }
  }

  function updateWorkerState(
    worker: WorkerProgressState,
    input: {
      stage: string;
      message: string;
      status: string;
      data: Record<string, unknown>;
    }
  ): void {
    const { stage, message, status, data } = input;
    if (message) {
      worker.message = message;
    }
    if (stage) {
      worker.stage = stage;
    }

    const totalPages = toInteger(data.total_pages);
    const currentPage = toInteger(data.current_page) ?? toInteger(data.page);
    const maxPages = toInteger(data.max_pages) ?? toInteger(data.tripadvisor_max_pages);
    const totalReviews = toInteger(data.total_reviews) ?? toInteger(data.total_results);
    const loadedReviews =
      toInteger(data.total_unique_reviews) ??
      toInteger(data.reviews_loaded) ??
      toInteger(data.scraped_review_count) ??
      toInteger(data.total_unique_reviews);

    if (totalPages !== null && totalPages > 0) worker.totalPages = totalPages;
    if (currentPage !== null && currentPage > 0) worker.currentPage = currentPage;
    if (maxPages !== null && maxPages > 0) worker.maxPages = maxPages;
    if (totalReviews !== null && totalReviews > 0) worker.totalReviews = totalReviews;
    if (loadedReviews !== null && loadedReviews >= 0) worker.loadedReviews = loadedReviews;

    if (status === "failed" || stage === "failed" || stage === "scrape_source_failed") {
      worker.status = "failed";
      worker.percent = Math.max(worker.percent, 100);
      return;
    }
    if (status === "queued") {
      worker.status = "queued";
    } else if (status === "done" || stage === "scraper_reviews_completed" || stage === "done") {
      worker.status = "done";
    } else if (stage || status === "running") {
      worker.status = "running";
    }

    const estimated = estimateWorkerPercent(worker, stage);
    worker.percent = clampPercent(Math.max(worker.percent, estimated));
    if (worker.status === "done") {
      worker.percent = 100;
    }
  }

  function estimateWorkerPercent(worker: WorkerProgressState, stage: string): number {
    if (stage === "scraper_reviews_progress") {
      const pageTotal = worker.totalPages ?? worker.maxPages;
      const pageCurrent = worker.currentPage;
      if (pageTotal && pageTotal > 0 && pageCurrent && pageCurrent > 0) {
        const ratio = Math.min(1, Math.max(0, pageCurrent / pageTotal));
        return 50 + ratio * 48;
      }
      if (worker.loadedReviews !== null && worker.totalReviews && worker.totalReviews > 0) {
        const ratio = Math.min(1, Math.max(0, worker.loadedReviews / worker.totalReviews));
        return 50 + ratio * 48;
      }
      return Math.min(95, worker.percent + 2);
    }
    if (stage in STAGE_BASE_PERCENT) {
      return STAGE_BASE_PERCENT[stage];
    }
    return worker.percent;
  }

  function appendLogLine(line: string, scrollToBottom = true): void {
    logLines.push(line);
    if (logLines.length > 900) {
      logLines = logLines.slice(logLines.length - 900);
    }
    logs.textContent = logLines.join("\n");
    if (scrollToBottom) {
      logs.scrollTop = logs.scrollHeight;
    }
  }

  function renderLiveState(): void {
    const selected = jobs.find((item) => String(item.job_id || "").trim() === selectedJobId);
    const displayTitle = selected ? resolveJobTitle(selected) : "Sin job seleccionado";
    const displayStage = state.jobStage || "-";
    const displayStatus = state.jobStatus || "unknown";
    liveTitle.textContent = `${displayTitle}`;
    liveJobMeta.textContent = `Job: ${selectedJobId || "-"} | Status: ${displayStatus} | Stage: ${displayStage}`;
    deleteJobButton.disabled = !selectedJobId;

    const totalPercentValue = computeTotalPercent(state);
    totalPercent.textContent = `${totalPercentValue}%`;
    totalFill.style.width = `${totalPercentValue}%`;
    analysisStatus.textContent = `Pipeline: ${state.analysisStage || state.jobStage || "-"}${
      state.analysisMessage || state.jobMessage ? ` | ${state.analysisMessage || state.jobMessage}` : ""
    }`;

    paintWorkerCard(googleCard, state.google_maps);
    paintWorkerCard(tripadvisorCard, state.tripadvisor);
  }

  async function refreshJobs(): Promise<void> {
    await loadJobsList();
  }

  function startJobsPolling(): void {
    if (jobsPollTimer !== null) return;
    jobsPollTimer = window.setInterval(() => {
      void loadJobsList();
    }, 8000);
  }

  function stopJobsPolling(): void {
    if (jobsPollTimer === null) return;
    window.clearInterval(jobsPollTimer);
    jobsPollTimer = null;
  }

  void loadJobsList();
  setFilterMode("active");

  AnimationController.mount(root, "view");
  return {
    key: "jobs",
    title: "Jobs lanzados",
    root,
    selectJob,
    refreshJobs,
    onShow: () => {
      startJobsPolling();
      if (selectedJobId) {
        void loadSelectedJob(selectedJobId);
      } else {
        void loadJobsList();
      }
    },
    onHide: () => {
      stopJobsPolling();
      stopJobStream();
    },
  };
}

function createWorkerCard(titleText: string): { root: HTMLElement; handle: WorkerCardHandle } {
  const root = createElement("article", "jobs-worker-card");
  const head = createElement("div", "jobs-worker-head");
  const title = createElement("span", "jobs-worker-title", titleText);
  const status = createElement("span", "jobs-status jobs-status--idle", "idle");
  head.append(title, status);
  root.append(head);

  const stage = createElement("div", "jobs-worker-stage muted", "-");
  root.append(stage);

  const track = createElement("div", "progress-track");
  const fill = createElement("div", "progress-fill");
  track.append(fill);
  root.append(track);

  const row = createElement("div", "jobs-worker-row");
  const percent = createElement("span", "jobs-worker-percent", "0%");
  const metrics = createElement("span", "jobs-worker-metrics muted", "Sin métricas todavía.");
  row.append(percent, metrics);
  root.append(row);

  return {
    root,
    handle: {
      status,
      stage,
      metrics,
      progressFill: fill,
      percent,
    },
  };
}

function paintWorkerCard(card: { handle: WorkerCardHandle }, worker: WorkerProgressState): void {
  const statusClass = normalizeStatusClass(worker.status);
  card.handle.status.className = `jobs-status jobs-status--${statusClass}`;
  card.handle.status.textContent = worker.status;
  card.handle.stage.textContent = worker.stage
    ? `${worker.stage}${worker.message ? ` | ${worker.message}` : ""}`
    : worker.message || "-";
  card.handle.progressFill.style.width = `${clampPercent(worker.percent)}%`;
  card.handle.percent.textContent = `${Math.round(clampPercent(worker.percent))}%`;
  card.handle.metrics.textContent = buildWorkerMetricsText(worker);
}

function buildWorkerMetricsText(worker: WorkerProgressState): string {
  const parts: string[] = [];
  const pageTotal = worker.totalPages ?? worker.maxPages;
  if (worker.currentPage !== null && pageTotal !== null) {
    parts.push(`Página ${worker.currentPage}/${pageTotal}`);
  } else if (worker.currentPage !== null) {
    parts.push(`Página ${worker.currentPage}`);
  }
  if (worker.loadedReviews !== null && worker.totalReviews !== null) {
    parts.push(`Reseñas ${worker.loadedReviews}/${worker.totalReviews}`);
  } else if (worker.loadedReviews !== null) {
    parts.push(`Reseñas ${worker.loadedReviews}`);
  }
  return parts.join(" | ") || "Sin métricas todavía.";
}

function formatLogLine(input: {
  stage: string;
  message: string;
  data: Record<string, unknown>;
  createdAt: string;
}): string {
  const timestamp = input.createdAt ? formatTime(input.createdAt) : formatTime(new Date().toISOString());
  const stage = input.stage || "progress";
  const source = normalizeSource(input.data.source);
  const sourceLabel = source ? `[${source}]` : "";
  const message = input.message || "";
  return `${timestamp} ${sourceLabel} ${stage} ${message}`.trim();
}

function formatTime(raw: string): string {
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return raw;
  }
  return date.toLocaleTimeString();
}

function resolveJobTitle(job: AnalyzeJobItem): string {
  const name = String(job.name || "").trim();
  if (name) return name;
  const payloadName = isRecord(job.payload) ? String(job.payload.name || "").trim() : "";
  if (payloadName) return payloadName;
  return "Análisis";
}

function normalizeStatusClass(status: string | undefined): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (!normalized) return "idle";
  if (normalized === "running") return "running";
  if (normalized === "queued") return "queued";
  if (normalized === "done") return "done";
  if (normalized === "failed") return "failed";
  if (normalized === "retrying") return "queued";
  if (normalized === "partial") return "queued";
  return "idle";
}

function normalizeSource(value: unknown): WorkerKey | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace("-", "_")
    .replace(" ", "_");
  if (normalized === "google_maps" || normalized === "googlemaps" || normalized === "google") {
    return "google_maps";
  }
  if (normalized === "tripadvisor" || normalized === "trip_advisor") {
    return "tripadvisor";
  }
  return null;
}

function createInitialPipelineState(): PipelineProgressState {
  return {
    jobStatus: "queued",
    jobStage: "",
    jobMessage: "",
    analysisStage: "",
    analysisMessage: "",
    google_maps: {
      key: "google_maps",
      label: "Google Maps",
      status: "idle",
      stage: "",
      message: "",
      percent: 0,
      currentPage: null,
      totalPages: null,
      maxPages: null,
      loadedReviews: null,
      totalReviews: null,
    },
    tripadvisor: {
      key: "tripadvisor",
      label: "TripAdvisor",
      status: "idle",
      stage: "",
      message: "",
      percent: 0,
      currentPage: null,
      totalPages: null,
      maxPages: null,
      loadedReviews: null,
      totalReviews: null,
    },
  };
}

function computeTotalPercent(state: PipelineProgressState): number {
  const raw = Math.round((state.google_maps.percent + state.tripadvisor.percent) / 2);
  if (state.jobStatus === "done") {
    return 100;
  }
  return clampPercent(raw);
}

function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function isActiveStatus(status: string | undefined): boolean {
  return ACTIVE_STATUSES.has(String(status || "").trim().toLowerCase());
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toInteger(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.round(value);
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}


