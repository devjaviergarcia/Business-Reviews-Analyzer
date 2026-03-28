import { AnimationController } from "../animations/controller";
import { createButton } from "../components/atoms/button";
import { ApiClient } from "../core/api-client";
import { clearElement, createElement, formatError } from "../core/dom";
import type { AnalyzeJobItem, JobEventItem, PaginatedResponse, ViewModule } from "../core/types";

type JobsViewDeps = {
  apiClient: ApiClient;
};

type JobsViewHandle = ViewModule & {
  selectJob: (jobId: string) => void;
  refreshJobs: () => Promise<void>;
};

type JobFilterMode = "active" | "all";
type SourceFilter = "all" | "google_maps" | "tripadvisor";
type DrawerPosition = "right" | "bottom";
type NodeKey = "scrape_google_maps" | "scrape_tripadvisor" | "analysis" | "report";
type NodeStatus = "idle" | "queued" | "running" | "done" | "failed" | "needs_human" | "waiting";
type ConnectorState = "idle" | "active" | "done" | "failed" | "waiting" | "human";
type ScrapeSource = "google_maps" | "tripadvisor";

type BusinessScrapeGroup = {
  key: string;
  businessName: string;
  rootBusinessId: string | null;
  canonicalNameNormalized: string | null;
  jobsBySource: Partial<Record<ScrapeSource, AnalyzeJobItem>>;
  latestUpdated: number;
  latestSource: ScrapeSource | null;
  latestJobId: string | null;
};

type TripAdvisorSessionState = {
  session_state?: string;
  availability_now?: boolean;
  last_human_intervention_at?: string | null;
  session_cookie_expires_at?: string | null;
  last_validation_result?: string;
  bot_detected_count?: number;
  last_error?: string | null;
};

type PipelineNodeState = {
  key: NodeKey;
  title: string;
  sourceBadge: string;
  status: NodeStatus;
  stage: string;
  message: string;
  progress: number;
  attempts: number | null;
  comments: number | null;
  durationSeconds: number | null;
  lastUpdated: string | null;
  error: string | null;
  jobId: string | null;
  outputUrl: string | null;
  events: JobEventItem[];
};

type PipelineNodeCardHandle = {
  root: HTMLButtonElement;
  title: HTMLElement;
  subtitle: HTMLElement;
  sourceBadge: HTMLElement;
  statusFlag: HTMLElement;
  statusDot: HTMLElement;
  statusText: HTMLElement;
  progressFill: HTMLElement;
  metrics: HTMLElement;
  hint: HTMLElement;
};

type ConnectorHandle = {
  root: HTMLElement;
};

type StreamKind = "scrape" | "analysis" | "report";

const ACTIVE_STATUSES = new Set(["queued", "running", "retrying", "partial", "needs_human"]);
const SCRAPE_STAGE_PROGRESS: Record<string, number> = {
  queued: 4,
  worker_started: 8,
  scrape_pipeline_started: 12,
  scraper_starting: 16,
  scraper_search_started: 24,
  scraper_search_completed: 38,
  scraper_listing_completed: 50,
  scraper_reviews_started: 62,
  scraper_reviews_progress: 74,
  scraper_reviews_completed: 100,
  handoff_analysis_queued: 100,
};
const ANALYSIS_STAGE_PROGRESS: Record<string, number> = {
  queued: 8,
  analysis_worker_started: 26,
  analysis_worker_summary: 92,
  done: 100,
};
const REPORT_STAGE_PROGRESS: Record<string, number> = {
  queued: 8,
  report_worker_started: 32,
  report_worker_completed: 92,
  done: 100,
};

export function createJobsView(deps: JobsViewDeps): JobsViewHandle {
  const root = createElement("section", "view-panel jobs6-view");

  const headerPanel = createElement("section", "panel jobs6-header-panel");
  headerPanel.append(createElement("h2", "panel__title", "Pipeline Operativo"));
  const subtitle = createElement(
    "div",
    "muted",
    "Seguimiento nodal de scrape/analyze con estado global de TripAdvisor en tiempo real."
  );
  headerPanel.append(subtitle);

  const sessionStrip = createElement("div", "jobs6-session-strip");
  const sessionStatus = createElement("span", "jobs6-badge jobs6-badge--idle", "TA: unknown");
  const sessionAvailability = createElement("span", "jobs6-kv", "Disponibilidad: -");
  const sessionCookieExpiry = createElement("span", "jobs6-kv", "Cookie expira: -");
  const sessionLastHuman = createElement("span", "jobs6-kv", "Última intervención: -");
  const sessionExtra = createElement("span", "jobs6-kv", "Validación: -");
  const sessionRefreshButton = createButton({ label: "Refrescar sesión TA", tone: "white" });
  sessionStrip.append(
    sessionStatus,
    sessionAvailability,
    sessionCookieExpiry,
    sessionLastHuman,
    sessionExtra,
    sessionRefreshButton
  );
  headerPanel.append(sessionStrip);
  root.append(headerPanel);

  const layout = createElement("div", "jobs6-layout");
  root.append(layout);

  const listPanel = createElement("section", "panel jobs6-list-panel");
  layout.append(listPanel);
  listPanel.append(createElement("h3", "panel__subtitle", "Jobs scrape"));

  const listControls = createElement("div", "jobs6-list-controls");
  const filterActiveButton = createButton({ label: "Activos", tone: "turquoise" });
  const filterAllButton = createButton({ label: "Todos", tone: "white" });
  const sourceSelect = createElement("select", "atom-input jobs6-source-filter") as HTMLSelectElement;
  sourceSelect.innerHTML =
    '<option value="all">Fuente: Todas</option><option value="google_maps">Fuente: Google</option><option value="tripadvisor">Fuente: Tripadvisor</option>';
  const refreshJobsButton = createButton({ label: "Recargar", tone: "white" });
  listControls.append(filterActiveButton, filterAllButton, sourceSelect, refreshJobsButton);
  listPanel.append(listControls);

  const listStatus = createElement("div", "muted", "Cargando jobs...");
  const jobsList = createElement("div", "jobs6-list");
  listPanel.append(listStatus, jobsList);

  const mainPanel = createElement("section", "panel jobs6-main-panel");
  layout.append(mainPanel);

  const mainHead = createElement("div", "jobs6-main-head");
  const selectedMeta = createElement("div", "muted", "Selecciona un negocio para visualizar el pipeline.");
  const mainActions = createElement("div", "form-actions");
  const reloadSelectedButton = createButton({ label: "Recargar job", tone: "white" }) as HTMLButtonElement;
  const traceFromStartButton = createButton({ label: "Traza SSE", tone: "white" }) as HTMLButtonElement;
  mainActions.append(reloadSelectedButton, traceFromStartButton);
  mainHead.append(selectedMeta, mainActions);
  mainPanel.append(mainHead);

  const pipelineWrap = createElement("section", "jobs6-pipeline-wrap");
  mainPanel.append(pipelineWrap);

  const sourcesStack = createElement("div", "jobs6-sources-stack");
  const googleNode = createPipelineNodeCard("scrape_google_maps", "GOOGLE SCRAPE", "google_maps");
  const tripNode = createPipelineNodeCard("scrape_tripadvisor", "TRIPADVISOR SCRAPE", "tripadvisor");
  sourcesStack.append(googleNode.root, tripNode.root);

  const connectorsCol = createElement("div", "jobs6-connectors-col");
  const connectorGoogle = createConnector();
  const connectorTrip = createConnector();
  connectorsCol.append(connectorGoogle.root, connectorTrip.root);

  const analysisCol = createElement("div", "jobs6-analysis-col");
  const analysisNode = createPipelineNodeCard("analysis", "ANALYZE", "analysis");
  const analysisToReportConnector = createConnector("inline");
  const reportNode = createPipelineNodeCard("report", "REPORT PDF", "report");
  analysisCol.append(analysisNode.root, analysisToReportConnector.root, reportNode.root);

  pipelineWrap.append(sourcesStack, connectorsCol, analysisCol);

  const logsPanel = createElement("section", "jobs6-log-panel");
  logsPanel.append(createElement("h3", "panel__subtitle", "Eventos"));
  const logs = createElement("pre", "code-block jobs6-log-block", "");
  logsPanel.append(logs);
  mainPanel.append(logsPanel);

  const drawer = createElement("aside", "jobs6-drawer jobs6-drawer--right");
  const drawerHead = createElement("div", "jobs6-drawer-head");
  const drawerTitle = createElement("h3", "jobs6-drawer-title", "Detalle nodo");
  const drawerActions = createElement("div", "jobs6-drawer-actions");
  const drawerPositionToggle = createButton({ label: "Posición: Derecha", tone: "white" });
  const drawerCloseButton = createButton({ label: "Cerrar", tone: "white" });
  drawerActions.append(drawerPositionToggle, drawerCloseButton);
  drawerHead.append(drawerTitle, drawerActions);

  const drawerBody = createElement("div", "jobs6-drawer-body");
  const drawerSummary = createElement("div", "jobs6-drawer-block jobs6-drawer-summary");
  const drawerStateLine = createElement("div", "jobs6-drawer-block");
  const drawerError = createElement("div", "jobs6-drawer-block jobs6-drawer-error muted", "Sin error.");
  const drawerTransitionsTitle = createElement("h4", "jobs6-drawer-section-title", "Transiciones recientes");
  const drawerTransitions = createElement("pre", "code-block jobs6-drawer-transitions", "");
  const drawerNodeActionsTitle = createElement("h4", "jobs6-drawer-section-title", "Acciones");
  const drawerNodeActions = createElement("div", "form-actions");
  const drawerRelaunchButton = createButton({ label: "Relanzar", tone: "turquoise" });
  const drawerRelaunchFromZeroButton = createButton({ label: "Relanzar de 0", tone: "turquoise" });
  const drawerLaunchLiveButton = createButton({ label: "Lanzar Live TA", tone: "turquoise" });
  const drawerDeleteButton = createButton({ label: "Eliminar job", tone: "white" });
  const drawerManualButton = createButton({ label: "Marcar manual TA", tone: "white" });
  const drawerOutputButton = createButton({ label: "Abrir output", tone: "white" });
  const drawerCopyJobButton = createButton({ label: "Copiar job_id", tone: "white" });
  drawerNodeActions.append(
    drawerRelaunchButton,
    drawerRelaunchFromZeroButton,
    drawerLaunchLiveButton,
    drawerDeleteButton,
    drawerManualButton,
    drawerOutputButton,
    drawerCopyJobButton
  );
  const drawerActionStatus = createElement("div", "muted", "");

  drawerBody.append(
    drawerSummary,
    drawerStateLine,
    drawerError,
    drawerTransitionsTitle,
    drawerTransitions,
    drawerNodeActionsTitle,
    drawerNodeActions,
    drawerActionStatus
  );

  drawer.append(drawerHead, drawerBody);
  root.append(drawer);

  const nodeCards: Record<NodeKey, PipelineNodeCardHandle> = {
    scrape_google_maps: googleNode,
    scrape_tripadvisor: tripNode,
    analysis: analysisNode,
    report: reportNode,
  };

  let filterMode: JobFilterMode = "active";
  let sourceFilter: SourceFilter = "all";
  let jobs: AnalyzeJobItem[] = [];
  let selectedBusinessKey: string | null = null;
  let selectedBusinessGroup: BusinessScrapeGroup | null = null;
  let deletingScrapeJobId: string | null = null;

  let analysisJobId: string | null = null;
  let reportJobId: string | null = null;
  let scrapeSourceJobIds: Partial<Record<ScrapeSource, string>> = {};

  let scrapeStreams: Partial<Record<ScrapeSource, EventSource>> = {};
  let analysisStream: EventSource | null = null;
  let analysisStreamJobId: string | null = null;
  let reportStream: EventSource | null = null;
  let reportStreamJobId: string | null = null;
  let loadedScrapeEvents: Record<ScrapeSource, number> = {
    google_maps: 0,
    tripadvisor: 0,
  };
  let loadedAnalysisEvents = 0;
  let loadedReportEvents = 0;

  let jobsPollTimer: number | null = null;
  let sessionPollTimer: number | null = null;
  let sessionState: TripAdvisorSessionState | null = null;

  let logsLines: string[] = [];
  let drawerOpen = false;
  let drawerNodeKey: NodeKey | null = null;
  let drawerPosition: DrawerPosition = "right";

  const nodes = createInitialNodes();

  for (const key of Object.keys(nodeCards) as NodeKey[]) {
    const card = nodeCards[key];
    card.root.addEventListener("click", () => openDrawer(key));
    card.root.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openDrawer(key);
      }
    });
  }

  filterActiveButton.addEventListener("click", () => {
    filterMode = "active";
    renderFilterButtons();
    renderJobsList();
  });
  filterAllButton.addEventListener("click", () => {
    filterMode = "all";
    renderFilterButtons();
    renderJobsList();
  });
  sourceSelect.addEventListener("change", () => {
    sourceFilter = (sourceSelect.value as SourceFilter) || "all";
    renderJobsList();
  });

  refreshJobsButton.addEventListener("click", () => {
    void loadJobsList();
  });

  sessionRefreshButton.addEventListener("click", () => {
    void loadTripadvisorSessionState();
  });

  reloadSelectedButton.addEventListener("click", () => {
    if (!selectedBusinessKey) return;
    void loadSelectedBusiness(selectedBusinessKey);
  });

  traceFromStartButton.addEventListener("click", () => {
    if (!selectedBusinessGroup) return;
    resetStreams();
    loadedScrapeEvents = { google_maps: 0, tripadvisor: 0 };
    loadedAnalysisEvents = 0;
    loadedReportEvents = 0;
    logsLines = [];
    logs.textContent = "";
    for (const source of ["google_maps", "tripadvisor"] as const) {
      const jobId = scrapeSourceJobIds[source];
      if (jobId) {
        startScrapeStream(source, jobId);
      }
    }
    if (analysisJobId) {
      startAnalysisStream(analysisJobId);
    }
    if (reportJobId) {
      startReportStream(reportJobId);
    }
  });

  drawerCloseButton.addEventListener("click", closeDrawer);
  drawerPositionToggle.addEventListener("click", () => {
    drawerPosition = drawerPosition === "right" ? "bottom" : "right";
    drawerPositionToggle.textContent = `Posición: ${drawerPosition === "right" ? "Derecha" : "Abajo"}`;
    renderDrawer();
  });

  drawerRelaunchButton.addEventListener("click", () => {
    void relaunchCurrentDrawerNode();
  });
  drawerRelaunchFromZeroButton.addEventListener("click", () => {
    void relaunchCurrentDrawerNode({ restartFromZero: true });
  });
  drawerLaunchLiveButton.addEventListener("click", () => {
    void launchTripadvisorLiveSession();
  });
  drawerDeleteButton.addEventListener("click", () => {
    void deleteCurrentDrawerNodeJob();
  });

  drawerManualButton.addEventListener("click", () => {
    void confirmManualTripadvisorSession();
  });

  drawerOutputButton.addEventListener("click", () => {
    const node = getDrawerNode();
    if (!node?.outputUrl) {
      drawerActionStatus.textContent = "No hay URL de salida para este nodo.";
      return;
    }
    window.open(node.outputUrl, "_blank", "noopener");
    drawerActionStatus.textContent = "Output abierto en nueva pestaña.";
  });

  drawerCopyJobButton.addEventListener("click", async () => {
    const node = getDrawerNode();
    if (!node?.jobId) {
      drawerActionStatus.textContent = "Este nodo no tiene job_id asociado.";
      return;
    }
    try {
      await navigator.clipboard.writeText(node.jobId);
      drawerActionStatus.textContent = `Copiado: ${node.jobId}`;
    } catch {
      drawerActionStatus.textContent = "No se pudo copiar al portapapeles.";
    }
  });

  const onWindowKeyDown = (event: KeyboardEvent): void => {
    if (event.key === "Escape") {
      closeDrawer();
    }
  };

  function renderFilterButtons(): void {
    filterActiveButton.classList.toggle("is-selected", filterMode === "active");
    filterAllButton.classList.toggle("is-selected", filterMode === "all");
  }

  async function loadJobsList(): Promise<void> {
    try {
      listStatus.textContent = "Cargando jobs...";
      const response = await deps.apiClient.get<PaginatedResponse<AnalyzeJobItem>>(
        "/business/scrape/jobs?page=1&page_size=100"
      );
      jobs = Array.isArray(response.items) ? response.items : [];
      jobs.sort((left, right) => sortByUpdated(right) - sortByUpdated(left));
      const groups = getRenderableBusinessGroups();
      listStatus.textContent = `${groups.length} negocios • ${jobs.length} jobs cargados`;
      renderJobsList();

      if (selectedBusinessKey) {
        selectedBusinessGroup = getBusinessGroupByKey(selectedBusinessKey);
      }

      if (!selectedBusinessKey && groups.length > 0) {
        selectJob(groups[0].key);
        return;
      }
      if (selectedBusinessKey && !selectedBusinessGroup) {
        selectedBusinessKey = null;
        selectedBusinessGroup = null;
        analysisJobId = null;
        reportJobId = null;
        scrapeSourceJobIds = {};
        resetStreams();
        resetPipelineState();
        const first = groups[0];
        if (first) {
          selectJob(first.key);
        }
      }
    } catch (error) {
      listStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  function getRenderableBusinessGroups(): BusinessScrapeGroup[] {
    const byBusiness = new Map<string, BusinessScrapeGroup>();

    for (const item of jobs) {
      const source = resolveSourceFromJob(item);
      const groupKey = resolveBusinessGroupKey(item);
      const existing = byBusiness.get(groupKey);
      const updatedAt = sortByUpdated(item);
      const businessName = resolveBusinessDisplayName(item);
      const rootBusinessId = resolveRootBusinessId(item);
      const canonicalNameNormalized = resolveCanonicalNameNormalized(item);

      if (!existing) {
        byBusiness.set(groupKey, {
          key: groupKey,
          businessName,
          rootBusinessId,
          canonicalNameNormalized,
          jobsBySource: { [source]: item },
          latestUpdated: updatedAt,
          latestSource: source,
          latestJobId: String(item.job_id || "").trim() || null,
        });
        continue;
      }

      const previous = existing.jobsBySource[source];
      if (!previous || sortByUpdated(previous) < updatedAt) {
        existing.jobsBySource[source] = item;
      }
      if (updatedAt >= existing.latestUpdated) {
        existing.latestUpdated = updatedAt;
        existing.latestSource = source;
        existing.latestJobId = String(item.job_id || "").trim() || null;
      }
      if (!existing.businessName || existing.businessName.startsWith("Negocio ")) {
        existing.businessName = businessName;
      }
      if (!existing.rootBusinessId && rootBusinessId) {
        existing.rootBusinessId = rootBusinessId;
      }
      if (!existing.canonicalNameNormalized && canonicalNameNormalized) {
        existing.canonicalNameNormalized = canonicalNameNormalized;
      }
    }

    const groups = Array.from(byBusiness.values());
    return groups
      .filter((group) => {
        const sourcesToInspect: ScrapeSource[] =
          sourceFilter === "all" ? ["google_maps", "tripadvisor"] : [sourceFilter];

        const hasAnySource = sourcesToInspect.some((source) => Boolean(group.jobsBySource[source]));
        if (!hasAnySource) {
          return false;
        }
        if (filterMode !== "active") {
          return true;
        }
        return sourcesToInspect.some((source) =>
          ACTIVE_STATUSES.has(String(group.jobsBySource[source]?.status || "").trim().toLowerCase())
        );
      })
      .sort((left, right) => right.latestUpdated - left.latestUpdated);
  }

  function getBusinessGroupByKey(groupKey: string): BusinessScrapeGroup | null {
    const normalized = String(groupKey || "").trim();
    if (!normalized) {
      return null;
    }
    return getRenderableBusinessGroups().find((group) => group.key === normalized) ?? null;
  }

  function renderJobsList(): void {
    clearElement(jobsList);
    const renderable = getRenderableBusinessGroups();
    if (renderable.length === 0) {
      jobsList.append(createElement("div", "muted", "No hay jobs para estos filtros."));
      return;
    }
    for (const group of renderable) {
      const deleteTarget = resolveGroupDeleteTarget(group, sourceFilter);

      const row = createElement("div", "jobs6-list-item-row");
      const itemButton = createElement("button", "jobs6-list-item anim-hover") as HTMLButtonElement;
      itemButton.type = "button";
      itemButton.disabled = deletingScrapeJobId !== null;
      itemButton.classList.toggle("jobs6-list-item--active", selectedBusinessKey === group.key);

      const top = createElement("div", "jobs6-list-item-top");
      const name = createElement("div", "jobs6-list-item-title", group.businessName);
      const statuses = createElement("div", "jobs6-list-source-statuses");
      statuses.append(
        createSourceStatusBadge("G", group.jobsBySource.google_maps),
        createSourceStatusBadge("T", group.jobsBySource.tripadvisor)
      );
      top.append(name, statuses);

      const meta = createElement(
        "div",
        "jobs6-list-item-meta",
        [
          `Business ID: ${(group.rootBusinessId || group.key).slice(0, 12)}`,
          `Google: ${shortJobLabel(group.jobsBySource.google_maps)}`,
          `Tripadvisor: ${shortJobLabel(group.jobsBySource.tripadvisor)}`,
        ].join(" • ")
      );

      itemButton.append(top, meta);
      itemButton.addEventListener("click", () => selectJob(group.key));

      const deleteButton = createButton({
        label: deletingScrapeJobId === deleteTarget.jobId ? "Eliminando..." : deleteTarget.label,
        tone: "white",
        className: "jobs6-list-item-delete",
      });
      deleteButton.disabled = deletingScrapeJobId !== null || !deleteTarget.jobId;
      deleteButton.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (!deleteTarget.jobId) return;
        void deleteScrapeJobFromList(deleteTarget.jobId, group.key);
      });

      row.append(itemButton, deleteButton);
      jobsList.append(row);
    }
  }

  async function deleteScrapeJobFromList(jobId: string, groupKey: string): Promise<void> {
    const normalized = String(jobId || "").trim();
    if (!normalized || deletingScrapeJobId) {
      return;
    }

    const confirmed = window.confirm(
      `Se eliminará el job ${normalized}. Si está activo se cancelará y luego se borrará. ¿Continuar?`
    );
    if (!confirmed) {
      return;
    }

    deletingScrapeJobId = normalized;
    listStatus.textContent = `Eliminando job ${normalized.slice(0, 8)}...`;
    renderJobsList();

    try {
      await deps.apiClient.delete(`/business/scrape/jobs/${encodeURIComponent(normalized)}`);

      const deletedFromSelectedBusiness = selectedBusinessKey === groupKey;

      await loadJobsList();

      if (deletedFromSelectedBusiness) {
        const refreshedSelected = selectedBusinessKey ? getBusinessGroupByKey(selectedBusinessKey) : null;
        if (refreshedSelected) {
          await loadSelectedBusiness(refreshedSelected.key);
        } else {
          const first = getRenderableBusinessGroups()[0];
          if (first) {
            selectJob(first.key);
          } else {
            selectedBusinessKey = null;
            selectedBusinessGroup = null;
            analysisJobId = null;
            reportJobId = null;
            scrapeSourceJobIds = {};
            resetStreams();
            resetPipelineState();
            renderDrawer();
          }
        }
      }
    } catch (error) {
      listStatus.textContent = `ERROR: ${formatError(error)}`;
    } finally {
      deletingScrapeJobId = null;
      renderJobsList();
    }
  }

  function resetPipelineState(): void {
    const base = createInitialNodes();
    for (const key of Object.keys(base) as NodeKey[]) {
      nodes[key] = base[key];
    }
    logsLines = [];
    logs.textContent = "";
    renderPipeline();
  }

  async function loadSelectedBusiness(groupKey: string): Promise<void> {
    const normalized = String(groupKey || "").trim();
    selectedBusinessKey = normalized || null;
    selectedBusinessGroup = normalized ? getBusinessGroupByKey(normalized) : null;
    renderJobsList();
    if (!selectedBusinessGroup) {
      return;
    }

    resetStreams();
    resetPipelineState();

    scrapeSourceJobIds = {};
    analysisJobId = null;
    reportJobId = null;
    loadedScrapeEvents = { google_maps: 0, tripadvisor: 0 };
    loadedAnalysisEvents = 0;
    loadedReportEvents = 0;

    selectedMeta.textContent = `Cargando negocio ${selectedBusinessGroup.businessName}...`;

    const sourceDetails: Partial<Record<ScrapeSource, AnalyzeJobItem>> = {};

    try {
      for (const source of ["google_maps", "tripadvisor"] as const) {
        const sourceJob = selectedBusinessGroup.jobsBySource[source];
        const sourceJobId = String(sourceJob?.job_id || "").trim();
        if (!sourceJobId) {
          setSourceNodeNotFound(source);
          continue;
        }

        scrapeSourceJobIds[source] = sourceJobId;
        const detail = await deps.apiClient.get<AnalyzeJobItem>(
          `/business/scrape/jobs/${encodeURIComponent(sourceJobId)}`
        );
        loadedScrapeEvents[source] = Array.isArray(detail.events) ? detail.events.length : 0;
        sourceDetails[source] = detail;
        applyScrapeJobSnapshotForSource(source, detail);
      }

      const preferredAnalysisSource = resolvePreferredAnalysisSource(sourceDetails);
      const preferredAnalysisJob =
        (preferredAnalysisSource && sourceDetails[preferredAnalysisSource]
          ? resolveAnalysisJobId(sourceDetails[preferredAnalysisSource] as AnalyzeJobItem)
          : null) ||
        (sourceDetails.google_maps ? resolveAnalysisJobId(sourceDetails.google_maps) : null) ||
        (sourceDetails.tripadvisor ? resolveAnalysisJobId(sourceDetails.tripadvisor) : null);
      analysisJobId = preferredAnalysisJob;

      if (analysisJobId) {
        await loadAnalysisJobSnapshot(analysisJobId);
        reportJobId = resolveReportJobId(nodes.analysis);
        if (reportJobId) {
          await loadReportJobSnapshot(reportJobId);
        } else {
          nodes.report = createInitialNodes().report;
          nodes.report.status = "waiting";
          nodes.report.stage = "report_not_enqueued";
          nodes.report.message = "No hay job de report asociado para este análisis.";
        }
      } else {
        nodes.analysis = createInitialNodes().analysis;
        nodes.analysis.status = "waiting";
        nodes.analysis.stage = "analysis_not_enqueued";
        nodes.analysis.message = "No hay job de análisis asociado para este negocio.";
        nodes.report = createInitialNodes().report;
        nodes.report.status = "waiting";
        nodes.report.stage = "waiting_analysis";
        nodes.report.message = "Esperando a que exista un job de análisis.";
      }

      renderPipeline();
      renderDrawer();

      for (const source of ["google_maps", "tripadvisor"] as const) {
        const sourceJobId = scrapeSourceJobIds[source];
        if (sourceJobId) {
          startScrapeStream(source, sourceJobId);
        }
      }
      if (analysisJobId) {
        startAnalysisStream(analysisJobId);
      }
      if (reportJobId) {
        startReportStream(reportJobId);
      }
    } catch (error) {
      selectedMeta.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  function selectJob(jobIdOrBusinessKey: string): void {
    const normalized = String(jobIdOrBusinessKey || "").trim();
    if (!normalized) return;

    const byBusinessKey = getBusinessGroupByKey(normalized);
    if (byBusinessKey) {
      void loadSelectedBusiness(byBusinessKey.key);
      return;
    }

    const byJob = getRenderableBusinessGroups().find(
      (group) =>
        String(group.jobsBySource.google_maps?.job_id || "").trim() === normalized ||
        String(group.jobsBySource.tripadvisor?.job_id || "").trim() === normalized
    );
    if (byJob) {
      void loadSelectedBusiness(byJob.key);
    }
  }

  function applyScrapeJobSnapshotForSource(
    source: ScrapeSource,
    job: AnalyzeJobItem,
    options?: { appendLogs?: boolean }
  ): void {
    const appendLogs = options?.appendLogs !== false;
    const nodeKey: NodeKey = source === "tripadvisor" ? "scrape_tripadvisor" : "scrape_google_maps";
    const node = nodes[nodeKey];
    updateNodeFromJobSnapshot(node, job, "scrape");
    node.title = resolveSourceDisplayName(job, source);
    node.sourceBadge = source;

    const snapshotEvents = Array.isArray(job.events) ? job.events : [];
    node.events = snapshotEvents;
    if (appendLogs) {
      for (const event of snapshotEvents) {
        appendLogLine(formatLogLine(event, "scrape", source));
      }
    }
  }

  function setSourceNodeNotFound(source: ScrapeSource): void {
    const nodeKey: NodeKey = source === "tripadvisor" ? "scrape_tripadvisor" : "scrape_google_maps";
    const base = createInitialNodes()[nodeKey];
    nodes[nodeKey] = {
      ...base,
      status: "failed",
      stage: "source_not_found",
      message: "NO ENCONTRADO: no existe job de scrape para esta fuente en el negocio.",
      progress: 100,
      error: "NO ENCONTRADO",
    };
  }

  async function loadAnalysisJobSnapshot(jobId: string, options?: { appendLogs?: boolean }): Promise<void> {
    const appendLogs = options?.appendLogs !== false;
    try {
      const detail = await deps.apiClient.get<AnalyzeJobItem>(
        `/business/analyze/jobs/${encodeURIComponent(jobId)}`
      );
      loadedAnalysisEvents = Array.isArray(detail.events) ? detail.events.length : 0;
      updateNodeFromJobSnapshot(nodes.analysis, detail, "analysis");
      nodes.analysis.events = Array.isArray(detail.events) ? detail.events : [];
      const nextReportJobId = resolveReportJobId(detail) || resolveReportJobId(nodes.analysis);
      if (nextReportJobId && nextReportJobId !== reportJobId) {
        reportJobId = nextReportJobId;
      }
      if (appendLogs) {
        for (const event of nodes.analysis.events) {
          appendLogLine(formatLogLine(event, "analysis", "analysis"));
        }
      }
    } catch (error) {
      nodes.analysis.status = "failed";
      nodes.analysis.error = formatError(error);
      nodes.analysis.message = "No se pudo cargar el job de análisis.";
      nodes.analysis.progress = 100;
    }
  }

  async function loadReportJobSnapshot(jobId: string, options?: { appendLogs?: boolean }): Promise<void> {
    const appendLogs = options?.appendLogs !== false;
    try {
      const detail = await deps.apiClient.get<AnalyzeJobItem>(
        `/business/report/jobs/${encodeURIComponent(jobId)}`
      );
      loadedReportEvents = Array.isArray(detail.events) ? detail.events.length : 0;
      updateNodeFromJobSnapshot(nodes.report, detail, "report");
      nodes.report.events = Array.isArray(detail.events) ? detail.events : [];
      if (appendLogs) {
        for (const event of nodes.report.events) {
          appendLogLine(formatLogLine(event, "report", "report"));
        }
      }
    } catch (error) {
      nodes.report.status = "failed";
      nodes.report.error = formatError(error);
      nodes.report.message = "No se pudo cargar el job de report.";
      nodes.report.progress = 100;
    }
  }

  function updateNodeFromJobSnapshot(
    node: PipelineNodeState,
    job: AnalyzeJobItem,
    kind: StreamKind
  ): void {
    const status = normalizeNodeStatus(job.status, job.progress?.stage);
    const stage = String(job.progress?.stage || "").trim();
    const message = String(job.progress?.message || "").trim();
    node.status = status;
    node.stage = stage;
    node.message = message;
    node.jobId = String(job.job_id || "").trim() || null;
    node.lastUpdated = String(job.updated_at || "").trim() || null;
    node.attempts = toInteger((job as unknown as Record<string, unknown>).attempts);
    node.durationSeconds = computeDurationSeconds(job.started_at, job.finished_at);
    node.error = String(job.error || "").trim() || null;

    const result = isRecord(job.result) ? job.result : null;
    if (result) {
      node.comments =
        toInteger(result.processed_review_count) ??
        toInteger(result.review_count) ??
        toInteger(result.dataset_review_count);
      if (kind === "report") {
        node.outputUrl = resolveReportOutputPath(result, deps.apiClient.getBaseUrl());
      } else {
        const website = String((isRecord(result.listing) ? result.listing.website : "") || "").trim();
        node.outputUrl = website || null;
      }
    }

    if (kind === "scrape") {
      node.progress = estimateScrapeProgress(stage, status, node.progress);
    } else if (kind === "analysis") {
      node.progress = estimateAnalysisProgress(stage, status, node.progress);
    } else {
      node.progress = estimateReportProgress(stage, status, node.progress);
    }
  }

  function startScrapeStream(source: ScrapeSource, jobId: string): void {
    stopScrapeStream(source);
    const fromIndex = loadedScrapeEvents[source];
    const stream = deps.apiClient.createEventSource(
      `/business/scrape/jobs/${encodeURIComponent(jobId)}/events?from_index=${fromIndex}`
    );
    scrapeStreams[source] = stream;

    stream.addEventListener("progress", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      const eventIndex = toInteger(payload.index);
      if (eventIndex !== null) {
        loadedScrapeEvents[source] = Math.max(loadedScrapeEvents[source], eventIndex);
      } else {
        loadedScrapeEvents[source] += 1;
      }
      applyScrapeEvent(payload, source);
    });

    stream.addEventListener("done", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      const nodeKey: NodeKey = source === "tripadvisor" ? "scrape_tripadvisor" : "scrape_google_maps";
      if (payload && typeof payload.status === "string") {
        nodes[nodeKey].status = normalizeNodeStatus(payload.status, "done");
      } else {
        nodes[nodeKey].status = "done";
      }
      nodes[nodeKey].progress = 100;
      appendLogLine(`[done][scrape][${source}] ${String(payload?.status || "done")}`);
      renderPipeline();
      stopScrapeStream(source);
      if (selectedBusinessKey) {
        void loadJobsList();
      }
      void syncSelectedScrapeSnapshot(source, jobId);
    });

    stream.addEventListener("heartbeat", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      const nodeKey: NodeKey = source === "tripadvisor" ? "scrape_tripadvisor" : "scrape_google_maps";
      const status = String(payload.status || "").trim();
      if (status) {
        nodes[nodeKey].status = normalizeNodeStatus(status, nodes[nodeKey].stage);
      }
      renderPipeline();
    });

    stream.addEventListener("error", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (payload?.error) {
        appendLogLine(`[error][scrape][${source}] ${String(payload.error)}`);
      }
    });

    stream.onerror = () => {
      appendLogLine(`[stream][scrape][${source}] desconectado`);
    };
  }

  function startAnalysisStream(jobId: string): void {
    if (analysisStreamJobId === jobId && analysisStream) {
      return;
    }
    stopAnalysisStream();
    analysisStreamJobId = jobId;

    const stream = deps.apiClient.createEventSource(
      `/business/analyze/jobs/${encodeURIComponent(jobId)}/events?from_index=${loadedAnalysisEvents}`
    );
    analysisStream = stream;

    stream.addEventListener("progress", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      const eventIndex = toInteger(payload.index);
      if (eventIndex !== null) {
        loadedAnalysisEvents = Math.max(loadedAnalysisEvents, eventIndex);
      } else {
        loadedAnalysisEvents += 1;
      }
      applyAnalysisEvent(payload);
    });

    stream.addEventListener("done", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      nodes.analysis.status = normalizeNodeStatus(String(payload?.status || "done"), "done");
      nodes.analysis.progress = 100;
      appendLogLine(`[done][analysis] ${String(payload?.status || "done")}`);
      renderPipeline();
      stopAnalysisStream();
      if (analysisJobId) {
        void syncAnalysisSnapshot(analysisJobId);
      }
      const nextReportJobId = resolveReportJobId(nodes.analysis);
      if (nextReportJobId && nextReportJobId !== reportJobId) {
        reportJobId = nextReportJobId;
        startReportStream(nextReportJobId);
        void syncReportSnapshot(nextReportJobId);
      }
    });

    stream.addEventListener("heartbeat", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      if (typeof payload.status === "string") {
        nodes.analysis.status = normalizeNodeStatus(payload.status, nodes.analysis.stage);
      }
      renderPipeline();
    });

    stream.addEventListener("error", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (payload?.error) {
        appendLogLine(`[error][analysis] ${String(payload.error)}`);
      }
    });

    stream.onerror = () => {
      appendLogLine("[stream][analysis] desconectado");
    };
  }

  function startReportStream(jobId: string): void {
    if (reportStreamJobId === jobId && reportStream) {
      return;
    }
    stopReportStream();
    reportStreamJobId = jobId;

    const stream = deps.apiClient.createEventSource(
      `/business/report/jobs/${encodeURIComponent(jobId)}/events?from_index=${loadedReportEvents}`
    );
    reportStream = stream;

    stream.addEventListener("progress", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      const eventIndex = toInteger(payload.index);
      if (eventIndex !== null) {
        loadedReportEvents = Math.max(loadedReportEvents, eventIndex);
      } else {
        loadedReportEvents += 1;
      }
      applyReportEvent(payload);
    });

    stream.addEventListener("done", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      nodes.report.status = normalizeNodeStatus(String(payload?.status || "done"), "done");
      nodes.report.progress = 100;
      appendLogLine(`[done][report] ${String(payload?.status || "done")}`);
      renderPipeline();
      stopReportStream();
      if (reportJobId) {
        void syncReportSnapshot(reportJobId);
      }
    });

    stream.addEventListener("heartbeat", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      if (typeof payload.status === "string") {
        nodes.report.status = normalizeNodeStatus(payload.status, nodes.report.stage);
      }
      renderPipeline();
    });

    stream.addEventListener("error", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (payload?.error) {
        appendLogLine(`[error][report] ${String(payload.error)}`);
      }
    });

    stream.onerror = () => {
      appendLogLine("[stream][report] desconectado");
    };
  }

  async function syncSelectedScrapeSnapshot(source: ScrapeSource, jobId: string): Promise<void> {
    try {
      const detail = await deps.apiClient.get<AnalyzeJobItem>(
        `/business/scrape/jobs/${encodeURIComponent(jobId)}`
      );
      applyScrapeJobSnapshotForSource(source, detail, { appendLogs: false });
      const nextAnalysisId = resolveAnalysisJobId(detail);
      if (nextAnalysisId && nextAnalysisId !== analysisJobId) {
        analysisJobId = nextAnalysisId;
        await loadAnalysisJobSnapshot(nextAnalysisId, { appendLogs: false });
        startAnalysisStream(nextAnalysisId);
        const nextReportJobId = resolveReportJobId(nodes.analysis);
        if (nextReportJobId) {
          reportJobId = nextReportJobId;
          await loadReportJobSnapshot(nextReportJobId, { appendLogs: false });
          startReportStream(nextReportJobId);
        }
      }
      renderPipeline();
      renderDrawer();
    } catch (error) {
      appendLogLine(`[sync][scrape] ${formatError(error)}`);
    }
  }

  async function syncAnalysisSnapshot(jobId: string): Promise<void> {
    try {
      await loadAnalysisJobSnapshot(jobId, { appendLogs: false });
      const nextReportJobId = resolveReportJobId(nodes.analysis);
      if (nextReportJobId && nextReportJobId !== reportJobId) {
        reportJobId = nextReportJobId;
      }
      if (reportJobId) {
        await loadReportJobSnapshot(reportJobId, { appendLogs: false });
      }
      renderPipeline();
      renderDrawer();
    } catch (error) {
      appendLogLine(`[sync][analysis] ${formatError(error)}`);
    }
  }

  async function syncReportSnapshot(jobId: string): Promise<void> {
    try {
      await loadReportJobSnapshot(jobId, { appendLogs: false });
      renderPipeline();
      renderDrawer();
    } catch (error) {
      appendLogLine(`[sync][report] ${formatError(error)}`);
    }
  }

  function applyScrapeEvent(payload: Record<string, unknown>, fallbackSource: ScrapeSource): void {
    const stage = String(payload.stage || "").trim();
    const message = String(payload.message || "").trim();
    const status = String(payload.status || "").trim();
    const eventData = isRecord(payload.data) ? payload.data : {};

    const sourceFromEvent = normalizeSource(String(eventData.source || ""));
    const effectiveSource: ScrapeSource = sourceFromEvent === "tripadvisor" || sourceFromEvent === "google_maps"
      ? sourceFromEvent
      : fallbackSource;
    const sourceKey: NodeKey = effectiveSource === "tripadvisor" ? "scrape_tripadvisor" : "scrape_google_maps";

    const node = nodes[sourceKey];
    if (status) {
      node.status = normalizeNodeStatus(status, stage);
    }
    if (stage) {
      node.stage = stage;
    }
    if (message) {
      node.message = message;
    }
    node.progress = estimateScrapeProgress(stage, node.status, node.progress);

    const reviewsCount =
      toInteger(eventData.processed_review_count) ??
      toInteger(eventData.scraped_review_count) ??
      toInteger(eventData.review_count) ??
      toInteger(eventData.dataset_review_count);
    if (reviewsCount !== null) {
      node.comments = reviewsCount;
    }

    const analysisIdFromEvent = String(eventData.analysis_job_id || "").trim();
    if (analysisIdFromEvent && analysisIdFromEvent !== analysisJobId) {
      analysisJobId = analysisIdFromEvent;
      nodes.analysis.jobId = analysisIdFromEvent;
      nodes.analysis.status = "queued";
      nodes.analysis.stage = "handoff_analysis_queued";
      nodes.analysis.message = `Analysis job encolado: ${analysisIdFromEvent}`;
      nodes.analysis.progress = Math.max(nodes.analysis.progress, 8);
      startAnalysisStream(analysisIdFromEvent);
      void syncAnalysisSnapshot(analysisIdFromEvent);
    }

    node.events.push({
      status,
      stage,
      message,
      data: eventData,
      created_at: String(payload.created_at || ""),
    });

    appendLogLine(formatLogLine(payload, "scrape", effectiveSource));
    renderPipeline();
    renderDrawer();
  }

  function applyAnalysisEvent(payload: Record<string, unknown>): void {
    const stage = String(payload.stage || "").trim();
    const message = String(payload.message || "").trim();
    const status = String(payload.status || "").trim();
    const eventData = isRecord(payload.data) ? payload.data : {};

    if (status) {
      nodes.analysis.status = normalizeNodeStatus(status, stage);
    }
    if (stage) {
      nodes.analysis.stage = stage;
    }
    if (message) {
      nodes.analysis.message = message;
    }
    nodes.analysis.progress = estimateAnalysisProgress(stage, nodes.analysis.status, nodes.analysis.progress);

    const comments =
      toInteger(eventData.processed_review_count) ??
      toInteger(eventData.review_count) ??
      toInteger(eventData.dataset_review_count);
    if (comments !== null) {
      nodes.analysis.comments = comments;
    }

    const reportIdFromEvent = String(eventData.report_job_id || "").trim();
    if (reportIdFromEvent && reportIdFromEvent !== reportJobId) {
      reportJobId = reportIdFromEvent;
      nodes.report.jobId = reportIdFromEvent;
      nodes.report.status = "queued";
      nodes.report.stage = "report_handoff_queued";
      nodes.report.message = `Report job encolado: ${reportIdFromEvent}`;
      nodes.report.progress = Math.max(nodes.report.progress, 8);
      startReportStream(reportIdFromEvent);
      void syncReportSnapshot(reportIdFromEvent);
    }

    nodes.analysis.events.push({
      status,
      stage,
      message,
      data: eventData,
      created_at: String(payload.created_at || ""),
    });

    appendLogLine(formatLogLine(payload, "analysis", "analysis"));
    renderPipeline();
    renderDrawer();
  }

  function applyReportEvent(payload: Record<string, unknown>): void {
    const stage = String(payload.stage || "").trim();
    const message = String(payload.message || "").trim();
    const status = String(payload.status || "").trim();
    const eventData = isRecord(payload.data) ? payload.data : {};

    if (status) {
      nodes.report.status = normalizeNodeStatus(status, stage);
    }
    if (stage) {
      nodes.report.stage = stage;
    }
    if (message) {
      nodes.report.message = message;
    }
    nodes.report.progress = estimateReportProgress(stage, nodes.report.status, nodes.report.progress);

    const artifacts = isRecord(eventData.report_artifacts) ? eventData.report_artifacts : null;
    if (artifacts) {
      const path = resolveReportOutputPath({ artifacts }, deps.apiClient.getBaseUrl());
      if (path) {
        nodes.report.outputUrl = path;
      }
    }

    nodes.report.events.push({
      status,
      stage,
      message,
      data: eventData,
      created_at: String(payload.created_at || ""),
    });

    appendLogLine(formatLogLine(payload, "report", "report"));
    renderPipeline();
    renderDrawer();
  }

  function appendLogLine(line: string): void {
    logsLines.push(line);
    if (logsLines.length > 1200) {
      logsLines = logsLines.slice(logsLines.length - 1200);
    }
    logs.textContent = logsLines.join("\n");
    logs.scrollTop = logs.scrollHeight;
  }

  function renderPipeline(): void {
    const summaryText = selectedBusinessGroup
      ? `Negocio ${selectedBusinessGroup.businessName} • Google: ${nodes.scrape_google_maps.status.toUpperCase()} • Tripadvisor: ${nodes.scrape_tripadvisor.status.toUpperCase()} • Analysis: ${nodes.analysis.status.toUpperCase()} • Report: ${nodes.report.status.toUpperCase()}`
      : "Selecciona un negocio para visualizar el pipeline.";
    selectedMeta.textContent = summaryText;

    for (const key of Object.keys(nodeCards) as NodeKey[]) {
      paintNode(nodeCards[key], nodes[key]);
    }

    paintConnector(
      connectorGoogle,
      resolveConnectorState(nodes.scrape_google_maps, nodes.analysis)
    );
    paintConnector(
      connectorTrip,
      resolveConnectorState(nodes.scrape_tripadvisor, nodes.analysis)
    );
    paintConnector(
      analysisToReportConnector,
      resolveAnalysisToReportConnectorState(nodes.analysis, nodes.report)
    );
  }

  function openDrawer(nodeKey: NodeKey): void {
    drawerNodeKey = nodeKey;
    drawerOpen = true;
    renderDrawer();
  }

  function closeDrawer(): void {
    drawerOpen = false;
    renderDrawer();
  }

  function renderDrawer(): void {
    drawer.classList.toggle("jobs6-drawer--open", drawerOpen);
    drawer.classList.toggle("jobs6-drawer--right", drawerPosition === "right");
    drawer.classList.toggle("jobs6-drawer--bottom", drawerPosition === "bottom");

    const node = getDrawerNode();
    if (!drawerOpen || !node) {
      drawerTitle.textContent = "Detalle nodo";
      drawerSummary.textContent = "Selecciona un nodo para ver detalle.";
      drawerStateLine.textContent = "-";
      drawerError.textContent = "Sin error.";
      drawerTransitions.textContent = "";
      drawerActionStatus.textContent = "";
      drawerManualButton.classList.add("hidden");
      drawerLaunchLiveButton.classList.add("hidden");
      drawerRelaunchButton.removeAttribute("disabled");
      drawerRelaunchFromZeroButton.removeAttribute("disabled");
      drawerRelaunchFromZeroButton.classList.remove("hidden");
      drawerLaunchLiveButton.removeAttribute("disabled");
      drawerDeleteButton.removeAttribute("disabled");
      drawerOutputButton.removeAttribute("disabled");
      drawerCopyJobButton.removeAttribute("disabled");
      return;
    }

    drawerTitle.textContent = `${node.title} · ${node.jobId ? node.jobId.slice(0, 8) : "-"}`;
    drawerSummary.textContent = [
      `Fuente: ${node.sourceBadge}`,
      `job_id: ${node.jobId || "-"}`,
      `Intentos: ${node.attempts ?? "-"}`,
      `Comments: ${node.comments ?? "-"}`,
      `Duración: ${formatDuration(node.durationSeconds)}`,
      `Última actualización: ${formatDateTime(node.lastUpdated)}`,
    ].join("\n");

    drawerStateLine.textContent = `Estado: ${node.status.toUpperCase()} • Stage: ${node.stage || "-"}${
      node.message ? ` • ${node.message}` : ""
    }`;

    drawerError.textContent = node.error ? `ERROR: ${node.error}` : "Sin error.";

    const eventKind: StreamKind =
      node.key === "analysis" ? "analysis" : node.key === "report" ? "report" : "scrape";
    const transitions = node.events
      .slice(-30)
      .map((event) => formatLogLine(event, eventKind, node.sourceBadge));
    drawerTransitions.textContent = transitions.join("\n");

    drawerManualButton.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerLaunchLiveButton.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerRelaunchFromZeroButton.classList.toggle("hidden", node.key === "analysis" || node.key === "report");
    drawerRelaunchButton.toggleAttribute("disabled", !node.jobId);
    drawerRelaunchFromZeroButton.toggleAttribute("disabled", !node.jobId || node.key === "analysis" || node.key === "report");
    drawerLaunchLiveButton.toggleAttribute("disabled", node.key !== "scrape_tripadvisor" || !node.jobId);
    drawerDeleteButton.toggleAttribute("disabled", !node.jobId);
    drawerOutputButton.toggleAttribute("disabled", !node.outputUrl);
    drawerCopyJobButton.toggleAttribute("disabled", !node.jobId);
  }

  function getDrawerNode(): PipelineNodeState | null {
    if (!drawerNodeKey) return null;
    return nodes[drawerNodeKey] || null;
  }

  function isActiveRelaunchConflict(message: string): boolean {
    const normalized = String(message || "").toLowerCase();
    return (
      normalized.includes("active jobs cannot be relaunched") ||
      normalized.includes("already active") ||
      normalized.includes("already running")
    );
  }

  function isForceFieldUnsupported(message: string): boolean {
    const normalized = String(message || "").toLowerCase();
    return normalized.includes("extra_forbidden") && normalized.includes("force");
  }

  function isRestartFromZeroUnsupported(message: string): boolean {
    const normalized = String(message || "").toLowerCase();
    return normalized.includes("extra_forbidden") && normalized.includes("restart_from_zero");
  }

  async function relaunchCurrentDrawerNode(options?: { restartFromZero?: boolean }): Promise<void> {
    const node = getDrawerNode();
    if (!node?.jobId) {
      drawerActionStatus.textContent = "Este nodo no tiene job_id para relanzar.";
      return;
    }
    const restartFromZero = Boolean(options?.restartFromZero);

    drawerActionStatus.textContent = restartFromZero ? "Relanzando de 0..." : "Relanzando...";
    try {
      const basePath =
        node.key === "analysis"
          ? "/business/analyze/jobs"
          : node.key === "report"
            ? "/business/report/jobs"
            : "/business/scrape/jobs";
      const endpoint = `${basePath}/${encodeURIComponent(node.jobId)}/relaunch`;
      let forced = false;
      let relaunchedJobId = node.jobId;
      try {
        const firstPayload = restartFromZero
          ? { force: true, restart_from_zero: true }
          : {};
        const response = await deps.apiClient.post<{ job_id?: string }>(endpoint, firstPayload);
        const responseJobId = String(response?.job_id || "").trim();
        if (responseJobId) {
          relaunchedJobId = responseJobId;
        }
      } catch (error) {
        const message = formatError(error);
        if (restartFromZero) {
          if (isForceFieldUnsupported(message) || isRestartFromZeroUnsupported(message)) {
            drawerActionStatus.textContent =
              "ERROR: La API actual no soporta 'Relanzar de 0'. Actualiza/reconstruye backend.";
            return;
          }
          throw error;
        }
        if (!isActiveRelaunchConflict(message)) {
          throw error;
        }
        const confirmed = window.confirm(
          "Ya hay una ejecución activa para este job. ¿Quieres forzar relanzado?\n\n" +
            "Esto creará un nuevo job en cola con el mismo payload."
        );
        if (!confirmed) {
          drawerActionStatus.textContent = "Relanzado cancelado por el usuario.";
          return;
        }
        forced = true;
        drawerActionStatus.textContent = "Relanzando (forzado)...";
        let response: { job_id?: string };
        try {
          response = await deps.apiClient.post<{ job_id?: string }>(endpoint, { force: true });
        } catch (forceError) {
          const forceMessage = formatError(forceError);
          if (isForceFieldUnsupported(forceMessage)) {
            drawerActionStatus.textContent =
              "ERROR: La API actual no soporta relanzado forzado todavía. Actualiza/reconstruye backend.";
            return;
          }
          throw forceError;
        }
        const responseJobId = String(response?.job_id || "").trim();
        if (responseJobId) {
          relaunchedJobId = responseJobId;
        }
      }
      drawerActionStatus.textContent = forced
        ? `Job relanzado (forzado): ${relaunchedJobId}`
        : restartFromZero
          ? `Job relanzado de 0: ${relaunchedJobId}`
          : `Job relanzado: ${relaunchedJobId}`;
      if (node.key === "analysis") {
        analysisJobId = relaunchedJobId;
        await syncAnalysisSnapshot(relaunchedJobId);
        startAnalysisStream(relaunchedJobId);
      } else if (node.key === "report") {
        reportJobId = relaunchedJobId;
        await syncReportSnapshot(relaunchedJobId);
        startReportStream(relaunchedJobId);
      } else {
        await loadJobsList();
        const group = getRenderableBusinessGroups().find(
          (item) =>
            String(item.jobsBySource.google_maps?.job_id || "").trim() === relaunchedJobId ||
            String(item.jobsBySource.tripadvisor?.job_id || "").trim() === relaunchedJobId ||
            String(item.jobsBySource.google_maps?.job_id || "").trim() === node.jobId ||
            String(item.jobsBySource.tripadvisor?.job_id || "").trim() === node.jobId
        );
        if (group) {
          await loadSelectedBusiness(group.key);
        }
      }
      if (node.key === "analysis" || node.key === "report") {
        void loadJobsList();
      }
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function confirmManualTripadvisorSession(): Promise<void> {
    drawerActionStatus.textContent = "Confirmando sesión manual...";
    try {
      await deps.apiClient.post("/tripadvisor/session-state/manual-confirm", {
        relaunch_pending_tripadvisor_jobs: false,
      });
      await loadTripadvisorSessionState();
      drawerActionStatus.textContent = "Sesión manual confirmada.";
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function launchTripadvisorLiveSession(): Promise<void> {
    const node = getDrawerNode();
    if (!node || node.key !== "scrape_tripadvisor") {
      drawerActionStatus.textContent = "Lanzar Live solo aplica a TripAdvisor.";
      return;
    }
    const fallbackTripadvisorJobId =
      String(selectedBusinessGroup?.jobsBySource.tripadvisor?.job_id || "").trim() || null;
    const replayJobId = String(node.jobId || "").trim() || fallbackTripadvisorJobId;
    if (!replayJobId) {
      drawerActionStatus.textContent =
        "No hay job_id de TripAdvisor para lanzar Live en modo replay.";
      return;
    }
    drawerActionStatus.textContent = "Lanzando sesión live de TripAdvisor (replay del job completo)...";
    try {
      const payload = {
        reason: `ui_live_replay:${replayJobId}:${node.status || "unknown"}`,
        display: ":0",
        job_id: replayJobId,
      };
      const response = await deps.apiClient.post<{
        ok?: boolean;
        skipped?: boolean;
        reason?: string;
        already_running?: boolean;
        mode?: string;
        job_id?: string | null;
        log_file?: string;
        live_session?: { pid?: number | null };
      }>("/tripadvisor/live-session/launch", payload);
      if (response?.ok === false || response?.skipped) {
        drawerActionStatus.textContent = `No disponible: ${response?.reason || "bridge_disabled"}.`;
        return;
      }
      const pid = response?.live_session?.pid;
      const mode = String(response?.mode || "").trim();
      const logFile = String(response?.log_file || "").trim();
      if (response?.already_running) {
        drawerActionStatus.textContent = `Sesión live ya en ejecución${pid ? ` (pid=${pid})` : ""}${mode ? ` [modo=${mode}]` : ""}.`;
      } else {
        drawerActionStatus.textContent = `Sesión live lanzada${pid ? ` (pid=${pid})` : ""}${mode ? ` [modo=${mode}]` : ""}. ${logFile ? `Log: ${logFile}` : ""}`.trim();
      }
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function deleteCurrentDrawerNodeJob(): Promise<void> {
    const node = getDrawerNode();
    if (!node?.jobId) {
      drawerActionStatus.textContent = "Este nodo no tiene job_id para borrar.";
      return;
    }
    const confirmed = window.confirm(
      `Se eliminará el job ${node.jobId}. Si está activo se cancelará y luego se borrará. ¿Continuar?`
    );
    if (!confirmed) {
      return;
    }

    drawerActionStatus.textContent = "Eliminando job...";
    try {
      const basePath =
        node.key === "analysis"
          ? "/business/analyze/jobs"
          : node.key === "report"
            ? "/business/report/jobs"
            : "/business/scrape/jobs";
      await deps.apiClient.delete(`${basePath}/${encodeURIComponent(node.jobId)}`);
      drawerActionStatus.textContent = `Job eliminado: ${node.jobId}`;
      const selectedGroupKeyBeforeDelete = selectedBusinessKey;
      const selectedGroupBeforeDelete = selectedBusinessGroup;
      const deletedFromSelectedBusiness =
        node.key !== "analysis" &&
        Boolean(
          selectedGroupBeforeDelete &&
            (String(selectedGroupBeforeDelete.jobsBySource.google_maps?.job_id || "").trim() === node.jobId ||
              String(selectedGroupBeforeDelete.jobsBySource.tripadvisor?.job_id || "").trim() === node.jobId)
        );
      await loadJobsList();

      if (analysisJobId === node.jobId && node.key === "analysis") {
        analysisJobId = null;
        nodes.analysis = createInitialNodes().analysis;
        renderPipeline();
        renderDrawer();
        return;
      }
      if (reportJobId === node.jobId && node.key === "report") {
        reportJobId = null;
        nodes.report = createInitialNodes().report;
        renderPipeline();
        renderDrawer();
        return;
      }

      if (deletedFromSelectedBusiness && selectedGroupKeyBeforeDelete) {
        const refreshed = getBusinessGroupByKey(selectedGroupKeyBeforeDelete);
        if (refreshed) {
          await loadSelectedBusiness(refreshed.key);
        } else {
          const first = getRenderableBusinessGroups()[0];
          if (first) {
            selectJob(first.key);
          } else {
            selectedBusinessKey = null;
            selectedBusinessGroup = null;
            analysisJobId = null;
            reportJobId = null;
            scrapeSourceJobIds = {};
            resetStreams();
            resetPipelineState();
            renderDrawer();
          }
        }
      }
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function loadTripadvisorSessionState(): Promise<void> {
    try {
      const state = await deps.apiClient.get<TripAdvisorSessionState>("/tripadvisor/session-state");
      sessionState = state;
      renderSessionState();
    } catch (error) {
      sessionStatus.textContent = "TA: error";
      sessionStatus.className = "jobs6-badge jobs6-badge--failed";
      sessionAvailability.textContent = "Disponibilidad: error";
      sessionExtra.textContent = `Validación: ${formatError(error)}`;
    }
  }

  function renderSessionState(): void {
    const state = sessionState;
    if (!state) return;

    const raw = String(state.session_state || "invalid").trim().toLowerCase();
    const availability = Boolean(state.availability_now);

    let statusClass = "jobs6-badge--failed";
    let statusText = `TA: ${raw || "invalid"}`;
    if (raw === "valid" && availability) {
      statusClass = "jobs6-badge--done";
      statusText = "TA: valid";
    } else if (raw === "expired") {
      statusClass = "jobs6-badge--waiting";
      statusText = "TA: expired";
    } else if (raw === "invalid") {
      statusClass = "jobs6-badge--human";
      statusText = "TA: needs_human";
    }

    sessionStatus.className = `jobs6-badge ${statusClass}`;
    sessionStatus.textContent = statusText;
    sessionAvailability.textContent = `Disponibilidad: ${availability ? "sí" : "no"}`;
    sessionCookieExpiry.textContent = `Cookie expira: ${formatDateTime(state.session_cookie_expires_at || null)}`;
    sessionLastHuman.textContent = `Última intervención: ${formatDateTime(state.last_human_intervention_at || null)}`;
    sessionExtra.textContent = `Validación: ${String(state.last_validation_result || "-")} • Bot count: ${String(
      state.bot_detected_count ?? "-"
    )}`;
  }

  function startPollers(): void {
    if (jobsPollTimer === null) {
      jobsPollTimer = window.setInterval(() => {
        void loadJobsList();
      }, 8000);
    }
    if (sessionPollTimer === null) {
      sessionPollTimer = window.setInterval(() => {
        void loadTripadvisorSessionState();
      }, 12000);
    }
  }

  function stopPollers(): void {
    if (jobsPollTimer !== null) {
      window.clearInterval(jobsPollTimer);
      jobsPollTimer = null;
    }
    if (sessionPollTimer !== null) {
      window.clearInterval(sessionPollTimer);
      sessionPollTimer = null;
    }
  }

  function resetStreams(): void {
    stopScrapeStream();
    stopAnalysisStream();
    stopReportStream();
    loadedScrapeEvents = { google_maps: 0, tripadvisor: 0 };
    loadedAnalysisEvents = 0;
    loadedReportEvents = 0;
  }

  function stopScrapeStream(source?: ScrapeSource): void {
    if (source) {
      const stream = scrapeStreams[source];
      if (!stream) return;
      stream.close();
      delete scrapeStreams[source];
      return;
    }
    for (const key of ["google_maps", "tripadvisor"] as const) {
      const stream = scrapeStreams[key];
      if (!stream) continue;
      stream.close();
      delete scrapeStreams[key];
    }
  }

  function stopAnalysisStream(): void {
    if (!analysisStream) return;
    analysisStream.close();
    analysisStream = null;
    analysisStreamJobId = null;
  }

  function stopReportStream(): void {
    if (!reportStream) return;
    reportStream.close();
    reportStream = null;
    reportStreamJobId = null;
  }

  async function refreshJobs(): Promise<void> {
    await loadJobsList();
  }

  renderFilterButtons();
  renderPipeline();
  renderDrawer();

  void loadJobsList();
  void loadTripadvisorSessionState();

  AnimationController.mount(root, "view");

  return {
    key: "jobs",
    title: "Pipeline",
    root,
    selectJob,
    refreshJobs,
    onShow: () => {
      startPollers();
      window.addEventListener("keydown", onWindowKeyDown);
      if (selectedBusinessKey) {
        void loadSelectedBusiness(selectedBusinessKey);
      } else {
        void loadJobsList();
      }
      void loadTripadvisorSessionState();
    },
    onHide: () => {
      stopPollers();
      resetStreams();
      window.removeEventListener("keydown", onWindowKeyDown);
      closeDrawer();
    },
  };
}

function createPipelineNodeCard(key: NodeKey, titleText: string, sourceBadgeText: string): PipelineNodeCardHandle {
  const root = createElement("button", "jobs6-node anim-hover") as HTMLButtonElement;
  root.type = "button";
  root.dataset.node = key;
  root.tabIndex = 0;

  const top = createElement("div", "jobs6-node-top");
  const titleWrap = createElement("div", "jobs6-node-title-wrap");
  const title = createElement("div", "jobs6-node-title", titleText);
  const subtitle = createElement("div", "jobs6-node-subtitle", "job: -");
  titleWrap.append(title, subtitle);
  const sourceBadge = createElement("span", "jobs6-source-badge", sourceBadgeText);
  const statusFlag = createElement("span", "jobs6-flag jobs6-flag--idle", "IDLE");
  top.append(titleWrap, sourceBadge, statusFlag);

  const statusRow = createElement("div", "jobs6-node-status-row");
  const statusDot = createElement("span", "jobs6-status-dot jobs6-status-dot--idle", "");
  const statusText = createElement("span", "jobs6-node-status-text", "idle");
  statusRow.append(statusDot, statusText);

  const progressTrack = createElement("div", "jobs6-node-progress-track");
  const progressFill = createElement("div", "jobs6-node-progress-fill");
  progressTrack.append(progressFill);

  const divider = createElement("div", "jobs6-node-divider");
  const metrics = createElement("div", "jobs6-node-metrics", "Intentos: - | Comments: - | Duración: -");
  const hint = createElement("div", "jobs6-node-hint", "Click para detalle");

  root.append(top, statusRow, progressTrack, divider, metrics, hint);

  return {
    root,
    title,
    subtitle,
    sourceBadge,
    statusFlag,
    statusDot,
    statusText,
    progressFill,
    metrics,
    hint,
  };
}

function createConnector(mode: "default" | "inline" = "default"): ConnectorHandle {
  const className =
    mode === "inline"
      ? "jobs6-connector jobs6-connector--inline jobs6-connector--idle"
      : "jobs6-connector jobs6-connector--idle";
  return {
    root: createElement("div", className),
  };
}

function paintConnector(handle: ConnectorHandle, state: ConnectorState): void {
  const isInline = handle.root.classList.contains("jobs6-connector--inline");
  handle.root.className = `jobs6-connector jobs6-connector--${state}${isInline ? " jobs6-connector--inline" : ""}`;
}

function resolveConnectorState(scrapeNode: PipelineNodeState, analysisNode: PipelineNodeState): ConnectorState {
  if (scrapeNode.status === "failed") return "failed";
  if (scrapeNode.status === "needs_human") return "human";
  if (scrapeNode.status === "waiting") return "waiting";
  if (scrapeNode.status === "running") return "active";
  if (scrapeNode.status === "done" && analysisNode.status === "done") return "done";
  if (scrapeNode.status === "done" && (analysisNode.status === "queued" || analysisNode.status === "running")) return "active";
  if (scrapeNode.status === "queued") return "waiting";
  return "idle";
}

function resolveAnalysisToReportConnectorState(
  analysisNode: PipelineNodeState,
  reportNode: PipelineNodeState
): ConnectorState {
  if (reportNode.status === "failed") return "failed";
  if (reportNode.status === "needs_human") return "human";
  if (reportNode.status === "queued") return "waiting";
  if (reportNode.status === "running") return "active";
  if (analysisNode.status === "failed") return "failed";
  if (analysisNode.status === "done" && reportNode.status === "done") return "done";
  if (analysisNode.status === "done" && reportNode.status === "waiting") {
    return "active";
  }
  if (analysisNode.status === "running" || analysisNode.status === "queued") return "waiting";
  return "idle";
}

function paintNode(handle: PipelineNodeCardHandle, node: PipelineNodeState | undefined): void {
  if (!node) {
    handle.root.dataset.status = "idle";
    handle.statusFlag.className = "jobs6-flag jobs6-flag--idle";
    handle.statusFlag.textContent = "IDLE";
    handle.statusDot.className = "jobs6-status-dot jobs6-status-dot--idle";
    handle.statusText.textContent = "idle";
    handle.subtitle.textContent = "job: -";
    handle.progressFill.style.width = "0%";
    handle.metrics.textContent = "Intentos: - | Comments: - | Duración: -";
    handle.hint.textContent = "Sin job asociado";
    return;
  }
  const statusClass = statusClassFromRaw(node.status);
  handle.root.dataset.status = node.status;
  handle.statusFlag.className = `jobs6-flag jobs6-flag--${statusClass}`;
  handle.statusFlag.textContent = normalizeStatusFlagText(node.status);

  handle.statusDot.className = `jobs6-status-dot jobs6-status-dot--${statusClass}`;
  handle.statusText.textContent = `${node.status}${node.stage ? ` • ${node.stage}` : ""}`;

  handle.subtitle.textContent = `job: ${node.jobId ? node.jobId.slice(0, 8) : "-"}`;
  handle.sourceBadge.textContent = node.sourceBadge;

  handle.progressFill.style.width = `${clampPercent(node.progress)}%`;
  handle.metrics.textContent = `Intentos: ${node.attempts ?? "-"} | Comments: ${node.comments ?? "-"} | Duración: ${formatDuration(
    node.durationSeconds
  )}`;

  const actionHint = node.jobId ? "Click para detalle" : "Sin job asociado";
  handle.hint.textContent = actionHint;

  const tooltip = [
    `Estado: ${node.status}`,
    `Última actualización: ${formatDateTime(node.lastUpdated)}`,
    `Sugerencia: ${node.status === "needs_human" ? "Intervenir manualmente" : "Abrir detalle"}`,
  ].join(" • ");
  handle.root.title = tooltip;
}

function createInitialNodes(): Record<NodeKey, PipelineNodeState> {
  return {
    scrape_google_maps: {
      key: "scrape_google_maps",
      title: "GOOGLE SCRAPE",
      sourceBadge: "google_maps",
      status: "idle",
      stage: "",
      message: "",
      progress: 0,
      attempts: null,
      comments: null,
      durationSeconds: null,
      lastUpdated: null,
      error: null,
      jobId: null,
      outputUrl: null,
      events: [],
    },
    scrape_tripadvisor: {
      key: "scrape_tripadvisor",
      title: "TRIPADVISOR SCRAPE",
      sourceBadge: "tripadvisor",
      status: "idle",
      stage: "",
      message: "",
      progress: 0,
      attempts: null,
      comments: null,
      durationSeconds: null,
      lastUpdated: null,
      error: null,
      jobId: null,
      outputUrl: null,
      events: [],
    },
    analysis: {
      key: "analysis",
      title: "ANALYZE",
      sourceBadge: "analysis",
      status: "idle",
      stage: "",
      message: "",
      progress: 0,
      attempts: null,
      comments: null,
      durationSeconds: null,
      lastUpdated: null,
      error: null,
      jobId: null,
      outputUrl: null,
      events: [],
    },
    report: {
      key: "report",
      title: "REPORT PDF",
      sourceBadge: "report",
      status: "idle",
      stage: "",
      message: "",
      progress: 0,
      attempts: null,
      comments: null,
      durationSeconds: null,
      lastUpdated: null,
      error: null,
      jobId: null,
      outputUrl: null,
      events: [],
    },
  };
}

function resolveAnalysisJobId(job: AnalyzeJobItem): string | null {
  const result = isRecord(job.result) ? job.result : null;
  const handoff = result && isRecord(result.analysis_handoff) ? result.analysis_handoff : null;
  const fromResult = String((handoff?.analysis_job_id as string | undefined) || "").trim();
  if (fromResult) {
    return fromResult;
  }

  const events = Array.isArray(job.events) ? job.events : [];
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    const data = isRecord(event?.data) ? event.data : null;
    const candidate = String((data?.analysis_job_id as string | undefined) || "").trim();
    if (candidate) {
      return candidate;
    }
  }
  return null;
}

function resolveReportJobId(analysisSource: PipelineNodeState | AnalyzeJobItem): string | null {
  const asJob = analysisSource as AnalyzeJobItem;
  const result = isRecord(asJob.result) ? asJob.result : null;
  const handoff = result && isRecord(result.report_handoff) ? result.report_handoff : null;
  const fromResult = String((handoff?.report_job_id as string | undefined) || "").trim();
  if (fromResult) {
    return fromResult;
  }

  const rawEvents = Array.isArray(asJob.events) ? asJob.events : [];
  const fromEvents = [...rawEvents].reverse();
  for (const event of fromEvents) {
    const data = isRecord(event?.data) ? event.data : null;
    const candidate = String((data?.report_job_id as string | undefined) || "").trim();
    if (candidate) {
      return candidate;
    }
  }

  const asNode = analysisSource as PipelineNodeState;
  const messageCandidate = String(asNode.message || "").match(/[0-9a-f]{24}/i);
  if (messageCandidate?.[0]) {
    return String(messageCandidate[0]).trim();
  }
  return null;
}

function resolveReportOutputPath(result: Record<string, unknown>, apiBaseUrl: string): string | null {
  const artifacts = isRecord(result.artifacts) ? result.artifacts : null;
  if (!artifacts) {
    return null;
  }
  const pdf = isRecord(artifacts.pdf) ? artifacts.pdf : null;
  const html = isRecord(artifacts.html) ? artifacts.html : null;
  const json = isRecord(artifacts.json) ? artifacts.json : null;

  const preferredPath = String((pdf?.path as string | undefined) || "").trim();
  if (preferredPath) {
    return normalizeOutputUrl(preferredPath, apiBaseUrl);
  }
  const htmlPath = String((html?.path as string | undefined) || "").trim();
  if (htmlPath) {
    return normalizeOutputUrl(htmlPath, apiBaseUrl);
  }
  const jsonPath = String((json?.path as string | undefined) || "").trim();
  if (jsonPath) {
    return normalizeOutputUrl(jsonPath, apiBaseUrl);
  }
  return null;
}

function normalizeOutputUrl(pathOrUrl: string, apiBaseUrl: string): string {
  const value = String(pathOrUrl || "").trim();
  if (!value) return "";
  if (value.startsWith("http://") || value.startsWith("https://") || value.startsWith("blob:")) {
    return value;
  }
  const normalizedPath = value.startsWith("file://") ? value.slice("file://".length) : value;
  const normalizedBase = String(apiBaseUrl || "").trim().replace(/\/+$/, "");
  if (normalizedPath.startsWith("/business/report/artifacts")) {
    return `${normalizedBase}${normalizedPath}`;
  }
  return `${normalizedBase}/business/report/artifacts?path=${encodeURIComponent(normalizedPath)}`;
}

function sortByUpdated(job: AnalyzeJobItem): number {
  const timestamp = Date.parse(String(job.updated_at || job.created_at || ""));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function resolveJobTitle(job: AnalyzeJobItem): string {
  const name = String(job.name || "").trim();
  if (name) return name;
  const payload = isRecord(job.payload) ? job.payload : null;
  const payloadName = String((payload?.name as string | undefined) || "").trim();
  if (payloadName) return payloadName;
  return "Job scrape";
}

function resolveBusinessDisplayName(job: AnalyzeJobItem): string {
  const canonical = String(job.canonical_name || "").trim();
  if (canonical) return canonical;
  const payload = isRecord(job.payload) ? job.payload : null;
  const payloadCanonical = String((payload?.canonical_name as string | undefined) || "").trim();
  if (payloadCanonical) return payloadCanonical;
  const sourceName = String(job.source_name || "").trim();
  if (sourceName) return sourceName;
  return resolveJobTitle(job);
}

function resolveCanonicalNameNormalized(job: AnalyzeJobItem): string | null {
  const fromJob = String(job.canonical_name_normalized || "").trim();
  if (fromJob) return fromJob;
  const payload = isRecord(job.payload) ? job.payload : null;
  const fromPayload = String((payload?.canonical_name_normalized as string | undefined) || "").trim();
  if (fromPayload) return fromPayload;
  return null;
}

function resolveRootBusinessId(job: AnalyzeJobItem): string | null {
  const fromJob = String(job.root_business_id || "").trim();
  if (fromJob) return fromJob;
  const payload = isRecord(job.payload) ? job.payload : null;
  const fromPayload = String((payload?.root_business_id as string | undefined) || "").trim();
  if (fromPayload) return fromPayload;
  return null;
}

function resolveBusinessGroupKey(job: AnalyzeJobItem): string {
  const rootId = resolveRootBusinessId(job);
  if (rootId) return `business:${rootId}`;

  const inferredBusinessId = resolveInferredBusinessId(job);
  if (inferredBusinessId) return `business:${inferredBusinessId}`;

  const groupingName = resolveBusinessGroupingNameNormalized(job);
  if (groupingName) return `name:${groupingName}`;

  const fallbackName = normalizeBusinessLabel(resolveBusinessDisplayName(job));
  if (fallbackName) return `fallback:${fallbackName}`;
  return `job:${String(job.job_id || "").trim()}`;
}

function normalizeBusinessLabel(value: string): string {
  return String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function resolveBusinessGroupingNameNormalized(job: AnalyzeJobItem): string | null {
  const canonical = resolveCanonicalNameNormalized(job);
  if (canonical) return canonical;

  const nameNormalized = String(job.name_normalized || "").trim();
  if (nameNormalized) return nameNormalized;

  const payload = isRecord(job.payload) ? job.payload : null;
  const payloadNameNormalized = String((payload?.name_normalized as string | undefined) || "").trim();
  if (payloadNameNormalized) return payloadNameNormalized;

  const rawCanonical = String(job.canonical_name || "").trim();
  if (rawCanonical) return normalizeBusinessLabel(rawCanonical) || null;

  const payloadCanonical = String((payload?.canonical_name as string | undefined) || "").trim();
  if (payloadCanonical) return normalizeBusinessLabel(payloadCanonical) || null;

  const rawName = String(job.name || "").trim();
  if (rawName) return normalizeBusinessLabel(rawName) || null;

  const payloadName = String((payload?.name as string | undefined) || "").trim();
  if (payloadName) return normalizeBusinessLabel(payloadName) || null;

  return null;
}

function resolveInferredBusinessId(job: AnalyzeJobItem): string | null {
  const result = isRecord(job.result) ? job.result : null;
  const fromResult = String((result?.business_id as string | undefined) || "").trim();
  if (fromResult) return fromResult;

  const payload = isRecord(job.payload) ? job.payload : null;
  const fromPayload = String((payload?.business_id as string | undefined) || "").trim();
  if (fromPayload) return fromPayload;

  const events = Array.isArray(job.events) ? job.events : [];
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const eventData = isRecord(events[index]?.data) ? events[index].data : null;
    if (!eventData) continue;
    const fromEvent = String((eventData.business_id as string | undefined) || "").trim();
    if (fromEvent) return fromEvent;
  }
  return null;
}

function createSourceStatusBadge(prefix: "G" | "T", job?: AnalyzeJobItem): HTMLElement {
  if (!job) {
    return createElement("span", "jobs6-status jobs6-status--failed", `${prefix}: NO ENCONTRADO`);
  }
  const status = String(job.status || "unknown").toUpperCase();
  return createElement(
    "span",
    `jobs6-status jobs6-status--${statusClassFromRaw(job.status)}`,
    `${prefix}: ${status}`
  );
}

function shortJobLabel(job?: AnalyzeJobItem): string {
  if (!job?.job_id) return "NO ENCONTRADO";
  return `${job.job_id.slice(0, 8)} (${String(job.status || "unknown").toUpperCase()})`;
}

function resolveSourceDisplayName(job: AnalyzeJobItem, source: ScrapeSource): string {
  const sourceName = String(job.source_name || "").trim();
  if (sourceName) return sourceName;
  const payload = isRecord(job.payload) ? job.payload : null;
  const payloadSourceName = String((payload?.source_name as string | undefined) || "").trim();
  if (payloadSourceName) return payloadSourceName;
  const jobName = String(job.name || "").trim();
  if (jobName) return jobName;
  const payloadName = String((payload?.name as string | undefined) || "").trim();
  if (payloadName) return payloadName;
  return source === "google_maps" ? "Google Maps" : "Tripadvisor";
}

function resolveGroupDeleteTarget(
  group: BusinessScrapeGroup,
  sourceFilter: SourceFilter
): { jobId: string | null; label: string } {
  if (sourceFilter === "google_maps") {
    const jobId = String(group.jobsBySource.google_maps?.job_id || "").trim() || null;
    return { jobId, label: "Eliminar G" };
  }
  if (sourceFilter === "tripadvisor") {
    const jobId = String(group.jobsBySource.tripadvisor?.job_id || "").trim() || null;
    return { jobId, label: "Eliminar T" };
  }

  const preferredSource = group.latestSource;
  if (preferredSource) {
    const preferredJobId = String(group.jobsBySource[preferredSource]?.job_id || "").trim() || null;
    if (preferredJobId) {
      return { jobId: preferredJobId, label: preferredSource === "google_maps" ? "Eliminar G" : "Eliminar T" };
    }
  }

  const fallbackGoogle = String(group.jobsBySource.google_maps?.job_id || "").trim() || null;
  if (fallbackGoogle) {
    return { jobId: fallbackGoogle, label: "Eliminar G" };
  }
  const fallbackTrip = String(group.jobsBySource.tripadvisor?.job_id || "").trim() || null;
  if (fallbackTrip) {
    return { jobId: fallbackTrip, label: "Eliminar T" };
  }
  return { jobId: null, label: "Sin job" };
}

function resolvePreferredAnalysisSource(
  details: Partial<Record<ScrapeSource, AnalyzeJobItem>>
): ScrapeSource | null {
  let selected: ScrapeSource | null = null;
  let selectedTimestamp = 0;
  for (const source of ["google_maps", "tripadvisor"] as const) {
    const detail = details[source];
    if (!detail || !resolveAnalysisJobId(detail)) continue;
    const timestamp = sortByUpdated(detail);
    if (timestamp >= selectedTimestamp) {
      selected = source;
      selectedTimestamp = timestamp;
    }
  }
  return selected;
}

function resolveSourceFromJob(job: AnalyzeJobItem): "google_maps" | "tripadvisor" {
  const queueName = String(job.queue_name || "").trim().toLowerCase();
  if (queueName.includes("tripadvisor")) return "tripadvisor";
  if (queueName.includes("google")) return "google_maps";

  const payload = isRecord(job.payload) ? job.payload : null;
  const result = isRecord(job.result) ? job.result : null;

  const fromPayloadSource = normalizeSource(String((payload?.source as string | undefined) || ""));
  if (fromPayloadSource === "google_maps" || fromPayloadSource === "tripadvisor") {
    return fromPayloadSource;
  }

  const payloadSources = Array.isArray(payload?.sources) ? payload.sources : [];
  if (payloadSources.length === 1) {
    const source = normalizeSource(String(payloadSources[0] || ""));
    if (source === "google_maps" || source === "tripadvisor") {
      return source;
    }
  }

  const pipeline = isRecord(result?.pipeline) ? result.pipeline : null;
  const fromPipelineSource = normalizeSource(String((pipeline?.source as string | undefined) || ""));
  if (fromPipelineSource === "google_maps" || fromPipelineSource === "tripadvisor") {
    return fromPipelineSource;
  }

  const resultSources = isRecord(result?.sources) ? result.sources : null;
  if (resultSources) {
    if ("tripadvisor" in resultSources) return "tripadvisor";
    if ("google_maps" in resultSources || "google" in resultSources) return "google_maps";
  }

  const events = Array.isArray(job.events) ? job.events : [];
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const eventData = isRecord(events[index]?.data) ? events[index]?.data : null;
    if (!eventData) continue;
    const sourceCandidate = normalizeSource(String((eventData.source as string | undefined) || ""));
    if (sourceCandidate === "google_maps" || sourceCandidate === "tripadvisor") {
      return sourceCandidate;
    }
    const queueCandidate = String((eventData.queue_name as string | undefined) || "").trim().toLowerCase();
    if (queueCandidate.includes("tripadvisor")) return "tripadvisor";
    if (queueCandidate.includes("google")) return "google_maps";
  }

  return "google_maps";
}

function normalizeSource(value: string): "google_maps" | "tripadvisor" | "analysis" | null {
  const normalized = String(value || "").trim().toLowerCase().replace("-", "_").replace(" ", "_");
  if (
    normalized === "google_maps" ||
    normalized === "googlemaps" ||
    normalized === "google" ||
    normalized === "scrape_google_maps"
  ) {
    return "google_maps";
  }
  if (normalized === "tripadvisor" || normalized === "trip_advisor" || normalized === "scrape_tripadvisor") {
    return "tripadvisor";
  }
  if (normalized === "analysis") {
    return "analysis";
  }
  return null;
}

function normalizeNodeStatus(rawStatus: string | undefined, stage: string | undefined): NodeStatus {
  const status = String(rawStatus || "").trim().toLowerCase();
  const normalizedStage = String(stage || "").trim().toLowerCase();

  if (status === "needs_human" || normalizedStage.includes("needs_human")) return "needs_human";
  if (status === "failed" || normalizedStage === "failed" || normalizedStage.includes("source_failed")) return "failed";
  if (status === "done" || normalizedStage === "done") return "done";
  if (status === "running") return "running";
  if (status === "queued" || normalizedStage === "queued") return "queued";
  if (status === "retrying" || status === "partial") return "running";
  return "idle";
}

function normalizeStatusFlagText(status: NodeStatus): string {
  if (status === "needs_human") return "NEEDS_HUMAN";
  if (status === "waiting") return "WAITING";
  return status.toUpperCase();
}

function statusClassFromRaw(status: string | NodeStatus | undefined): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "needs_human") return "needs-human";
  if (normalized === "waiting") return "waiting";
  if (normalized === "running") return "running";
  if (normalized === "queued") return "queued";
  if (normalized === "done") return "done";
  if (normalized === "failed") return "failed";
  return "idle";
}

function estimateScrapeProgress(stage: string, status: NodeStatus, current: number): number {
  if (status === "done") return 100;
  if (status === "failed" || status === "needs_human") return Math.max(current, 100);
  const normalizedStage = String(stage || "").trim().toLowerCase();
  if (normalizedStage in SCRAPE_STAGE_PROGRESS) {
    return Math.max(current, SCRAPE_STAGE_PROGRESS[normalizedStage]);
  }
  if (status === "running") return Math.max(current, 14);
  if (status === "queued") return Math.max(current, 4);
  return current;
}

function estimateAnalysisProgress(stage: string, status: NodeStatus, current: number): number {
  if (status === "done") return 100;
  if (status === "failed") return Math.max(current, 100);
  const normalizedStage = String(stage || "").trim().toLowerCase();
  if (normalizedStage in ANALYSIS_STAGE_PROGRESS) {
    return Math.max(current, ANALYSIS_STAGE_PROGRESS[normalizedStage]);
  }
  if (status === "running") return Math.max(current, 24);
  if (status === "queued") return Math.max(current, 8);
  return current;
}

function estimateReportProgress(stage: string, status: NodeStatus, current: number): number {
  if (status === "done") return 100;
  if (status === "failed") return Math.max(current, 100);
  const normalizedStage = String(stage || "").trim().toLowerCase();
  if (normalizedStage in REPORT_STAGE_PROGRESS) {
    return Math.max(current, REPORT_STAGE_PROGRESS[normalizedStage]);
  }
  if (status === "running") return Math.max(current, 22);
  if (status === "queued") return Math.max(current, 8);
  return current;
}

function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, Math.round(value)));
}

function parseEventData(event: MessageEvent<string>): Record<string, unknown> | null {
  try {
    return JSON.parse(event.data) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function formatLogLine(
  event: Record<string, unknown> | JobEventItem,
  kind: StreamKind,
  source: string
): string {
  const stage = String(event.stage || "progress").trim();
  const message = String(event.message || "").trim();
  const createdAt = String(event.created_at || "").trim();
  const timestamp = createdAt ? formatTime(createdAt) : formatTime(new Date().toISOString());
  const src = source || "-";
  return `${timestamp} [${kind}] [${src}] ${stage}${message ? ` • ${message}` : ""}`;
}

function formatTime(raw: string): string {
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return raw;
  }
  return date.toLocaleTimeString();
}

function formatDateTime(raw: string | null | undefined): string {
  const value = String(raw || "").trim();
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function computeDurationSeconds(startedAt: string | undefined, finishedAt: string | undefined): number | null {
  const start = Date.parse(String(startedAt || ""));
  if (!Number.isFinite(start)) return null;
  const endRaw = Date.parse(String(finishedAt || ""));
  const end = Number.isFinite(endRaw) ? endRaw : Date.now();
  if (end < start) return null;
  return Math.max(0, Math.round((end - start) / 1000));
}

function formatDuration(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) return "-";
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  const rem = seconds % 60;
  return `${mins}m ${rem}s`;
}

function toInteger(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.round(value);
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
