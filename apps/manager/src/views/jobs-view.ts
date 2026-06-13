import { AnimationController } from "../animations/controller";
import { createButton } from "../components/atoms/button";
import { ApiClient } from "../core/api-client";
import { extractLocalArtifactPath, normalizeArtifactOutputUrl } from "../core/artifact-url";
import { clearElement, createElement, formatError, parseOptionalFloat, parseOptionalInteger } from "../core/dom";
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
type NodeKey = "scrape_google_maps" | "scrape_tripadvisor" | "analysis" | "study_hydration" | "report";
type NodeStatus =
  | "idle"
  | "queued"
  | "running"
  | "done"
  | "failed"
  | "needs_human"
  | "waiting"
  | "skipped"
  | "reused"
  | "ready"
  | "not_in_study";
type ConnectorState = "idle" | "active" | "done" | "failed" | "waiting" | "human";
type ScrapeSource = "google_maps" | "tripadvisor";
type LiveDisplayMode = "native" | "xvfb";
const LOCAL_ARTIFACT_OPENER_URL = "http://127.0.0.1:8766/open";

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

type TripAdvisorLiveSessionState = {
  running?: boolean;
  pid?: number | null;
  state?: string;
  finished_reason?: string | null;
  mode?: string | null;
  reason?: string | null;
  job_id?: string | null;
  live_display_mode?: string | null;
  display?: string | null;
  log_file?: string | null;
  started_at_ts?: number | null;
  updated_at_ts?: number | null;
};

type TripAdvisorLiveSessionLogTail = {
  ok?: boolean;
  live_session?: TripAdvisorLiveSessionState | null;
  log_file?: string | null;
  log_tail?: string | null;
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

type StreamKind = "scrape" | "analysis" | "hydration" | "report";

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
const HYDRATION_STAGE_PROGRESS: Record<string, number> = {
  queued: 8,
  study_hydration_queued: 12,
  study_hydration_started: 26,
  waiting_benchmark: 42,
  waiting_geogrid: 64,
  study_hydration_completed: 92,
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
  const sessionLiveStatus = createElement("span", "jobs6-badge jobs6-badge--idle", "TA Live: idle");
  const sessionLiveMeta = createElement("span", "jobs6-kv", "Live: -");
  const sessionRefreshButton = createButton({ label: "Refrescar sesión TA", tone: "white" });
  sessionStrip.append(
    sessionStatus,
    sessionAvailability,
    sessionCookieExpiry,
    sessionLastHuman,
    sessionExtra,
    sessionLiveStatus,
    sessionLiveMeta,
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
  const analysisToHydrationConnector = createConnector("inline");
  const hydrationNode = createPipelineNodeCard("study_hydration", "STUDY HYDRATION", "hydration");
  const hydrationToReportConnector = createConnector("inline");
  const reportNode = createPipelineNodeCard("report", "REPORT PDF", "report");
  analysisCol.append(
    analysisNode.root,
    analysisToHydrationConnector.root,
    hydrationNode.root,
    hydrationToReportConnector.root,
    reportNode.root
  );

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
  const drawerTripadvisorLiveTitle = createElement(
    "h4",
    "jobs6-drawer-section-title hidden",
    "Sesión live TA"
  );
  const drawerTripadvisorLiveSummary = createElement(
    "div",
    "jobs6-drawer-block hidden"
  );
  const drawerTripadvisorLiveActions = createElement("div", "form-actions hidden");
  const drawerTripadvisorLiveRefreshButton = createButton({
    label: "Refrescar logs TA",
    tone: "white",
  });
  const drawerTripadvisorLiveStopButton = createButton({
    label: "Parar sesión live TA",
    tone: "white",
  });
  drawerTripadvisorLiveActions.append(
    drawerTripadvisorLiveRefreshButton,
    drawerTripadvisorLiveStopButton
  );
  const drawerTripadvisorLiveLog = createElement(
    "pre",
    "code-block jobs6-drawer-transitions hidden",
    ""
  );
  const drawerRelaunchConfigTitle = createElement(
    "h4",
    "jobs6-drawer-section-title hidden",
    "Relanzar con ajustes (Tripadvisor)"
  );
  const drawerRelaunchConfig = createElement("div", "jobs6-drawer-block hidden");
  const drawerTripadvisorNameInput = createElement("input", "atom-input") as HTMLInputElement;
  drawerTripadvisorNameInput.placeholder = "Nombre en Tripadvisor (opcional)";
  const drawerTripadvisorMaxPagesInput = createElement("input", "atom-input") as HTMLInputElement;
  drawerTripadvisorMaxPagesInput.type = "number";
  drawerTripadvisorMaxPagesInput.min = "1";
  drawerTripadvisorMaxPagesInput.placeholder = "Tripadvisor max pages (opcional)";
  const drawerTripadvisorPagesPercentInput = createElement("input", "atom-input") as HTMLInputElement;
  drawerTripadvisorPagesPercentInput.type = "number";
  drawerTripadvisorPagesPercentInput.min = "0.1";
  drawerTripadvisorPagesPercentInput.max = "100";
  drawerTripadvisorPagesPercentInput.step = "0.1";
  drawerTripadvisorPagesPercentInput.placeholder = "Tripadvisor pages percent (opcional)";
  const drawerRelaunchConfigHint = createElement(
    "div",
    "muted",
    "Si completas estos campos, se usarán en el payload del relanzado."
  );
  drawerRelaunchConfig.append(
    createElement("label", "form-label", "Nombre Tripadvisor"),
    drawerTripadvisorNameInput,
    createElement("label", "form-label", "Tripadvisor max pages"),
    drawerTripadvisorMaxPagesInput,
    createElement("label", "form-label", "Tripadvisor pages percent"),
    drawerTripadvisorPagesPercentInput,
    drawerRelaunchConfigHint
  );
  const drawerAnalysisLaunchConfigTitle = createElement(
    "h4",
    "jobs6-drawer-section-title hidden",
    "Lanzar analysis"
  );
  const drawerAnalysisLaunchConfig = createElement("div", "jobs6-drawer-block hidden");
  const drawerAnalysisReportProfileSelect = createElement("select", "atom-input") as HTMLSelectElement;
  drawerAnalysisReportProfileSelect.innerHTML =
    '<option value="client_audit" selected>Client audit</option><option value="classic">Classic</option>';
  const drawerAnalysisLaunchResearchInput = createElement(
    "input",
    "atom-input"
  ) as HTMLInputElement;
  drawerAnalysisLaunchResearchInput.type = "checkbox";
  drawerAnalysisLaunchResearchInput.checked = false;
  const drawerAnalysisStudyResolutionModeSelect = createElement(
    "select",
    "atom-input"
  ) as HTMLSelectElement;
  drawerAnalysisStudyResolutionModeSelect.innerHTML =
    '<option value="auto_ttl" selected>Auto TTL</option><option value="reuse_latest">Reuse latest</option><option value="refresh_now">Refresh now</option>';
  const drawerAnalysisIncludeCompetitorsInput = createElement(
    "input",
    "atom-input"
  ) as HTMLInputElement;
  drawerAnalysisIncludeCompetitorsInput.type = "checkbox";
  drawerAnalysisIncludeCompetitorsInput.checked = true;
  const drawerAnalysisIncludeGeogridInput = createElement(
    "input",
    "atom-input"
  ) as HTMLInputElement;
  drawerAnalysisIncludeGeogridInput.type = "checkbox";
  drawerAnalysisIncludeGeogridInput.checked = false;
  const drawerAnalysisLaunchHint = createElement("div", "muted", "");
  const drawerAnalysisReportProfileRow = appendDrawerField(
    "Perfil reporte",
    drawerAnalysisReportProfileSelect
  );
  const drawerAnalysisLaunchResearchRow = appendDrawerField(
    "Lanzar research",
    drawerAnalysisLaunchResearchInput
  );
  const drawerAnalysisStudyResolutionModeRow = appendDrawerField(
    "Resolución estudio",
    drawerAnalysisStudyResolutionModeSelect
  );
  const drawerAnalysisIncludeCompetitorsRow = appendDrawerField(
    "Incluir competidores",
    drawerAnalysisIncludeCompetitorsInput
  );
  const drawerAnalysisIncludeGeogridRow = appendDrawerField(
    "Incluir geogrid",
    drawerAnalysisIncludeGeogridInput
  );
  drawerAnalysisLaunchConfig.append(
    drawerAnalysisReportProfileRow,
    drawerAnalysisLaunchResearchRow,
    drawerAnalysisStudyResolutionModeRow,
    drawerAnalysisIncludeCompetitorsRow,
    drawerAnalysisIncludeGeogridRow,
    drawerAnalysisLaunchHint
  );
  const drawerHydrationDependenciesTitle = createElement(
    "h4",
    "jobs6-drawer-section-title hidden",
    "Dependencias hydration"
  );
  const drawerHydrationDependenciesBlock = createElement("div", "jobs6-drawer-block hidden");
  const drawerHydrationDependenciesSummary = createElement(
    "pre",
    "code-block jobs6-drawer-transitions",
    ""
  );
  const drawerHydrationDependenciesHint = createElement(
    "div",
    "muted",
    "Benchmark y geogrid se pueden relanzar desde aquí. El modo live usa el display seleccionado abajo."
  );
  const drawerHydrationDependencyActions = createElement("div", "form-actions");
  const drawerRelaunchBenchmarkAutoButton = createButton({
    label: "Benchmark auto",
    tone: "white",
  });
  const drawerRelaunchBenchmarkLiveButton = createButton({
    label: "Benchmark live",
    tone: "turquoise",
  });
  const drawerRelaunchGeogridAutoButton = createButton({
    label: "Geogrid auto",
    tone: "white",
  });
  const drawerRelaunchGeogridLiveButton = createButton({
    label: "Geogrid live",
    tone: "turquoise",
  });
  drawerHydrationDependencyActions.append(
    drawerRelaunchBenchmarkAutoButton,
    drawerRelaunchBenchmarkLiveButton,
    drawerRelaunchGeogridAutoButton,
    drawerRelaunchGeogridLiveButton
  );
  drawerHydrationDependenciesBlock.append(
    drawerHydrationDependenciesSummary,
    drawerHydrationDependencyActions,
    drawerHydrationDependenciesHint
  );
  const drawerNodeActionsTitle = createElement("h4", "jobs6-drawer-section-title", "Acciones");
  const drawerLiveModeTitle = createElement("h4", "jobs6-drawer-section-title", "Modo live");
  const drawerLiveModeBlock = createElement("div", "jobs6-drawer-block");
  const drawerLiveModeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  drawerLiveModeSelect.innerHTML =
    '<option value="native">Live nativo</option><option value="xvfb">Live bajo Xvfb</option>';
  const drawerLiveModeHint = createElement(
    "div",
    "muted",
    "Native usa tu display local. Xvfb abre Chromium headed dentro de un display virtual."
  );
  drawerLiveModeBlock.append(
    createElement("label", "form-label", "Display live"),
    drawerLiveModeSelect,
    drawerLiveModeHint
  );
  const drawerNodeActions = createElement("div", "form-actions");
  const drawerRelaunchButton = createButton({ label: "Relanzar", tone: "turquoise" });
  const drawerRelaunchFromZeroButton = createButton({ label: "Relanzar de 0", tone: "turquoise" });
  const drawerForceAnalyzeButton = createButton({
    label: "Forzar analyze sin rescrape",
    tone: "orange",
  });
  const drawerLaunchLiveButton = createButton({ label: "Lanzar Live TA", tone: "turquoise" });
  const drawerSkipTripadvisorButton = createButton({
    label: "Omitir TA y continuar",
    tone: "orange",
  });
  const drawerDeleteButton = createButton({ label: "Eliminar job", tone: "white" });
  const drawerManualButton = createButton({ label: "Marcar manual TA", tone: "white" });
  const drawerOutputButton = createButton({ label: "Abrir output", tone: "white" });
  const drawerCopyJobButton = createButton({ label: "Copiar job_id", tone: "white" });
  drawerNodeActions.append(
    drawerRelaunchButton,
    drawerRelaunchFromZeroButton,
    drawerForceAnalyzeButton,
    drawerLaunchLiveButton,
    drawerSkipTripadvisorButton,
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
    drawerTripadvisorLiveTitle,
    drawerTripadvisorLiveSummary,
    drawerTripadvisorLiveActions,
    drawerTripadvisorLiveLog,
    drawerRelaunchConfigTitle,
    drawerRelaunchConfig,
    drawerAnalysisLaunchConfigTitle,
    drawerAnalysisLaunchConfig,
    drawerHydrationDependenciesTitle,
    drawerHydrationDependenciesBlock,
    drawerLiveModeTitle,
    drawerLiveModeBlock,
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
    study_hydration: hydrationNode,
    report: reportNode,
  };

  let filterMode: JobFilterMode = "active";
  let sourceFilter: SourceFilter = "all";
  let jobs: AnalyzeJobItem[] = [];
  let selectedBusinessKey: string | null = null;
  let selectedBusinessGroup: BusinessScrapeGroup | null = null;
  let deletingScrapeJobId: string | null = null;

  let analysisJobId: string | null = null;
  let preparationJobId: string | null = null;
  let hydrationPreparationId: string | null = null;
  let hydrationPreparationSnapshot: Record<string, unknown> | null = null;
  let reportJobId: string | null = null;
  let scrapeSourceJobIds: Partial<Record<ScrapeSource, string>> = {};

  let scrapeStreams: Partial<Record<ScrapeSource, EventSource>> = {};
  let analysisStream: EventSource | null = null;
  let analysisStreamJobId: string | null = null;
  let preparationStream: EventSource | null = null;
  let preparationStreamJobId: string | null = null;
  let reportStream: EventSource | null = null;
  let reportStreamJobId: string | null = null;
  let loadedScrapeEvents: Record<ScrapeSource, number> = {
    google_maps: 0,
    tripadvisor: 0,
  };
  let loadedAnalysisEvents = 0;
  let loadedPreparationEvents = 0;
  let loadedReportEvents = 0;

  let jobsPollTimer: number | null = null;
  let sessionPollTimer: number | null = null;
  let tripadvisorLiveSessionPollTimer: number | null = null;
  let sessionState: TripAdvisorSessionState | null = null;
  let tripadvisorLiveSessionTail: TripAdvisorLiveSessionLogTail | null = null;
  let tripadvisorLiveSessionError: string | null = null;

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
  drawerAnalysisReportProfileSelect.addEventListener("change", () => {
    syncDrawerAnalysisLaunchConfig();
  });
  drawerAnalysisLaunchResearchInput.addEventListener("change", () => {
    syncDrawerAnalysisLaunchConfig();
  });
  drawerTripadvisorLiveRefreshButton.addEventListener("click", () => {
    void loadTripadvisorLiveSessionTail({ force: true });
  });
  drawerTripadvisorLiveStopButton.addEventListener("click", () => {
    void stopTripadvisorLiveSession();
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
    if (preparationJobId) {
      startPreparationStream(preparationJobId);
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
  drawerForceAnalyzeButton.addEventListener("click", () => {
    void forceAnalyzeWithoutRescrape();
  });
  drawerLaunchLiveButton.addEventListener("click", () => {
    void launchCurrentScrapeJobLive();
  });
  drawerRelaunchBenchmarkAutoButton.addEventListener("click", () => {
    void relaunchHydrationDependency("benchmark", "automatic");
  });
  drawerRelaunchBenchmarkLiveButton.addEventListener("click", () => {
    void relaunchHydrationDependency("benchmark", "live");
  });
  drawerRelaunchGeogridAutoButton.addEventListener("click", () => {
    void relaunchHydrationDependency("geogrid", "automatic");
  });
  drawerRelaunchGeogridLiveButton.addEventListener("click", () => {
    void relaunchHydrationDependency("geogrid", "live");
  });
  drawerSkipTripadvisorButton.addEventListener("click", () => {
    void skipCurrentTripadvisorNode();
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
    const localPath = extractLocalArtifactPath(node.outputUrl);
    if (localPath) {
      drawerActionStatus.textContent = "Abriendo archivo local...";
      void fetch(LOCAL_ARTIFACT_OPENER_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: localPath }),
      })
        .then(async (response) => {
          if (!response.ok) {
            throw new Error(await response.text());
          }
          return response.json();
        })
        .then(() => {
          drawerActionStatus.textContent = "Archivo abierto con la aplicación local.";
        })
        .catch((error: unknown) => {
          drawerActionStatus.textContent = formatError(error);
        });
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
        preparationJobId = null;
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
    hydrationPreparationId = null;
    hydrationPreparationSnapshot = null;
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
    preparationJobId = null;
    hydrationPreparationId = null;
    hydrationPreparationSnapshot = null;
    reportJobId = null;
    loadedScrapeEvents = { google_maps: 0, tripadvisor: 0 };
    loadedAnalysisEvents = 0;
    loadedPreparationEvents = 0;
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

      if (!analysisJobId) {
        const fallbackBusinessId = String(selectedBusinessGroup.rootBusinessId || "").trim();
        if (fallbackBusinessId) {
          try {
            const latestAnalysisJob = await deps.apiClient.get<AnalyzeJobItem>(
              `/business/${encodeURIComponent(fallbackBusinessId)}/analyze/latest-job`
            );
            const latestAnalysisJobId = String(latestAnalysisJob.job_id || "").trim();
            if (latestAnalysisJobId) {
              analysisJobId = latestAnalysisJobId;
            }
          } catch (error) {
            const fallbackMessage = formatError(error).toLowerCase();
            if (!fallbackMessage.includes("404") && !fallbackMessage.includes("not found")) {
              throw error;
            }
          }
        }
      }

      if (analysisJobId) {
        const analysisDetail = await loadAnalysisJobSnapshot(analysisJobId);
        const hydratedReport = analysisDetail ? isHydratedClientAuditJob(analysisDetail) : false;
        preparationJobId = analysisDetail ? resolvePreparationJobId(analysisDetail) : null;
        if (hydratedReport) {
          if (preparationJobId) {
            const preparationDetail = await loadPreparationJobSnapshot(preparationJobId);
            reportJobId = preparationDetail ? resolveFinalReportJobId(preparationDetail) : null;
          } else {
            nodes.study_hydration = createInitialNodes().study_hydration;
            nodes.study_hydration.status = "queued";
            nodes.study_hydration.stage = "study_hydration_pending";
            nodes.study_hydration.message = "La hidratación está pendiente de encolarse o sincronizarse.";
          }
        } else {
          hydrationPreparationId = null;
          hydrationPreparationSnapshot = null;
          nodes.study_hydration = createSkippedHydrationNode();
          reportJobId = analysisDetail ? resolveReportJobId(analysisDetail) : null;
        }
        if (reportJobId) {
          await loadReportJobSnapshot(reportJobId);
        } else {
          nodes.report = createInitialNodes().report;
          nodes.report.status = hydratedReport ? "waiting" : "waiting";
          nodes.report.stage = hydratedReport ? "waiting_study_hydration" : "report_not_enqueued";
          nodes.report.message = hydratedReport
            ? "Esperando a que termine study hydration para encolar el render final."
            : "No hay job de report asociado para este análisis.";
        }
      } else {
        nodes.analysis = createInitialNodes().analysis;
        nodes.analysis.status = "waiting";
        nodes.analysis.stage = "analysis_not_enqueued";
        nodes.analysis.message = "No hay job de análisis asociado para este negocio.";
        nodes.study_hydration = createInitialNodes().study_hydration;
        nodes.study_hydration.status = "waiting";
        nodes.study_hydration.stage = "waiting_analysis";
        nodes.study_hydration.message = "Esperando a que exista un job de análisis.";
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
      if (preparationJobId) {
        startPreparationStream(preparationJobId);
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

  async function loadAnalysisJobSnapshot(
    jobId: string,
    options?: { appendLogs?: boolean }
  ): Promise<AnalyzeJobItem | null> {
    const appendLogs = options?.appendLogs !== false;
    try {
      const detail = await deps.apiClient.get<AnalyzeJobItem>(
        `/business/analyze/jobs/${encodeURIComponent(jobId)}`
      );
      loadedAnalysisEvents = Array.isArray(detail.events) ? detail.events.length : 0;
      updateNodeFromJobSnapshot(nodes.analysis, detail, "analysis");
      nodes.analysis.events = Array.isArray(detail.events) ? detail.events : [];
      const nextReportJobId = resolveReportJobId(detail);
      if (nextReportJobId && nextReportJobId !== reportJobId) {
        reportJobId = nextReportJobId;
      }
      if (appendLogs) {
        for (const event of nodes.analysis.events) {
          appendLogLine(formatLogLine(event, "analysis", "analysis"));
        }
      }
      return detail;
    } catch (error) {
      nodes.analysis.status = "failed";
      nodes.analysis.error = formatError(error);
      nodes.analysis.message = "No se pudo cargar el job de análisis.";
      nodes.analysis.progress = 100;
      return null;
    }
  }

  async function loadPreparationJobSnapshot(
    jobId: string,
    options?: { appendLogs?: boolean }
  ): Promise<AnalyzeJobItem | null> {
    const appendLogs = options?.appendLogs !== false;
    try {
      const detail = await deps.apiClient.get<AnalyzeJobItem>(
        `/business/report/jobs/${encodeURIComponent(jobId)}`
      );
      const resolvedPreparationId = resolvePreparationDocumentId(detail);
      if (resolvedPreparationId) {
        hydrationPreparationId = resolvedPreparationId;
        await loadHydrationPreparationDocument(resolvedPreparationId);
        const latestPrepareJobId = resolveLatestPrepareJobIdFromPreparation(hydrationPreparationSnapshot);
        if (latestPrepareJobId && latestPrepareJobId !== jobId) {
          preparationJobId = latestPrepareJobId;
          return await loadPreparationJobSnapshot(latestPrepareJobId, options);
        }
      } else {
        hydrationPreparationId = null;
        hydrationPreparationSnapshot = null;
      }
      loadedPreparationEvents = Array.isArray(detail.events) ? detail.events.length : 0;
      updateHydrationNodeFromJobSnapshot(nodes.study_hydration, detail);
      nodes.study_hydration.events = Array.isArray(detail.events) ? detail.events : [];
      const nextReportJobId = resolveFinalReportJobId(detail);
      if (nextReportJobId && nextReportJobId !== reportJobId) {
        reportJobId = nextReportJobId;
      }
      if (appendLogs) {
        for (const event of nodes.study_hydration.events) {
          appendLogLine(formatLogLine(event, "hydration", "hydration"));
        }
      }
      return detail;
    } catch (error) {
      nodes.study_hydration.status = "failed";
      nodes.study_hydration.error = formatError(error);
      nodes.study_hydration.message = "No se pudo cargar el job de study hydration.";
      nodes.study_hydration.progress = 100;
      return null;
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

  async function loadHydrationPreparationDocument(
    preparationId: string
  ): Promise<Record<string, unknown> | null> {
    try {
      const preparation = await deps.apiClient.get<Record<string, unknown>>(
        `/business/report/preparations/${encodeURIComponent(preparationId)}`
      );
      hydrationPreparationSnapshot = preparation;
      applyHydrationPreparationToNode(preparation);
      return preparation;
    } catch (error) {
      hydrationPreparationSnapshot = null;
      appendLogLine(`[sync][hydration-preparation] ${formatError(error)}`);
      return null;
    }
  }

  function applyHydrationPreparationToNode(preparation: Record<string, unknown> | null): void {
    if (!preparation) return;
    const derivedStatus = deriveHydrationResultStatus(preparation);
    const hydrationStatus = String((preparation.hydration_status as string | undefined) || "").trim();
    const latestPrepareJobId = resolveLatestPrepareJobIdFromPreparation(preparation);
    if (latestPrepareJobId) {
      nodes.study_hydration.jobId = latestPrepareJobId;
    }
    if (derivedStatus) {
      nodes.study_hydration.status = derivedStatus;
    }
    if (hydrationStatus) {
      nodes.study_hydration.stage = hydrationStatus;
    }
    const described = describeHydrationState(preparation);
    if (described) {
      nodes.study_hydration.message = described;
    }
    nodes.study_hydration.progress = estimateHydrationProgress(
      nodes.study_hydration.stage,
      nodes.study_hydration.status,
      nodes.study_hydration.progress
    );
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

  function updateHydrationNodeFromJobSnapshot(node: PipelineNodeState, job: AnalyzeJobItem): void {
    const status = normalizeNodeStatus(job.status, job.progress?.stage);
    const stage = String(job.progress?.stage || "").trim();
    const message = String(job.progress?.message || "").trim();
    const result = isRecord(job.result) ? job.result : null;

    node.status = deriveHydrationSnapshotStatus(job, result) || status;
    node.stage = stage;
    node.message = message || describeHydrationState(result);
    node.jobId = String(job.job_id || "").trim() || null;
    node.lastUpdated = String(job.updated_at || "").trim() || null;
    node.attempts = toInteger((job as unknown as Record<string, unknown>).attempts);
    node.durationSeconds = computeDurationSeconds(job.started_at, job.finished_at);
    node.error = String(job.error || "").trim() || null;
    node.progress = estimateHydrationProgress(stage, node.status, node.progress);
    node.outputUrl = null;

    if (result) {
      const finalReportJobId = String((result.final_report_job_id as string | undefined) || "").trim();
      if (finalReportJobId) {
        reportJobId = finalReportJobId;
      }
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

  function startPreparationStream(jobId: string): void {
    if (preparationStreamJobId === jobId && preparationStream) {
      return;
    }
    stopPreparationStream();
    preparationStreamJobId = jobId;

    const stream = deps.apiClient.createEventSource(
      `/business/report/jobs/${encodeURIComponent(jobId)}/events?from_index=${loadedPreparationEvents}`
    );
    preparationStream = stream;

    stream.addEventListener("progress", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      const eventIndex = toInteger(payload.index);
      if (eventIndex !== null) {
        loadedPreparationEvents = Math.max(loadedPreparationEvents, eventIndex);
      } else {
        loadedPreparationEvents += 1;
      }
      applyPreparationEvent(payload);
    });

    stream.addEventListener("done", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      const resolvedStatus = deriveHydrationResultStatus(payload);
      nodes.study_hydration.status = resolvedStatus ?? "ready";
      nodes.study_hydration.progress = 100;
      appendLogLine(`[done][study_hydration] ${String(payload?.status || "done")}`);
      renderPipeline();
      stopPreparationStream();
      if (preparationJobId) {
        void syncPreparationSnapshot(preparationJobId);
      }
    });

    stream.addEventListener("heartbeat", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (!payload) return;
      if (typeof payload.status === "string") {
        nodes.study_hydration.status = normalizeNodeStatus(payload.status, nodes.study_hydration.stage);
      }
      renderPipeline();
    });

    stream.addEventListener("error", (event) => {
      const payload = parseEventData(event as MessageEvent<string>);
      if (payload?.error) {
        appendLogLine(`[error][study_hydration] ${String(payload.error)}`);
      }
    });

    stream.onerror = () => {
      appendLogLine("[stream][study_hydration] desconectado");
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
        const analysisDetail = await loadAnalysisJobSnapshot(nextAnalysisId, { appendLogs: false });
        startAnalysisStream(nextAnalysisId);
        const hydratedReport = analysisDetail ? isHydratedClientAuditJob(analysisDetail) : false;
        if (hydratedReport) {
          preparationJobId = analysisDetail ? resolvePreparationJobId(analysisDetail) : null;
          if (preparationJobId) {
            const preparationDetail = await loadPreparationJobSnapshot(preparationJobId, { appendLogs: false });
            startPreparationStream(preparationJobId);
            const nextReportJobId = preparationDetail ? resolveFinalReportJobId(preparationDetail) : null;
            if (nextReportJobId) {
              reportJobId = nextReportJobId;
              await loadReportJobSnapshot(nextReportJobId, { appendLogs: false });
              startReportStream(nextReportJobId);
            } else {
              reportJobId = null;
              nodes.report = createInitialNodes().report;
              nodes.report.status = "waiting";
              nodes.report.stage = "waiting_study_hydration";
              nodes.report.message = "Esperando a que study hydration encole el reporte final.";
            }
          } else {
            nodes.study_hydration = createInitialNodes().study_hydration;
            nodes.study_hydration.status = "queued";
            nodes.study_hydration.stage = "study_hydration_pending";
            nodes.study_hydration.message = "Esperando a que se materialice el job de study hydration.";
            reportJobId = null;
            nodes.report = createInitialNodes().report;
            nodes.report.status = "waiting";
            nodes.report.stage = "waiting_study_hydration";
            nodes.report.message = "Esperando a que study hydration encole el reporte final.";
          }
        } else {
          hydrationPreparationId = null;
          hydrationPreparationSnapshot = null;
          nodes.study_hydration = createSkippedHydrationNode();
          const nextReportJobId = analysisDetail ? resolveReportJobId(analysisDetail) : null;
          if (nextReportJobId) {
            reportJobId = nextReportJobId;
            await loadReportJobSnapshot(nextReportJobId, { appendLogs: false });
            startReportStream(nextReportJobId);
          } else {
            reportJobId = null;
            nodes.report = createInitialNodes().report;
            nodes.report.status = "waiting";
            nodes.report.stage = "report_not_enqueued";
            nodes.report.message = "No hay job de report asociado para este análisis.";
          }
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
      const analysisDetail = await loadAnalysisJobSnapshot(jobId, { appendLogs: false });
      if (!analysisDetail) {
        renderPipeline();
        renderDrawer();
        return;
      }
      if (isHydratedClientAuditJob(analysisDetail)) {
        preparationJobId = resolvePreparationJobId(analysisDetail);
        if (preparationJobId) {
          const preparationDetail = await loadPreparationJobSnapshot(preparationJobId, { appendLogs: false });
          startPreparationStream(preparationJobId);
          const nextReportJobId = preparationDetail ? resolveFinalReportJobId(preparationDetail) : null;
          if (nextReportJobId && nextReportJobId !== reportJobId) {
            reportJobId = nextReportJobId;
          } else if (!nextReportJobId) {
            reportJobId = null;
            nodes.report = createInitialNodes().report;
            nodes.report.status = "waiting";
            nodes.report.stage = "waiting_study_hydration";
            nodes.report.message = "Esperando a que study hydration encole el reporte final.";
          }
        } else {
          nodes.study_hydration = createInitialNodes().study_hydration;
          nodes.study_hydration.status = "queued";
          nodes.study_hydration.stage = "study_hydration_pending";
          nodes.study_hydration.message = "Esperando a que se materialice el job de study hydration.";
          reportJobId = null;
          nodes.report = createInitialNodes().report;
          nodes.report.status = "waiting";
          nodes.report.stage = "waiting_study_hydration";
          nodes.report.message = "Esperando a que study hydration encole el reporte final.";
        }
      } else {
        hydrationPreparationId = null;
        hydrationPreparationSnapshot = null;
        nodes.study_hydration = createSkippedHydrationNode();
        const nextReportJobId = resolveReportJobId(analysisDetail);
        if (nextReportJobId && nextReportJobId !== reportJobId) {
          reportJobId = nextReportJobId;
        } else if (!nextReportJobId) {
          reportJobId = null;
          nodes.report = createInitialNodes().report;
          nodes.report.status = "waiting";
          nodes.report.stage = "report_not_enqueued";
          nodes.report.message = "No hay job de report asociado para este análisis.";
        }
      }
      if (reportJobId) {
        await loadReportJobSnapshot(reportJobId, { appendLogs: false });
        startReportStream(reportJobId);
      }
      renderPipeline();
      renderDrawer();
    } catch (error) {
      appendLogLine(`[sync][analysis] ${formatError(error)}`);
    }
  }

  async function syncPreparationSnapshot(jobId: string): Promise<void> {
    try {
      const preparationDetail = await loadPreparationJobSnapshot(jobId, { appendLogs: false });
      const nextReportJobId = preparationDetail ? resolveFinalReportJobId(preparationDetail) : null;
      if (nextReportJobId && nextReportJobId !== reportJobId) {
        reportJobId = nextReportJobId;
      } else if (!nextReportJobId) {
        reportJobId = null;
        nodes.report = createInitialNodes().report;
        nodes.report.status = "waiting";
        nodes.report.stage = "waiting_study_hydration";
        nodes.report.message = "Esperando a que study hydration encole el reporte final.";
      }
      if (reportJobId) {
        await loadReportJobSnapshot(reportJobId, { appendLogs: false });
        startReportStream(reportJobId);
      }
      renderPipeline();
      renderDrawer();
    } catch (error) {
      appendLogLine(`[sync][study_hydration] ${formatError(error)}`);
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

    const preparationIdFromEvent = String(eventData.preparation_job_id || "").trim();
    if (preparationIdFromEvent && preparationIdFromEvent !== preparationJobId) {
      preparationJobId = preparationIdFromEvent;
      nodes.study_hydration.jobId = preparationIdFromEvent;
      nodes.study_hydration.status = "queued";
      nodes.study_hydration.stage = "study_hydration_queued";
      nodes.study_hydration.message = `Study hydration encolado: ${preparationIdFromEvent}`;
      nodes.study_hydration.progress = Math.max(nodes.study_hydration.progress, 12);
      startPreparationStream(preparationIdFromEvent);
      void syncPreparationSnapshot(preparationIdFromEvent);
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

  function applyPreparationEvent(payload: Record<string, unknown>): void {
    const stage = String(payload.stage || "").trim();
    const message = String(payload.message || "").trim();
    const status = String(payload.status || "").trim();
    const eventData = isRecord(payload.data) ? payload.data : {};

    if (status) {
      nodes.study_hydration.status = normalizeNodeStatus(status, stage);
    }
    if (stage) {
      nodes.study_hydration.stage = stage;
    }
    if (message) {
      nodes.study_hydration.message = message;
    } else {
      const hydrationMessage = describeHydrationState(eventData);
      if (hydrationMessage) {
        nodes.study_hydration.message = hydrationMessage;
      }
    }
    nodes.study_hydration.progress = estimateHydrationProgress(
      stage,
      nodes.study_hydration.status,
      nodes.study_hydration.progress
    );

    const derivedStatus = deriveHydrationResultStatus(eventData);
    if (derivedStatus) {
      nodes.study_hydration.status = derivedStatus;
    }
    const finalReportJobId = String(eventData.final_report_job_id || "").trim();
    if (finalReportJobId && finalReportJobId !== reportJobId) {
      reportJobId = finalReportJobId;
      nodes.report.jobId = finalReportJobId;
      nodes.report.status = "queued";
      nodes.report.stage = "report_handoff_queued";
      nodes.report.message = `Report job encolado: ${finalReportJobId}`;
      nodes.report.progress = Math.max(nodes.report.progress, 8);
      startReportStream(finalReportJobId);
      void syncReportSnapshot(finalReportJobId);
    }

    nodes.study_hydration.events.push({
      status,
      stage,
      message,
      data: eventData,
      created_at: String(payload.created_at || ""),
    });

    appendLogLine(formatLogLine(payload, "hydration", "hydration"));
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
      ? `Negocio ${selectedBusinessGroup.businessName} • Google: ${nodes.scrape_google_maps.status.toUpperCase()} • Tripadvisor: ${nodes.scrape_tripadvisor.status.toUpperCase()} • Analysis: ${nodes.analysis.status.toUpperCase()} • Study: ${nodes.study_hydration.status.toUpperCase()} • Report: ${nodes.report.status.toUpperCase()}`
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
      analysisToHydrationConnector,
      resolveAnalysisToHydrationConnectorState(nodes.analysis, nodes.study_hydration, nodes.report)
    );
    paintConnector(
      hydrationToReportConnector,
      resolveHydrationToReportConnectorState(nodes.study_hydration, nodes.report)
    );
  }

  function syncDrawerAnalysisLaunchConfig(): void {
    const isClassic = drawerAnalysisReportProfileSelect.value === "classic";
    const launchResearch = !isClassic && drawerAnalysisLaunchResearchInput.checked;
    drawerAnalysisLaunchResearchInput.disabled = isClassic;
    drawerAnalysisStudyResolutionModeSelect.disabled = !launchResearch;
    drawerAnalysisIncludeCompetitorsInput.disabled = !launchResearch;
    drawerAnalysisIncludeGeogridInput.disabled = !launchResearch;
    drawerAnalysisStudyResolutionModeRow.classList.toggle("hidden", !launchResearch);
    drawerAnalysisIncludeCompetitorsRow.classList.toggle("hidden", !launchResearch);
    drawerAnalysisIncludeGeogridRow.classList.toggle("hidden", !launchResearch);

    if (isClassic) {
      drawerAnalysisLaunchResearchInput.checked = false;
      drawerAnalysisStudyResolutionModeSelect.value = "auto_ttl";
      drawerAnalysisIncludeCompetitorsInput.checked = false;
      drawerAnalysisIncludeGeogridInput.checked = false;
      drawerAnalysisLaunchHint.textContent =
        "Classic genera el reporte actual y no lanza study hydration.";
      return;
    }

    if (!launchResearch) {
      drawerAnalysisStudyResolutionModeSelect.value = "auto_ttl";
      drawerAnalysisIncludeCompetitorsInput.checked = false;
      drawerAnalysisIncludeGeogridInput.checked = false;
      drawerAnalysisLaunchHint.textContent =
        "Sin research se lanza el client audit base usando solo las reseñas y análisis guardados.";
      return;
    }

    if (!drawerAnalysisIncludeCompetitorsInput.checked) {
      drawerAnalysisIncludeCompetitorsInput.checked = true;
    }
    drawerAnalysisLaunchHint.textContent =
      "Con research se activa study hydration: benchmark reutilizable o refresh, competidores y geogrid opcional.";
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
      drawerTripadvisorLiveTitle.classList.add("hidden");
      drawerTripadvisorLiveSummary.classList.add("hidden");
      drawerTripadvisorLiveActions.classList.add("hidden");
      drawerTripadvisorLiveLog.classList.add("hidden");
      drawerTripadvisorLiveSummary.textContent = "";
      drawerTripadvisorLiveLog.textContent = "";
      drawerRelaunchConfigTitle.classList.add("hidden");
      drawerRelaunchConfig.classList.add("hidden");
      drawerAnalysisLaunchConfigTitle.classList.add("hidden");
      drawerAnalysisLaunchConfig.classList.add("hidden");
      drawerActionStatus.textContent = "";
      drawerManualButton.classList.add("hidden");
      drawerForceAnalyzeButton.classList.add("hidden");
      drawerLaunchLiveButton.classList.add("hidden");
      drawerSkipTripadvisorButton.classList.add("hidden");
      drawerRelaunchButton.removeAttribute("disabled");
      drawerRelaunchFromZeroButton.removeAttribute("disabled");
      drawerForceAnalyzeButton.removeAttribute("disabled");
      drawerRelaunchFromZeroButton.classList.remove("hidden");
      drawerLaunchLiveButton.removeAttribute("disabled");
      drawerSkipTripadvisorButton.removeAttribute("disabled");
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
      node.key === "analysis"
        ? "analysis"
        : node.key === "report"
          ? "report"
          : node.key === "study_hydration"
            ? "hydration"
            : "scrape";
    const transitions = node.events
      .slice(-30)
      .map((event) => formatLogLine(event, eventKind, node.sourceBadge));
    drawerTransitions.textContent = transitions.join("\n");

    drawerManualButton.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerForceAnalyzeButton.classList.toggle(
      "hidden",
      node.key !== "analysis" || Boolean(node.jobId)
    );
    const showHydrationDependencyControls = node.key === "study_hydration";
    drawerLaunchLiveButton.classList.toggle(
      "hidden",
      node.key === "analysis" || node.key === "report" || node.key === "study_hydration"
    );
    drawerSkipTripadvisorButton.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerLaunchLiveButton.textContent =
      node.key === "scrape_tripadvisor" ? "Abrir Needs Human TA" : "Lanzar Live";
    drawerLiveModeTitle.classList.toggle(
      "hidden",
      node.key === "analysis" || node.key === "report"
    );
    drawerLiveModeBlock.classList.toggle(
      "hidden",
      node.key === "analysis" || node.key === "report"
    );
    drawerRelaunchConfigTitle.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerRelaunchConfig.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerTripadvisorLiveTitle.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerTripadvisorLiveSummary.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerTripadvisorLiveActions.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerTripadvisorLiveLog.classList.toggle("hidden", node.key !== "scrape_tripadvisor");
    drawerHydrationDependenciesTitle.classList.toggle("hidden", !showHydrationDependencyControls);
    drawerHydrationDependenciesBlock.classList.toggle("hidden", !showHydrationDependencyControls);
    drawerRelaunchFromZeroButton.classList.toggle(
      "hidden",
      node.key === "analysis" || node.key === "report" || node.key === "study_hydration"
    );
    const showAnalysisLaunchConfig = node.key === "analysis" && !node.jobId;
    drawerAnalysisLaunchConfigTitle.classList.toggle("hidden", !showAnalysisLaunchConfig);
    drawerAnalysisLaunchConfig.classList.toggle("hidden", !showAnalysisLaunchConfig);
    drawerRelaunchButton.toggleAttribute("disabled", !node.jobId);
    drawerForceAnalyzeButton.toggleAttribute(
      "disabled",
      node.key !== "analysis" || Boolean(node.jobId) || !selectedBusinessGroup?.rootBusinessId
    );
    drawerRelaunchFromZeroButton.toggleAttribute(
      "disabled",
      !node.jobId || node.key === "analysis" || node.key === "report" || node.key === "study_hydration"
    );
    drawerLaunchLiveButton.toggleAttribute(
      "disabled",
      (node.key === "analysis" || node.key === "report" || node.key === "study_hydration") || !node.jobId
    );
    drawerSkipTripadvisorButton.toggleAttribute("disabled", node.key !== "scrape_tripadvisor" || !node.jobId);
    drawerDeleteButton.toggleAttribute("disabled", !node.jobId);
    drawerOutputButton.toggleAttribute("disabled", !node.outputUrl);
    drawerCopyJobButton.toggleAttribute("disabled", !node.jobId);
    renderHydrationDependencyDetails();

    if (node.key === "scrape_tripadvisor" && node.jobId) {
      const job = findJobById(node.jobId);
      const payload = isRecord(job?.payload) ? job.payload : null;
      const liveDisplayMode = String(
        (job?.live_display_mode as string | undefined) ||
          (payload?.live_display_mode as string | undefined) ||
          "native"
      ).trim();
      drawerLiveModeSelect.value = liveDisplayMode === "xvfb" ? "xvfb" : "native";
      if (!drawerTripadvisorNameInput.value.trim()) {
        const sourceName = String(
          (payload?.source_name as string | undefined) ||
            (payload?.name as string | undefined) ||
            ""
        ).trim();
        if (sourceName) drawerTripadvisorNameInput.value = sourceName;
      }
      if (!drawerTripadvisorMaxPagesInput.value.trim()) {
        const maxPages = String(
          (payload?.tripadvisor_max_pages as number | string | undefined) || ""
        ).trim();
        if (maxPages) drawerTripadvisorMaxPagesInput.value = maxPages;
      }
      if (!drawerTripadvisorPagesPercentInput.value.trim()) {
        const pagesPercent = String(
          (payload?.tripadvisor_pages_percent as number | string | undefined) || ""
        ).trim();
        if (pagesPercent) drawerTripadvisorPagesPercentInput.value = pagesPercent;
      }
    } else if (node.key === "scrape_google_maps" && node.jobId) {
      const job = findJobById(node.jobId);
      const payload = isRecord(job?.payload) ? job.payload : null;
      const liveDisplayMode = String(
        (job?.live_display_mode as string | undefined) ||
          (payload?.live_display_mode as string | undefined) ||
          "native"
      ).trim();
      drawerLiveModeSelect.value = liveDisplayMode === "xvfb" ? "xvfb" : "native";
    } else {
      drawerLiveModeSelect.value = "native";
    }

    if (showAnalysisLaunchConfig) {
      syncDrawerAnalysisLaunchConfig();
    }

    if (node.key === "scrape_tripadvisor") {
      renderTripadvisorLiveSessionDetails();
      void loadTripadvisorLiveSessionTail();
    }
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
      const relaunchOverrides = buildRelaunchOverridesForNode(node);
      const basePath =
        node.key === "analysis"
          ? "/business/analyze/jobs"
          : node.key === "report" || node.key === "study_hydration"
            ? "/business/report/jobs"
            : "/business/scrape/jobs";
      const endpoint = `${basePath}/${encodeURIComponent(node.jobId)}/relaunch`;
      let forced = false;
      let relaunchedJobId = node.jobId;
      try {
        const firstPayload = restartFromZero
          ? { force: true, restart_from_zero: true, ...relaunchOverrides }
          : { ...relaunchOverrides };
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
          response = await deps.apiClient.post<{ job_id?: string }>(endpoint, {
            force: true,
            ...relaunchOverrides,
          });
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
      } else if (node.key === "study_hydration") {
        preparationJobId = relaunchedJobId;
        await syncPreparationSnapshot(relaunchedJobId);
        startPreparationStream(relaunchedJobId);
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
      if (node.key === "analysis" || node.key === "study_hydration" || node.key === "report") {
        void loadJobsList();
      }
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function forceAnalyzeWithoutRescrape(): Promise<void> {
    const node = getDrawerNode();
    if (!node || node.key !== "analysis") {
      drawerActionStatus.textContent = "Esta acción solo aplica al nodo de analysis.";
      return;
    }
    const businessId = String(selectedBusinessGroup?.rootBusinessId || "").trim();
    if (!businessId) {
      drawerActionStatus.textContent =
        "No se ha podido resolver el business_id para lanzar analysis sin rescrape.";
      return;
    }

    drawerActionStatus.textContent = "Encolando analysis desde reseñas guardadas...";
    try {
      const sourceJobId = resolvePreferredSourceJobIdForBusiness(selectedBusinessGroup);
      const reportProfile =
        drawerAnalysisReportProfileSelect.value === "classic" ? "classic" : "client_audit";
      const launchResearch =
        reportProfile === "client_audit" && drawerAnalysisLaunchResearchInput.checked;
      const reportComplexity = launchResearch ? "hydrated" : "basic";
      const includeCompetitors = launchResearch && drawerAnalysisIncludeCompetitorsInput.checked;
      const includeGeogrid = launchResearch && drawerAnalysisIncludeGeogridInput.checked;
      const response = await deps.apiClient.post<{ job_id?: string }>(
        "/business/analyze/jobs",
        {
          business_id: businessId,
          source_job_id: sourceJobId || undefined,
          report_profile: reportProfile,
          report_complexity: reportComplexity,
          report_cadence: "one_off",
          study_resolution_mode: launchResearch
            ? drawerAnalysisStudyResolutionModeSelect.value
            : "auto_ttl",
          include_competitors: includeCompetitors,
          include_geogrid: includeGeogrid,
        }
      );
      const forcedAnalysisJobId = String(response?.job_id || "").trim();
      if (!forcedAnalysisJobId) {
        throw new Error("La API no devolvió job_id para analysis.");
      }
      analysisJobId = forcedAnalysisJobId;
      drawerActionStatus.textContent = `Analysis encolado: ${forcedAnalysisJobId}`;
      await syncAnalysisSnapshot(forcedAnalysisJobId);
      startAnalysisStream(forcedAnalysisJobId);
      renderPipeline();
      renderDrawer();
      void loadJobsList();
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  function buildRelaunchOverridesForNode(node: PipelineNodeState): Record<string, unknown> {
    if (node.key !== "scrape_tripadvisor") {
      return {};
    }
    const payload: Record<string, unknown> = {};
    const tripadvisorName = drawerTripadvisorNameInput.value.trim();
    if (tripadvisorName) {
      payload.tripadvisor_name = tripadvisorName;
    }
    const tripadvisorMaxPages = parseOptionalInteger(drawerTripadvisorMaxPagesInput.value);
    const tripadvisorPagesPercent = parseOptionalFloat(drawerTripadvisorPagesPercentInput.value);
    if (
      tripadvisorPagesPercent !== null &&
      (tripadvisorPagesPercent <= 0 || tripadvisorPagesPercent > 100)
    ) {
      throw new Error("Tripadvisor pages percent debe estar entre 0 y 100.");
    }
    const scraperParams: Record<string, unknown> = {};
    if (tripadvisorMaxPages !== null) {
      scraperParams.scraper_tripadvisor_max_pages = tripadvisorMaxPages;
    }
    if (tripadvisorPagesPercent !== null) {
      scraperParams.scraper_tripadvisor_pages_percent = tripadvisorPagesPercent;
    }
    if (Object.keys(scraperParams).length > 0) {
      payload.scraper_params = scraperParams;
    }
    return payload;
  }

  function findJobById(jobId: string): AnalyzeJobItem | null {
    const normalizedTarget = String(jobId || "").trim();
    if (!normalizedTarget) return null;
    const match = jobs.find((item) => String(item.job_id || "").trim() === normalizedTarget);
    return match || null;
  }

  function getSelectedLiveDisplayMode(): LiveDisplayMode {
    return drawerLiveModeSelect.value === "xvfb" ? "xvfb" : "native";
  }

  function renderHydrationDependencyDetails(): void {
    const snapshot = hydrationPreparationSnapshot;
    const isHydrationDrawer = drawerNodeKey === "study_hydration";
    const benchmark = isRecord(snapshot?.dependencies) && isRecord(snapshot.dependencies.benchmark)
      ? snapshot.dependencies.benchmark
      : null;
    const geogrid = isRecord(snapshot?.dependencies) && isRecord(snapshot.dependencies.geogrid)
      ? snapshot.dependencies.geogrid
      : null;
    const includeGeogrid = Boolean(snapshot?.include_geogrid);

    drawerHydrationDependenciesSummary.textContent = isHydrationDrawer
      ? [
          `preparation_id: ${String((snapshot?.report_preparation_id as string | undefined) || hydrationPreparationId || "-").trim() || "-"}`,
          `hydration_status: ${String((snapshot?.hydration_status as string | undefined) || "-").trim() || "-"}`,
          `presence_state: ${String((snapshot?.business_presence_state as string | undefined) || "-").trim() || "-"}`,
          `latest_prepare_job_id: ${resolveLatestPrepareJobIdFromPreparation(snapshot) || "-"}`,
          `benchmark: ${benchmark ? `${String((benchmark.status as string | undefined) || "-").trim() || "-"} • job ${String((benchmark.job_id as string | undefined) || "-").trim() || "-"} • run ${String((benchmark.benchmark_run_id as string | undefined) || "-").trim() || "-"}` : "-"}`,
          `geogrid: ${geogrid ? `${String((geogrid.status as string | undefined) || "-").trim() || "-"} • job ${String((geogrid.job_id as string | undefined) || "-").trim() || "-"} • run ${String((geogrid.geo_grid_run_id as string | undefined) || "-").trim() || "-"}` : "skipped"}`,
        ].join("\n")
      : "";

    drawerRelaunchBenchmarkAutoButton.classList.toggle("hidden", !isHydrationDrawer);
    drawerRelaunchBenchmarkLiveButton.classList.toggle("hidden", !isHydrationDrawer);
    drawerRelaunchGeogridAutoButton.classList.toggle("hidden", !isHydrationDrawer || !includeGeogrid);
    drawerRelaunchGeogridLiveButton.classList.toggle("hidden", !isHydrationDrawer || !includeGeogrid);

    drawerRelaunchBenchmarkAutoButton.toggleAttribute("disabled", !isHydrationDrawer || !hydrationPreparationId);
    drawerRelaunchBenchmarkLiveButton.toggleAttribute("disabled", !isHydrationDrawer || !hydrationPreparationId);
    drawerRelaunchGeogridAutoButton.toggleAttribute(
      "disabled",
      !isHydrationDrawer || !hydrationPreparationId || !includeGeogrid
    );
    drawerRelaunchGeogridLiveButton.toggleAttribute(
      "disabled",
      !isHydrationDrawer || !hydrationPreparationId || !includeGeogrid
    );
  }

  async function relaunchHydrationDependency(
    dependencyName: "benchmark" | "geogrid",
    executionMode: "automatic" | "live"
  ): Promise<void> {
    if (!hydrationPreparationId) {
      drawerActionStatus.textContent = "No hay preparation_id disponible para relanzar hydration.";
      return;
    }
    const liveDisplayMode = executionMode === "live" ? getSelectedLiveDisplayMode() : "native";
    drawerActionStatus.textContent =
      executionMode === "live"
        ? `Relanzando ${dependencyName} en live (${liveDisplayMode})...`
        : `Relanzando ${dependencyName} en automático...`;
    try {
      const response = await deps.apiClient.post<Record<string, unknown>>(
        `/business/report/preparations/${encodeURIComponent(
          hydrationPreparationId
        )}/dependencies/${encodeURIComponent(dependencyName)}/relaunch`,
        {
          execution_mode: executionMode,
          live_display_mode: liveDisplayMode,
        }
      );
      hydrationPreparationSnapshot = response;
      applyHydrationPreparationToNode(response);
      renderPipeline();
      renderDrawer();
      drawerActionStatus.textContent =
        executionMode === "live"
          ? `${dependencyName} relanzado en live (${liveDisplayMode}).`
          : `${dependencyName} relanzado en automático.`;
      void loadJobsList();
      if (selectedBusinessKey) {
        window.setTimeout(() => {
          if (selectedBusinessKey) {
            void loadSelectedBusiness(selectedBusinessKey);
          }
        }, 2000);
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

  async function launchCurrentScrapeJobLive(): Promise<void> {
    const node = getDrawerNode();
    if (!node || (node.key !== "scrape_tripadvisor" && node.key !== "scrape_google_maps")) {
      drawerActionStatus.textContent = "Lanzar Live solo aplica a nodos de scrape.";
      return;
    }
    const sourceLabel = node.key === "scrape_tripadvisor" ? "TripAdvisor" : "Google Maps";
    const replayJobId = String(node.jobId || "").trim();
    if (!replayJobId) {
      drawerActionStatus.textContent = `No hay job_id de ${sourceLabel} para lanzar Live.`;
      return;
    }
    const liveDisplayMode = getSelectedLiveDisplayMode();
    if (node.key === "scrape_tripadvisor") {
      drawerActionStatus.textContent = "Abriendo sesión needs_human de TripAdvisor...";
      try {
        const response = await deps.apiClient.post<{
          ok?: boolean;
          already_running?: boolean;
          live_session?: { state?: string; pid?: number | null } | null;
        }>("/tripadvisor/live-session/launch", {
          reason: `ui_needs_human_tripadvisor:${replayJobId}:${node.status || "unknown"}`,
          job_id: replayJobId,
          live_display_mode: liveDisplayMode,
        });
        const alreadyRunning = Boolean(response?.already_running);
        const liveState = String(response?.live_session?.state || "").trim() || "unknown";
        const livePid = response?.live_session?.pid ?? null;
        drawerActionStatus.textContent = alreadyRunning
          ? `La sesión live de TripAdvisor ya estaba abierta${livePid ? ` (pid ${livePid})` : ""}.`
          : `Sesión needs_human de TripAdvisor abierta${livePid ? ` (pid ${livePid})` : ""}. Estado: ${liveState}.`;
        await loadTripadvisorSessionState();
        await loadTripadvisorLiveSessionTail({ force: true });
        return;
      } catch (error) {
        drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
        await loadTripadvisorLiveSessionTail({ force: true });
        return;
      }
    }
    drawerActionStatus.textContent = `Relanzando ${sourceLabel} en modo live (${liveDisplayMode})...`;
    try {
      const relaunchOverrides = buildRelaunchOverridesForNode(node);
      const response = await deps.apiClient.post<{ job_id?: string | null }>(
        `/business/scrape/jobs/${encodeURIComponent(replayJobId)}/relaunch`,
        {
          reason: `ui_live_relaunch:${replayJobId}:${node.status || "unknown"}`,
          execution_mode: "live",
          live_display_mode: liveDisplayMode,
          ...relaunchOverrides,
        }
      );
      const relaunchedJobId = String(response?.job_id || replayJobId).trim() || replayJobId;
      drawerActionStatus.textContent = `${sourceLabel} relanzado en modo live (${liveDisplayMode}): ${relaunchedJobId}`;
      await loadJobsList();
      const group = getRenderableBusinessGroups().find(
        (item) =>
          String(item.jobsBySource.google_maps?.job_id || "").trim() === relaunchedJobId ||
          String(item.jobsBySource.tripadvisor?.job_id || "").trim() === relaunchedJobId ||
          String(item.jobsBySource.google_maps?.job_id || "").trim() === replayJobId ||
          String(item.jobsBySource.tripadvisor?.job_id || "").trim() === replayJobId
      );
      if (group) {
        await loadSelectedBusiness(group.key);
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
          : node.key === "report" || node.key === "study_hydration"
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
      if (preparationJobId === node.jobId && node.key === "study_hydration") {
        preparationJobId = null;
        hydrationPreparationId = null;
        hydrationPreparationSnapshot = null;
        nodes.study_hydration = createInitialNodes().study_hydration;
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

  function shouldPollTripadvisorLiveSession(): boolean {
    if (drawerOpen && drawerNodeKey === "scrape_tripadvisor") {
      return true;
    }
    return Boolean(tripadvisorLiveSessionTail?.live_session?.running);
  }

  async function loadTripadvisorLiveSessionTail(options?: { force?: boolean }): Promise<void> {
    if (!options?.force && !shouldPollTripadvisorLiveSession()) {
      return;
    }
    try {
      const payload = await deps.apiClient.get<TripAdvisorLiveSessionLogTail>(
        "/tripadvisor/live-session/log-tail?max_chars=12000"
      );
      tripadvisorLiveSessionTail = payload;
      tripadvisorLiveSessionError = null;
      renderTripadvisorLiveSessionDetails();
    } catch (error) {
      tripadvisorLiveSessionError = formatError(error);
      renderTripadvisorLiveSessionDetails();
    }
  }

  function renderTripadvisorLiveSessionDetails(): void {
    const liveSession = tripadvisorLiveSessionTail?.live_session;
    if (tripadvisorLiveSessionError) {
      sessionLiveStatus.className = "jobs6-badge jobs6-badge--failed";
      sessionLiveStatus.textContent = "TA Live: error";
      sessionLiveMeta.textContent = `Live: ${tripadvisorLiveSessionError}`;
      if (drawerNodeKey === "scrape_tripadvisor") {
        drawerTripadvisorLiveSummary.textContent = "No se pudo leer el estado live de TripAdvisor.";
        drawerTripadvisorLiveLog.textContent = `ERROR: ${tripadvisorLiveSessionError}`;
        drawerTripadvisorLiveStopButton.setAttribute("disabled", "disabled");
      }
      return;
    }
    const liveState = String(liveSession?.state || "").trim().toLowerCase();
    const isRunning = Boolean(liveSession?.running);
    const liveDisplayMode = String(liveSession?.live_display_mode || "").trim() || "-";
    const liveMode = String(liveSession?.mode || "").trim() || "-";
    const liveJobId = String(liveSession?.job_id || "").trim() || "-";
    const finishedReason = String(liveSession?.finished_reason || "").trim() || "-";
    const liveDisplay = String(liveSession?.display || "").trim() || "-";
    const startedAt = formatTimestampValue(liveSession?.started_at_ts);
    const updatedAt = formatTimestampValue(liveSession?.updated_at_ts);
    const liveStatusText = isRunning ? "running" : liveState || "idle";
    const liveStatusClass = isRunning
      ? "jobs6-badge--running"
      : liveStatusText === "finished"
        ? "jobs6-badge--waiting"
        : liveStatusText === "stopping"
          ? "jobs6-badge--queued"
          : "jobs6-badge--idle";

    sessionLiveStatus.className = `jobs6-badge ${liveStatusClass}`;
    sessionLiveStatus.textContent = `TA Live: ${liveStatusText}`;
    sessionLiveMeta.textContent = `Live: ${liveDisplayMode} • pid ${String(liveSession?.pid ?? "-")} • ${liveMode}`;

    if (drawerNodeKey !== "scrape_tripadvisor") {
      return;
    }

    drawerTripadvisorLiveSummary.textContent = [
      `Estado: ${liveStatusText}`,
      `PID: ${String(liveSession?.pid ?? "-")}`,
      `Display mode: ${liveDisplayMode}`,
      `Modo bridge: ${liveMode}`,
      `Display: ${liveDisplay}`,
      `Job replay: ${liveJobId}`,
      `Reason: ${String(liveSession?.reason || "-")}`,
      `Started: ${startedAt}`,
      `Updated: ${updatedAt}`,
      `Finished reason: ${finishedReason}`,
      `Log file: ${String(tripadvisorLiveSessionTail?.log_file || liveSession?.log_file || "-")}`,
    ].join("\n");
    drawerTripadvisorLiveLog.textContent =
      tripadvisorLiveSessionError
        ? `ERROR: ${tripadvisorLiveSessionError}`
        : String(tripadvisorLiveSessionTail?.log_tail || "").trim() || "Sin logs todavía.";
    drawerTripadvisorLiveStopButton.toggleAttribute("disabled", !isRunning);
  }

  async function stopTripadvisorLiveSession(): Promise<void> {
    drawerActionStatus.textContent = "Parando sesión live TA...";
    try {
      await deps.apiClient.post("/tripadvisor/live-session/stop", {});
      await loadTripadvisorLiveSessionTail({ force: true });
      drawerActionStatus.textContent = "Sesión live TA detenida.";
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  function isCurrentTripadvisorLiveSessionForJob(jobId: string): boolean {
    const normalizedJobId = String(jobId || "").trim();
    if (!normalizedJobId) return false;
    const liveSession = tripadvisorLiveSessionTail?.live_session;
    if (!liveSession?.running) return false;
    return String(liveSession.job_id || "").trim() === normalizedJobId;
  }

  async function skipCurrentTripadvisorNode(): Promise<void> {
    const node = getDrawerNode();
    if (!node || node.key !== "scrape_tripadvisor" || !node.jobId) {
      drawerActionStatus.textContent = "Omitir solo aplica al nodo de TripAdvisor.";
      return;
    }

    const confirmed = window.confirm(
      "Se omitirá TripAdvisor para este job y la pipeline seguirá con analysis/report cuando ya pueda continuar.\n\n¿Continuar?"
    );
    if (!confirmed) {
      drawerActionStatus.textContent = "Omisión cancelada por el usuario.";
      return;
    }

    drawerActionStatus.textContent = "Omitiendo TripAdvisor y continuando pipeline...";
    try {
      await loadTripadvisorLiveSessionTail({ force: true });
      if (isCurrentTripadvisorLiveSessionForJob(node.jobId)) {
        drawerActionStatus.textContent = "Parando sesión live TA antes de omitir...";
        await deps.apiClient.post("/tripadvisor/live-session/stop", {});
        await loadTripadvisorLiveSessionTail({ force: true });
      }

      await deps.apiClient.post(
        `/business/scrape/jobs/${encodeURIComponent(node.jobId)}/resolve-live`,
        {
          resolution: "manual_skip",
          metadata: {
            source: "tripadvisor",
            trigger: "manager_ui_manual_skip",
            skipped_by_user: true,
            continue_pipeline: true,
          },
        }
      );

      drawerActionStatus.textContent = `TripAdvisor omitido para job ${node.jobId}.`;
      await loadJobsList();
      if (selectedBusinessKey) {
        await loadSelectedBusiness(selectedBusinessKey);
      }
      await loadTripadvisorLiveSessionTail({ force: true });
    } catch (error) {
      drawerActionStatus.textContent = `ERROR: ${formatError(error)}`;
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
    if (tripadvisorLiveSessionPollTimer === null) {
      tripadvisorLiveSessionPollTimer = window.setInterval(() => {
        if (!shouldPollTripadvisorLiveSession()) {
          return;
        }
        void loadTripadvisorLiveSessionTail();
      }, 5000);
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
    if (tripadvisorLiveSessionPollTimer !== null) {
      window.clearInterval(tripadvisorLiveSessionPollTimer);
      tripadvisorLiveSessionPollTimer = null;
    }
  }

  function resetStreams(): void {
    stopScrapeStream();
    stopAnalysisStream();
    stopPreparationStream();
    stopReportStream();
    loadedScrapeEvents = { google_maps: 0, tripadvisor: 0 };
    loadedAnalysisEvents = 0;
    loadedPreparationEvents = 0;
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

  function stopPreparationStream(): void {
    if (!preparationStream) return;
    preparationStream.close();
    preparationStream = null;
    preparationStreamJobId = null;
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
  void loadTripadvisorLiveSessionTail({ force: true });

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
      void loadTripadvisorLiveSessionTail({ force: true });
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

function appendDrawerField(labelText: string, input: HTMLElement): HTMLDivElement {
  const wrapper = createElement("div", "jobs6-drawer-form-row") as HTMLDivElement;
  wrapper.append(createElement("label", "form-label", labelText), input);
  return wrapper;
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

function resolveAnalysisToHydrationConnectorState(
  analysisNode: PipelineNodeState,
  hydrationNode: PipelineNodeState,
  reportNode: PipelineNodeState
): ConnectorState {
  if (analysisNode.status === "failed" || hydrationNode.status === "failed") return "failed";
  if (analysisNode.status === "running") return "active";
  if (analysisNode.status === "queued" || analysisNode.status === "waiting") return "waiting";
  if (!isHydrationDoneLike(hydrationNode.status) && (hydrationNode.status === "queued" || hydrationNode.status === "running")) {
    return hydrationNode.status === "running" ? "active" : "waiting";
  }
  if (analysisNode.status === "done" && isHydrationDoneLike(hydrationNode.status)) {
    if (reportNode.status === "running" || reportNode.status === "queued") return "done";
    return "done";
  }
  return "idle";
}

function resolveHydrationToReportConnectorState(
  hydrationNode: PipelineNodeState,
  reportNode: PipelineNodeState
): ConnectorState {
  if (hydrationNode.status === "failed" || reportNode.status === "failed") return "failed";
  if (reportNode.status === "running") return "active";
  if (reportNode.status === "queued") return "waiting";
  if (isHydrationDoneLike(hydrationNode.status) && reportNode.status === "done") return "done";
  if (isHydrationDoneLike(hydrationNode.status) && reportNode.status === "waiting") return "active";
  if (hydrationNode.status === "running") return "active";
  if (hydrationNode.status === "queued" || hydrationNode.status === "waiting") return "waiting";
  return "idle";
}

function isHydrationDoneLike(status: NodeStatus): boolean {
  return status === "done" || status === "ready" || status === "reused" || status === "skipped" || status === "not_in_study";
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
    study_hydration: {
      key: "study_hydration",
      title: "STUDY HYDRATION",
      sourceBadge: "hydration",
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

function createSkippedHydrationNode(): PipelineNodeState {
  return {
    ...createInitialNodes().study_hydration,
    status: "skipped",
    stage: "study_hydration_skipped",
    message: "Este informe no requiere hidratación de benchmark/geogrid.",
    progress: 100,
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

function resolvePreferredSourceJobIdForBusiness(group: BusinessScrapeGroup | null): string | null {
  if (!group) return null;
  if (group.latestSource) {
    const preferred = String(group.jobsBySource[group.latestSource]?.job_id || "").trim();
    if (preferred) {
      return preferred;
    }
  }
  const googleJobId = String(group.jobsBySource.google_maps?.job_id || "").trim();
  if (googleJobId) {
    return googleJobId;
  }
  const tripadvisorJobId = String(group.jobsBySource.tripadvisor?.job_id || "").trim();
  if (tripadvisorJobId) {
    return tripadvisorJobId;
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

function resolvePreparationJobId(analysisSource: PipelineNodeState | AnalyzeJobItem): string | null {
  const asJob = analysisSource as AnalyzeJobItem;
  const result = isRecord(asJob.result) ? asJob.result : null;
  const handoff = result && isRecord(result.study_hydration_handoff) ? result.study_hydration_handoff : null;
  const fromResult = String((handoff?.preparation_job_id as string | undefined) || "").trim();
  if (fromResult) {
    return fromResult;
  }

  const rawEvents = Array.isArray(asJob.events) ? asJob.events : [];
  for (const event of [...rawEvents].reverse()) {
    const data = isRecord(event?.data) ? event.data : null;
    const candidate = String((data?.preparation_job_id as string | undefined) || "").trim();
    if (candidate) {
      return candidate;
    }
  }
  return null;
}

function resolvePreparationDocumentId(preparationSource: PipelineNodeState | AnalyzeJobItem): string | null {
  const asJob = preparationSource as AnalyzeJobItem;
  const result = isRecord(asJob.result) ? asJob.result : null;
  const payload = isRecord(asJob.payload) ? asJob.payload : null;
  const fromResult = String((result?.report_preparation_id as string | undefined) || "").trim();
  if (fromResult) {
    return fromResult;
  }
  const fromPayload = String((payload?.preparation_id as string | undefined) || "").trim();
  if (fromPayload) {
    return fromPayload;
  }
  return null;
}

function resolveLatestPrepareJobIdFromPreparation(
  preparation: Record<string, unknown> | null | undefined
): string | null {
  if (!preparation) return null;
  const candidate = String((preparation.latest_prepare_job_id as string | undefined) || "").trim();
  return candidate || null;
}

function resolveFinalReportJobId(reportSource: PipelineNodeState | AnalyzeJobItem): string | null {
  const asJob = reportSource as AnalyzeJobItem;
  const result = isRecord(asJob.result) ? asJob.result : null;
  const fromResult = String((result?.final_report_job_id as string | undefined) || "").trim();
  if (fromResult) {
    return fromResult;
  }
  const rawEvents = Array.isArray(asJob.events) ? asJob.events : [];
  for (const event of [...rawEvents].reverse()) {
    const data = isRecord(event?.data) ? event.data : null;
    const candidate = String((data?.final_report_job_id as string | undefined) || "").trim();
    if (candidate) {
      return candidate;
    }
  }
  return null;
}

function isHydratedClientAuditJob(source: PipelineNodeState | AnalyzeJobItem): boolean {
  const asJob = source as AnalyzeJobItem;
  const result = isRecord(asJob.result) ? asJob.result : null;
  const payload = isRecord(asJob.payload) ? asJob.payload : null;
  const resultProfile = String((result?.report_profile as string | undefined) || "").trim().toLowerCase();
  const resultComplexity = String((result?.report_complexity as string | undefined) || "").trim().toLowerCase();
  if (resultProfile === "client_audit" && resultComplexity === "hydrated") {
    return true;
  }
  const payloadProfile = String((payload?.report_profile as string | undefined) || "").trim().toLowerCase();
  const payloadComplexity = String((payload?.report_complexity as string | undefined) || "").trim().toLowerCase();
  return payloadProfile === "client_audit" && payloadComplexity === "hydrated";
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
    return normalizeArtifactOutputUrl(preferredPath, apiBaseUrl);
  }
  const htmlPath = String((html?.path as string | undefined) || "").trim();
  if (htmlPath) {
    return normalizeArtifactOutputUrl(htmlPath, apiBaseUrl);
  }
  const jsonPath = String((json?.path as string | undefined) || "").trim();
  if (jsonPath) {
    return normalizeArtifactOutputUrl(jsonPath, apiBaseUrl);
  }
  return null;
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
  if (status === "skipped" || normalizedStage.includes("skipped")) return "skipped";
  if (status === "reused" || normalizedStage.includes("reused")) return "reused";
  if (status === "ready" || normalizedStage.includes("ready")) return "ready";
  if (status === "not_in_study" || normalizedStage.includes("not_in_study")) return "not_in_study";
  if (status === "failed" || normalizedStage === "failed" || normalizedStage.includes("source_failed")) return "failed";
  if (status === "done" || normalizedStage === "done") return "done";
  if (status === "running") return "running";
  if (status === "waiting" || normalizedStage.includes("waiting")) return "waiting";
  if (status === "queued" || normalizedStage === "queued") return "queued";
  if (status === "retrying" || status === "partial") return "running";
  return "idle";
}

function normalizeStatusFlagText(status: NodeStatus): string {
  if (status === "needs_human") return "NEEDS_HUMAN";
  if (status === "waiting") return "WAITING";
  if (status === "not_in_study") return "NOT_IN_STUDY";
  return status.toUpperCase();
}

function statusClassFromRaw(status: string | NodeStatus | undefined): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "needs_human") return "needs-human";
  if (normalized === "skipped") return "waiting";
  if (normalized === "reused" || normalized === "ready") return "done";
  if (normalized === "not_in_study") return "waiting";
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

function estimateHydrationProgress(stage: string, status: NodeStatus, current: number): number {
  if (isHydrationDoneLike(status) || status === "done") return 100;
  if (status === "failed") return Math.max(current, 100);
  const normalizedStage = String(stage || "").trim().toLowerCase();
  if (normalizedStage in HYDRATION_STAGE_PROGRESS) {
    return Math.max(current, HYDRATION_STAGE_PROGRESS[normalizedStage]);
  }
  if (status === "running") return Math.max(current, 26);
  if (status === "queued" || status === "waiting") return Math.max(current, 8);
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

function deriveHydrationSnapshotStatus(
  job: AnalyzeJobItem,
  result: Record<string, unknown> | null
): NodeStatus | null {
  const genericStatus = normalizeNodeStatus(job.status, job.progress?.stage);
  if (genericStatus === "failed" || genericStatus === "queued" || genericStatus === "running") {
    return genericStatus;
  }
  return deriveHydrationResultStatus(result);
}

function deriveHydrationResultStatus(payload: Record<string, unknown> | null | undefined): NodeStatus | null {
  if (!payload) return null;
  const hydrationStatus = String((payload.hydration_status as string | undefined) || "").trim().toLowerCase();
  const presence = String((payload.business_presence_state as string | undefined) || "").trim().toLowerCase();
  const hasStudyAbsence =
    presence === "not_in_latest_study" ||
    presence === "not_in_fresh_study" ||
    presence === "study_scope_unresolved";

  if (hydrationStatus === "ready_reused") return hasStudyAbsence ? "not_in_study" : "reused";
  if (hydrationStatus === "ready_refreshed") return hasStudyAbsence ? "not_in_study" : "ready";
  if (hydrationStatus === "ready_partial") {
    if (hasStudyAbsence) {
      return "not_in_study";
    }
    return "ready";
  }
  if (hydrationStatus === "waiting_benchmark" || hydrationStatus === "waiting_geogrid") return "waiting";
  if (hydrationStatus === "failed_scope_resolution" || hydrationStatus === "failed_hydration") return "failed";
  if (hasStudyAbsence) {
    return "not_in_study";
  }
  return null;
}

function describeHydrationState(payload: Record<string, unknown> | null): string {
  if (!payload) return "";
  const hydrationStatus = String((payload.hydration_status as string | undefined) || "").trim().toLowerCase();
  const benchmark = isRecord(payload.dependencies) && isRecord(payload.dependencies.benchmark)
    ? payload.dependencies.benchmark
    : null;
  const geogrid = isRecord(payload.dependencies) && isRecord(payload.dependencies.geogrid)
    ? payload.dependencies.geogrid
    : null;
  const benchmarkStatus = String((benchmark?.status as string | undefined) || "").trim();
  const geogridStatus = String((geogrid?.status as string | undefined) || "").trim();
  if (benchmarkStatus || geogridStatus) {
    const bits = [];
    if (benchmarkStatus) bits.push(`benchmark: ${benchmarkStatus}`);
    if (geogridStatus) bits.push(`geogrid: ${geogridStatus}`);
    return `${hydrationStatus || "study_hydration"} • ${bits.join(" · ")}`;
  }
  return hydrationStatus;
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

function formatTimestampValue(raw: number | null | undefined): string {
  if (typeof raw !== "number" || !Number.isFinite(raw) || raw <= 0) {
    return "-";
  }
  return formatDateTime(new Date(raw * 1000).toISOString());
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
