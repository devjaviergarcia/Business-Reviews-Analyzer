import { AnimationController } from "../animations/controller";
import { createReviewCard } from "../components/atoms/review-card";
import { createButton } from "../components/atoms/button";
import { createAnalysisNewForm } from "../components/forms/analysis-new-form";
import {
  createAnalysisReanalyzeForm,
  type ReanalyzeSuggestion,
} from "../components/forms/analysis-reanalyze-form";
import { ApiClient } from "../core/api-client";
import {
  clearElement,
  createElement,
  formatError,
  normalizeText,
  parseOptionalFloat,
  parseOptionalInteger,
} from "../core/dom";
import type {
  AnalyzeJobItem,
  BusinessSummary,
  PaginatedResponse,
  ReviewItem,
  ViewModule,
} from "../core/types";

type AnalysisViewDeps = {
  apiClient: ApiClient;
  onJobQueued?: (jobId: string) => void;
};

type CatalogEntry = {
  businessId: string;
  name: string;
  nameNormalized: string;
};

const JOB_STAGES = [
  "queued",
  "worker_started",
  "scrape_pipeline_started",
  "scraper_search_started",
  "scraper_search_completed",
  "scraper_listing_completed",
  "scraper_reviews_started",
  "scraper_reviews_completed",
  "handoff_analysis_queued",
  "analysis_worker_started",
  "analysis_worker_summary",
  "done",
];

export function createAnalysisView(deps: AnalysisViewDeps): ViewModule {
  const root = createElement("section", "view-panel");
  const modeBar = createElement("div", "panel mode-switch");
  modeBar.append(createElement("h2", "panel__title", "Análisis"));
  const modeActions = createElement("div", "form-actions");
  const newModeButton = createButton({ label: "Analizar nuevo", tone: "turquoise" });
  const reanalyzeModeButton = createButton({ label: "Reanalizar existente", tone: "white" });
  modeActions.append(newModeButton, reanalyzeModeButton);
  modeBar.append(modeActions);
  root.append(modeBar);

  const newForm = createAnalysisNewForm({
    onSubmit: async (values) => {
      newForm.statusLabel.textContent = "Encolando...";
      try {
        if (!values.name) {
          throw new Error("El nombre es obligatorio.");
        }
        const payload: Record<string, unknown> = {
          name: values.name,
          force: values.force,
          strategy: values.strategy,
          force_mode: values.forceMode || null,
        };
        if (values.googleMapsName) {
          payload.google_maps_name = values.googleMapsName;
        }
        if (values.tripadvisorName) {
          payload.tripadvisor_name = values.tripadvisorName;
        }
        const interactiveRounds = parseOptionalInteger(values.interactiveRounds);
        const htmlRounds = parseOptionalInteger(values.htmlRounds);
        const stableRounds = parseOptionalInteger(values.stableRounds);
        const tripadvisorMaxPages = parseOptionalInteger(values.tripadvisorMaxPages);
        const tripadvisorPagesPercent = parseOptionalFloat(values.tripadvisorPagesPercent);
        if (
          tripadvisorPagesPercent !== null &&
          (tripadvisorPagesPercent <= 0 || tripadvisorPagesPercent > 100)
        ) {
          throw new Error("TripAdvisor pages percent debe estar entre 0 y 100.");
        }
        if (
          interactiveRounds !== null ||
          htmlRounds !== null ||
          stableRounds !== null ||
          tripadvisorMaxPages !== null ||
          tripadvisorPagesPercent !== null
        ) {
          payload.scraper_params = {
            scraper_interactive_max_rounds: interactiveRounds,
            scraper_html_scroll_max_rounds: htmlRounds,
            scraper_html_stable_rounds: stableRounds,
            scraper_tripadvisor_max_pages: tripadvisorMaxPages,
            scraper_tripadvisor_pages_percent: tripadvisorPagesPercent,
          };
        }
        const response = await deps.apiClient.post<{ job_id?: string }>(
          "/business/scrape/jobs",
          payload
        );
        const queuedJobId = String(response.job_id || "").trim();
        if (!queuedJobId) {
          throw new Error("La API no devolvió job_id.");
        }
        setSelectedJob(queuedJobId);
        newForm.statusLabel.textContent = `Job encolado: ${queuedJobId}`;
        if (typeof deps.onJobQueued === "function") {
          deps.onJobQueued(queuedJobId);
        }
      } catch (error) {
        newForm.statusLabel.textContent = `ERROR: ${formatError(error)}`;
      }
    },
  });
  root.append(newForm.root);

  const reanalyzeForm = createAnalysisReanalyzeForm({
    onLoadCatalog: async () => {
      reanalyzeForm.statusLabel.textContent = "Cargando catálogo...";
      try {
        await loadCatalog();
        reanalyzeForm.statusLabel.textContent = `Catálogo cargado: ${catalog.length} negocios`;
        renderSuggestions(reanalyzeForm.searchInput.value);
      } catch (error) {
        reanalyzeForm.statusLabel.textContent = `ERROR: ${formatError(error)}`;
      }
    },
    onSearchTerm: (term) => {
      renderSuggestions(term);
    },
    onSubmit: async (values) => {
      reanalyzeForm.responseBlock.textContent = "";
      if (!selectedCatalogBusinessId) {
        reanalyzeForm.statusLabel.textContent = "Selecciona un negocio del autocompletado.";
        return;
      }
      reanalyzeForm.statusLabel.textContent = "Reanalizando...";
      try {
        const payload: Record<string, unknown> = {};
        const batchers = values.batchers
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean);
        if (batchers.length > 0) {
          payload.batchers = batchers;
        }
        const batchSize = parseOptionalInteger(values.batchSize);
        const poolSize = parseOptionalInteger(values.poolSize);
        if (batchSize !== null) {
          payload.batch_size = batchSize;
        }
        if (poolSize !== null) {
          payload.max_reviews_pool = poolSize;
        }
        const response = await deps.apiClient.post<Record<string, unknown>>(
          `/business/${encodeURIComponent(selectedCatalogBusinessId)}/reanalyze`,
          payload
        );
        reanalyzeForm.statusLabel.textContent = "Reanálisis completado.";
        reanalyzeForm.responseBlock.textContent = JSON.stringify(response, null, 2);
      } catch (error) {
        reanalyzeForm.statusLabel.textContent = `ERROR: ${formatError(error)}`;
      }
    },
  });
  reanalyzeForm.root.classList.add("hidden");
  root.append(reanalyzeForm.root);

  const progressPanel = createElement("section", "panel");
  progressPanel.append(createElement("h2", "panel__title", "Progreso de job"));
  const jobLabel = createElement("div", "muted", "Job: -");
  const stageLabel = createElement("div", "muted", "Stage: -");
  const reviewsLabel = createElement("div", "muted", "Reviews: 0 / ?");
  progressPanel.append(jobLabel, stageLabel);

  const stageTrack = createElement("div", "progress-track");
  const stageFill = createElement("div", "progress-fill");
  stageTrack.append(stageFill);
  progressPanel.append(stageTrack);

  progressPanel.append(reviewsLabel);
  const reviewsTrack = createElement("div", "progress-track");
  const reviewsFill = createElement("div", "progress-fill progress-fill-alt");
  reviewsTrack.append(reviewsFill);
  progressPanel.append(reviewsTrack);

  const progressActions = createElement("div", "form-actions");
  const reloadJobButton = createButton({
    label: "Reload job",
    tone: "white",
    onClick: async () => {
      if (!selectedJobId) return;
      await loadJobDetail(selectedJobId);
    },
  });
  const stopStreamButton = createButton({
    label: "Stop stream",
    tone: "white",
    onClick: () => stopJobStream(),
  });
  progressActions.append(reloadJobButton, stopStreamButton);
  progressPanel.append(progressActions);

  const split = createElement("div", "split");
  const jobJson = createElement("pre", "code-block", "");
  const jobEvents = createElement("pre", "code-block", "");
  split.append(jobJson, jobEvents);
  progressPanel.append(split);

  progressPanel.append(createElement("h3", "panel__subtitle", "Preview reseñas"));
  const reviewCardsContainer = createElement("div", "review-card-list");
  progressPanel.append(reviewCardsContainer);
  root.append(progressPanel);

  let activeMode: "new" | "reanalyze" = "new";
  let selectedJobId: string | null = null;
  let selectedBusinessId: string | null = null;
  let selectedJobStream: EventSource | null = null;
  let reviewsLoaded = 0;
  let listingTotalReviews: number | null = null;
  let previewRefreshTimer: number | null = null;

  let catalogLoaded = false;
  let selectedCatalogBusinessId: string | null = null;
  let catalog: CatalogEntry[] = [];

  newModeButton.addEventListener("click", () => setMode("new"));
  reanalyzeModeButton.addEventListener("click", () => setMode("reanalyze"));

  function setMode(mode: "new" | "reanalyze"): void {
    activeMode = mode;
    newModeButton.classList.toggle("is-selected", activeMode === "new");
    reanalyzeModeButton.classList.toggle("is-selected", activeMode === "reanalyze");
    newForm.root.classList.toggle("hidden", activeMode !== "new");
    reanalyzeForm.root.classList.toggle("hidden", activeMode !== "reanalyze");
  }

  setMode("new");

  function setSelectedJob(jobIdValue: string): void {
    selectedJobId = jobIdValue;
    selectedBusinessId = null;
    reviewsLoaded = 0;
    listingTotalReviews = null;
    jobEvents.textContent = "";
    jobJson.textContent = "";
    clearElement(reviewCardsContainer);
    updateProgressBars("queued");
    jobLabel.textContent = `Job: ${jobIdValue}`;
    startJobStream(jobIdValue);
  }

  function startJobStream(jobIdValue: string): void {
    stopJobStream();
    const stream = deps.apiClient.createEventSource(
      `/business/scrape/jobs/${encodeURIComponent(jobIdValue)}/events`
    );
    selectedJobStream = stream;
    appendEventLine(`[open] ${jobIdValue}`);

    stream.addEventListener("progress", (event) => {
      handleJobEvent((event as MessageEvent<string>).data);
    });
    stream.addEventListener("done", (event) => {
      handleJobEvent((event as MessageEvent<string>).data);
      stopJobStream();
    });
    stream.onerror = () => {
      appendEventLine("[error] stream disconnected");
    };
  }

  function stopJobStream(): void {
    if (selectedJobStream) {
      selectedJobStream.close();
      selectedJobStream = null;
    }
  }

  function appendEventLine(line: string): void {
    jobEvents.textContent += `${line}\n`;
    jobEvents.scrollTop = jobEvents.scrollHeight;
  }

  function handleJobEvent(raw: string): void {
    appendEventLine(raw);
    let parsed: Record<string, unknown> = {};
    try {
      parsed = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      return;
    }

    const stage = String(parsed.stage || parsed.event || "").trim() || "running";
    const data = ((parsed.data as Record<string, unknown> | undefined) || {}) as Record<string, unknown>;
    const scrapeResult = ((data.scrape_result as Record<string, unknown> | undefined) || {}) as Record<
      string,
      unknown
    >;

    const businessIdRaw = String(
      data.business_id || scrapeResult.business_id || (parsed.business_id as string) || ""
    ).trim();
    if (businessIdRaw) {
      selectedBusinessId = businessIdRaw;
    }
    const listingTotalRaw = data.total_reviews;
    if (typeof listingTotalRaw === "number") {
      listingTotalReviews = listingTotalRaw;
    }
    const loadedRaw =
      (typeof data.reviews_loaded === "number" ? data.reviews_loaded : null) ??
      (typeof data.scraped_review_count === "number" ? data.scraped_review_count : null) ??
      (typeof scrapeResult.scraped_review_count === "number" ? scrapeResult.scraped_review_count : null);
    if (typeof loadedRaw === "number") {
      reviewsLoaded = Math.max(reviewsLoaded, loadedRaw);
    }

    updateProgressBars(stage);
    if (selectedBusinessId && (typeof loadedRaw === "number" || stage === "done")) {
      schedulePreviewRefresh();
    }
    if (stage === "done" && selectedJobId) {
      void loadJobDetail(selectedJobId);
    }
  }

  function updateProgressBars(stage: string): void {
    const stageIndex = JOB_STAGES.indexOf(stage);
    const normalizedIndex = stageIndex >= 0 ? stageIndex : 0;
    const stagePct = Math.round(((normalizedIndex + 1) / JOB_STAGES.length) * 100);
    stageFill.style.width = `${Math.max(0, Math.min(100, stagePct))}%`;
    stageLabel.textContent = `Stage: ${stage} (${stagePct}%)`;

    const reviewsPct =
      listingTotalReviews && listingTotalReviews > 0
        ? Math.min(100, Math.round((reviewsLoaded / listingTotalReviews) * 100))
        : reviewsLoaded > 0
          ? 100
          : 0;
    reviewsFill.style.width = `${reviewsPct}%`;
    reviewsLabel.textContent = `Reviews: ${reviewsLoaded} / ${listingTotalReviews ?? "?"}`;
  }

  async function loadJobDetail(jobIdValue: string): Promise<void> {
    try {
      const detail = await deps.apiClient.get<AnalyzeJobItem | Record<string, unknown>>(
        `/business/scrape/jobs/${encodeURIComponent(jobIdValue)}`
      );
      jobJson.textContent = JSON.stringify(detail, null, 2);
    } catch (error) {
      jobJson.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  function schedulePreviewRefresh(): void {
    if (previewRefreshTimer !== null) {
      window.clearTimeout(previewRefreshTimer);
    }
    previewRefreshTimer = window.setTimeout(() => {
      void refreshReviewPreview();
    }, 300);
  }

  async function refreshReviewPreview(): Promise<void> {
    if (!selectedBusinessId) {
      return;
    }
    try {
      const response = await deps.apiClient.get<PaginatedResponse<ReviewItem>>(
        `/business/${encodeURIComponent(selectedBusinessId)}/reviews?page=1&page_size=8&order=desc`
      );
      const items = response.items || [];
      clearElement(reviewCardsContainer);
      for (const item of items) {
        reviewCardsContainer.append(createReviewCard(item));
      }
    } catch {
      // best-effort preview
    }
  }

  async function loadCatalog(): Promise<void> {
    if (catalogLoaded) {
      return;
    }
    const map = new Map<string, CatalogEntry>();
    let page = 1;
    let hasNext = true;
    while (hasNext) {
      const data = await deps.apiClient.get<PaginatedResponse<BusinessSummary>>(
        `/business?page=${page}&page_size=100`
      );
      const items = data.items || [];
      for (const business of items) {
        const businessId = String(business.business_id || "").trim();
        const name = String(business.name || "").trim();
        if (!businessId || !name) {
          continue;
        }
        if (!map.has(businessId)) {
          map.set(businessId, {
            businessId,
            name,
            nameNormalized: normalizeText(name),
          });
        }
      }
      hasNext = Boolean(data.has_next);
      page += 1;
    }
    catalog = Array.from(map.values()).sort((a, b) =>
      a.name.localeCompare(b.name, "es", { sensitivity: "base" })
    );
    catalogLoaded = true;
  }

  function renderSuggestions(term: string): void {
    if (!catalogLoaded) {
      reanalyzeForm.setSearchCount("Carga primero el catálogo.");
      reanalyzeForm.setSuggestions([], () => {});
      return;
    }
    const needle = normalizeText(term || "");
    const filtered = needle
      ? catalog.filter((item) => item.nameNormalized.includes(needle))
      : catalog;
    const limited = filtered.slice(0, 25);
    reanalyzeForm.setSearchCount(
      `${filtered.length} coincidencias${filtered.length > 25 ? " (mostrando 25)" : ""}`
    );
    const suggestions: ReanalyzeSuggestion[] = limited.map((item) => ({
      businessId: item.businessId,
      name: item.name,
    }));
    reanalyzeForm.setSuggestions(suggestions, (item) => {
      selectedCatalogBusinessId = item.businessId;
      reanalyzeForm.selectedLabel.textContent = `Seleccionado: ${item.name} (${item.businessId})`;
    });
  }

  AnimationController.mount(root, "view");
  return {
    key: "analysis",
    title: "Análisis",
    root,
    onShow: () => {},
    onHide: () => {},
  };
}
