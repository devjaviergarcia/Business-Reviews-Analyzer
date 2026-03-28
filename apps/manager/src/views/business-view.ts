import { AnimationController } from "../animations/controller";
import { createReviewCard } from "../components/atoms/review-card";
import { createButton } from "../components/atoms/button";
import { createInput } from "../components/atoms/input";
import { ApiClient } from "../core/api-client";
import {
  clearElement,
  createElement,
  escapeHtml,
  formatError,
  parseOptionalFloat,
  parseOptionalInteger,
} from "../core/dom";
import type {
  BusinessSourceOverview,
  BusinessSourcesOverviewResponse,
  BusinessSummary,
  PaginatedResponse,
  ReviewItem,
  ViewModule,
} from "../core/types";

type BusinessViewDeps = {
  apiClient: ApiClient;
};

type ReviewsResponse = PaginatedResponse<ReviewItem> & {
  rating_gte?: number | null;
  rating_lte?: number | null;
  order?: string;
  source?: string | null;
  scrape_type?: string | null;
  pagination_scope?: "source" | "all_sources" | string;
  source_pagination?: Record<
    string,
    {
      total?: number;
      page_size?: number;
      total_pages?: number;
    }
  >;
  source_counts?: Record<string, number>;
  total_comments?: number;
};

type UnknownRecord = Record<string, unknown>;

export function createBusinessView(deps: BusinessViewDeps): ViewModule {
  const root = createElement("section", "view-panel business6-view");

  const searchPanel = createElement("section", "panel form-panel business6-search-panel");
  searchPanel.append(createElement("h2", "panel__title", "Negocios"));
  const searchRow = createElement("div", "form-actions");
  const searchInput = createInput({ placeholder: "Buscar por nombre..." });
  const searchButton = createButton({ label: "Buscar", tone: "turquoise" });
  const statusLabel = createElement("span", "muted", "");
  searchRow.append(searchInput, searchButton, statusLabel);
  searchPanel.append(searchRow);

  const tableWrap = createElement("div", "scroll-table");
  const businessTableBody = createElement("tbody");
  tableWrap.innerHTML = `
    <table class="data-table">
      <thead><tr><th>Nombre</th><th>Fuentes de scraping</th></tr></thead>
    </table>
  `;
  const table = tableWrap.querySelector("table");
  if (!table) {
    throw new Error("table not found");
  }
  table.append(businessTableBody);
  searchPanel.append(tableWrap);
  root.append(searchPanel);

  const detailPanel = createElement("section", "panel business6-detail-panel");
  detailPanel.append(createElement("h2", "panel__title", "Negocio seleccionado"));
  const selectedLabel = createElement("div", "muted", "No business selected.");

  const detailActions = createElement("div", "form-actions");
  const refreshDetailButton = createButton({ label: "Refrescar detalle", tone: "white" });
  const deleteBusinessButton = createButton({ label: "Eliminar negocio", tone: "white" });
  const detailActionStatus = createElement("span", "muted", "");
  detailActions.append(refreshDetailButton, deleteBusinessButton, detailActionStatus);

  const detailSummaryGrid = createElement("div", "business6-detail-grid");
  const businessSummaryCard = createElement("article", "business6-card");
  const analysisSummaryCard = createElement("article", "business6-card");
  const generatedSummaryCard = createElement("article", "business6-card");
  detailSummaryGrid.append(businessSummaryCard, analysisSummaryCard, generatedSummaryCard);

  const sourcesHead = createElement("div", "business6-sources-head");
  const sourcesTitle = createElement("h3", "panel__subtitle", "Fuentes de scraping");
  const sourcesStatus = createElement("span", "muted", "Sin cargar fuentes.");
  sourcesHead.append(sourcesTitle, sourcesStatus);
  const sourceViewActions = createElement("div", "form-actions business6-source-view-actions");
  const sourcesGrid = createElement("div", "business6-sources-grid");

  const debugDetails = createElement("details", "business6-debug") as HTMLDetailsElement;
  const debugSummary = createElement("summary", "business6-debug-summary", "Ver JSON técnico");
  const debugSplit = createElement("div", "split");
  const detailBlock = createElement("pre", "code-block", "");
  const analysisBlock = createElement("pre", "code-block", "");
  const analysesBlock = createElement("pre", "code-block", "");
  const reportBlock = createElement("pre", "code-block", "");
  const sourcesOverviewBlock = createElement("pre", "code-block", "");
  debugSplit.append(detailBlock, analysisBlock, analysesBlock, reportBlock);
  debugDetails.append(debugSummary, debugSplit, sourcesOverviewBlock);

  detailPanel.append(
    selectedLabel,
    detailActions,
    detailSummaryGrid,
    sourcesHead,
    sourceViewActions,
    sourcesGrid,
    debugDetails
  );
  root.append(detailPanel);

  const reviewsPanel = createElement("section", "panel form-panel business6-reviews-panel");
  reviewsPanel.append(createElement("h2", "panel__title", "Reseñas"));

  const filterGrid = createElement("div", "form-grid");
  const gteSelect = createElement("select", "atom-input") as HTMLSelectElement;
  const lteSelect = createElement("select", "atom-input") as HTMLSelectElement;
  gteSelect.innerHTML = buildRatingOptions();
  lteSelect.innerHTML = buildRatingOptions();
  const pageSizeInput = createInput({ type: "number", min: "1", max: "100", value: "20" });
  const sourceSelect = createElement("select", "atom-input") as HTMLSelectElement;
  sourceSelect.innerHTML = `
    <option value="">Todas las fuentes</option>
    <option value="google_maps">Google Maps</option>
    <option value="tripadvisor">Tripadvisor</option>
  `;
  const orderSelect = createElement("select", "atom-input") as HTMLSelectElement;
  orderSelect.innerHTML = `
    <option value="asc-date">Ascendente por fecha</option>
    <option value="desc-date">Descendente por fecha</option>
    <option value="asc-rating">Ascendente por rating</option>
    <option value="desc-rating" selected>Descendente por rating</option>
  `;

  filterGrid.append(createElement("label", "form-label", "Rating mínimo"), gteSelect);
  filterGrid.append(createElement("label", "form-label", "Rating máximo"), lteSelect);
  filterGrid.append(createElement("label", "form-label", "Fuente"), sourceSelect);
  filterGrid.append(createElement("label", "form-label", "Orden"), orderSelect);
  filterGrid.append(createElement("label", "form-label", "Tamaño de página"), pageSizeInput);
  reviewsPanel.append(filterGrid);

  const reviewActions = createElement("div", "form-actions");
  const loadReviewsButton = createButton({ label: "Cargar reseñas", tone: "orange" });
  const prevButton = createButton({ label: "Prev", tone: "white" });
  const nextButton = createButton({ label: "Next", tone: "white" });
  reviewActions.append(loadReviewsButton, prevButton, nextButton);
  reviewsPanel.append(reviewActions);

  const reviewsMeta = createElement("div", "business6-reviews-meta");
  const reviewsStatus = createElement("span", "muted", "");
  const pageLabel = createElement("span", "muted", "");
  reviewsMeta.append(reviewsStatus, pageLabel);

  const sourceCountsStrip = createElement("div", "business6-source-counts");
  const reviewsList = createElement("div", "business6-review-list review-card-list");

  reviewsPanel.append(reviewsMeta, sourceCountsStrip, reviewsList);
  root.append(reviewsPanel);

  let selectedBusinessId: string | null = null;
  let selectedBusinessName: string | null = null;
  let reviewsPage = 1;
  let lastReviewsResponse: ReviewsResponse | null = null;

  renderBusinessSummaryCard(
    businessSummaryCard,
    {},
    null,
    null
  );
  renderAnalysisSummaryCard(analysisSummaryCard, { info: "Selecciona un negocio para cargar análisis." });
  renderGeneratedAnalysesCard(generatedSummaryCard, null, null, deps.apiClient.getBaseUrl());

  searchButton.addEventListener("click", () => {
    void searchBusinesses();
  });

  loadReviewsButton.addEventListener("click", () => {
    reviewsPage = 1;
    void loadReviews();
  });

  sourceSelect.addEventListener("change", () => {
    reviewsPage = 1;
    highlightSourceCards(sourceSelect.value || null);
    void loadReviews();
  });

  prevButton.addEventListener("click", () => {
    if (reviewsPage <= 1) return;
    reviewsPage -= 1;
    void loadReviews();
  });

  nextButton.addEventListener("click", () => {
    if (!lastReviewsResponse?.has_next) return;
    reviewsPage += 1;
    void loadReviews();
  });

  refreshDetailButton.addEventListener("click", () => {
    if (!selectedBusinessId || !selectedBusinessName) return;
    void selectBusiness(selectedBusinessId, selectedBusinessName);
  });

  deleteBusinessButton.addEventListener("click", () => {
    void deleteSelectedBusiness();
  });

  async function searchBusinesses(): Promise<void> {
    statusLabel.textContent = "Buscando...";
    try {
      const name = searchInput.value.trim();
      const params = new URLSearchParams({ page: "1", page_size: "20" });
      if (name) params.set("name", name);
      const response = await deps.apiClient.get<PaginatedResponse<BusinessSummary>>(
        `/business?${params.toString()}`
      );
      const items = response.items || [];
      statusLabel.textContent = `${items.length} resultados`;
      businessTableBody.innerHTML = items
        .map((item) => {
          const id = escapeHtml(String(item.business_id || ""));
          return `<tr>
            <td><button class="table-link" data-business-id="${id}">${escapeHtml(item.name || "")}</button></td>
            <td>${escapeHtml(formatBusinessSourcesSummary(item))}</td>
          </tr>`;
        })
        .join("");
      root.querySelectorAll<HTMLButtonElement>("button[data-business-id]").forEach((button) => {
        button.addEventListener("click", () => {
          const businessId = button.dataset.businessId;
          if (!businessId) return;
          void selectBusiness(businessId, button.textContent || "");
        });
      });
    } catch (error) {
      statusLabel.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function selectBusiness(businessId: string, businessName: string): Promise<void> {
    selectedBusinessId = businessId;
    selectedBusinessName = businessName;
    selectedLabel.textContent = `Seleccionado: ${businessName} (${businessId})`;
    detailActionStatus.textContent = "";

    sourcesStatus.textContent = "Cargando fuentes...";
    clearElement(sourcesGrid);
    clearElement(reviewsList);
    clearElement(sourceCountsStrip);
    reviewsStatus.textContent = "";
    pageLabel.textContent = "";

    detailBlock.textContent = "Cargando detalle...";
    analysisBlock.textContent = "Cargando análisis...";
    analysesBlock.textContent = "Cargando historial de análisis...";
    reportBlock.textContent = "Cargando reporte...";
    sourcesOverviewBlock.textContent = "Cargando fuentes...";

    try {
      const [detail, analysisResponse, analysesResponse, reportResponse] = await Promise.all([
        deps.apiClient.get<UnknownRecord>(`/business/${encodeURIComponent(businessId)}`),
        deps.apiClient
          .get<UnknownRecord>(`/business/${encodeURIComponent(businessId)}/analysis`)
          .catch((error: unknown) => ({ error: formatError(error) } as UnknownRecord)),
        deps.apiClient
          .get<PaginatedResponse<UnknownRecord>>(`/business/${encodeURIComponent(businessId)}/analyses?page=1&page_size=10`)
          .catch((error: unknown) => ({ error: formatError(error), items: [] } as unknown as PaginatedResponse<UnknownRecord>),
        ),
        deps.apiClient
          .get<UnknownRecord>(`/business/${encodeURIComponent(businessId)}/report`)
          .catch((error: unknown) => ({ error: formatError(error) } as UnknownRecord)),
      ]);

      renderBusinessSummaryCard(businessSummaryCard, detail, businessName, businessId);
      renderAnalysisSummaryCard(analysisSummaryCard, analysisResponse);
      renderGeneratedAnalysesCard(
        generatedSummaryCard,
        analysesResponse,
        reportResponse,
        deps.apiClient.getBaseUrl()
      );

      detailBlock.textContent = JSON.stringify(detail, null, 2);
      analysisBlock.textContent = JSON.stringify(analysisResponse, null, 2);
      analysesBlock.textContent = JSON.stringify(analysesResponse, null, 2);
      reportBlock.textContent = JSON.stringify(reportResponse, null, 2);
    } catch (error) {
      const message = formatError(error);
      renderBusinessSummaryCard(
        businessSummaryCard,
        { error: message },
        businessName,
        businessId
      );
      renderAnalysisSummaryCard(analysisSummaryCard, { error: message });
      renderGeneratedAnalysesCard(
        generatedSummaryCard,
        { error: message } as unknown as PaginatedResponse<UnknownRecord>,
        null,
        deps.apiClient.getBaseUrl()
      );
      detailBlock.textContent = `ERROR: ${message}`;
      analysisBlock.textContent = "";
      analysesBlock.textContent = "";
      reportBlock.textContent = "";
    }

    await loadSourcesOverview(businessId);
    reviewsPage = 1;
    await loadReviews();
  }

  async function loadSourcesOverview(businessId: string): Promise<void> {
    sourcesStatus.textContent = "Cargando fuentes...";
    try {
      const overview = await deps.apiClient.get<BusinessSourcesOverviewResponse>(
        `/business/${encodeURIComponent(businessId)}/sources?comments_preview_size=3`
      );

      const sourceItems = Array.isArray(overview.sources)
        ? overview.sources
        : [];

      clearElement(sourcesGrid);
      if (sourceItems.length === 0) {
        sourcesGrid.append(
          createElement("div", "muted business6-empty", "No hay fuentes registradas para este negocio.")
        );
        setSourceFilterOptions(sourceSelect, []);
        renderSourceViewActions([], sourceViewActions, sourceSelect, () => {
          reviewsPage = 1;
          highlightSourceCards(sourceSelect.value || null);
          void loadReviews();
        });
      } else {
        const overviewRecord = overview as unknown as UnknownRecord;
        const rawAvailableSources = Array.isArray(overviewRecord.available_sources)
          ? overviewRecord.available_sources.map((source) => String(source || ""))
          : [];
        const sourceItemSources = sourceItems.map((item) => String(item.source || ""));
        const availableSources = normalizeSupportedSources([...rawAvailableSources, ...sourceItemSources]);
        setSourceFilterOptions(sourceSelect, availableSources);
        renderSourceViewActions(availableSources, sourceViewActions, sourceSelect, () => {
          reviewsPage = 1;
          highlightSourceCards(sourceSelect.value || null);
          void loadReviews();
        });
        for (const item of sourceItems) {
          sourcesGrid.append(createSourceOverviewCard(item));
        }
      }

      sourcesStatus.textContent = `${sourceItems.length} fuente(s) • ${overview.total_comments ?? 0} comentarios totales`;
      highlightSourceCards(sourceSelect.value || null);
      sourcesOverviewBlock.textContent = JSON.stringify(overview, null, 2);
    } catch (error) {
      clearElement(sourcesGrid);
      sourcesGrid.append(createElement("div", "muted business6-empty", `ERROR: ${formatError(error)}`));
      sourcesStatus.textContent = "No se pudo cargar fuentes.";
      setSourceFilterOptions(sourceSelect, []);
      renderSourceViewActions([], sourceViewActions, sourceSelect, () => {
        reviewsPage = 1;
        highlightSourceCards(sourceSelect.value || null);
        void loadReviews();
      });
      sourcesOverviewBlock.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function loadReviews(): Promise<void> {
    if (!selectedBusinessId) {
      reviewsStatus.textContent = "Selecciona un negocio primero.";
      return;
    }

    reviewsStatus.textContent = "Cargando reseñas...";
    try {
      const params = new URLSearchParams();
      params.set("page", String(reviewsPage));
      params.set("page_size", String(parseOptionalInteger(pageSizeInput.value) ?? 20));
      params.set("order", orderSelect.value);

      if (sourceSelect.value) {
        params.set("scrape_type", sourceSelect.value);
      }
      const gte = parseOptionalFloat(gteSelect.value);
      const lte = parseOptionalFloat(lteSelect.value);
      if (gte !== null) params.set("rating_gte", String(gte));
      if (lte !== null) params.set("rating_lte", String(lte));

      const response = await deps.apiClient.get<ReviewsResponse>(
        `/business/${encodeURIComponent(selectedBusinessId)}/comments?${params.toString()}`
      );

      lastReviewsResponse = response;
      const items = Array.isArray(response.items) ? response.items : [];

      clearElement(reviewsList);
      if (items.length === 0) {
        reviewsList.append(createElement("div", "muted business6-empty", "No hay reseñas para este filtro."));
      } else {
        for (const item of items) {
          reviewsList.append(createBusinessReviewCard(item));
        }
      }

      renderSourceCounts(sourceCountsStrip, response.source_counts || {});
      reviewsStatus.textContent = `${items.length} comentario(s) cargados`;
      const selectedScrapeType = String(response.scrape_type || sourceSelect.value || "").trim();
      if (selectedScrapeType && response.source_pagination?.[selectedScrapeType]) {
        const sourcePageMeta = response.source_pagination[selectedScrapeType];
        const sourceTotal = Number(sourcePageMeta.total ?? response.total ?? 0);
        const sourceTotalPages = Number(sourcePageMeta.total_pages ?? response.total_pages ?? 0);
        const pageCurrent = Number(response.page ?? reviewsPage);
        pageLabel.textContent = `Página ${pageCurrent}/${sourceTotalPages || 1} • ${sourceDisplayName(
          selectedScrapeType
        )}: ${sourceTotal}`;
      } else {
        pageLabel.textContent = `Página ${response.page ?? reviewsPage} • Total ${response.total ?? 0}`;
      }
      prevButton.disabled = (response.page ?? reviewsPage) <= 1;
      nextButton.disabled = !response.has_next;
    } catch (error) {
      reviewsStatus.textContent = `ERROR: ${formatError(error)}`;
      prevButton.disabled = true;
      nextButton.disabled = true;
    }
  }

  async function deleteSelectedBusiness(): Promise<void> {
    if (!selectedBusinessId) {
      detailActionStatus.textContent = "No hay negocio seleccionado.";
      return;
    }

    const businessLabel = selectedBusinessName || selectedBusinessId;
    const confirmed = window.confirm(
      `Se eliminará el negocio '${businessLabel}' y sus datos relacionados. ¿Continuar?`
    );
    if (!confirmed) {
      return;
    }

    detailActionStatus.textContent = "Eliminando negocio...";
    try {
      await deps.apiClient.delete<Record<string, unknown>>(
        `/business/${encodeURIComponent(selectedBusinessId)}`
      );

      detailActionStatus.textContent = "Negocio eliminado.";
      selectedBusinessId = null;
      selectedBusinessName = null;
      selectedLabel.textContent = "No business selected.";
      renderBusinessSummaryCard(businessSummaryCard, {}, null, null);
      renderAnalysisSummaryCard(analysisSummaryCard, { info: "Selecciona un negocio para cargar análisis." });
      renderGeneratedAnalysesCard(generatedSummaryCard, null, null, deps.apiClient.getBaseUrl());
      clearElement(sourcesGrid);
      clearElement(sourceViewActions);
      clearElement(reviewsList);
      clearElement(sourceCountsStrip);
      detailBlock.textContent = "";
      analysisBlock.textContent = "";
      analysesBlock.textContent = "";
      reportBlock.textContent = "";
      sourcesOverviewBlock.textContent = "";
      reviewsStatus.textContent = "";
      pageLabel.textContent = "";
      lastReviewsResponse = null;
      await searchBusinesses();
    } catch (error) {
      detailActionStatus.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  void searchBusinesses();
  AnimationController.mount(root, "view");
  return {
    key: "business",
    title: "Negocios",
    root,
    onShow: () => {},
    onHide: () => {
      clearElement(reviewsList);
    },
  };
}

function buildRatingOptions(): string {
  return `
    <option value="">Sin filtro</option>
    <option value="1">1</option>
    <option value="2">2</option>
    <option value="3">3</option>
    <option value="4">4</option>
    <option value="5">5</option>
  `;
}

function renderBusinessSummaryCard(
  container: HTMLElement,
  detail: UnknownRecord,
  fallbackName: string | null,
  businessId: string | null
): void {
  clearElement(container);

  const title = createElement("h4", "business6-card__title", "Ficha del negocio");
  container.append(title);

  const detailError = stringFromUnknown(detail.error);
  if (detailError) {
    container.append(createElement("div", "muted", `No se pudo cargar detalle: ${detailError}`));
    return;
  }

  const listing = asRecord(detail.listing);
  const stats = asRecord(detail.stats);

  container.append(
    createKeyValueRow("Nombre", stringFromUnknown(detail.name) || fallbackName || "-"),
    createKeyValueRow("Business ID", businessId || "-"),
    createKeyValueRow("Rating", formatRating(listing.overall_rating)),
    createKeyValueRow("Total reviews", stringFromUnknown(listing.total_reviews) || "-"),
    createKeyValueRow("Review count", stringFromUnknown(detail.review_count) || "-"),
    createKeyValueRow("Dirección", stringFromUnknown(listing.address) || "-"),
    createKeyValueRow("Teléfono", stringFromUnknown(listing.phone) || "-"),
    createKeyValueRow("Website", stringFromUnknown(listing.website) || "-"),
    createKeyValueRow("Avg sentiment", formatRating(stats.avg_rating)),
    createKeyValueRow("Actualizado", formatDateTime(stringFromUnknown(detail.updated_at)))
  );
}

function renderAnalysisSummaryCard(container: HTMLElement, analysis: UnknownRecord): void {
  clearElement(container);
  container.append(createElement("h4", "business6-card__title", "Último análisis"));

  const analysisError = stringFromUnknown(analysis.error);
  if (analysisError) {
    container.append(createElement("div", "muted", `No disponible: ${analysisError}`));
    return;
  }

  const mainTopics = toStringList(analysis.main_topics);
  const strengths = toStringList(analysis.key_strengths);
  const weaknesses = toStringList(analysis.key_weaknesses);

  container.append(
    createKeyValueRow("ID análisis", stringFromUnknown(analysis.id) || "-"),
    createKeyValueRow("Sentimiento", stringFromUnknown(analysis.overall_sentiment) || "-"),
    createKeyValueRow("Tema principal", mainTopics.slice(0, 3).join(" • ") || "-"),
    createKeyValueRow("Fortalezas", strengths.slice(0, 2).join(" • ") || "-"),
    createKeyValueRow("Debilidades", weaknesses.slice(0, 2).join(" • ") || "-"),
    createKeyValueRow("Reporte generado", formatDateTime(stringFromUnknown(analysis.report_generated_at))),
    createKeyValueRow("Creado", formatDateTime(stringFromUnknown(analysis.created_at))),
    createKeyValueRow("Actualizado", formatDateTime(stringFromUnknown(analysis.updated_at)))
  );

  const summary = stringFromUnknown(analysis.summary);
  if (summary) {
    const summaryBlock = createElement("p", "business6-summary-text", summary);
    container.append(summaryBlock);
  }
}

function renderGeneratedAnalysesCard(
  container: HTMLElement,
  analysesResponse: PaginatedResponse<UnknownRecord> | null,
  reportResponse: UnknownRecord | null,
  apiBaseUrl: string
): void {
  clearElement(container);
  container.append(createElement("h4", "business6-card__title", "Análisis generados"));

  if (!analysesResponse) {
    container.append(createElement("div", "muted", "Selecciona un negocio para cargar historial."));
    return;
  }

  const analysesError = stringFromUnknown((analysesResponse as unknown as UnknownRecord).error);
  if (analysesError) {
    container.append(createElement("div", "muted", `No disponible: ${analysesError}`));
    return;
  }

  const analysisItems = Array.isArray(analysesResponse.items) ? analysesResponse.items : [];
  container.append(createKeyValueRow("Total análisis", String(analysesResponse.total ?? analysisItems.length ?? 0)));

  if (analysisItems.length === 0) {
    container.append(createElement("div", "muted", "No hay análisis generados todavía."));
  } else {
    const historyList = createElement("div", "business6-analysis-history");
    for (const item of analysisItems.slice(0, 6)) {
      const id = stringFromUnknown(item.id) || "-";
      const createdAt = formatDateTime(stringFromUnknown(item.created_at));
      const reportGeneratedAt = formatDateTime(stringFromUnknown(item.report_generated_at));
      const statusText = reportGeneratedAt !== "-" ? "PDF generado" : "Sin PDF todavía";
      historyList.append(
        createElement(
          "div",
          "business6-analysis-history-item",
          `${id.slice(0, 8)} • ${createdAt} • ${statusText}`
        )
      );
    }
    container.append(historyList);
  }

  if (!reportResponse) {
    return;
  }
  const reportError = stringFromUnknown(reportResponse.error);
  if (reportError) {
    container.append(createElement("div", "muted", `Reporte completo: ${reportError}`));
    return;
  }

  const reportArtifacts = asRecord(reportResponse.report_artifacts);
  const previewArtifacts = asRecord(reportResponse.preview_report_artifacts);
  const artifactActions = createElement("div", "form-actions");
  const mainPdfPath = artifactPathFromArtifacts(reportArtifacts, "pdf");
  const mainHtmlPath = artifactPathFromArtifacts(reportArtifacts, "html");
  const previewPdfPath = artifactPathFromArtifacts(previewArtifacts, "pdf");

  const buttons = [
    { label: "Abrir PDF completo", path: mainPdfPath },
    { label: "Abrir HTML completo", path: mainHtmlPath },
    { label: "Abrir PDF preview", path: previewPdfPath },
  ];

  for (const buttonInfo of buttons) {
    if (!buttonInfo.path) continue;
    const button = createButton({ label: buttonInfo.label, tone: "white" });
    button.addEventListener("click", () => {
      window.open(
        normalizeLocalPathToUrl(buttonInfo.path as string, apiBaseUrl),
        "_blank",
        "noopener"
      );
    });
    artifactActions.append(button);
  }

  if (artifactActions.childElementCount > 0) {
    container.append(createElement("div", "business6-card__section-title", "Artefactos"), artifactActions);
  }
}

function artifactPathFromArtifacts(artifacts: UnknownRecord, key: "pdf" | "html" | "json"): string | null {
  const bucket = asRecord(artifacts[key]);
  const path = stringFromUnknown(bucket.path);
  return path || null;
}

function normalizeLocalPathToUrl(pathOrUrl: string, apiBaseUrl: string): string {
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

function createSourceOverviewCard(item: BusinessSourceOverview): HTMLElement {
  const card = createElement("article", "business6-source-card");
  const sourceValue = String(item.source || "").trim().toLowerCase();
  card.dataset.source = sourceValue;

  const latestJob = asRecord(item.latest_job);
  const sourceProfile = asRecord(item.source_profile);
  const activeDataset = asRecord(item.active_dataset);

  const sourceHeader = createElement("div", "business6-source-header");
  const sourceBadge = createElement(
    "span",
    `business6-source-badge ${sourceBadgeClass(sourceValue)}`,
    sourceDisplayName(sourceValue)
  );
  const jobStatusRaw = stringFromUnknown(latestJob.status) || "idle";
  const jobStatus = createElement(
    "span",
    `jobs-status jobs-status--${statusClassFromRaw(jobStatusRaw)}`,
    jobStatusRaw.toUpperCase()
  );
  sourceHeader.append(sourceBadge, jobStatus);

  const sourceName = createElement(
    "div",
    "business6-source-name",
    stringFromUnknown(sourceProfile.source_business_name) || "Sin nombre de fuente"
  );

  const sourceSub = createElement(
    "div",
    "business6-source-sub",
    `Comentarios: ${item.comments_count ?? 0} • Dataset activo: ${stringFromUnknown(activeDataset.id) || "-"}`
  );

  const sourceMeta = createElement("div", "business6-source-meta");
  sourceMeta.append(
    createKeyValueRow("Job ID", stringFromUnknown(latestJob.job_id) || "-"),
    createKeyValueRow("Queue", stringFromUnknown(latestJob.queue_name) || "-"),
    createKeyValueRow("Stage", stringFromUnknown(asRecord(latestJob.progress).stage) || "-"),
    createKeyValueRow("Dataset status", stringFromUnknown(activeDataset.status) || "-"),
    createKeyValueRow("Dataset reviews", stringFromUnknown(activeDataset.review_count) || "-")
  );

  card.append(sourceHeader, sourceName, sourceSub, sourceMeta);

  const latestComments = Array.isArray(item.latest_comments) ? item.latest_comments : [];
  const previewTitle = createElement("div", "business6-preview-title", "Últimas reseñas");
  card.append(previewTitle);

  if (latestComments.length === 0) {
    card.append(createElement("div", "muted", "Sin reseñas recientes en esta fuente."));
    return card;
  }

  const previewList = createElement("div", "business6-preview-list");
  for (const review of latestComments.slice(0, 3)) {
    previewList.append(createCompactReviewItem(review));
  }
  card.append(previewList);

  return card;
}

function highlightSourceCards(source: string | null): void {
  document.querySelectorAll<HTMLElement>(".business6-source-card").forEach((card) => {
    const cardSource = String(card.dataset.source || "").trim().toLowerCase();
    const isActive = !source || source === cardSource;
    card.classList.toggle("business6-source-card--active", isActive);
    card.classList.toggle("business6-source-card--muted", !isActive);
  });
}

function setSourceFilterOptions(select: HTMLSelectElement, sources: string[]): void {
  const normalized = normalizeSupportedSources(sources);
  const previous = select.value;
  const options = ['<option value=\"\">Todas las fuentes</option>'];
  for (const source of normalized) {
    options.push(`<option value=\"${source}\">${sourceDisplayName(source)}</option>`);
  }
  select.innerHTML = options.join("");
  select.value = normalized.includes(previous) ? previous : "";
}

function renderSourceViewActions(
  sources: string[],
  container: HTMLElement,
  sourceSelect: HTMLSelectElement,
  onChange: () => void
): void {
  clearElement(container);

  const available = normalizeSupportedSources(sources);

  const allButton = createButton({ label: "Ver todo", tone: "white" });
  allButton.classList.toggle("is-selected", !sourceSelect.value);
  allButton.addEventListener("click", () => {
    sourceSelect.value = "";
    onChange();
  });
  container.append(allButton);

  for (const source of available) {
    const button = createButton({ label: sourceDisplayName(source), tone: "white" });
    button.classList.toggle("is-selected", sourceSelect.value === source);
    button.addEventListener("click", () => {
      sourceSelect.value = source;
      onChange();
    });
    container.append(button);
  }
}

function createCompactReviewItem(review: ReviewItem): HTMLElement {
  const row = createElement("article", "business6-preview-item");
  const meta = createElement(
    "div",
    "business6-preview-meta",
    `${review.author_name || "Anónimo"} • ${typeof review.rating === "number" ? `${review.rating}/5` : "-"} • ${
      review.relative_time || ""
    }`
  );
  const text = createElement("p", "business6-preview-text", (review.text || "").slice(0, 220));
  row.append(meta, text);
  return row;
}

function createBusinessReviewCard(review: ReviewItem): HTMLElement {
  const card = createReviewCard(review);
  card.classList.add("business6-review-card");

  const reviewRecord = review as unknown as UnknownRecord;
  const sourceValue = stringFromUnknown(reviewRecord.source) || "unknown";
  const sourcePill = createElement(
    "span",
    `business6-source-pill ${sourceBadgeClass(sourceValue)}`,
    sourceDisplayName(sourceValue)
  );

  const head = card.querySelector(".review-card__head");
  if (head) {
    head.append(sourcePill);
  }

  const writtenDate = stringFromUnknown(reviewRecord.written_date);
  if (writtenDate) {
    card.append(createElement("div", "business6-review-extra muted", writtenDate));
  }

  const ownerReplyText =
    stringFromUnknown(reviewRecord.owner_reply_text) || stringFromUnknown(reviewRecord.owner_reply);
  if (ownerReplyText) {
    const ownerReplyAuthor =
      stringFromUnknown(reviewRecord.owner_reply_author_name) || "Propietario";
    const ownerReplyDate =
      stringFromUnknown(reviewRecord.owner_reply_written_date) ||
      stringFromUnknown(reviewRecord.owner_reply_relative_time);
    const ownerReplyMeta = ownerReplyDate
      ? `${ownerReplyAuthor} • ${ownerReplyDate}`
      : ownerReplyAuthor;
    const ownerReplyBox = createElement("div", "business6-review-owner-reply");
    ownerReplyBox.append(
      createElement("div", "business6-review-owner-reply__meta muted", ownerReplyMeta),
      createElement("p", "business6-review-owner-reply__text", ownerReplyText)
    );
    card.append(ownerReplyBox);
  }
  return card;
}

function renderSourceCounts(container: HTMLElement, counts: Record<string, number>): void {
  clearElement(container);
  const entries = Object.entries(counts);
  if (entries.length === 0) {
    container.append(createElement("span", "muted", "Sin distribución por fuente."));
    return;
  }

  for (const [source, count] of entries) {
    const chip = createElement(
      "span",
      `business6-source-count-chip ${sourceBadgeClass(source)}`,
      `${sourceDisplayName(source)}: ${count}`
    );
    container.append(chip);
  }
}

function createKeyValueRow(label: string, value: string): HTMLElement {
  const row = createElement("div", "business6-kv-row");
  row.append(createElement("span", "business6-kv-label", label), createElement("span", "business6-kv-value", value));
  return row;
}

function asRecord(value: unknown): UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as UnknownRecord)
    : {};
}

function stringFromUnknown(value: unknown): string {
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return "";
}

function toStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => stringFromUnknown(item))
    .filter((item) => Boolean(item));
}

function formatRating(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return `${value.toFixed(1)}/5`;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number.parseFloat(value);
    if (Number.isFinite(parsed)) {
      return `${parsed.toFixed(1)}/5`;
    }
  }
  return "-";
}

function sourceDisplayName(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "google_maps" || normalized === "google" || normalized === "googlemaps") {
    return "Google Maps";
  }
  if (normalized === "tripadvisor" || normalized === "trip_advisor") {
    return "Tripadvisor";
  }
  return normalized || "Unknown";
}

function sourceBadgeClass(value: string): string {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "google_maps" || normalized === "google" || normalized === "googlemaps") {
    return "business6-source--google";
  }
  if (normalized === "tripadvisor" || normalized === "trip_advisor") {
    return "business6-source--tripadvisor";
  }
  return "business6-source--unknown";
}

function normalizeSupportedSources(sources: string[]): string[] {
  return Array.from(
    new Set(
      sources
        .map((source) => String(source || "").trim().toLowerCase())
        .filter((source) => source === "google_maps" || source === "tripadvisor")
    )
  );
}

function formatBusinessSourcesSummary(item: BusinessSummary): string {
  const businessRecord = item as unknown as UnknownRecord;
  const rawSources = businessRecord.sources_available;
  if (Array.isArray(rawSources)) {
    const labels = rawSources
      .map((source) => sourceDisplayName(String(source || "")))
      .filter((value) => Boolean(value));
    if (labels.length > 0) {
      return labels.join(" | ");
    }
  }
  return sourceDisplayName(stringFromUnknown(businessRecord.source) || "unknown");
}

function statusClassFromRaw(status: string): string {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "needs_human") return "needs-human";
  if (normalized === "waiting") return "waiting";
  if (normalized === "running") return "running";
  if (normalized === "queued") return "queued";
  if (normalized === "done") return "done";
  if (normalized === "failed") return "failed";
  return "idle";
}

function formatDateTime(value: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}
