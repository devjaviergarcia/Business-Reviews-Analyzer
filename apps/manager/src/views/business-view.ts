import { AnimationController } from "../animations/controller";
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
};

export function createBusinessView(deps: BusinessViewDeps): ViewModule {
  const root = createElement("section", "view-panel");

  const searchPanel = createElement("section", "panel form-panel");
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
      <thead><tr><th>Nombre</th><th>Rating</th><th>Total reviews</th><th>Address</th></tr></thead>
    </table>
  `;
  const table = tableWrap.querySelector("table");
  if (!table) {
    throw new Error("table not found");
  }
  table.append(businessTableBody);
  searchPanel.append(tableWrap);
  root.append(searchPanel);

  const detailPanel = createElement("section", "panel");
  detailPanel.append(createElement("h2", "panel__title", "Detalle / reporte"));
  const selectedLabel = createElement("div", "muted", "No business selected.");
  const split = createElement("div", "split");
  const detailBlock = createElement("pre", "code-block", "");
  const analysisBlock = createElement("pre", "code-block", "");
  split.append(detailBlock, analysisBlock);
  detailPanel.append(selectedLabel, split);
  root.append(detailPanel);

  const reviewsPanel = createElement("section", "panel form-panel");
  reviewsPanel.append(createElement("h2", "panel__title", "Reseñas"));
  const filterGrid = createElement("div", "form-grid");
  const gteSelect = createElement("select", "atom-input") as HTMLSelectElement;
  const lteSelect = createElement("select", "atom-input") as HTMLSelectElement;
  gteSelect.innerHTML = buildRatingOptions();
  lteSelect.innerHTML = buildRatingOptions();
  const pageSizeInput = createInput({ type: "number", min: "1", max: "100", value: "20" });
  const orderSelect = createElement("select", "atom-input") as HTMLSelectElement;
  orderSelect.innerHTML = `
    <option value="asc-date">Ascendente por fecha</option>
    <option value="desc-date">Descendente por fecha</option>
    <option value="asc-rating">Ascendente por rating</option>
    <option value="desc-rating" selected>Descendente por rating</option>
  `;

  filterGrid.append(createElement("label", "form-label", "Rating minimo"), gteSelect);
  filterGrid.append(createElement("label", "form-label", "Rating maximo"), lteSelect);
  filterGrid.append(createElement("label", "form-label", "Orden"), orderSelect);
  filterGrid.append(createElement("label", "form-label", "Tamano de pagina"), pageSizeInput);
  reviewsPanel.append(filterGrid);

  const reviewActions = createElement("div", "form-actions");
  const loadReviewsButton = createButton({ label: "Cargar reseñas", tone: "orange" });
  const prevButton = createButton({ label: "Prev", tone: "white" });
  const nextButton = createButton({ label: "Next", tone: "white" });
  const reviewsStatus = createElement("span", "muted", "");
  const pageLabel = createElement("span", "muted", "");
  reviewActions.append(loadReviewsButton, prevButton, nextButton, reviewsStatus, pageLabel);
  reviewsPanel.append(reviewActions);

  const reviewsTableWrap = createElement("div", "scroll-table");
  const reviewsBody = createElement("tbody");
  reviewsTableWrap.innerHTML = `
    <table class="data-table">
      <thead><tr><th>Author</th><th>Rating</th><th>When</th><th>Text</th></tr></thead>
    </table>
  `;
  const reviewsTable = reviewsTableWrap.querySelector("table");
  if (!reviewsTable) {
    throw new Error("reviews table not found");
  }
  reviewsTable.append(reviewsBody);
  reviewsPanel.append(reviewsTableWrap);
  root.append(reviewsPanel);

  let selectedBusinessId: string | null = null;
  let reviewsPage = 1;
  let lastReviewsResponse: ReviewsResponse | null = null;

  searchButton.addEventListener("click", () => {
    void searchBusinesses();
  });
  loadReviewsButton.addEventListener("click", () => {
    reviewsPage = 1;
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
            <td>${escapeHtml(String(item.overall_rating ?? ""))}</td>
            <td>${escapeHtml(String(item.total_reviews ?? ""))}</td>
            <td>${escapeHtml(item.address || "")}</td>
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
    selectedLabel.textContent = `Seleccionado: ${businessName} (${businessId})`;
    detailBlock.textContent = "Cargando detalle...";
    analysisBlock.textContent = "Cargando analysis...";
    try {
      const [detail, analysis] = await Promise.all([
        deps.apiClient.get<Record<string, unknown>>(`/business/${encodeURIComponent(businessId)}`),
        deps.apiClient
          .get<Record<string, unknown>>(`/business/${encodeURIComponent(businessId)}/analysis`)
          .catch((error: unknown) => ({ error: formatError(error) })),
      ]);
      detailBlock.textContent = JSON.stringify(detail, null, 2);
      analysisBlock.textContent = JSON.stringify(analysis, null, 2);
    } catch (error) {
      detailBlock.textContent = `ERROR: ${formatError(error)}`;
    }
    reviewsPage = 1;
    await loadReviews();
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
      const gte = parseOptionalFloat(gteSelect.value);
      const lte = parseOptionalFloat(lteSelect.value);
      if (gte !== null) params.set("rating_gte", String(gte));
      if (lte !== null) params.set("rating_lte", String(lte));

      const response = await deps.apiClient.get<ReviewsResponse>(
        `/business/${encodeURIComponent(selectedBusinessId)}/reviews?${params.toString()}`
      );
      lastReviewsResponse = response;
      const items = response.items || [];
      reviewsBody.innerHTML = items
        .map(
          (item) =>
            `<tr><td>${escapeHtml(item.author_name || "")}</td><td>${escapeHtml(
              String(item.rating ?? "")
            )}</td><td>${escapeHtml(item.relative_time || "")}</td><td>${escapeHtml(
              (item.text || "").slice(0, 160)
            )}</td></tr>`
        )
        .join("");
      reviewsStatus.textContent = `${items.length} reseñas cargadas`;
      pageLabel.textContent = `page ${response.page ?? reviewsPage} / total ${
        response.total ?? 0
      }`;
    } catch (error) {
      reviewsStatus.textContent = `ERROR: ${formatError(error)}`;
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
      clearElement(reviewsBody);
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
