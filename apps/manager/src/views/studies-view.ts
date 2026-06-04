import { createButton } from "../components/atoms/button";
import { createInput } from "../components/atoms/input";
import { ApiClient } from "../core/api-client";
import { clearElement, createElement, escapeHtml, formatError } from "../core/dom";
import type {
  GeoCityItem,
  GeoGridBusinessStats,
  GeoGridRunItem,
  GeoGridStatsResponse,
  PaginatedResponse,
  ViewModule,
} from "../core/types";

type StudiesViewDeps = {
  apiClient: ApiClient;
  onJobQueued?: (jobId: string) => void;
};

const ACTIVE_STATUSES = new Set(["queued", "running", "partial"]);
const LEAFLET_STYLE_ID = "leaflet-style";
const LEAFLET_SCRIPT_ID = "leaflet-script";

let leafletLoadPromise: Promise<void> | null = null;

function ensureLeaflet(): Promise<void> {
  const win = window as Window & { L?: unknown };
  if (win.L) {
    return Promise.resolve();
  }
  if (leafletLoadPromise) {
    return leafletLoadPromise;
  }
  leafletLoadPromise = new Promise<void>((resolve, reject) => {
    if (!document.getElementById(LEAFLET_STYLE_ID)) {
      const link = document.createElement("link");
      link.id = LEAFLET_STYLE_ID;
      link.rel = "stylesheet";
      link.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
      link.integrity = "sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=";
      link.crossOrigin = "";
      document.head.append(link);
    }
    const existingScript = document.getElementById(LEAFLET_SCRIPT_ID) as HTMLScriptElement | null;
    if (existingScript) {
      existingScript.addEventListener("load", () => resolve(), { once: true });
      existingScript.addEventListener("error", () => reject(new Error("No se pudo cargar Leaflet.")), { once: true });
      return;
    }
    const script = document.createElement("script");
    script.id = LEAFLET_SCRIPT_ID;
    script.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
    script.integrity = "sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=";
    script.crossOrigin = "";
    script.onload = () => resolve();
    script.onerror = () => reject(new Error("No se pudo cargar Leaflet."));
    document.head.append(script);
  });
  return leafletLoadPromise;
}

export function createStudiesView(deps: StudiesViewDeps): ViewModule {
  const root = createElement("section", "view-panel studies-view");

  const launchPanel = createElement("section", "panel form-panel");
  launchPanel.append(createElement("h2", "panel__title", "Estudios · Geo Grid"));
  launchPanel.append(
    createElement(
      "p",
      "muted",
      "Lanza una busqueda desde cada punto de la ciudad y guarda el top local para estudiar visibilidad por zonas."
    )
  );

  const form = createElement("form", "form-grid") as HTMLFormElement;
  const keywordInput = createInput({ placeholder: "Merienda Cordoba", value: "Merienda Cordoba" });
  const citySelect = createElement("select", "atom-input") as HTMLSelectElement;
  const topNSelect = createElement("select", "atom-input") as HTMLSelectElement;
  const providerModeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  providerModeSelect.innerHTML = [
    '<option value="maps_live">Maps live (clic + listing)</option>',
    '<option value="uule">UULE (SERP geolocalizada)</option>',
  ].join("");
  const gridSizeInput = createInput({ type: "number", value: "7", min: "3", max: "21", step: "2" });
  const gridSpacingInput = createInput({ type: "number", value: "0.4", min: "0.05", max: "20", step: "0.05" });
  const uuleRadiusInput = createInput({ type: "number", value: "1000", min: "100", max: "50000", step: "100" });
  const throttleInput = createInput({ type: "number", value: "1200", min: "100", max: "15000", step: "100" });

  topNSelect.innerHTML = [5, 10, 15, 20, 30, 50, 100]
    .map((value) => `<option value="${value}"${value === 10 ? " selected" : ""}>Top ${value} por punto</option>`)
    .join("");
  appendLabeled(form, "Keyword", keywordInput);
  appendLabeled(form, "Ciudad", citySelect);
  appendLabeled(form, "Negocios por punto", topNSelect);
  appendLabeled(form, "Modo", providerModeSelect);
  appendLabeled(form, "Grid size (NxN)", gridSizeInput);
  appendLabeled(form, "Espaciado (km)", gridSpacingInput);
  appendLabeled(form, "Radio UULE (m)", uuleRadiusInput);
  appendLabeled(form, "Throttle (ms)", throttleInput);

  const launchActions = createElement("div", "form-actions");
  const launchButton = createButton({ label: "Lanzar estudio", tone: "orange", type: "submit" });
  const launchStatus = createElement("span", "muted", "");
  launchActions.append(launchButton, launchStatus);
  form.append(launchActions);
  launchPanel.append(form);

  const runsPanel = createElement("section", "panel form-panel");
  runsPanel.append(createElement("h2", "panel__title", "Geo grids realizados"));
  const filters = createElement("div", "form-grid");
  const runCityFilter = createElement("select", "atom-input") as HTMLSelectElement;
  const runStatusFilter = createElement("select", "atom-input") as HTMLSelectElement;
  runStatusFilter.innerHTML = [
    '<option value="">Todos los estados</option>',
    '<option value="queued">queued</option>',
    '<option value="running">running</option>',
    '<option value="partial">partial</option>',
    '<option value="completed">completed</option>',
    '<option value="failed">failed</option>',
  ].join("");
  appendLabeled(filters, "Ciudad", runCityFilter);
  appendLabeled(filters, "Estado", runStatusFilter);

  const runActions = createElement("div", "form-actions");
  const refreshRunsButton = createButton({ label: "Actualizar", tone: "turquoise" });
  const openJobButton = createButton({ label: "Ver job", tone: "white" });
  const runStatus = createElement("span", "muted", "");
  runActions.append(refreshRunsButton, openJobButton, runStatus);
  const progressWrap = createElement("div", "geo-progress");
  const progressBar = createElement("div", "geo-progress__bar");
  progressWrap.append(progressBar);
  const runsTableWrap = createElement("div", "scroll-table");
  const runsTable = createElement("table", "data-table");
  runsTableWrap.append(runsTable);
  runsPanel.append(filters, runActions, progressWrap, runsTableWrap);

  const detailPanel = createElement("section", "panel form-panel");
  detailPanel.append(createElement("h2", "panel__title", "Visualizacion del estudio"));
  const detailMeta = createElement("div", "geo-detail-meta muted", "Selecciona un estudio para cargar mapa y estadisticas.");
  const businessSearch = createInput({ placeholder: "Buscar negocio dentro del estudio" });
  const businessSelect = createElement("select", "atom-input") as HTMLSelectElement;
  appendLabeled(detailPanel, "Negocio concreto", businessSearch);
  appendLabeled(detailPanel, "Resultados deduplicados", businessSelect);
  const statsGrid = createElement("div", "geo-stats-grid");
  const mapHost = createElement("div", "geo-map-card");
  const resultsWrap = createElement("div", "scroll-table");
  const businessTable = createElement("table", "data-table");
  resultsWrap.append(businessTable);
  detailPanel.append(detailMeta, statsGrid, mapHost, resultsWrap);

  root.append(launchPanel, runsPanel, detailPanel);

  let cities: GeoCityItem[] = [];
  let runs: GeoGridRunItem[] = [];
  let selectedRun: GeoGridRunItem | null = null;
  let selectedStats: GeoGridStatsResponse | null = null;
  let pollTimer: number | null = null;
  let leafletMap: { remove: () => void } | null = null;
  let mapRenderToken = 0;

  async function loadCities(): Promise<void> {
    cities = await deps.apiClient.get<GeoCityItem[]>("/crm/geo-cities");
    const options = cities
      .map((city) => `<option value="${escapeHtml(city.city_slug)}">${escapeHtml(city.city)} (${city.point_count} puntos)</option>`)
      .join("");
    citySelect.innerHTML = options || '<option value="">Sin ciudades disponibles</option>';
    runCityFilter.innerHTML = `<option value="">Todas</option>${options}`;
  }

  function syncProviderControls(): void {
    const isUule = providerModeSelect.value === "uule";
    gridSizeInput.disabled = !isUule;
    gridSpacingInput.disabled = !isUule;
    uuleRadiusInput.disabled = !isUule;
    throttleInput.disabled = !isUule;
  }

  async function loadRuns(): Promise<void> {
    const params = new URLSearchParams({ page: "1", page_size: "50" });
    if (runCityFilter.value) params.set("city_slug", runCityFilter.value);
    if (runStatusFilter.value) params.set("status", runStatusFilter.value);
    const response = await deps.apiClient.get<PaginatedResponse<GeoGridRunItem>>(`/crm/geo-grid-runs?${params}`);
    runs = response.items || [];
    renderRuns();
    runStatus.textContent = `${runs.length} estudios cargados`;
  }

  async function loadRunDetail(runId: string): Promise<void> {
    selectedRun = await deps.apiClient.get<GeoGridRunItem>(`/crm/geo-grid-runs/${encodeURIComponent(runId)}`);
    selectedStats = await deps.apiClient.get<GeoGridStatsResponse>(`/crm/geo-grid-runs/${encodeURIComponent(runId)}/stats`);
    renderRuns();
    renderDetail();
    updatePollingState();
  }

  function renderRuns(): void {
    const selectedId = selectedRun?.geo_grid_run_id || "";
    runsTable.innerHTML = `
      <thead><tr>
        <th>Fecha</th><th>Keyword</th><th>Ciudad</th><th>Modo</th><th>Top</th><th>Progreso</th><th>Estado</th><th></th>
      </tr></thead>
      <tbody>
        ${runs
          .map((run) => {
            const percent = estimateProgress(run);
            const active = run.geo_grid_run_id === selectedId ? " data-row-active=\"true\"" : "";
            const providerMode = String(run.provider_mode || (run.metrics?.provider_mode as string) || "maps_live");
            return `<tr${active}>
              <td>${formatDate(run.created_at)}</td>
              <td>${escapeHtml(run.keyword || "")}</td>
              <td>${escapeHtml(run.city || "")}</td>
              <td>${escapeHtml(providerMode)}</td>
              <td>${run.top_n || "-"}</td>
              <td>${percent}% · ${run.completed_points || 0}/${run.point_count || 0} puntos</td>
              <td><span class="pill">${escapeHtml(run.status || "")}</span></td>
              <td><button class="inline-button" data-run-id="${escapeHtml(run.geo_grid_run_id)}">Ver</button></td>
            </tr>`;
          })
          .join("")}
      </tbody>`;
    runsTable.querySelectorAll<HTMLButtonElement>("button[data-run-id]").forEach((button) => {
      button.addEventListener("click", () => {
        const runId = button.dataset.runId || "";
        void loadRunDetail(runId).catch(showError);
      });
    });
    if (selectedRun) {
      const percent = estimateProgress(selectedRun);
      progressBar.style.width = `${percent}%`;
      runStatus.textContent = `${selectedRun.status} · ${percent}% · ${selectedRun.completed_points}/${selectedRun.point_count} puntos`;
    } else {
      progressBar.style.width = "0%";
    }
  }

  function renderDetail(): void {
    if (!selectedRun || !selectedStats) {
      clearElement(statsGrid);
      clearElement(mapHost);
      businessTable.innerHTML = "";
      return;
    }
    const summary = selectedStats.summary || {};
    const visibilityScore = Number(summary.visibility_score || 0);
    const providerMode = String(summary.provider_mode || selectedRun.provider_mode || "maps_live");
    detailMeta.textContent = `${selectedRun.keyword} · ${selectedRun.city} · ${summary.unique_businesses || 0} negocios unicos · ${summary.total_results || 0} posiciones · modo ${providerMode} · visibilidad ${visibilityScore.toFixed(1)}%`;
    renderStatsCards();
    renderBusinessSelector();
    renderMap();
    renderBusinessTable();
  }

  function renderStatsCards(): void {
    clearElement(statsGrid);
    if (!selectedStats) return;
    const cards = [
      ["Mejores por cobertura", selectedStats.leaders],
      ["Peores posiciones", selectedStats.weakest],
      ["Mas consistentes", selectedStats.most_consistent],
      ["Mas dispersos", selectedStats.most_dispersed],
    ] as const;
    for (const [title, rows] of cards) {
      const card = createElement("div", "geo-stat-card");
      card.innerHTML = `<h3>${escapeHtml(title)}</h3>${renderMiniList(rows.slice(0, 5))}`;
      statsGrid.append(card);
    }
  }

  function renderBusinessSelector(): void {
    if (!selectedStats) return;
    const q = businessSearch.value.trim().toLowerCase();
    const rows = selectedStats.businesses.filter((business) =>
      String(business.business_name || "").toLowerCase().includes(q)
    );
    const current = businessSelect.value;
    businessSelect.innerHTML = [
      '<option value="">Vista general</option>',
      ...rows.map(
        (business) =>
          `<option value="${escapeHtml(business.business_key)}">${escapeHtml(business.business_name)} · ${business.coverage_percent}% · avg ${business.avg_rank ?? "-"}</option>`
      ),
    ].join("");
    if (current && rows.some((business) => business.business_key === current)) {
      businessSelect.value = current;
    }
  }

  function renderMap(): void {
    if (!selectedStats) return;
    const points = selectedStats.points;
    mapRenderToken += 1;
    const currentToken = mapRenderToken;
    if (leafletMap) {
      leafletMap.remove();
      leafletMap = null;
    }
    clearElement(mapHost);
    if (!points.length) {
      mapHost.textContent = "Sin puntos con resultados todavia.";
      return;
    }
    const selectedBusinessKey = businessSelect.value;
    const mapId = `geo-map-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    mapHost.innerHTML = `<div id="${mapId}" class="geo-map-leaflet" role="img" aria-label="Mapa GeoGrid OpenStreetMap"></div>
      <div class="geo-legend"><span><b class="dot good"></b>Top 1-3</span><span><b class="dot ok"></b>Top 4-10</span><span><b class="dot weak"></b>Top >10</span><span><b class="dot missing"></b>No aparece</span></div>`;
    void ensureLeaflet()
      .then(() => {
        if (currentToken !== mapRenderToken) return;
        const win = window as Window & { L?: any };
        if (!win.L) return;
        const L = win.L;
        const mapNode = document.getElementById(mapId);
        if (!mapNode) return;
        const map = L.map(mapId, {
          zoomControl: true,
          attributionControl: true,
        });
        leafletMap = map;
        const osmLayer = L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          maxZoom: 19,
          attribution: "&copy; OpenStreetMap contributors",
        });
        const cartoLayer = L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
          maxZoom: 20,
          attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
        });
        osmLayer.addTo(map);
        L.control.layers(
          {
            OpenStreetMap: osmLayer,
            "Carto Light": cartoLayer,
          },
          {},
          { collapsed: true }
        ).addTo(map);

        const bounds: Array<[number, number]> = [];
        for (const point of points) {
          const lat = Number(point.lat);
          const lng = Number(point.lng);
          if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
          bounds.push([lat, lng]);
          const selectedResult = selectedBusinessKey
            ? point.top_results.find((item) => item.business_key === selectedBusinessKey)
            : point.top_results[0];
          const rank = selectedResult?.rank || null;
          const fill = resolveRankColor(rank);
          const label = selectedResult ? `#${rank} ${selectedResult.business_name}` : "Sin aparicion";
          const marker = L.circleMarker([lat, lng], {
            radius: 8,
            color: "#2b1b12",
            weight: 1.3,
            fillColor: fill,
            fillOpacity: 0.9,
          });
          const pointName = point.point_label || `Punto ${point.point_order}`;
          marker.bindPopup(
            `<strong>${escapeHtml(String(pointName))}</strong><br>${escapeHtml(String(label))}<br><small>Lat ${lat.toFixed(5)} · Lng ${lng.toFixed(5)}</small>`
          );
          marker.addTo(map);
        }
        if (bounds.length) {
          map.fitBounds(bounds, { padding: [26, 26] });
        } else {
          map.setView([37.8882, -4.7794], 12);
        }
      })
      .catch((error: unknown) => {
        if (currentToken !== mapRenderToken) return;
        mapHost.innerHTML = `<p class="muted">${escapeHtml(formatError(error))}</p>`;
      });
  }

  function renderBusinessTable(): void {
    if (!selectedStats) return;
    const selectedKey = businessSelect.value;
    const rows = selectedKey
      ? selectedStats.businesses.filter((business) => business.business_key === selectedKey)
      : selectedStats.businesses;
    businessTable.innerHTML = `<thead><tr>
      <th>Negocio</th><th>Cobertura</th><th>Avg rank</th><th>Mejor</th><th>Peor</th><th>Consistencia</th><th>Top 3</th><th>Rating</th>
    </tr></thead><tbody>${rows
      .slice(0, selectedKey ? 1 : 100)
      .map(
        (business) => `<tr>
          <td>${escapeHtml(business.business_name || "")}</td>
          <td>${business.coverage_percent}% (${business.appearances})</td>
          <td>${business.avg_rank ?? "-"}</td>
          <td>${business.best_rank ?? "-"}</td>
          <td>${business.worst_rank ?? "-"}</td>
          <td>${business.rank_stddev}</td>
          <td>${business.top_3_count}</td>
          <td>${business.rating ?? "-"} (${business.review_count ?? "-"})</td>
        </tr>`
      )
      .join("")}</tbody>`;
  }

  function renderMiniList(rows: GeoGridBusinessStats[]): string {
    if (!rows.length) return '<p class="muted">Sin datos.</p>';
    return `<ol>${rows
      .map(
        (item) =>
          `<li>${escapeHtml(item.business_name)} <span>${item.coverage_percent}% · avg ${item.avg_rank ?? "-"} · std ${item.rank_stddev}</span></li>`
      )
      .join("")}</ol>`;
  }

  function estimateProgress(run: GeoGridRunItem): number {
    const total = Number(run.total_units || 0);
    if (total <= 0) return 0;
    return Math.max(0, Math.min(100, Math.round((Number(run.completed_units || 0) / total) * 100)));
  }

  function resolveRankColor(rank: number | null): string {
    if (!rank) return "#8b8178";
    if (rank <= 3) return "#2f9e44";
    if (rank <= 10) return "#d9a322";
    return "#c44c2f";
  }

  function formatDate(value?: string | null): string {
    if (!value) return "-";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
  }

  function appendLabeled(parent: HTMLElement, label: string, control: HTMLElement): void {
    const wrapper = createElement("label", "form-field");
    wrapper.append(createElement("span", "form-field__label", label), control);
    parent.append(wrapper);
  }

  function showError(error: unknown): void {
    launchStatus.textContent = formatError(error);
    runStatus.textContent = formatError(error);
  }

  function updatePollingState(): void {
    if (!selectedRun || !ACTIVE_STATUSES.has(selectedRun.status)) {
      stopPolling();
      return;
    }
    startPolling();
  }

  function startPolling(): void {
    if (pollTimer !== null) return;
    pollTimer = window.setInterval(() => {
      if (selectedRun?.geo_grid_run_id) {
        void loadRunDetail(selectedRun.geo_grid_run_id).catch(showError);
      }
      void loadRuns().catch(showError);
    }, 4000);
  }

  function stopPolling(): void {
    if (pollTimer === null) return;
    window.clearInterval(pollTimer);
    pollTimer = null;
  }

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    launchStatus.textContent = "Encolando estudio...";
    const providerMode = providerModeSelect.value === "uule" ? "uule" : "maps_live";
    const body: Record<string, unknown> = {
      keyword: keywordInput.value.trim(),
      city_slug: citySelect.value,
      top_n: Number(topNSelect.value || 10),
      provider_mode: providerMode,
    };
    if (providerMode === "uule") {
      body.grid_size = Number(gridSizeInput.value || 7);
      body.grid_spacing_km = Number(gridSpacingInput.value || 0.4);
      body.uule_radius_m = Number(uuleRadiusInput.value || 1000);
      body.throttle_ms = Number(throttleInput.value || 1200);
    }
    deps.apiClient
      .post<{ job_id?: string; geo_grid_run_id?: string }>("/crm/geo-grid-runs", {
        ...body,
      })
      .then(async (response) => {
        launchStatus.textContent = `Estudio lanzado: ${response.geo_grid_run_id || ""}`;
        await loadRuns();
        if (response.geo_grid_run_id) await loadRunDetail(response.geo_grid_run_id);
      })
      .catch(showError);
  });

  refreshRunsButton.addEventListener("click", () => void loadRuns().catch(showError));
  runCityFilter.addEventListener("change", () => void loadRuns().catch(showError));
  runStatusFilter.addEventListener("change", () => void loadRuns().catch(showError));
  providerModeSelect.addEventListener("change", () => syncProviderControls());
  businessSearch.addEventListener("input", () => {
    renderBusinessSelector();
    renderBusinessTable();
  });
  businessSelect.addEventListener("change", () => {
    renderMap();
    renderBusinessTable();
  });
  openJobButton.addEventListener("click", () => {
    if (selectedRun?.job_id && deps.onJobQueued) deps.onJobQueued(selectedRun.job_id);
  });

  return {
    key: "studies",
    title: "Estudios",
    root,
    onShow: () => {
      void loadCities()
        .then(loadRuns)
        .catch(showError);
      syncProviderControls();
      updatePollingState();
    },
    onHide: () => {
      stopPolling();
      if (leafletMap) {
        leafletMap.remove();
        leafletMap = null;
      }
    },
  };
}
