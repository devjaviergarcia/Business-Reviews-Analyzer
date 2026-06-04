import { AnimationController } from "../animations/controller";
import { createButton } from "../components/atoms/button";
import { createInput } from "../components/atoms/input";
import { ApiClient } from "../core/api-client";
import { clearElement, createElement, escapeHtml, formatError } from "../core/dom";
import type { CRMCampaignItem, CRMLeadItem, PaginatedResponse, ViewModule } from "../core/types";

type CRMViewDeps = {
  apiClient: ApiClient;
  onJobQueued?: (jobId: string) => void;
};

type LeadSortField = "updated_at" | "business_name" | "score" | "status" | "consent_status" | "source";
type LeadSortDir = "asc" | "desc";

type LeadsQueryFilters = {
  q: string;
  status: string;
  consent_status: string;
  source: string;
};

export function createCRMView(deps: CRMViewDeps): ViewModule {
  const root = createElement("section", "view-panel");
  root.classList.add("crm-view");

  const discoveryPanel = createElement("section", "panel form-panel");
  discoveryPanel.append(createElement("h2", "panel__title", "CRM · Búsqueda de leads"));
  const discoveryForm = createElement("form", "form-grid") as HTMLFormElement;
  const queryInput = createInput({ placeholder: "restaurante cordoba" });
  const cityInput = createInput({ placeholder: "Ciudad (opcional)" });
  const categoryInput = createInput({ placeholder: "Categoría (opcional)" });
  const limitInput = createInput({ type: "number", min: "1", max: "5000", value: "100" });
  appendLabeled(discoveryForm, "Búsqueda", queryInput);
  appendLabeled(discoveryForm, "Ciudad", cityInput);
  appendLabeled(discoveryForm, "Categoría", categoryInput);
  appendLabeled(discoveryForm, "Límite", limitInput);

  const discoveryActions = createElement("div", "form-actions");
  const discoveryStatus = createElement("span", "muted", "");
  const discoveryButton = createButton({ label: "Lanzar discovery", tone: "orange", type: "submit" });
  discoveryActions.append(discoveryButton, discoveryStatus);
  discoveryForm.append(discoveryActions);
  discoveryPanel.append(discoveryForm);

  const discoveryRunsPanel = createElement("section", "panel form-panel");
  discoveryRunsPanel.append(createElement("h2", "panel__title", "Discovery Runs"));
  const discoveryRunsActions = createElement("div", "form-actions");
  const discoveryRunsRefreshButton = createButton({ label: "Actualizar runs", tone: "turquoise" });
  const discoveryRunsStatus = createElement("span", "muted", "");
  discoveryRunsActions.append(discoveryRunsRefreshButton, discoveryRunsStatus);
  const discoveryRunsTableWrap = createElement("div", "scroll-table");
  const discoveryRunsTable = createElement("table", "data-table");
  discoveryRunsTableWrap.append(discoveryRunsTable);
  discoveryRunsPanel.append(discoveryRunsActions, discoveryRunsTableWrap);

  const leadsPanel = createElement("section", "panel form-panel");
  leadsPanel.append(createElement("h2", "panel__title", "Negocios CRM (discovery)"));
  const leadsControls = createElement("div", "form-grid");
  const leadsSearch = createInput({ placeholder: "Buscar por nombre, email, web" });
  const leadsStatusFilter = createElement("select", "atom-input") as HTMLSelectElement;
  leadsStatusFilter.innerHTML = [
    '<option value="">Todos los estados</option>',
    '<option value="prospecto">prospecto</option>',
    '<option value="contactado">contactado</option>',
    '<option value="lead_report_sent">lead_report_sent</option>',
    '<option value="form_1_done">form_1_done</option>',
    '<option value="full_report_sent">full_report_sent</option>',
    '<option value="form_2_done">form_2_done</option>',
    '<option value="cliente">cliente</option>',
    '<option value="new">new</option>',
    '<option value="enriching">enriching</option>',
    '<option value="ready">ready</option>',
    '<option value="pipeline_queued">pipeline_queued</option>',
    '<option value="pipeline_running">pipeline_running</option>',
    '<option value="pipeline_done">pipeline_done</option>',
    '<option value="contactable">contactable</option>',
    '<option value="paused">paused</option>',
    '<option value="won">won</option>',
    '<option value="lost">lost</option>',
  ].join("");
  const consentFilter = createElement("select", "atom-input") as HTMLSelectElement;
  consentFilter.innerHTML = [
    '<option value="">Consentimiento: todos</option>',
    '<option value="missing">missing</option>',
    '<option value="granted">granted</option>',
    '<option value="revoked">revoked</option>',
    '<option value="denied">denied</option>',
  ].join("");
  const leadsSourceFilter = createElement("select", "atom-input") as HTMLSelectElement;
  leadsSourceFilter.innerHTML = [
    '<option value="" selected>Todas las fuentes</option>',
    '<option value="landing">landing</option>',
    '<option value="manual">manual</option>',
    '<option value="google_maps_live_discovery">Solo discovery live</option>',
    '<option value="google_maps">google_maps</option>',
    '<option value="tripadvisor">tripadvisor</option>',
    '<option value="research_google_maps">research_google_maps</option>',
  ].join("");
  const leadsPageSizeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  leadsPageSizeSelect.innerHTML = [
    '<option value="25">25 / página</option>',
    '<option value="50" selected>50 / página</option>',
    '<option value="100">100 / página</option>',
    '<option value="200">200 / página</option>',
  ].join("");
  const leadsSortBy = createElement("select", "atom-input") as HTMLSelectElement;
  leadsSortBy.innerHTML = [
    '<option value="updated_at" selected>Orden: última actualización</option>',
    '<option value="business_name">Orden: nombre</option>',
    '<option value="score">Orden: score</option>',
    '<option value="status">Orden: estado</option>',
    '<option value="consent_status">Orden: consentimiento</option>',
    '<option value="source">Orden: fuente</option>',
  ].join("");
  const leadsSortDir = createElement("select", "atom-input") as HTMLSelectElement;
  leadsSortDir.innerHTML = [
    '<option value="desc" selected>Descendente</option>',
    '<option value="asc">Ascendente</option>',
  ].join("");

  appendLabeled(leadsControls, "Buscar", leadsSearch);
  appendLabeled(leadsControls, "Estado", leadsStatusFilter);
  appendLabeled(leadsControls, "Consentimiento", consentFilter);
  appendLabeled(leadsControls, "Fuente", leadsSourceFilter);
  appendLabeled(leadsControls, "Tamaño página", leadsPageSizeSelect);
  appendLabeled(leadsControls, "Ordenar por", leadsSortBy);
  appendLabeled(leadsControls, "Dirección", leadsSortDir);

  const leadsActions = createElement("div", "form-actions");
  const leadsRefreshButton = createButton({ label: "Actualizar leads", tone: "turquoise" });
  const selectAllMatchingButton = createButton({ label: "Seleccionar todos (filtro)", tone: "white" });
  const clearSelectionButton = createButton({ label: "Limpiar selección", tone: "white" });
  const deleteSelectedButton = createButton({ label: "Eliminar seleccionados", tone: "orange" });
  const leadsStatusLabel = createElement("span", "muted", "");
  leadsActions.append(
    leadsRefreshButton,
    selectAllMatchingButton,
    clearSelectionButton,
    deleteSelectedButton,
    leadsStatusLabel
  );

  const leadsPagination = createElement("div", "form-actions");
  const leadsPrevPageButton = createButton({ label: "← Anterior", tone: "white" });
  const leadsNextPageButton = createButton({ label: "Siguiente →", tone: "white" });
  const leadsPageLabel = createElement("span", "muted", "Página 1");
  leadsPagination.append(leadsPrevPageButton, leadsNextPageButton, leadsPageLabel);

  const leadsTableWrap = createElement("div", "scroll-table");
  const leadsTable = createElement("table", "data-table");
  leadsTableWrap.append(leadsTable);

  const leadDetail = createElement("pre", "code-block");
  leadDetail.textContent = "Selecciona un lead para ver detalle";

  leadsPanel.append(
    leadsControls,
    leadsActions,
    leadsPagination,
    leadsTableWrap,
    createElement("h3", "panel__subtitle", "Detalle lead"),
    leadDetail
  );

  const campaignsPanel = createElement("section", "panel form-panel");
  campaignsPanel.append(createElement("h2", "panel__title", "Campañas"));
  const campaignForm = createElement("form", "form-grid") as HTMLFormElement;
  const campaignNameInput = createInput({ placeholder: "Nombre de campaña" });
  const campaignSourceMode = createElement("select", "atom-input") as HTMLSelectElement;
  campaignSourceMode.innerHTML = [
    '<option value="auto">auto (usar fuentes disponibles)</option>',
    '<option value="combined">combined (mezclar fuentes)</option>',
    '<option value="single">single (una fuente)</option>',
  ].join("");
  const campaignSelectedSource = createElement("select", "atom-input") as HTMLSelectElement;
  campaignSelectedSource.innerHTML = [
    '<option value="">(ninguna)</option>',
    '<option value="google_maps">google_maps</option>',
    '<option value="tripadvisor">tripadvisor</option>',
  ].join("");
  appendLabeled(campaignForm, "Nombre", campaignNameInput);
  appendLabeled(campaignForm, "source_mode", campaignSourceMode);
  appendLabeled(campaignForm, "selected_source", campaignSelectedSource);
  const campaignActions = createElement("div", "form-actions");
  const campaignCreateButton = createButton({ label: "Crear campaña", tone: "orange", type: "submit" });
  const campaignRefreshButton = createButton({ label: "Actualizar campañas", tone: "turquoise" });
  const campaignStatusLabel = createElement("span", "muted", "");
  campaignActions.append(campaignCreateButton, campaignRefreshButton, campaignStatusLabel);
  campaignForm.append(campaignActions);

  const campaignsTableWrap = createElement("div", "scroll-table");
  const campaignsTable = createElement("table", "data-table");
  campaignsTableWrap.append(campaignsTable);
  campaignsPanel.append(campaignForm, campaignsTableWrap);

  const activityPanel = createElement("section", "panel form-panel");
  activityPanel.append(createElement("h2", "panel__title", "Actividad CRM"));
  const activityActions = createElement("div", "form-actions");
  const eventsRefreshButton = createButton({ label: "Actualizar actividad", tone: "turquoise" });
  const eventsStatusLabel = createElement("span", "muted", "");
  activityActions.append(eventsRefreshButton, eventsStatusLabel);
  const eventsTableWrap = createElement("div", "scroll-table");
  const eventsTable = createElement("table", "data-table");
  eventsTableWrap.append(eventsTable);
  activityPanel.append(activityActions, eventsTableWrap);

  root.append(discoveryPanel, discoveryRunsPanel, leadsPanel, campaignsPanel, activityPanel);

  let leadsCache: CRMLeadItem[] = [];
  let leadsPage = 1;
  let leadsPageSize = 50;
  let leadsTotal = 0;
  let leadsTotalPages = 0;
  let leadsHasNext = false;
  let leadsHasPrev = false;

  let allMatchingSelected = false;
  const selectedLeadIds = new Set<string>();
  const deselectedLeadIds = new Set<string>();

  function buildLeadsFilters(): LeadsQueryFilters {
    return {
      q: leadsSearch.value.trim(),
      status: leadsStatusFilter.value.trim(),
      consent_status: consentFilter.value.trim(),
      source: leadsSourceFilter.value.trim(),
    };
  }

  function getLeadId(item: CRMLeadItem): string {
    return String(item.lead_id || "").trim();
  }

  function isLeadSelected(leadId: string): boolean {
    if (!leadId) return false;
    if (allMatchingSelected) {
      return !deselectedLeadIds.has(leadId);
    }
    return selectedLeadIds.has(leadId);
  }

  function selectedLeadCount(): number {
    if (allMatchingSelected) {
      return Math.max(0, leadsTotal - deselectedLeadIds.size);
    }
    return selectedLeadIds.size;
  }

  function resetLeadSelection(): void {
    allMatchingSelected = false;
    selectedLeadIds.clear();
    deselectedLeadIds.clear();
  }

  function updateLeadsMetaLabel(): void {
    const selected = selectedLeadCount();
    const current = leadsCache.length;
    leadsStatusLabel.textContent = `Mostrando ${current} de ${leadsTotal} · seleccionados ${selected}`;
  }

  function updateLeadsPaginationLabel(): void {
    const currentPage = Math.max(1, leadsPage);
    const totalPages = Math.max(1, leadsTotalPages || 1);
    leadsPageLabel.textContent = `Página ${currentPage} / ${totalPages}`;
    leadsPrevPageButton.disabled = !leadsHasPrev;
    leadsNextPageButton.disabled = !leadsHasNext;
  }

  function toggleLeadSort(field: LeadSortField): void {
    const currentField = (leadsSortBy.value.trim() || "updated_at") as LeadSortField;
    const currentDir = (leadsSortDir.value.trim() || "desc") as LeadSortDir;
    const nextDir: LeadSortDir = currentField === field ? (currentDir === "desc" ? "asc" : "desc") : "desc";
    leadsSortBy.value = field;
    leadsSortDir.value = nextDir;
    void refreshLeads({ resetPage: true });
  }

  async function refreshLeads(options?: { resetPage?: boolean; resetSelection?: boolean }): Promise<void> {
    if (options?.resetPage) {
      leadsPage = 1;
    }
    if (options?.resetSelection) {
      resetLeadSelection();
    }

    leadsStatusLabel.textContent = "Cargando leads...";
    try {
      const requestedPageSize = Number.parseInt(leadsPageSizeSelect.value.trim() || "50", 10);
      leadsPageSize = Number.isFinite(requestedPageSize) ? Math.max(1, Math.min(requestedPageSize, 200)) : 50;
      const sortBy = (leadsSortBy.value.trim() || "updated_at") as LeadSortField;
      const sortDir = (leadsSortDir.value.trim() || "desc") as LeadSortDir;
      const filters = buildLeadsFilters();

      const params = new URLSearchParams();
      params.set("page", String(leadsPage));
      params.set("page_size", String(leadsPageSize));
      params.set("sort_by", sortBy);
      params.set("sort_dir", sortDir);
      if (filters.q) params.set("q", filters.q);
      if (filters.status) params.set("status", filters.status);
      if (filters.consent_status) params.set("consent_status", filters.consent_status);
      if (filters.source) params.set("source", filters.source);

      const response = await deps.apiClient.get<PaginatedResponse<CRMLeadItem>>(`/crm/leads?${params.toString()}`);
      leadsCache = Array.isArray(response.items) ? response.items : [];
      leadsTotal = Number(response.total || 0);
      leadsTotalPages = Number(response.total_pages || 0);
      leadsHasNext = Boolean(response.has_next);
      leadsHasPrev = Boolean(response.has_prev);
      leadsPage = Number(response.page || leadsPage || 1);
      renderLeadsTable(leadsCache);
      updateLeadsPaginationLabel();
      updateLeadsMetaLabel();
    } catch (error) {
      leadsStatusLabel.textContent = `ERROR: ${formatError(error)}`;
      leadsCache = [];
      leadsTotal = 0;
      leadsTotalPages = 0;
      leadsHasNext = false;
      leadsHasPrev = false;
      renderLeadsTable([]);
      updateLeadsPaginationLabel();
    }
  }

  function renderLeadsTable(items: CRMLeadItem[]): void {
    clearElement(leadsTable);
    const thead = createElement("thead");
    const headRow = createElement("tr");

    const selectAllHead = createElement("th");
    const selectAllCheckbox = createInput({ type: "checkbox" }) as HTMLInputElement;
    selectAllHead.append(selectAllCheckbox);

    const visibleLeadIds = items.map((item) => getLeadId(item)).filter((value) => Boolean(value));
    const selectedVisibleCount = visibleLeadIds.filter((leadId) => isLeadSelected(leadId)).length;
    const allVisibleSelected = Boolean(visibleLeadIds.length && selectedVisibleCount === visibleLeadIds.length);
    const partiallySelected = Boolean(selectedVisibleCount > 0 && selectedVisibleCount < visibleLeadIds.length);
    selectAllCheckbox.checked = allVisibleSelected;
    selectAllCheckbox.indeterminate = partiallySelected;
    selectAllCheckbox.addEventListener("change", () => {
      const checked = Boolean(selectAllCheckbox.checked);
      for (const leadId of visibleLeadIds) {
        if (allMatchingSelected) {
          if (checked) {
            deselectedLeadIds.delete(leadId);
          } else {
            deselectedLeadIds.add(leadId);
          }
        } else if (checked) {
          selectedLeadIds.add(leadId);
        } else {
          selectedLeadIds.delete(leadId);
        }
      }
      renderLeadsTable(leadsCache);
      updateLeadsMetaLabel();
    });

    const activeSortBy = (leadsSortBy.value.trim() || "updated_at") as LeadSortField;
    const activeSortDir = (leadsSortDir.value.trim() || "desc") as LeadSortDir;
    const sortArrow = activeSortDir === "asc" ? "↑" : "↓";

    const createSortableHeader = (label: string, field: LeadSortField): HTMLTableCellElement => {
      const th = createElement("th");
      const button = createElement("button", "table-sort-button", label) as HTMLButtonElement;
      button.type = "button";
      if (activeSortBy === field) {
        button.classList.add("is-active");
        button.textContent = `${label} ${sortArrow}`;
      }
      button.addEventListener("click", () => {
        toggleLeadSort(field);
      });
      th.append(button);
      return th;
    };

    headRow.append(createSortableHeader("Lead", "business_name"));
    headRow.append(createElement("th", "", "Contacto"));
    headRow.append(createSortableHeader("Estado", "status"));
    headRow.append(createSortableHeader("Consent", "consent_status"));
    headRow.append(createSortableHeader("Fuente", "source"));
    headRow.append(createSortableHeader("Score", "score"));
    headRow.append(createSortableHeader("Act.", "updated_at"));
    headRow.append(createElement("th", "", "Acciones"));
    headRow.prepend(selectAllHead);
    thead.append(headRow);
    leadsTable.append(thead);

    const tbody = createElement("tbody");
    if (!items.length) {
      const row = createElement("tr");
      row.innerHTML = '<td colspan="9" class="muted">No hay leads.</td>';
      tbody.append(row);
      leadsTable.append(tbody);
      return;
    }

    for (const item of items) {
      const row = createElement("tr");
      const leadId = getLeadId(item);
      const leadName = escapeHtml(String(item.business_name || "(sin nombre)"));
      const contact = [item.phone, item.website]
        .filter((v) => String(v || "").trim())
        .join(" · ");
      const consentStatus = String(item.legal?.consent_status || "missing");
      const source = String(item.source || "-");
      const status = String(item.status || "-");
      const score = typeof item.score === "number" ? item.score.toFixed(1) : "-";
      const updatedAt = String(item.updated_at || "").trim();

      const selectCell = createElement("td");
      const rowCheckbox = createInput({ type: "checkbox" }) as HTMLInputElement;
      rowCheckbox.checked = isLeadSelected(leadId);
      rowCheckbox.addEventListener("change", () => {
        const checked = Boolean(rowCheckbox.checked);
        if (allMatchingSelected) {
          if (checked) {
            deselectedLeadIds.delete(leadId);
          } else {
            deselectedLeadIds.add(leadId);
          }
        } else if (checked) {
          selectedLeadIds.add(leadId);
        } else {
          selectedLeadIds.delete(leadId);
        }
        renderLeadsTable(leadsCache);
        updateLeadsMetaLabel();
      });
      selectCell.append(rowCheckbox);

      const actionsCell = createElement("td");
      const viewButton = createButton({ label: "Ver", tone: "white" });
      viewButton.addEventListener("click", () => {
        void loadLeadDetail(leadId);
      });
      const pipelineButton = createButton({ label: "Pipeline", tone: "orange" });
      pipelineButton.addEventListener("click", () => {
        void launchLeadPipeline({
          leadId,
          businessName: String(item.business_name || "").trim(),
        });
      });
      const grantConsentButton = createButton({ label: "Consent+", tone: "turquoise" });
      grantConsentButton.addEventListener("click", () => {
        void grantLeadConsent(leadId);
      });
      actionsCell.append(viewButton, pipelineButton, grantConsentButton);

      row.innerHTML = `<td><strong>${leadName}</strong><br><span class="muted">${escapeHtml(leadId)}</span></td><td>${escapeHtml(
        contact || "-"
      )}</td><td>${escapeHtml(status)}</td><td>${escapeHtml(consentStatus)}</td><td>${escapeHtml(
        source
      )}</td><td>${escapeHtml(score)}</td><td>${escapeHtml(updatedAt || "-")}</td>`;
      row.prepend(selectCell);
      row.append(actionsCell);
      tbody.append(row);
    }

    leadsTable.append(tbody);
  }

  async function deleteSelectedLeads(): Promise<void> {
    const selectedCount = selectedLeadCount();
    if (selectedCount <= 0) {
      leadsStatusLabel.textContent = "No hay leads seleccionados para eliminar.";
      return;
    }

    const confirmationMessage = allMatchingSelected
      ? `Se eliminarán ${selectedCount} leads que coinciden con el filtro actual. ¿Continuar?`
      : `Se eliminarán ${selectedCount} leads seleccionados. ¿Continuar?`;
    if (!window.confirm(confirmationMessage)) {
      return;
    }

    leadsStatusLabel.textContent = "Eliminando leads...";
    try {
      const filters = buildLeadsFilters();
      const payload: Record<string, unknown> = allMatchingSelected
        ? {
            delete_all_matching: true,
            exclude_lead_ids: Array.from(deselectedLeadIds.values()),
            status: filters.status || null,
            consent_status: filters.consent_status || null,
            source: filters.source || null,
            q: filters.q || null,
          }
        : {
            lead_ids: Array.from(selectedLeadIds.values()),
          };
      const response = await deps.apiClient.post<Record<string, unknown>>(`/crm/leads/bulk-delete`, payload);
      const deletedCount = Number(response.deleted_count || 0);
      leadsStatusLabel.textContent = `Eliminados ${deletedCount} leads.`;
      resetLeadSelection();
      await refreshLeads();
    } catch (error) {
      leadsStatusLabel.textContent = `ERROR eliminando leads: ${formatError(error)}`;
    }
  }

  async function loadLeadDetail(leadId: string): Promise<void> {
    leadDetail.textContent = "Cargando detalle...";
    try {
      const payload = await deps.apiClient.get<Record<string, unknown>>(
        `/crm/leads/${encodeURIComponent(leadId)}?sync_pipeline_refs=true`
      );
      leadDetail.textContent = JSON.stringify(payload, null, 2);
      await refreshLeads();
    } catch (error) {
      leadDetail.textContent = `ERROR: ${formatError(error)}`;
    }
  }

  async function launchLeadPipeline(payload: { leadId: string; businessName: string }): Promise<void> {
    const leadId = String(payload.leadId || "").trim();
    const businessName = String(payload.businessName || "").trim();
    if (!businessName) {
      leadsStatusLabel.textContent = `ERROR pipeline: el lead ${leadId || "(sin id)"} no tiene nombre de negocio.`;
      return;
    }

    leadsStatusLabel.textContent = `Lanzando pipeline para ${businessName}...`;
    try {
      const response = await deps.apiClient.post<Record<string, unknown>>(`/business/scrape/jobs`, {
        name: businessName,
        force: false,
      });
      const queuedJobId = String(response.job_id || response.primary_job_id || "").trim();
      if (queuedJobId && deps.onJobQueued) {
        deps.onJobQueued(queuedJobId);
      }
      if (leadId) {
        try {
          await deps.apiClient.patch<Record<string, unknown>>(`/crm/leads/${encodeURIComponent(leadId)}`, {
            status: "pipeline_queued",
          });
        } catch {
          // El job ya está encolado; si falla el patch de estado no bloqueamos.
        }
      }
      leadsStatusLabel.textContent = `Pipeline en cola para ${businessName} (${queuedJobId || "sin job_id"}).`;
      await refreshLeads();
    } catch (error) {
      leadsStatusLabel.textContent = `ERROR pipeline: ${formatError(error)}`;
    }
  }

  async function grantLeadConsent(leadId: string): Promise<void> {
    leadsStatusLabel.textContent = `Actualizando consentimiento de ${leadId}...`;
    try {
      const nowIso = new Date().toISOString();
      await deps.apiClient.patch<Record<string, unknown>>(`/crm/leads/${encodeURIComponent(leadId)}`, {
        consent_status: "granted",
        consent_proof: {
          granted_at: nowIso,
          source: "manual_ui",
          legal_text_version: "v1",
          evidence: "Consentimiento registrado manualmente desde manager UI",
        },
      });
      leadsStatusLabel.textContent = `Consentimiento concedido para ${leadId}`;
      await refreshLeads();
    } catch (error) {
      leadsStatusLabel.textContent = `ERROR consentimiento: ${formatError(error)}`;
    }
  }

  async function refreshCampaigns(): Promise<void> {
    campaignStatusLabel.textContent = "Cargando campañas...";
    try {
      const response = await deps.apiClient.get<PaginatedResponse<CRMCampaignItem>>(`/crm/campaigns?page=1&page_size=100`);
      const items = Array.isArray(response.items) ? response.items : [];
      renderCampaignsTable(items);
      campaignStatusLabel.textContent = `Campañas: ${items.length}`;
    } catch (error) {
      campaignStatusLabel.textContent = `ERROR: ${formatError(error)}`;
      renderCampaignsTable([]);
    }
  }

  function renderCampaignsTable(items: CRMCampaignItem[]): void {
    clearElement(campaignsTable);
    const thead = createElement("thead");
    thead.innerHTML = "<tr><th>Campaña</th><th>Estado</th><th>Scope</th><th>Métricas</th><th>Acciones</th></tr>";
    campaignsTable.append(thead);

    const tbody = createElement("tbody");
    if (!items.length) {
      const row = createElement("tr");
      row.innerHTML = '<td colspan="5" class="muted">No hay campañas.</td>';
      tbody.append(row);
      campaignsTable.append(tbody);
      return;
    }

    for (const item of items) {
      const row = createElement("tr");
      const campaignId = String(item.campaign_id || "").trim();
      const metrics = item.metrics && typeof item.metrics === "object" ? item.metrics : {};
      const targeted = Number(metrics["targeted_leads"] || 0);
      const sent = Number(metrics["messages_sent"] || 0);
      const actionsCell = createElement("td");
      const launchButton = createButton({ label: "Lanzar", tone: "orange" });
      launchButton.addEventListener("click", () => {
        void launchCampaign(campaignId);
      });
      const dispatchButton = createButton({ label: "Programar envíos", tone: "turquoise" });
      dispatchButton.addEventListener("click", () => {
        void enqueueCampaignDueDispatch(campaignId);
      });
      actionsCell.append(launchButton, dispatchButton);

      row.innerHTML = `<td><strong>${escapeHtml(String(item.name || "(sin nombre)"))}</strong><br><span class="muted">${escapeHtml(
        campaignId
      )}</span></td><td>${escapeHtml(String(item.status || "-"))}</td><td>${escapeHtml(
        String(item.source_mode || "auto")
      )}${item.selected_source ? ` · ${escapeHtml(String(item.selected_source))}` : ""}</td><td>${escapeHtml(
        `targeted=${targeted} · sent=${sent}`
      )}</td>`;
      row.append(actionsCell);
      tbody.append(row);
    }
    campaignsTable.append(tbody);
  }

  async function createCampaign(): Promise<void> {
    campaignStatusLabel.textContent = "Creando campaña...";
    try {
      const name = campaignNameInput.value.trim();
      if (!name) {
        campaignStatusLabel.textContent = "Escribe un nombre de campaña.";
        return;
      }
      const sourceMode = campaignSourceMode.value;
      const selectedSourceValue = campaignSelectedSource.value.trim();
      await deps.apiClient.post<Record<string, unknown>>(`/crm/campaigns`, {
        name,
        source_mode: sourceMode,
        selected_source: sourceMode === "single" && selectedSourceValue ? selectedSourceValue : null,
        audience_filter: {},
      });
      campaignStatusLabel.textContent = `Campaña '${name}' creada.`;
      campaignNameInput.value = "";
      await refreshCampaigns();
    } catch (error) {
      campaignStatusLabel.textContent = `ERROR creando campaña: ${formatError(error)}`;
    }
  }

  async function launchCampaign(campaignId: string): Promise<void> {
    campaignStatusLabel.textContent = `Lanzando campaña ${campaignId}...`;
    try {
      const response = await deps.apiClient.post<Record<string, unknown>>(
        `/crm/campaigns/${encodeURIComponent(campaignId)}/launch`,
        {}
      );
      campaignStatusLabel.textContent = `Campaña lanzada. Mensajes creados: ${String(response.messages_created || 0)}`;
      await refreshCampaigns();
      await refreshEvents();
    } catch (error) {
      campaignStatusLabel.textContent = `ERROR al lanzar campaña: ${formatError(error)}`;
    }
  }

  async function enqueueCampaignDueDispatch(campaignId: string): Promise<void> {
    campaignStatusLabel.textContent = `Programando envíos para ${campaignId}...`;
    try {
      const response = await deps.apiClient.post<Record<string, unknown>>(
        `/crm/campaigns/dispatch-due?campaign_id=${encodeURIComponent(campaignId)}&limit=500`,
        {}
      );
      campaignStatusLabel.textContent = `Dispatch jobs en cola: ${String(response.queued_dispatch_jobs || 0)}`;
      await refreshEvents();
    } catch (error) {
      campaignStatusLabel.textContent = `ERROR al programar envíos: ${formatError(error)}`;
    }
  }

  async function refreshEvents(): Promise<void> {
    eventsStatusLabel.textContent = "Cargando actividad...";
    try {
      const response = await deps.apiClient.get<PaginatedResponse<Record<string, unknown>>>(`/crm/events?page=1&page_size=80`);
      const items = Array.isArray(response.items) ? response.items : [];
      renderEventsTable(items);
      eventsStatusLabel.textContent = `Eventos: ${items.length}`;
    } catch (error) {
      eventsStatusLabel.textContent = `ERROR: ${formatError(error)}`;
      renderEventsTable([]);
    }
  }

  function renderEventsTable(items: Record<string, unknown>[]): void {
    clearElement(eventsTable);
    const thead = createElement("thead");
    thead.innerHTML = "<tr><th>Fecha</th><th>Tipo</th><th>Lead</th><th>Campaña</th><th>Detalle</th></tr>";
    eventsTable.append(thead);

    const tbody = createElement("tbody");
    if (!items.length) {
      const row = createElement("tr");
      row.innerHTML = '<td colspan="5" class="muted">Sin actividad.</td>';
      tbody.append(row);
      eventsTable.append(tbody);
      return;
    }

    for (const item of items) {
      const row = createElement("tr");
      const createdAt = String(item.created_at || "");
      const eventType = String(item.event_type || "event");
      const leadId = String(item.lead_id || "-");
      const campaignId = String(item.campaign_id || "-");
      const data = item.data && typeof item.data === "object" ? JSON.stringify(item.data) : "";
      row.innerHTML = `<td>${escapeHtml(createdAt)}</td><td>${escapeHtml(eventType)}</td><td>${escapeHtml(
        leadId
      )}</td><td>${escapeHtml(campaignId)}</td><td><span class="muted">${escapeHtml(data).slice(0, 220)}</span></td>`;
      tbody.append(row);
    }
    eventsTable.append(tbody);
  }

  async function refreshDiscoveryRuns(): Promise<void> {
    discoveryRunsStatus.textContent = "Cargando runs...";
    try {
      const response = await deps.apiClient.get<PaginatedResponse<Record<string, unknown>>>(
        `/crm/discovery-runs?page=1&page_size=80`
      );
      const items = Array.isArray(response.items) ? response.items : [];
      renderDiscoveryRunsTable(items);
      discoveryRunsStatus.textContent = `Runs: ${items.length}`;
    } catch (error) {
      discoveryRunsStatus.textContent = `ERROR runs: ${formatError(error)}`;
      renderDiscoveryRunsTable([]);
    }
  }

  function renderDiscoveryRunsTable(items: Record<string, unknown>[]): void {
    clearElement(discoveryRunsTable);
    const thead = createElement("thead");
    thead.innerHTML =
      "<tr><th>Run</th><th>Estado</th><th>Query</th><th>Candidatos</th><th>Insertados</th><th>Actualizados</th><th>Inicio</th><th>Acción</th></tr>";
    discoveryRunsTable.append(thead);

    const tbody = createElement("tbody");
    if (!items.length) {
      const row = createElement("tr");
      row.innerHTML = '<td colspan="8" class="muted">Sin runs.</td>';
      tbody.append(row);
      discoveryRunsTable.append(tbody);
      return;
    }

    for (const item of items) {
      const metrics = item.metrics && typeof item.metrics === "object" ? (item.metrics as Record<string, unknown>) : {};
      const runId = String(item.discovery_run_id || "-");
      const status = String(item.status || "-");
      const query = String(item.query || "-");
      const candidates = Number(metrics.cards_seen || 0);
      const inserted = Number(metrics.inserted || 0);
      const updated = Number(metrics.updated || 0);
      const startedAt = String(item.started_at || item.created_at || "-");
      const row = createElement("tr");
      row.innerHTML = `<td><span class=\"mono\">${escapeHtml(runId).slice(0, 12)}</span></td><td>${escapeHtml(
        status
      )}</td><td>${escapeHtml(query)}</td><td>${candidates}</td><td>${inserted}</td><td>${updated}</td><td>${escapeHtml(
        startedAt
      )}</td><td><button class=\"btn btn--white btn--sm\" data-run-query=\"${escapeHtml(query)}\">Ver leads</button></td>`;
      const button = row.querySelector("button[data-run-query]") as HTMLButtonElement | null;
      if (button) {
        button.addEventListener("click", () => {
          leadsSearch.value = query;
          void refreshLeads({ resetPage: true, resetSelection: true });
        });
      }
      tbody.append(row);
    }
    discoveryRunsTable.append(tbody);
  }

  discoveryForm.addEventListener("submit", (event) => {
    event.preventDefault();
    void (async () => {
      discoveryStatus.textContent = "Encolando discovery...";
      try {
        const query = queryInput.value.trim();
        if (!query) {
          discoveryStatus.textContent = "Escribe una búsqueda.";
          return;
        }

        const limitValue = Number.parseInt(limitInput.value.trim() || "100", 10);
        const payload = {
          query,
          city: cityInput.value.trim() || null,
          category: categoryInput.value.trim() || null,
          limit: Number.isFinite(limitValue) ? Math.max(1, Math.min(limitValue, 5000)) : 100,
          source: "auto_live_google_maps",
        };
        const response = await deps.apiClient.post<Record<string, unknown>>(`/crm/leads/discovery-jobs`, payload);
        discoveryStatus.textContent = `Discovery en cola (job ${String(response.job_id || "-")})`;
        resetLeadSelection();
        await refreshLeads({ resetPage: true });
        await refreshDiscoveryRuns();
      } catch (error) {
        discoveryStatus.textContent = `ERROR discovery: ${formatError(error)}`;
      }
    })();
  });

  campaignForm.addEventListener("submit", (event) => {
    event.preventDefault();
    void createCampaign();
  });

  leadsRefreshButton.addEventListener("click", () => {
    void refreshLeads();
  });
  discoveryRunsRefreshButton.addEventListener("click", () => {
    void refreshDiscoveryRuns();
  });
  selectAllMatchingButton.addEventListener("click", () => {
    allMatchingSelected = true;
    selectedLeadIds.clear();
    deselectedLeadIds.clear();
    renderLeadsTable(leadsCache);
    updateLeadsMetaLabel();
  });
  clearSelectionButton.addEventListener("click", () => {
    resetLeadSelection();
    renderLeadsTable(leadsCache);
    updateLeadsMetaLabel();
  });
  deleteSelectedButton.addEventListener("click", () => {
    void deleteSelectedLeads();
  });

  leadsPrevPageButton.addEventListener("click", () => {
    if (!leadsHasPrev) return;
    leadsPage = Math.max(1, leadsPage - 1);
    void refreshLeads();
  });
  leadsNextPageButton.addEventListener("click", () => {
    if (!leadsHasNext) return;
    leadsPage += 1;
    void refreshLeads();
  });

  const controlsThatReset = [
    leadsStatusFilter,
    consentFilter,
    leadsSourceFilter,
    leadsPageSizeSelect,
    leadsSortBy,
    leadsSortDir,
  ];
  for (const control of controlsThatReset) {
    control.addEventListener("change", () => {
      void refreshLeads({ resetPage: true, resetSelection: true });
    });
  }
  leadsSearch.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    void refreshLeads({ resetPage: true, resetSelection: true });
  });

  campaignRefreshButton.addEventListener("click", () => {
    void refreshCampaigns();
  });
  eventsRefreshButton.addEventListener("click", () => {
    void refreshEvents();
  });

  const onShow = () => {
    void refreshLeads({ resetPage: true });
    void refreshDiscoveryRuns();
    void refreshCampaigns();
    void refreshEvents();
  };

  AnimationController.mount(root, "view");

  return {
    key: "crm",
    title: "CRM",
    root,
    onShow,
    onHide: () => {},
  };
}

function appendLabeled(form: HTMLElement, labelText: string, input: HTMLElement): void {
  form.append(createElement("label", "form-label", labelText), input);
}
