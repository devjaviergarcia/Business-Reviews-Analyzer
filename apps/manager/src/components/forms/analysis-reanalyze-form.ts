import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";
import { createButton } from "../atoms/button";
import { createInput } from "../atoms/input";

type AnalysisReanalyzeFormOptions = {
  onLoadCatalog: () => void;
  onSearchTerm: (term: string) => void;
  onSubmit: (values: {
    batchers: string;
    batchSize: string;
    poolSize: string;
    sourceScope: "all" | "google_maps" | "tripadvisor";
    reportProfile: "classic" | "client_audit";
    reportComplexity: "basic" | "hydrated";
    reportCadence: "one_off" | "monthly" | "quarterly";
    studyResolutionMode: "auto_ttl" | "reuse_latest" | "refresh_now";
    includeCompetitors: boolean;
    includeGeogrid: boolean;
  }) => void;
};

type FormRowHandle = {
  label: HTMLElement;
  field: HTMLElement;
};

export type ReanalyzeSuggestion = { businessId: string; name: string };

export type AnalysisReanalyzeFormHandle = {
  root: HTMLElement;
  statusLabel: HTMLElement;
  selectedLabel: HTMLElement;
  responseBlock: HTMLElement;
  searchInput: HTMLInputElement;
  setSuggestions: (items: ReanalyzeSuggestion[], onPick: (item: ReanalyzeSuggestion) => void) => void;
  setSearchCount: (text: string) => void;
};

export function createAnalysisReanalyzeForm(
  options: AnalysisReanalyzeFormOptions
): AnalysisReanalyzeFormHandle {
  const root = createElement("section", "panel form-panel");
  root.append(createElement("h2", "panel__title", "Generar reporte desde reseñas guardadas"));

  const topActions = createElement("div", "form-actions");
  const loadButton = createButton({
    label: "Cargar catálogo",
    tone: "turquoise",
    onClick: options.onLoadCatalog,
  });
  const statusLabel = createElement("span", "muted", "");
  topActions.append(loadButton, statusLabel);
  root.append(topActions);

  const searchRow = createElement("div", "form-grid");
  const searchInput = createInput({
    placeholder: "Buscar negocio...",
    disabled: false,
  });
  const searchCount = createElement("span", "muted", "");
  searchInput.addEventListener("input", () => {
    options.onSearchTerm(searchInput.value);
  });
  searchRow.append(createElement("label", "form-label", "Autocompletar"), searchInput);
  root.append(searchRow);
  root.append(searchCount);

  const suggestions = createElement("div", "suggestion-list");
  root.append(suggestions);

  const selectedLabel = createElement("div", "muted", "Negocio no seleccionado.");
  root.append(selectedLabel);

  const form = createElement("form", "form-grid") as HTMLFormElement;
  const sourceScopeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  sourceScopeSelect.innerHTML = `
    <option value="all" selected>Todas las fuentes</option>
    <option value="google_maps">Solo Google Maps</option>
    <option value="tripadvisor">Solo Tripadvisor</option>
  `;
  const batchersInput = createInput({ placeholder: "latest_text,balanced_rating" });
  const batchSizeInput = createInput({ type: "number", min: "1", placeholder: "opcional" });
  const poolSizeInput = createInput({ type: "number", min: "1", placeholder: "opcional" });
  const reportProfileSelect = createElement("select", "atom-input") as HTMLSelectElement;
  reportProfileSelect.innerHTML = `
    <option value="client_audit" selected>Client audit</option>
    <option value="classic">Classic</option>
  `;
  const launchResearchInput = createElement("input", "atom-input") as HTMLInputElement;
  launchResearchInput.type = "checkbox";
  launchResearchInput.checked = false;
  const reportCadenceSelect = createElement("select", "atom-input") as HTMLSelectElement;
  reportCadenceSelect.innerHTML = `
    <option value="one_off" selected>One-off</option>
    <option value="monthly">Monthly</option>
    <option value="quarterly">Quarterly</option>
  `;
  const studyResolutionModeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  studyResolutionModeSelect.innerHTML = `
    <option value="auto_ttl" selected>Auto TTL</option>
    <option value="reuse_latest">Reuse latest</option>
    <option value="refresh_now">Refresh now</option>
  `;
  const includeCompetitorsInput = createElement("input", "atom-input") as HTMLInputElement;
  includeCompetitorsInput.type = "checkbox";
  includeCompetitorsInput.checked = true;
  const includeGeogridInput = createElement("input", "atom-input") as HTMLInputElement;
  includeGeogridInput.type = "checkbox";
  includeGeogridInput.checked = false;
  const researchHelp = createElement("div", "muted", "");

  appendLabeled(form, "Fuentes para el reporte", sourceScopeSelect);
  appendLabeled(form, "Batchers", batchersInput);
  appendLabeled(form, "Batch size", batchSizeInput);
  appendLabeled(form, "Pool size", poolSizeInput);
  appendLabeled(form, "Perfil de reporte", reportProfileSelect);
  appendLabeled(form, "Lanzar research", launchResearchInput);
  const researchHelpRow = appendLabeled(form, "Modo research", researchHelp);
  appendLabeled(form, "Cadencia", reportCadenceSelect);
  const studyResolutionModeRow = appendLabeled(
    form,
    "Resolución estudio",
    studyResolutionModeSelect
  );
  const includeCompetitorsRow = appendLabeled(
    form,
    "Usar benchmark / competidores",
    includeCompetitorsInput
  );
  const includeGeogridRow = appendLabeled(form, "Incluir geogrid", includeGeogridInput);
  const syncAuditControls = (): void => {
    const isClassic = reportProfileSelect.value === "classic";
    const shouldLaunchResearch = !isClassic && launchResearchInput.checked;
    launchResearchInput.disabled = isClassic;
    studyResolutionModeSelect.disabled = !shouldLaunchResearch;
    includeCompetitorsInput.disabled = !shouldLaunchResearch;
    includeGeogridInput.disabled = !shouldLaunchResearch;
    toggleRow(researchHelpRow, true);
    toggleRow(studyResolutionModeRow, shouldLaunchResearch);
    toggleRow(includeCompetitorsRow, shouldLaunchResearch);
    toggleRow(includeGeogridRow, shouldLaunchResearch);
    if (isClassic) {
      launchResearchInput.checked = false;
      studyResolutionModeSelect.value = "auto_ttl";
      includeCompetitorsInput.checked = false;
      includeGeogridInput.checked = false;
      researchHelp.textContent = "Classic genera el reporte actual y no lanza research comercial.";
      return;
    }
    if (!shouldLaunchResearch) {
      studyResolutionModeSelect.value = "auto_ttl";
      includeCompetitorsInput.checked = false;
      includeGeogridInput.checked = false;
      researchHelp.textContent =
        "Sin research se genera el client audit base usando las reseñas y el análisis ya guardados.";
      return;
    }
    if (!includeCompetitorsInput.checked) {
      includeCompetitorsInput.checked = true;
    }
    researchHelp.textContent =
      "Con research se activa la hidratación comercial: benchmark reutilizable o refresh, competidores y geogrid opcional.";
  };
  reportProfileSelect.addEventListener("change", syncAuditControls);
  launchResearchInput.addEventListener("change", syncAuditControls);
  const submitActions = createElement("div", "form-actions");
  submitActions.append(createButton({ label: "Generar reporte", tone: "orange", type: "submit" }));
  form.append(submitActions);
  syncAuditControls();
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const reportProfile = reportProfileSelect.value as "classic" | "client_audit";
    const reportComplexity =
      reportProfile === "client_audit" && launchResearchInput.checked ? "hydrated" : "basic";
    options.onSubmit({
      sourceScope: sourceScopeSelect.value as "all" | "google_maps" | "tripadvisor",
      batchers: batchersInput.value.trim(),
      batchSize: batchSizeInput.value.trim(),
      poolSize: poolSizeInput.value.trim(),
      reportProfile,
      reportComplexity,
      reportCadence: reportCadenceSelect.value as "one_off" | "monthly" | "quarterly",
      studyResolutionMode: studyResolutionModeSelect.value as
        | "auto_ttl"
        | "reuse_latest"
        | "refresh_now",
      includeCompetitors: includeCompetitorsInput.checked,
      includeGeogrid: includeGeogridInput.checked,
    });
  });
  root.append(form);

  const responseBlock = createElement("pre", "code-block", "");
  root.append(responseBlock);

  AnimationController.mount(root, "form");

  return {
    root,
    statusLabel,
    selectedLabel,
    responseBlock,
    searchInput,
    setSuggestions: (items, onPick) => {
      suggestions.innerHTML = "";
      for (const item of items) {
        const button = createButton({
          label: item.name,
          tone: "white",
          className: "suggestion-button",
          onClick: () => onPick(item),
        });
        suggestions.append(button);
      }
    },
    setSearchCount: (text) => {
      searchCount.textContent = text;
    },
  };
}

function appendLabeled(form: HTMLElement, labelText: string, input: HTMLElement): FormRowHandle {
  const label = createElement("label", "form-label", labelText);
  form.append(label, input);
  return { label, field: input };
}

function toggleRow(row: FormRowHandle, visible: boolean): void {
  row.label.classList.toggle("hidden", !visible);
  row.field.classList.toggle("hidden", !visible);
}
