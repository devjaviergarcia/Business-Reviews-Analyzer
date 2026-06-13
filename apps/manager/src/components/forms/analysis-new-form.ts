import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";
import { createButton } from "../atoms/button";
import { createInput } from "../atoms/input";

export type AnalysisNewFormValues = {
  name: string;
  sourceScope: "all" | "google_maps" | "tripadvisor";
  launchMode: "automatic" | "live_native" | "live_xvfb";
  googleMapsName: string;
  tripadvisorName: string;
  strategy: string;
  force: boolean;
  forceMode: string;
  interactiveRounds: string;
  htmlRounds: string;
  stableRounds: string;
  tripadvisorMaxPages: string;
  tripadvisorPagesPercent: string;
  reportProfile: "classic" | "client_audit";
  reportComplexity: "basic" | "hydrated";
  reportCadence: "one_off" | "monthly" | "quarterly";
  studyResolutionMode: "auto_ttl" | "reuse_latest" | "refresh_now";
  includeCompetitors: boolean;
  includeGeogrid: boolean;
};

type AnalysisNewFormOptions = {
  onSubmit: (values: AnalysisNewFormValues) => void;
};

type FormRowHandle = {
  label: HTMLElement;
  field: HTMLElement;
};

export type AnalysisNewFormHandle = {
  root: HTMLElement;
  statusLabel: HTMLElement;
  fields: {
    nameInput: HTMLInputElement;
    launchModeSelect: HTMLSelectElement;
    googleMapsNameInput: HTMLInputElement;
    tripadvisorNameInput: HTMLInputElement;
    strategySelect: HTMLSelectElement;
    forceInput: HTMLInputElement;
    forceModeSelect: HTMLSelectElement;
    interactiveInput: HTMLInputElement;
    htmlRoundsInput: HTMLInputElement;
    stableInput: HTMLInputElement;
    tripadvisorMaxPagesInput: HTMLInputElement;
    tripadvisorPagesPercentInput: HTMLInputElement;
  };
};

export function createAnalysisNewForm(options: AnalysisNewFormOptions): AnalysisNewFormHandle {
  const root = createElement("section", "panel form-panel");
  root.append(createElement("h2", "panel__title", "Capturar reseñas (pipeline de scrape)"));

  const form = createElement("form", "form-grid") as HTMLFormElement;
  root.append(form);

  const nameInput = createInput({ placeholder: "Nombre del negocio" });
  const sourceScopeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  sourceScopeSelect.innerHTML = `
    <option value="all" selected>Todas las fuentes</option>
    <option value="google_maps">Solo Google Maps</option>
    <option value="tripadvisor">Solo Tripadvisor</option>
  `;
  const launchModeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  launchModeSelect.innerHTML = `
    <option value="automatic" selected>Automático</option>
    <option value="live_native">Live nativo (debug visible)</option>
    <option value="live_xvfb">Live bajo Xvfb (oculto)</option>
  `;
  const googleMapsNameInput = createInput({ placeholder: "Nombre en Google Maps (opcional)" });
  const tripadvisorNameInput = createInput({ placeholder: "Nombre en Tripadvisor (opcional)" });
  const strategySelect = createElement("select", "atom-input") as HTMLSelectElement;
  strategySelect.innerHTML = `
    <option value="scroll_copy" selected>scroll_copy (recomendado)</option>
    <option value="interactive">interactive (legacy)</option>
  `;
  const forceInput = createInput({ type: "checkbox" });
  const forceModeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  forceModeSelect.innerHTML = `
    <option value="fallback_existing" selected>fallback_existing (usa guardado si scrapea 0)</option>
    <option value="strict_rescrape">strict_rescrape (falla si scrapea 0)</option>
  `;
  const interactiveInput = createInput({
    type: "number",
    min: "1",
    placeholder: "opcional",
  });
  const htmlRoundsInput = createInput({
    type: "number",
    min: "0",
    placeholder: "opcional",
  });
  const stableInput = createInput({
    type: "number",
    min: "2",
    placeholder: "opcional",
  });
  const tripadvisorMaxPagesInput = createInput({
    type: "number",
    min: "1",
    placeholder: "opcional",
  });
  const tripadvisorPagesPercentInput = createInput({
    type: "number",
    min: "0.1",
    max: "100",
    step: "0.1",
    placeholder: "opcional",
  });
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
  const researchHelp = createElement("div", "muted");

  appendLabeled(form, "Nombre", nameInput);
  appendLabeled(form, "Fuentes para el pipeline", sourceScopeSelect);
  appendLabeled(form, "Modo de lanzamiento", launchModeSelect);
  const googleNameRow = appendLabeled(form, "Nombre Google Maps", googleMapsNameInput);
  const tripadvisorNameRow = appendLabeled(form, "Nombre Tripadvisor", tripadvisorNameInput);
  const strategyRow = appendLabeled(form, "Estrategia Google", strategySelect);
  const googleHelp = createElement("div", "muted");
  const googleHelpRow = appendLabeled(form, "Ayuda Google", googleHelp);
  const forceRow = appendLabeled(form, "Forzar recaptura", forceInput);
  const forceModeRow = appendLabeled(form, "Política si scrapea 0 reseñas", forceModeSelect);
  const interactiveRow = appendLabeled(form, "Interactive rounds", interactiveInput);
  const htmlRoundsRow = appendLabeled(form, "Scroll rounds", htmlRoundsInput);
  const stableRow = appendLabeled(form, "Stable rounds", stableInput);
  const tripadvisorMaxPagesRow = appendLabeled(form, "TripAdvisor max pages", tripadvisorMaxPagesInput);
  const tripadvisorPagesPercentRow = appendLabeled(
    form,
    "TripAdvisor pages percent",
    tripadvisorPagesPercentInput
  );
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

  const updateFieldVisibility = (): void => {
    const sourceScope = sourceScopeSelect.value as "all" | "google_maps" | "tripadvisor";
    const includesGoogle = sourceScope !== "tripadvisor";
    const includesTripadvisor = sourceScope !== "google_maps";
    const strategy = strategySelect.value;
    const isInteractive = strategy === "interactive";
    const isForceEnabled = forceInput.checked;
    const isClassic = reportProfileSelect.value === "classic";
    const shouldLaunchResearch = !isClassic && launchResearchInput.checked;

    toggleRow(googleNameRow, includesGoogle);
    toggleRow(strategyRow, includesGoogle);
    toggleRow(googleHelpRow, includesGoogle);
    toggleRow(forceRow, includesGoogle);
    toggleRow(forceModeRow, includesGoogle && isForceEnabled);
    toggleRow(interactiveRow, includesGoogle && isInteractive);
    toggleRow(htmlRoundsRow, includesGoogle && !isInteractive);
    toggleRow(stableRow, includesGoogle && !isInteractive);

    toggleRow(tripadvisorNameRow, includesTripadvisor);
    toggleRow(tripadvisorMaxPagesRow, includesTripadvisor);
    toggleRow(tripadvisorPagesPercentRow, includesTripadvisor);

    if (isInteractive) {
      googleHelp.textContent =
        "interactive es el modo legacy: hace scroll/click sobre el DOM y luego extrae. " +
        "Interactive rounds es el número máximo de ciclos.";
    } else {
      googleHelp.textContent =
        "scroll_copy es el modo recomendado. Scroll rounds es el tope duro de scrolls. " +
        "Stable rounds es cuántas vueltas seguidas sin reseñas nuevas esperamos antes de parar.";
    }

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
        "Sin research, al terminar el scrape se lanzará el client audit base usando reseñas y análisis guardados.";
      return;
    }

    if (!includeCompetitorsInput.checked) {
      includeCompetitorsInput.checked = true;
    }
    researchHelp.textContent =
      "Con research, al cerrar el scrape se encadena la hidratación comercial: benchmark reutilizable o refresh, competidores y geogrid opcional.";
  };

  sourceScopeSelect.addEventListener("change", updateFieldVisibility);
  strategySelect.addEventListener("change", updateFieldVisibility);
  forceInput.addEventListener("change", updateFieldVisibility);
  reportProfileSelect.addEventListener("change", updateFieldVisibility);
  launchResearchInput.addEventListener("change", updateFieldVisibility);
  updateFieldVisibility();

  const actions = createElement("div", "form-actions");
  const submitButton = createButton({ label: "Lanzar scrape", tone: "orange", type: "submit" });
  const statusLabel = createElement("span", "muted", "");
  actions.append(submitButton, statusLabel);
  form.append(actions);

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const reportProfile = reportProfileSelect.value as "classic" | "client_audit";
    const reportComplexity =
      reportProfile === "client_audit" && launchResearchInput.checked ? "hydrated" : "basic";
    options.onSubmit({
      name: nameInput.value.trim(),
      sourceScope: sourceScopeSelect.value as "all" | "google_maps" | "tripadvisor",
      launchMode: launchModeSelect.value as "automatic" | "live_native" | "live_xvfb",
      googleMapsName: googleMapsNameInput.value.trim(),
      tripadvisorName: tripadvisorNameInput.value.trim(),
      strategy: strategySelect.value,
      force: forceInput.checked,
      forceMode: forceModeSelect.value,
      interactiveRounds: interactiveInput.value,
      htmlRounds: htmlRoundsInput.value,
      stableRounds: stableInput.value,
      tripadvisorMaxPages: tripadvisorMaxPagesInput.value,
      tripadvisorPagesPercent: tripadvisorPagesPercentInput.value,
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

  AnimationController.mount(root, "form");
  return {
    root,
    statusLabel,
    fields: {
      nameInput,
      launchModeSelect,
      googleMapsNameInput,
      tripadvisorNameInput,
      strategySelect,
      forceInput,
      forceModeSelect,
      interactiveInput,
      htmlRoundsInput,
      stableInput,
      tripadvisorMaxPagesInput,
      tripadvisorPagesPercentInput,
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
