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

  const updateFieldVisibility = (): void => {
    const sourceScope = sourceScopeSelect.value as "all" | "google_maps" | "tripadvisor";
    const includesGoogle = sourceScope !== "tripadvisor";
    const includesTripadvisor = sourceScope !== "google_maps";
    const strategy = strategySelect.value;
    const isInteractive = strategy === "interactive";
    const isForceEnabled = forceInput.checked;

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
  };

  sourceScopeSelect.addEventListener("change", updateFieldVisibility);
  strategySelect.addEventListener("change", updateFieldVisibility);
  forceInput.addEventListener("change", updateFieldVisibility);
  updateFieldVisibility();

  const actions = createElement("div", "form-actions");
  const submitButton = createButton({ label: "Lanzar scrape", tone: "orange", type: "submit" });
  const statusLabel = createElement("span", "muted", "");
  actions.append(submitButton, statusLabel);
  form.append(actions);

  form.addEventListener("submit", (event) => {
    event.preventDefault();
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
