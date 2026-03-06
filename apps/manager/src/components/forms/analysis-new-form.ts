import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";
import { createButton } from "../atoms/button";
import { createInput } from "../atoms/input";

export type AnalysisNewFormValues = {
  name: string;
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

export type AnalysisNewFormHandle = {
  root: HTMLElement;
  statusLabel: HTMLElement;
  fields: {
    nameInput: HTMLInputElement;
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
  root.append(createElement("h2", "panel__title", "Analizar nuevo (solo cola)"));

  const form = createElement("form", "form-grid") as HTMLFormElement;
  root.append(form);

  const nameInput = createInput({ placeholder: "Nombre del negocio" });
  const strategySelect = createElement("select", "atom-input") as HTMLSelectElement;
  strategySelect.innerHTML = `<option value="scroll_copy" selected>scroll_copy</option><option value="interactive">interactive</option>`;
  const forceInput = createInput({ type: "checkbox" });
  const forceModeSelect = createElement("select", "atom-input") as HTMLSelectElement;
  forceModeSelect.innerHTML = `<option value="fallback_existing" selected>fallback_existing</option><option value="strict_rescrape">strict_rescrape</option>`;
  const interactiveInput = createInput({ type: "number", min: "1", placeholder: "opcional" });
  const htmlRoundsInput = createInput({ type: "number", min: "0", placeholder: "opcional" });
  const stableInput = createInput({ type: "number", min: "2", placeholder: "opcional" });
  const tripadvisorMaxPagesInput = createInput({ type: "number", min: "1", placeholder: "opcional" });
  const tripadvisorPagesPercentInput = createInput({
    type: "number",
    min: "0.1",
    max: "100",
    step: "0.1",
    placeholder: "opcional",
  });

  appendLabeled(form, "Nombre", nameInput);
  appendLabeled(form, "Strategy", strategySelect);
  appendLabeled(form, "Force", forceInput);
  appendLabeled(form, "Force mode", forceModeSelect);
  appendLabeled(form, "Interactive rounds", interactiveInput);
  appendLabeled(form, "HTML scroll rounds", htmlRoundsInput);
  appendLabeled(form, "HTML stable rounds", stableInput);
  appendLabeled(form, "TripAdvisor max pages", tripadvisorMaxPagesInput);
  appendLabeled(form, "TripAdvisor pages percent", tripadvisorPagesPercentInput);

  const actions = createElement("div", "form-actions");
  const submitButton = createButton({ label: "Lanzar análisis", tone: "orange", type: "submit" });
  const statusLabel = createElement("span", "muted", "");
  actions.append(submitButton, statusLabel);
  form.append(actions);

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    options.onSubmit({
      name: nameInput.value.trim(),
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

function appendLabeled(form: HTMLElement, labelText: string, input: HTMLElement): void {
  form.append(createElement("label", "form-label", labelText), input);
}
