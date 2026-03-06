import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";
import { createButton } from "../atoms/button";
import { createInput } from "../atoms/input";

type AnalysisReanalyzeFormOptions = {
  onLoadCatalog: () => void;
  onSearchTerm: (term: string) => void;
  onSubmit: (values: { batchers: string; batchSize: string; poolSize: string }) => void;
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
  root.append(createElement("h2", "panel__title", "Reanalizar existente"));

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
  const batchersInput = createInput({ placeholder: "latest_text,balanced_rating" });
  const batchSizeInput = createInput({ type: "number", min: "1", placeholder: "opcional" });
  const poolSizeInput = createInput({ type: "number", min: "1", placeholder: "opcional" });
  form.append(createElement("label", "form-label", "Batchers"), batchersInput);
  form.append(createElement("label", "form-label", "Batch size"), batchSizeInput);
  form.append(createElement("label", "form-label", "Pool size"), poolSizeInput);
  const submitActions = createElement("div", "form-actions");
  submitActions.append(createButton({ label: "Reanalizar", tone: "orange", type: "submit" }));
  form.append(submitActions);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    options.onSubmit({
      batchers: batchersInput.value.trim(),
      batchSize: batchSizeInput.value.trim(),
      poolSize: poolSizeInput.value.trim(),
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
