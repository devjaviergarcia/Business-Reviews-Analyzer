import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";
import { createButton } from "../atoms/button";
import { createInput } from "../atoms/input";

type ApiConfigFormOptions = {
  initialBaseUrl: string;
  onSave: (baseUrl: string) => void;
  onHealth: (baseUrl: string) => void;
  onProbe: (path: string) => void;
};

export type ApiConfigFormHandle = {
  root: HTMLElement;
  baseInput: HTMLInputElement;
  statusLabel: HTMLElement;
  probePathInput: HTMLInputElement;
  probeResult: HTMLElement;
};

export function createApiConfigForm(options: ApiConfigFormOptions): ApiConfigFormHandle {
  const root = createElement("section", "panel form-panel");
  const title = createElement("h2", "panel__title", "API");
  root.append(title);

  const row = createElement("div", "form-grid");
  root.append(row);

  const baseLabel = createElement("label", "form-label", "Base URL");
  const baseInput = createInput({ value: options.initialBaseUrl, placeholder: "http://localhost:8000" });
  row.append(baseLabel, baseInput);

  const actions = createElement("div", "form-actions");
  const saveButton = createButton({
    label: "Guardar base",
    tone: "turquoise",
    onClick: () => options.onSave(baseInput.value.trim()),
  });
  const healthButton = createButton({
    label: "Health check",
    tone: "orange",
    onClick: () => options.onHealth(baseInput.value.trim()),
  });
  const statusLabel = createElement("span", "muted", "");
  actions.append(saveButton, healthButton, statusLabel);
  root.append(actions);

  const probeTitle = createElement("h3", "panel__subtitle", "Probar endpoint");
  root.append(probeTitle);

  const probeRow = createElement("div", "form-grid");
  const probeLabel = createElement("label", "form-label", "GET path");
  const probePathInput = createInput({ value: "/health", placeholder: "/business?page=1&page_size=10" });
  probeRow.append(probeLabel, probePathInput);
  root.append(probeRow);

  const probeActions = createElement("div", "form-actions");
  const probeButton = createButton({
    label: "Ejecutar GET",
    tone: "white",
    onClick: () => options.onProbe(probePathInput.value.trim()),
  });
  probeActions.append(probeButton);
  root.append(probeActions);

  const probeResult = createElement("pre", "code-block", "");
  root.append(probeResult);

  AnimationController.mount(root, "form");
  return {
    root,
    baseInput,
    statusLabel,
    probePathInput,
    probeResult,
  };
}
