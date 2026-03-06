import { createApiConfigForm } from "../components/forms/api-config-form";
import { createElement, formatError } from "../core/dom";
import type { ViewModule } from "../core/types";
import { ApiClient } from "../core/api-client";
import { AnimationController } from "../animations/controller";

type ApiViewDeps = {
  apiClient: ApiClient;
};

export function createApiView(deps: ApiViewDeps): ViewModule {
  const root = createElement("section", "view-panel");

  const form = createApiConfigForm({
    initialBaseUrl: deps.apiClient.getBaseUrl(),
    onSave: (baseUrl) => {
      deps.apiClient.setBaseUrl(baseUrl);
      localStorage.setItem("bra_api_base", deps.apiClient.getBaseUrl());
      form.statusLabel.textContent = `Base guardada: ${deps.apiClient.getBaseUrl()}`;
    },
    onHealth: async (baseUrl) => {
      form.statusLabel.textContent = "Comprobando health...";
      try {
        deps.apiClient.setBaseUrl(baseUrl);
        const response = await deps.apiClient.get<Record<string, unknown>>("/health");
        form.statusLabel.textContent = `Health OK: ${JSON.stringify(response)}`;
      } catch (error) {
        form.statusLabel.textContent = `ERROR: ${formatError(error)}`;
      }
    },
    onProbe: async (path) => {
      form.probeResult.textContent = "Cargando...";
      try {
        const cleanPath = path.startsWith("/") ? path : `/${path}`;
        const response = await deps.apiClient.get<Record<string, unknown>>(cleanPath);
        form.probeResult.textContent = JSON.stringify(response, null, 2);
      } catch (error) {
        form.probeResult.textContent = `ERROR: ${formatError(error)}`;
      }
    },
  });

  root.append(form.root);
  AnimationController.mount(root, "view");

  return {
    key: "api",
    title: "API",
    root,
    onShow: () => {},
    onHide: () => {},
  };
}
