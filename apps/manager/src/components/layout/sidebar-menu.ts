import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";
import type { MenuKey } from "../../core/types";

type MenuItem = { key: MenuKey; label: string };

type ActiveJobCard = {
  jobId: string;
  title: string;
  status: string;
  progressPercent: number;
  onClick: () => void;
};

type SidebarMenuOptions = {
  items: MenuItem[];
  initial: MenuKey;
  onSelect: (key: MenuKey) => void;
};

export type SidebarMenuHandle = {
  root: HTMLElement;
  setActive: (key: MenuKey) => void;
  setActiveJob: (job: ActiveJobCard | null) => void;
};

export function createSidebarMenu(options: SidebarMenuOptions): SidebarMenuHandle {
  const root = createElement("aside", "sidebar");
  const brand = createElement("div", "sidebar__brand");
  brand.innerHTML = `<span class="sidebar__dot"></span><span class="sidebar__title">Review Manager</span>`;
  root.append(brand);

  const nav = createElement("nav", "sidebar__nav");
  root.append(nav);

  const footer = createElement("div", "sidebar__footer");
  root.append(footer);
  const activeCard = createElement("button", "sidebar__active-job hidden") as HTMLButtonElement;
  activeCard.type = "button";
  const spinner = createElement("span", "sidebar__active-spinner");
  const cardBody = createElement("span", "sidebar__active-body");
  const cardTitle = createElement("span", "sidebar__active-title", "Análisis en curso");
  const cardMeta = createElement("span", "sidebar__active-meta");
  cardBody.append(cardTitle, cardMeta);
  activeCard.append(spinner, cardBody);
  footer.append(activeCard);

  const buttonMap = new Map<MenuKey, HTMLButtonElement>();

  for (const item of options.items) {
    const button = createElement("button", "sidebar__item", item.label) as HTMLButtonElement;
    button.type = "button";
    button.addEventListener("click", () => {
      options.onSelect(item.key);
    });
    nav.append(button);
    buttonMap.set(item.key, button);
    AnimationController.mount(button, "atom");
    AnimationController.attachHover(button, "atom");
  }

  const setActive = (key: MenuKey): void => {
    for (const [itemKey, button] of buttonMap) {
      button.classList.toggle("sidebar__item--active", itemKey === key);
    }
  };

  const setActiveJob = (job: ActiveJobCard | null): void => {
    if (!job) {
      activeCard.classList.add("hidden");
      activeCard.onclick = null;
      cardMeta.textContent = "";
      return;
    }
    activeCard.classList.remove("hidden");
    const label = `${job.title} | ${job.status} | ${Math.max(0, Math.min(100, Math.round(job.progressPercent)))}%`;
    cardMeta.textContent = `${label} · ${job.jobId}`;
    activeCard.onclick = () => {
      job.onClick();
    };
  };

  setActive(options.initial);
  AnimationController.mount(root, "view");
  return { root, setActive, setActiveJob };
}
