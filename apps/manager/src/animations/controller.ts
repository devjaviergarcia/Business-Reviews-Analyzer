export type AnimationLevel = "atom" | "form" | "view";

type AnimationPreset = {
  enterMs: number;
  enterY: string;
  hoverMs: number;
  hoverTranslateY: string;
  hoverScale: string;
};

const PRESETS: Record<AnimationLevel, AnimationPreset> = {
  atom: {
    enterMs: 140,
    enterY: "0px",
    hoverMs: 120,
    hoverTranslateY: "-2px",
    hoverScale: "1.01",
  },
  form: {
    enterMs: 220,
    enterY: "8px",
    hoverMs: 140,
    hoverTranslateY: "-1px",
    hoverScale: "1.0",
  },
  view: {
    enterMs: 300,
    enterY: "14px",
    hoverMs: 160,
    hoverTranslateY: "0px",
    hoverScale: "1.0",
  },
};

function applyPreset(element: HTMLElement, level: AnimationLevel): void {
  const preset = PRESETS[level];
  element.style.setProperty("--anim-enter-ms", `${preset.enterMs}ms`);
  element.style.setProperty("--anim-enter-y", preset.enterY);
  element.style.setProperty("--anim-hover-ms", `${preset.hoverMs}ms`);
  element.style.setProperty("--anim-hover-translate-y", preset.hoverTranslateY);
  element.style.setProperty("--anim-hover-scale", preset.hoverScale);
}

export const AnimationController = {
  mount(element: HTMLElement, level: AnimationLevel): void {
    applyPreset(element, level);
    element.classList.add("anim-enter", `anim-level-${level}`);
    requestAnimationFrame(() => {
      element.classList.add("anim-enter-active");
    });
  },
  attachHover(element: HTMLElement, level: AnimationLevel = "atom"): void {
    applyPreset(element, level);
    element.classList.add("anim-hover");
  },
};
