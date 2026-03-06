import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";

export type BoxTone = "turquoise" | "orange" | "white";

export function createBox(tone: BoxTone = "white", className = ""): HTMLDivElement {
  const box = createElement("div", "atom-box") as HTMLDivElement;
  box.classList.add(`atom-box--${tone}`);
  if (className) {
    box.classList.add(className);
  }
  AnimationController.mount(box, "atom");
  return box;
}
