import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";

export type ButtonTone = "turquoise" | "orange" | "white";

export type ButtonProps = {
  label: string;
  tone?: ButtonTone;
  type?: "button" | "submit";
  onClick?: () => void | Promise<void>;
  className?: string;
  title?: string;
};

export function createButton(props: ButtonProps): HTMLButtonElement {
  const button = createElement("button", "atom-button") as HTMLButtonElement;
  button.textContent = props.label;
  button.type = props.type ?? "button";
  button.classList.add(`atom-button--${props.tone ?? "turquoise"}`);
  if (props.className) {
    button.classList.add(props.className);
  }
  if (props.title) {
    button.title = props.title;
  }
  if (props.onClick) {
    button.addEventListener("click", () => {
      void props.onClick?.();
    });
  }
  AnimationController.mount(button, "atom");
  AnimationController.attachHover(button, "atom");
  return button;
}
