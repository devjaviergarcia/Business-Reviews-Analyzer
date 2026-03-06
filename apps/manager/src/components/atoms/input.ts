import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";

export type InputProps = {
  placeholder?: string;
  type?: string;
  value?: string;
  min?: string;
  max?: string;
  step?: string;
  disabled?: boolean;
  className?: string;
};

export function createInput(props: InputProps = {}): HTMLInputElement {
  const input = createElement("input", "atom-input") as HTMLInputElement;
  input.type = props.type ?? "text";
  if (props.placeholder) {
    input.placeholder = props.placeholder;
  }
  if (typeof props.value === "string") {
    input.value = props.value;
  }
  if (typeof props.min === "string") {
    input.min = props.min;
  }
  if (typeof props.max === "string") {
    input.max = props.max;
  }
  if (typeof props.step === "string") {
    input.step = props.step;
  }
  if (props.disabled) {
    input.disabled = true;
  }
  if (props.className) {
    input.classList.add(props.className);
  }
  AnimationController.mount(input, "atom");
  AnimationController.attachHover(input, "atom");
  return input;
}
