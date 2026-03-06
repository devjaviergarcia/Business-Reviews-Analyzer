import { AnimationController } from "../../animations/controller";
import { createElement } from "../../core/dom";
import type { ReviewItem } from "../../core/types";

export function createReviewCard(review: ReviewItem): HTMLElement {
  const card = createElement("article", "review-card atom-box atom-box--white") as HTMLElement;

  const head = createElement("div", "review-card__head");
  const author = createElement("strong", "review-card__author", review.author_name || "Unknown");
  const rating = createElement(
    "span",
    "review-card__rating",
    typeof review.rating === "number" ? `${review.rating}/5` : "-"
  );
  const when = createElement("span", "review-card__time", review.relative_time || "");

  head.append(author, rating, when);
  card.append(head);

  const body = createElement("p", "review-card__text", review.text || "");
  card.append(body);

  AnimationController.mount(card, "atom");
  AnimationController.attachHover(card, "atom");
  return card;
}
