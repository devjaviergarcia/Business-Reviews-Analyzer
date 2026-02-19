import re
from statistics import mean


class ReviewPreprocessor:
    _CONTROL_CHARS_REGEX = re.compile(r"[\x00-\x1F\x7F]")
    _NUMBER_REGEX = re.compile(r"(\d+)")

    def process(self, reviews: list[dict]) -> list[dict]:
        processed_reviews: list[dict] = []

        for review in reviews:
            item = dict(review)
            text = self._clean_text(item.get("text", ""))
            owner_reply_text = self._clean_text(item.get("owner_reply", ""))
            rating = self._coerce_rating(item.get("rating"))
            relative_time = self._clean_text(item.get("relative_time", ""))

            item["text"] = text
            item["owner_reply"] = owner_reply_text
            item["rating"] = rating
            item["relative_time"] = relative_time
            item["has_text"] = bool(text)
            item["has_owner_reply"] = bool(owner_reply_text)
            item["relative_time_bucket"] = self._relative_time_bucket(relative_time)

            processed_reviews.append(item)

        return processed_reviews

    def compute_stats(self, reviews: list[dict]) -> dict:
        if not reviews:
            return {
                "avg_rating": 0.0,
                "rating_distribution": {str(i): 0 for i in range(1, 6)},
                "response_rate": 0.0,
                "total_with_text": 0,
                "sentiment_by_time": {},
            }

        ratings = [self._coerce_rating(item.get("rating")) for item in reviews]
        total = len(reviews)
        total_with_text = sum(1 for item in reviews if bool(item.get("has_text") or item.get("text")))
        total_with_reply = sum(1 for item in reviews if bool(item.get("has_owner_reply") or item.get("owner_reply")))

        rating_distribution = {str(i): 0 for i in range(1, 6)}
        for rating in ratings:
            star = int(round(rating))
            star = min(max(star, 1), 5)
            rating_distribution[str(star)] += 1

        sentiment_by_time: dict[str, dict[str, int]] = {}
        for item in reviews:
            bucket = str(item.get("relative_time_bucket") or self._relative_time_bucket(str(item.get("relative_time", ""))))
            if bucket not in sentiment_by_time:
                sentiment_by_time[bucket] = {"positive": 0, "neutral": 0, "negative": 0}

            rating = self._coerce_rating(item.get("rating"))
            if rating >= 4.0:
                sentiment_by_time[bucket]["positive"] += 1
            elif rating <= 2.0:
                sentiment_by_time[bucket]["negative"] += 1
            else:
                sentiment_by_time[bucket]["neutral"] += 1

        return {
            "avg_rating": round(mean(ratings), 2),
            "rating_distribution": rating_distribution,
            "response_rate": round(total_with_reply / total, 4),
            "total_with_text": total_with_text,
            "sentiment_by_time": sentiment_by_time,
        }

    def _clean_text(self, text: object) -> str:
        value = str(text or "")
        value = self._CONTROL_CHARS_REGEX.sub(" ", value)
        value = re.sub(r"\s+", " ", value)
        return value.strip()

    def _coerce_rating(self, rating: object) -> float:
        if isinstance(rating, (int, float)):
            return float(rating)

        rating_str = self._clean_text(rating)
        if not rating_str:
            return 0.0

        rating_str = rating_str.replace(",", ".")
        try:
            return float(rating_str)
        except ValueError:
            match = self._NUMBER_REGEX.search(rating_str)
            return float(match.group(1)) if match else 0.0

    def _relative_time_bucket(self, relative_time: str) -> str:
        if not relative_time:
            return "unknown"

        value = relative_time.lower()

        if any(term in value for term in ("just now", "moments ago", "hace un momento")):
            return "recent"

        number_match = self._NUMBER_REGEX.search(value)
        amount = int(number_match.group(1)) if number_match else 1

        if any(term in value for term in ("day", "days", "week", "weeks", "hour", "hours", "minute", "minutes", "dia", "dias", "semana", "semanas", "hora", "horas", "minuto", "minutos")):
            return "recent"

        if any(term in value for term in ("month", "months", "mes", "meses")):
            if amount < 3:
                return "recent"
            if amount <= 12:
                return "medium"
            return "old"

        if any(term in value for term in ("year", "years", "ano", "anos", "año", "años")):
            return "medium" if amount <= 1 else "old"

        return "unknown"
