import asyncio
import json

from google import genai
from google.genai import errors as genai_errors

from src.config import settings
from src.models.analysis import ReviewAnalysis


class ReviewLLMAnalyzer:
    def __init__(self, model_name: str | None = None) -> None:
        self.model_name = model_name or settings.gemini_model
        self.fallback_models = ["gemini-flash-latest", "gemini-2.5-flash"]
        self.client = genai.Client(api_key=settings.gemini_api_key) if settings.gemini_api_key else None

    async def analyze(self, business_name: str, reviews: list[dict], stats: dict) -> ReviewAnalysis:
        if not self.client:
            return self._fallback_analysis(business_name, stats)

        review_payload = []
        for review in reviews[:30]:
            review_payload.append(
                {
                    "rating": review.get("rating"),
                    "date": review.get("date"),
                    "text": review.get("text"),
                    "owner_reply": review.get("owner_reply"),
                }
            )

        prompt = (
            "You are a review analyst. Return ONLY valid JSON with this exact schema:\n"
            "{\n"
            '  "overall_sentiment": "positive|mixed|negative",\n'
            '  "main_topics": ["string"],\n'
            '  "strengths": ["string"],\n'
            '  "weaknesses": ["string"],\n'
            '  "suggested_owner_reply": "string"\n'
            "}\n"
            "Rules: no markdown, no extra keys, and all list items must be short.\n"
            f"Business: {business_name}\n"
            f"Stats: {json.dumps(stats, ensure_ascii=False)}\n"
            f"Reviews: {json.dumps(review_payload, ensure_ascii=False)}"
        )

        try:
            response_text = await asyncio.to_thread(
                self._generate_content,
                prompt,
            )
            return self._parse_analysis(response_text, business_name, stats)
        except Exception:
            return self._fallback_analysis(business_name, stats)

    def _generate_content(self, prompt: str) -> str:
        candidates = list(dict.fromkeys([self.model_name, *self.fallback_models]))
        last_error: Exception | None = None

        for model_name in candidates:
            try:
                response = self.client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                extracted_text = self._extract_text(response)
                if extracted_text:
                    return extracted_text
                return "{}"
            except genai_errors.ClientError as exc:
                last_error = exc
                if exc.code == 404:
                    continue
                raise

        if last_error:
            raise last_error
        return "{}"

    def _parse_analysis(self, response_text: str, business_name: str, stats: dict) -> ReviewAnalysis:
        normalized = response_text.strip()
        if normalized.startswith("```"):
            normalized = normalized.strip("`")
            if normalized.lower().startswith("json"):
                normalized = normalized[4:].strip()

        data = json.loads(normalized)
        sentiment = str(data.get("overall_sentiment", "mixed")).lower().strip()
        if sentiment not in {"positive", "mixed", "negative"}:
            sentiment = "mixed"

        main_topics = self._safe_str_list(data.get("main_topics"))
        strengths = self._safe_str_list(data.get("strengths"))
        weaknesses = self._safe_str_list(data.get("weaknesses"))
        suggested_owner_reply = str(data.get("suggested_owner_reply", "")).strip()

        if not suggested_owner_reply:
            suggested_owner_reply = (
                f"Thank you for your feedback about {business_name}. "
                "We appreciate your comments and we are working on improvements."
            )

        return ReviewAnalysis(
            overall_sentiment=sentiment,
            main_topics=main_topics,
            strengths=strengths,
            weaknesses=weaknesses,
            suggested_owner_reply=suggested_owner_reply,
        )

    def _safe_str_list(self, value: object) -> list[str]:
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()][:8]

    def _extract_text(self, response: object) -> str:
        texts: list[str] = []
        candidates = getattr(response, "candidates", None) or []
        for candidate in candidates:
            content = getattr(candidate, "content", None)
            if content is None:
                continue
            for part in getattr(content, "parts", None) or []:
                text = getattr(part, "text", None)
                if text:
                    texts.append(str(text).strip())
        return "\n".join([text for text in texts if text]).strip()

    def _fallback_analysis(self, business_name: str, stats: dict) -> ReviewAnalysis:
        avg_rating = float(stats.get("avg_rating", 0.0))

        if avg_rating >= 4.0:
            sentiment = "positive"
        elif avg_rating <= 2.5:
            sentiment = "negative"
        else:
            sentiment = "mixed"

        return ReviewAnalysis(
            overall_sentiment=sentiment,
            main_topics=["service", "food quality", "waiting time"],
            strengths=["Friendly staff", "Consistent quality"],
            weaknesses=["Long waiting times in peak hours"],
            suggested_owner_reply=(
                f"Thank you for your feedback about {business_name}. "
                "We appreciate your comments and we are working on improvements."
            ),
        )
