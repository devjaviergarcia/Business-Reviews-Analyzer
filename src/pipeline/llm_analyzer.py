from src.models.analysis import ReviewAnalysis


class ReviewLLMAnalyzer:
    async def analyze(self, business_name: str, reviews: list[dict], stats: dict) -> ReviewAnalysis:
        _ = reviews
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
