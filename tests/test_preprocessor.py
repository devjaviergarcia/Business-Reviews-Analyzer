from src.pipeline.preprocessor import ReviewPreprocessor


def test_process_cleans_text_and_rating() -> None:
    preprocessor = ReviewPreprocessor()

    raw_reviews = [
        {
            "author_name": "Alice",
            "rating": "4,5",
            "relative_time": "2 months ago",
            "text": "  Great place!\n\n",
            "owner_reply": " Thanks for coming! ",
        }
    ]

    processed = preprocessor.process(raw_reviews)
    assert processed[0]["text"] == "Great place!"
    assert processed[0]["rating"] == 4.5
    assert processed[0]["relative_time_bucket"] == "recent"
    assert processed[0]["has_text"] is True
    assert processed[0]["has_owner_reply"] is True


def test_process_detects_old_reviews() -> None:
    preprocessor = ReviewPreprocessor()

    raw_reviews = [{"rating": 2, "relative_time": "2 years ago", "text": "Bad service"}]
    processed = preprocessor.process(raw_reviews)

    assert processed[0]["relative_time_bucket"] == "old"


def test_compute_stats_returns_expected_values() -> None:
    preprocessor = ReviewPreprocessor()

    reviews = preprocessor.process(
        [
            {"rating": 5, "relative_time": "1 month ago", "text": "Excellent", "owner_reply": "Thanks"},
            {"rating": 3, "relative_time": "6 months ago", "text": "Ok"},
            {"rating": 1, "relative_time": "2 years ago", "text": "Terrible"},
        ]
    )

    stats = preprocessor.compute_stats(reviews)

    assert stats["avg_rating"] == 3.0
    assert stats["rating_distribution"] == {"1": 1, "2": 0, "3": 1, "4": 0, "5": 1}
    assert stats["response_rate"] == 0.3333
    assert stats["total_with_text"] == 3
