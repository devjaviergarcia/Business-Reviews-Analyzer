from __future__ import annotations

from datetime import datetime, timezone

from src.workers.report_worker import ReportWorker


def test_intro_context_text_hides_technical_identifiers() -> None:
    worker = ReportWorker.__new__(ReportWorker)
    analysis_doc = {
        "dataset_id": "69c7cfa1a79fed2455c34d73",
        "created_at": datetime(2026, 3, 28, 12, 56, 31, tzinfo=timezone.utc),
    }
    review_docs = [
        {"source": "google_maps", "scraped_at": datetime(2026, 3, 27, tzinfo=timezone.utc)},
        {"source": "tripadvisor", "scraped_at": datetime(2026, 3, 26, tzinfo=timezone.utc)},
    ]

    text = worker._build_intro_context_text(
        business_name="El Patrón 2",
        analysis_doc=analysis_doc,
        review_docs=review_docs,
    )

    assert "69c7cfa1a79fed2455c34d73" not in text
    assert "T12:56:31" not in text
    assert "Google Maps" in text
    assert "Tripadvisor" in text
    assert "de marzo de 2026" in text
