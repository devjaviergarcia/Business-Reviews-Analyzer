from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


ANNEX_REVIEW_ROW_FIELDS = [
    "review_index",
    "customer_key",
    "cluster_id",
    "cluster_label",
    "source",
    "author_name",
    "rating",
    "sentiment",
    "expectation_gap",
    "satisfaction",
    "tranquility_aggressiveness",
    "improvement_intent",
    "dominant_problem",
    "has_owner_reply",
    "owner_reply_excerpt",
    "review_excerpt",
]



def write_annex_review_rows_csv(*, annexes_payload: dict[str, Any], csv_path: Path) -> None:
    full_data = annexes_payload.get("full_data")
    if not isinstance(full_data, dict):
        full_data = {}
    review_rows = full_data.get("review_rows")
    if not isinstance(review_rows, list):
        review_rows = []

    with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=ANNEX_REVIEW_ROW_FIELDS)
        writer.writeheader()
        for row in review_rows:
            if not isinstance(row, dict):
                continue
            safe_row = {key: row.get(key) for key in ANNEX_REVIEW_ROW_FIELDS}
            writer.writerow(safe_row)
