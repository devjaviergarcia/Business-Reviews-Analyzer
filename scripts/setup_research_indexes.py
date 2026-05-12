"""
setup_research_indexes.py
=========================
Crea los índices de MongoDB para las colecciones research_terms y research_leads.
Ejecutar una sola vez:

    python scripts/setup_research_indexes.py
"""

import sys
from pathlib import Path

from pymongo import MongoClient, ASCENDING, DESCENDING

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings


def main() -> None:
    client = MongoClient(settings.mongo_uri)
    db = client[settings.db_name]

    # research_terms
    db["research_terms"].create_index([("term_id", ASCENDING)], unique=True)
    db["research_terms"].create_index([("term_key", ASCENDING)])
    db["research_terms"].create_index([("captured_at", DESCENDING)])
    print("research_terms: índices creados")

    # research_leads
    db["research_leads"].create_index([("listing_id", ASCENDING)], unique=True)
    db["research_leads"].create_index([("term_id", ASCENDING)])
    db["research_leads"].create_index([("term_key", ASCENDING)])
    db["research_leads"].create_index([("rating", ASCENDING)])
    db["research_leads"].create_index([("in_range", ASCENDING)])
    db["research_leads"].create_index([
        ("term_key", ASCENDING),
        ("rating", ASCENDING),
        ("in_range", ASCENDING),
    ])
    print("research_leads: índices creados")

    client.close()
    print("\nListo. Colecciones: research_terms, research_leads")


if __name__ == "__main__":
    main()
