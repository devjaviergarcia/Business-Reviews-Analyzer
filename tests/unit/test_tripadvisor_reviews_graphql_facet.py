from __future__ import annotations

import importlib.util
import json
import re
import sys
import unittest
from pathlib import Path


def _load_tripadvisor_graphql_facet_class():
    module_path = (
        Path(__file__)
        .resolve()
        .parents[2]
        / "src/scraping_tripadvisor/browser_scraper_facets/browser_reviews_graphql_facet.py"
    )
    spec = importlib.util.spec_from_file_location("test_tripadvisor_reviews_graphql_facet_module", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.TripadvisorBrowserReviewsGraphqlFacet


TripadvisorBrowserReviewsGraphqlFacet = _load_tripadvisor_graphql_facet_class()


class _DummyTripadvisorGraphqlFacet(TripadvisorBrowserReviewsGraphqlFacet):
    def _clean_text(self, value):
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def _extract_review_id_from_href(self, href):
        match = re.search(r"-r(\d+)-", str(href or ""))
        return match.group(1) if match else ""

    def _parse_rating(self, value):
        text = self._clean_text(value).replace(",", ".")
        match = re.search(r"(\d+(?:\.\d+)?)", text)
        if not match:
            return None
        parsed = float(match.group(1))
        return parsed if 0.0 <= parsed <= 5.0 else None

    def _parse_total_reviews(self, value):
        digits = re.sub(r"[^\d]", "", self._clean_text(value))
        return int(digits) if digits else None


class TripadvisorReviewsGraphqlFacetTests(unittest.TestCase):
    def test_parses_review_batch_from_har(self) -> None:
        har_path = Path(__file__).resolve().parents[2] / "data/archive/entrydata-ta-business.har"
        har = json.loads(har_path.read_text())
        entry = har["log"]["entries"][702]
        request_batch = json.loads(entry["request"]["postData"]["text"])
        response_batch = json.loads(entry["response"]["content"]["text"])

        facet = _DummyTripadvisorGraphqlFacet()
        batch = None
        for request_item, response_item in zip(request_batch, response_batch):
            batch = facet._parse_tripadvisor_graphql_review_batch(
                request_item=request_item,
                response_item=response_item,
            )
            if batch is not None:
                break

        self.assertIsNotNone(batch)
        assert batch is not None
        self.assertEqual(batch.offset, 0)
        self.assertEqual(batch.limit, 15)
        self.assertEqual(batch.total_count, 182)
        self.assertEqual(len(batch.reviews), 15)

        first_review = batch.reviews[0]
        self.assertEqual(first_review["review_id"], "1063123507")
        self.assertEqual(first_review["author_name"], "FULGENCIO P")
        self.assertEqual(first_review["review_title"], "Experiencia para recomendar")
        self.assertEqual(first_review["source_capture"], "graphql")


if __name__ == "__main__":
    unittest.main()
