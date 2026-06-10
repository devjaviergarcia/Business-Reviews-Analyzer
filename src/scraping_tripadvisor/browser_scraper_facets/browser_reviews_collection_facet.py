from __future__ import annotations

from .browser_reviews_orchestration_facet import TripadvisorBrowserReviewsOrchestrationFacet
from .browser_reviews_page_collection_facet import TripadvisorBrowserReviewsPageCollectionFacet


class TripadvisorBrowserReviewsCollectionFacet(
    TripadvisorBrowserReviewsOrchestrationFacet,
    TripadvisorBrowserReviewsPageCollectionFacet,
):
    pass
