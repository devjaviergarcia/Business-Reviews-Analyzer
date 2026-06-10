from __future__ import annotations

from .browser_reviews_navigation_facet import TripadvisorBrowserReviewsNavigationFacet
from .browser_reviews_pagination_state_facet import TripadvisorBrowserReviewsPaginationStateFacet


class TripadvisorBrowserReviewsPaginationFacet(
    TripadvisorBrowserReviewsNavigationFacet,
    TripadvisorBrowserReviewsPaginationStateFacet,
):
    pass
