from __future__ import annotations

from .browser_review_dom_facet import TripadvisorBrowserReviewDomFacet
from .browser_review_identity_facet import TripadvisorBrowserReviewIdentityFacet
from .browser_review_owner_reply_facet import TripadvisorBrowserReviewOwnerReplyFacet


class TripadvisorBrowserReviewCardFacet(
    TripadvisorBrowserReviewDomFacet,
    TripadvisorBrowserReviewOwnerReplyFacet,
    TripadvisorBrowserReviewIdentityFacet,
):
    pass
