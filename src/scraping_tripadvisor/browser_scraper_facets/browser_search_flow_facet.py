from __future__ import annotations

from .browser_search_entry_facet import TripadvisorBrowserSearchEntryFacet
from .browser_search_results_facet import TripadvisorBrowserSearchResultsFacet
from .browser_search_typeahead_facet import TripadvisorBrowserSearchTypeaheadFacet


class TripadvisorBrowserSearchFlowFacet(
    TripadvisorBrowserSearchEntryFacet,
    TripadvisorBrowserSearchTypeaheadFacet,
    TripadvisorBrowserSearchResultsFacet,
):
    pass
