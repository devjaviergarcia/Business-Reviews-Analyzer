from typing import Final

# Selector strategy based on UI structure and behavior attributes.
# Avoid concrete ids because they change frequently in Google Maps.
SELECTOR_PATTERNS: Final[dict[str, tuple[str, ...]]] = {
    # Search controls
    "SEARCH_INPUT": (
        "div[role='search'] input[role='combobox'][name='q']",
        "form[jsaction*='searchboxFormSubmit'] input[name='q']",
        "div[role='search'] input[role='combobox']",
    ),
    "SEARCH_BUTTON": (
        "div[role='search'] button[jsaction*='omnibox.search']",
        "div[role='search'] button[aria-label*='busqueda' i]",
        "div[role='search'] button[aria-label*='search' i]",
    ),
    "SUGGESTION_ROWS": (
        "div[role='grid'][aria-label*='sugerencias' i] [jsaction*='suggestion.select']",
        "div[role='grid'][aria-label*='suggestions' i] [jsaction*='suggestion.select']",
        "div[role='grid'] [jsaction*='suggestion.select']",
        "div[role='grid'] [data-suggestion-index]",
    ),
    # Search results list (left panel)
    "RESULTS_FEED": (
        "div[role='feed']",
        "div[aria-label*='Resultados' i][role='feed']",
        "div[aria-label*='Results' i][role='feed']",
    ),
    "RESULT_ITEMS": (
        "div[role='feed'] a[href*='/maps/place/']",
        "div[role='feed'] a.hfpxzc",
        "div[role='feed'] [jsaction*='pane.result'] a[href*='/maps/place/']",
        "div[role='feed'] [data-result-index]",
    ),
    # Listing readiness signals
    "LISTING_READY": (
        "h1.DUwDvf",
        "button[role='tab'][aria-label*='rese' i]",
        "button[role='tab'][aria-label*='review' i]",
        "div[jsaction*='reviewChart.moreReviews']",
        "button[data-item-id='address']",
    ),
    # Listing fields
    "BUSINESS_NAME": (
        "h1.DUwDvf",
        "h1[class*='DUwDvf']",
    ),
    "REVIEWS_TAB": (
        "button[role='tab'][aria-label*='rese' i]",
        "button[role='tab'][aria-label*='review' i]",
        "button[role='tab'][jsaction*='tabs.tabClick'][aria-label*='rese' i]",
        "button[role='tab'][jsaction*='tabs.tabClick'][aria-label*='review' i]",
        "button[role='tab']:has(.Gpq6kf:has-text('Rese'))",
    ),
    "REVIEWS_BUTTON": (
        "button[jsaction*='reviewChart.moreReviews']",
        "div[jsaction*='reviewChart.moreReviews'] button",
        "button[jsaction*='.reviewChart.moreReviews']",
        "button[aria-label*='más rese' i]",
        "button[aria-label*='mas rese' i]",
        "button[aria-label*='more review' i]",
        "button:has(span:has-text('Más rese'))",
        "button:has(span:has-text('More review'))",
        "button:has(div:has-text('Rese'))",
        "button:has(div:has-text('Review'))",
    ),
    "MORE_REVIEWS_BUTTON": (
        "div.m6QErb.Hk4XGb.QoaCgb button[aria-label*='más rese' i]",
        "div.m6QErb.Hk4XGb.QoaCgb button[aria-label*='mas rese' i]",
        "div.m6QErb.Hk4XGb.QoaCgb button[aria-label*='more review' i]",
        "button.M77dve[aria-label*='más rese' i]",
        "button.M77dve[aria-label*='mas rese' i]",
        "button.M77dve[aria-label*='more review' i]",
        "button:has(span.wNNZR:has-text('Más rese'))",
        "button:has(span.wNNZR:has-text('More review'))",
    ),
    "REVIEWS_PANEL_READY": (
        "button[aria-label*='ordenar rese' i]",
        "button[aria-label*='sort review' i]",
        "input[aria-label*='buscar rese' i]",
        "input[aria-label*='search review' i]",
        "div[role='radiogroup'][aria-label*='filtrar rese' i]",
        "div[role='radiogroup'][aria-label*='filter review' i]",
    ),
    "LISTING_ADDRESS": (
        "button[data-item-id='address'] .Io6YTe",
        "button[data-item-id='address']",
    ),
    "LISTING_WEBSITE": (
        "button[data-item-id='authority'] .Io6YTe",
        "button[data-item-id='authority']",
    ),
    "LISTING_PHONE": (
        "button[data-item-id^='phone:'] .Io6YTe",
        "button[data-item-id^='phone:']",
    ),
    "LISTING_RATING": (
        "div.F7nice [aria-label*='estrella' i]",
        "div[role='img'][aria-label*='estrella' i]",
        "div[role='img'][aria-label*='star' i]",
    ),
    "LISTING_TOTAL_REVIEWS": (
        "div.F7nice [aria-label*='rese' i]",
        "button[jsaction*='reviewChart.moreReviews']",
        "button[aria-label*='rese' i]",
    ),
    "LISTING_CATEGORIES": (
        "button[jsaction*='.category']",
        "button[jsaction*='pane.wfvdle'][aria-label*='rest' i]",
        "div.fontBodyMedium button",
    ),
    # Review cards and fields
    "REVIEW_CARDS": (
        "div.jftiEf[data-review-id]",
        "div[data-review-id].jftiEf",
        "div.jftiEf.fontBodyMedium",
        "div.jftiEf",
        "div[data-review-id][jsaction*='.review.in']",
        "div[jsaction*='.review.in'][data-review-id]",
    ),
    "AUTHOR_NAME": (
        "div.d4r55",
        "[aria-label][data-review-id]",
    ),
    "RATING_LABEL": (
        "span.kvMYJc[role='img']",
        "[role='img'][aria-label*='estrella' i]",
        "[role='img'][aria-label*='star' i]",
    ),
    "RELATIVE_TIME": (
        "span.rsqaWe",
    ),
    "REVIEW_TEXT": (
        ".MyEned .wiI7pd",
        "div.MyEned span.wiI7pd",
    ),
    "REVIEW_EXPAND": (
        "button[jsaction*='.review.expandReview']",
    ),
    "REVIEW_PHOTOS": (
        "button[data-photo-index][data-review-id]",
    ),
    "OWNER_REPLY_BLOCK": (
        "div:has(> div > span.fontTitleSmall:has-text('Respuesta del propietario'))",
        "div:has(> div > span:has-text('Respuesta del propietario'))",
        "div:has(> div > span:has-text('Owner response'))",
        "div.CDe7pd",
        "div:has(> div > span):has(> div.wiI7pd)",
    ),
    "OWNER_REPLY_LABEL": (
        "span.fontTitleSmall",
        "span:has-text('Respuesta del propietario')",
        "span:has-text('Owner response')",
    ),
    "OWNER_REPLY_TIME": (
        ".DZSIDd",
        "span:has-text('Hace')",
        "span:has-text('ago')",
    ),
    "OWNER_REPLY_TEXT": (
        ".wiI7pd",
        "div[lang]",
    ),
}

# Backward-compatible alias for older imports.
SELECTORS = SELECTOR_PATTERNS

