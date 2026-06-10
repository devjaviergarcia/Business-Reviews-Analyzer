from __future__ import annotations

from typing import Any, Callable


SanitizePayloadFn = Callable[[Any], Any]


class BusinessSummaryRuntime:
    def __init__(self, *, sanitize_response_payload: SanitizePayloadFn) -> None:
        self._sanitize_response_payload = sanitize_response_payload

    async def build_cached_response(
        self,
        *,
        businesses: Any,
        reviews: Any,
        analyses: Any,
        name_normalized: str,
        strategy: str,
    ) -> dict[str, Any] | None:
        business_doc = await businesses.find_one({'name_normalized': name_normalized})
        if not business_doc:
            return None
        business_id = str(business_doc['_id'])
        latest_analysis = await analyses.find_one({'business_id': business_id}, sort=[('created_at', -1)])
        if latest_analysis is None:
            return None
        latest_analysis.pop('_id', None)
        review_count = await reviews.count_documents({'business_id': business_id})
        return self._sanitize_response_payload(
            {
                'business_id': business_id,
                'name': business_doc.get('name', ''),
                'cached': True,
                'strategy': strategy,
                'listing': business_doc.get('listing'),
                'stats': business_doc.get('stats', {}),
                'review_count': review_count,
                'scraped_review_count': business_doc.get('scraped_review_count'),
                'processed_review_count': business_doc.get('processed_review_count'),
                'listing_total_reviews': (business_doc.get('listing') or {}).get('total_reviews'),
                'analysis': latest_analysis,
            }
        )

    def serialize_business_doc(self, *, business_doc: dict[str, Any], review_count: int, include_listing: bool) -> dict[str, Any]:
        payload = {
            'business_id': str(business_doc.get('_id')),
            'name': business_doc.get('name', ''),
            'name_normalized': business_doc.get('name_normalized', ''),
            'source': business_doc.get('source', 'google_maps'),
            'stats': business_doc.get('stats', {}),
            'review_count': review_count,
            'last_scraped_at': business_doc.get('last_scraped_at'),
            'created_at': business_doc.get('created_at'),
            'updated_at': business_doc.get('updated_at'),
            'latest_analysis_id': business_doc.get('latest_analysis_id'),
        }
        if include_listing:
            payload['listing'] = business_doc.get('listing')
        return payload

    def serialize_business_summary_doc(
        self,
        *,
        business_doc: dict[str, Any],
        latest_analysis: dict[str, Any] | None,
        include_listing: bool,
    ) -> dict[str, Any]:
        listing_raw = business_doc.get('listing')
        listing = listing_raw if isinstance(listing_raw, dict) else {}
        categories_raw = listing.get('categories')
        categories = [str(item).strip() for item in categories_raw] if isinstance(categories_raw, list) else []
        review_count_raw = business_doc.get('review_count', 0)
        try:
            review_count = max(0, int(review_count_raw))
        except (TypeError, ValueError):
            review_count = 0
        payload = {
            'business_id': str(business_doc.get('_id')),
            'name': str(business_doc.get('name', '') or ''),
            'description': self.build_business_description(
                business_doc=business_doc,
                latest_analysis=latest_analysis,
                categories=categories,
            ),
            'source': business_doc.get('source', 'google_maps'),
            'review_count': review_count,
            'address': listing.get('address'),
            'phone': listing.get('phone'),
            'website': listing.get('website'),
            'overall_rating': listing.get('overall_rating'),
            'total_reviews': listing.get('total_reviews'),
            'categories': categories,
            'last_scraped_at': business_doc.get('last_scraped_at'),
            'created_at': business_doc.get('created_at'),
            'updated_at': business_doc.get('updated_at'),
            'latest_analysis_id': business_doc.get('latest_analysis_id'),
        }
        if include_listing:
            payload['listing'] = listing
        return payload

    def build_business_description(
        self,
        *,
        business_doc: dict[str, Any],
        latest_analysis: dict[str, Any] | None,
        categories: list[str],
    ) -> str:
        if latest_analysis:
            sentiment = str(latest_analysis.get('overall_sentiment', '') or '').strip()
            topics_raw = latest_analysis.get('main_topics')
            topics = [str(item).strip() for item in topics_raw] if isinstance(topics_raw, list) else []
            topics = [item for item in topics if item][:3]
            if sentiment and topics:
                return f"Sentiment: {sentiment}. Main topics: {', '.join(topics)}."
            if sentiment:
                return f'Sentiment: {sentiment}.'
            if topics:
                return f"Main topics: {', '.join(topics)}."
        filtered_categories = [item for item in categories if item][:3]
        if filtered_categories:
            return f"Categories: {', '.join(filtered_categories)}."
        business_name = str(business_doc.get('name', '') or '').strip()
        if business_name:
            return f'Google Maps business profile for {business_name}.'
        return 'Google Maps business profile.'

    @staticmethod
    def serialize_review_doc(review_doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(review_doc)
        payload['id'] = str(payload.pop('_id'))
        payload.pop('fingerprint', None)
        return payload

    @staticmethod
    def serialize_analysis_doc(analysis_doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(analysis_doc)
        payload['id'] = str(payload.pop('_id'))
        return payload

    @staticmethod
    def serialize_analysis_job_doc(job_doc: dict[str, Any]) -> dict[str, Any]:
        payload = dict(job_doc)
        payload['job_id'] = str(payload.pop('_id'))
        return payload
