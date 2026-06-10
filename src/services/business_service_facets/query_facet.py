from __future__ import annotations

from typing import Any


class BusinessServiceQueryFacet:

    async def get_business(self, business_id: str, include_listing: bool = True) -> dict:
        return await self.query_service.get_business(business_id=business_id, include_listing=include_listing)

    async def list_businesses(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
        include_listing: bool = False,
    ) -> dict:
        return await self.query_service.list_businesses(
            page=page,
            page_size=page_size,
            include_listing=include_listing,
        )

    async def get_business_reviews(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        return await self.query_service.get_business_reviews(
            business_id=business_id,
            page=page,
            page_size=page_size,
        )

    async def get_business_analysis(self, business_id: str) -> dict:
        return await self.query_service.get_business_analysis(business_id=business_id)

    async def list_business_analyses(
        self,
        business_id: str,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> dict:
        return await self.query_service.list_business_analyses(
            business_id=business_id,
            page=page,
            page_size=page_size,
        )

    async def get_business_sources_overview(
        self,
        *,
        business_id: str,
        comments_preview_size: int = 5,
    ) -> dict[str, Any]:
        return await self.query_service.get_business_sources_overview(
            business_id=business_id,
            comments_preview_size=comments_preview_size,
        )

    async def list_business_comments(
        self,
        *,
        business_id: str,
        source: str | None = None,
        scrape_type: str | None = None,
        page: int = 1,
        page_size: int = 50,
        rating_gte: float | None = None,
        rating_lte: float | None = None,
        order: str = "desc-date",
    ) -> dict[str, Any]:
        return await self.query_service.list_business_comments(
            business_id=business_id,
            source=source,
            scrape_type=scrape_type,
            page=page,
            page_size=page_size,
            rating_gte=rating_gte,
            rating_lte=rating_lte,
            order=order,
        )
