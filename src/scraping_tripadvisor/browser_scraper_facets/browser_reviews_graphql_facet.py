from __future__ import annotations

import asyncio
import inspect
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from time import monotonic
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from playwright.async_api import Response


@dataclass(slots=True)
class TripadvisorGraphqlReviewPageBatch:
    offset: int
    limit: int
    total_count: int
    language: str
    captured_at_monotonic: float
    reviews: list[dict[str, Any]]


class TripadvisorBrowserReviewsGraphqlFacet:
    _TRIPADVISOR_REVIEW_LIST_QUERY_ID = "ef1a9f94012220d3"
    _TRIPADVISOR_GRAPHQL_ENDPOINT_FRAGMENT = "/data/graphql/ids"

    async def _install_tripadvisor_graphql_review_capture(self) -> None:
        page = self._require_page()
        if getattr(self, "_tripadvisor_graphql_review_capture_listener_installed", False):
            return

        def _handle_response(response: Response) -> None:
            try:
                response_url = str(getattr(response, "url", "") or "")
            except Exception:
                return
            if self._TRIPADVISOR_GRAPHQL_ENDPOINT_FRAGMENT not in response_url:
                return
            try:
                task = asyncio.create_task(self._capture_tripadvisor_graphql_review_response(response))
            except RuntimeError:
                return
            self._tripadvisor_graphql_review_capture_tasks.add(task)
            task.add_done_callback(self._finish_tripadvisor_graphql_capture_task)

        page.on("response", _handle_response)
        self._tripadvisor_graphql_review_capture_listener_installed = True

    def _finish_tripadvisor_graphql_capture_task(self, task: asyncio.Task[Any]) -> None:
        self._tripadvisor_graphql_review_capture_tasks.discard(task)
        try:
            task.result()
        except Exception:
            return

    async def _capture_tripadvisor_graphql_review_response(self, response: Response) -> None:
        request = getattr(response, "request", None)
        if request is None:
            return

        method = str(getattr(request, "method", "") or "").upper()
        if method != "POST":
            return

        post_data_text = str(await self._resolve_maybe_async_value(getattr(request, "post_data", "")) or "")
        if self._TRIPADVISOR_REVIEW_LIST_QUERY_ID not in post_data_text:
            return

        response_text = str(await self._resolve_maybe_async_value(getattr(response, "text", "")) or "")
        if not response_text:
            return

        try:
            request_batch = json.loads(post_data_text)
            response_batch = json.loads(response_text)
        except json.JSONDecodeError:
            return

        if isinstance(request_batch, dict):
            request_batch = [request_batch]
        if isinstance(response_batch, dict):
            response_batch = [response_batch]
        if not isinstance(request_batch, list) or not isinstance(response_batch, list):
            return

        for request_item, response_item in zip(request_batch, response_batch):
            batch = self._parse_tripadvisor_graphql_review_batch(
                request_item=request_item,
                response_item=response_item,
            )
            if batch is None:
                continue
            self._tripadvisor_graphql_review_batches_by_offset[batch.offset] = batch
            self._last_tripadvisor_graphql_reviews_offset = batch.offset

    async def _resolve_maybe_async_value(self, accessor: Any) -> Any:
        value = accessor() if callable(accessor) else accessor
        if inspect.isawaitable(value):
            return await value
        return value

    def _parse_tripadvisor_graphql_review_batch(
        self,
        *,
        request_item: Any,
        response_item: Any,
    ) -> TripadvisorGraphqlReviewPageBatch | None:
        if not isinstance(request_item, dict) or not isinstance(response_item, dict):
            return None

        query_id = self._clean_text(
            ((request_item.get("extensions") or {}).get("preRegisteredQueryId"))
        )
        if query_id != self._TRIPADVISOR_REVIEW_LIST_QUERY_ID:
            return None

        variables = request_item.get("variables") or {}
        if not isinstance(variables, dict):
            variables = {}

        response_data = response_item.get("data") or {}
        if not isinstance(response_data, dict):
            return None

        payload = self._locate_tripadvisor_graphql_reviews_payload(response_data)
        if payload is None:
            return None

        raw_reviews = payload.get("reviews") or []
        if not isinstance(raw_reviews, list):
            raw_reviews = []

        try:
            offset = max(0, int(variables.get("offset", 0) or 0))
        except (TypeError, ValueError):
            offset = 0

        parsed_limit = variables.get("limit", len(raw_reviews))
        try:
            limit = max(0, int(parsed_limit or 0))
        except (TypeError, ValueError):
            limit = len(raw_reviews)

        total_count = self._parse_total_reviews(payload.get("totalCount")) or len(raw_reviews)
        language = self._clean_text(variables.get("language") or "")

        normalized_reviews = [
            self._normalize_tripadvisor_graphql_review(raw_review)
            for raw_review in raw_reviews
            if isinstance(raw_review, dict)
        ]
        normalized_reviews = [item for item in normalized_reviews if item]
        if not normalized_reviews:
            return None

        return TripadvisorGraphqlReviewPageBatch(
            offset=offset,
            limit=limit,
            total_count=total_count,
            language=language,
            captured_at_monotonic=monotonic(),
            reviews=normalized_reviews,
        )

    def _locate_tripadvisor_graphql_reviews_payload(self, response_data: dict[str, Any]) -> dict[str, Any] | None:
        direct_payload = response_data.get("ReviewsProxy_getReviewListPageForLocation")
        if isinstance(direct_payload, list) and direct_payload and isinstance(direct_payload[0], dict):
            return direct_payload[0]
        if isinstance(direct_payload, dict):
            return direct_payload

        for value in response_data.values():
            if isinstance(value, list) and value and isinstance(value[0], dict) and isinstance(value[0].get("reviews"), list):
                return value[0]
            if isinstance(value, dict) and isinstance(value.get("reviews"), list):
                return value
        return None

    def _normalize_tripadvisor_graphql_review(self, raw_review: dict[str, Any]) -> dict[str, Any]:
        review_route = self._clean_text(
            (((raw_review.get("reviewDetailPageWrapper") or {}).get("reviewDetailPageRoute") or {}).get("url"))
        )
        review_id = self._clean_text(raw_review.get("id") or "") or self._extract_review_id_from_href(review_route)

        published_date = self._clean_text(raw_review.get("publishedDate") or "")
        created_date = self._clean_text(raw_review.get("createdDate") or "")
        written_date = published_date or created_date
        relative_time = self._tripadvisor_relative_time_from_iso_date(written_date) or written_date

        user_profile = raw_review.get("userProfile") or {}
        if not isinstance(user_profile, dict):
            user_profile = {}

        author_name = self._clean_text(
            user_profile.get("displayName")
            or raw_review.get("username")
            or ""
        )

        normalized_review: dict[str, Any] = {
            "source": "tripadvisor",
            "source_capture": "graphql",
            "review_id": str(review_id or "").strip(),
            "author_name": author_name,
            "rating": self._parse_rating(raw_review.get("rating")) or 0.0,
            "relative_time": relative_time,
            "text": self._clean_text(raw_review.get("text") or ""),
            "review_title": self._clean_text(raw_review.get("title") or ""),
            "written_date": written_date,
        }

        if review_route:
            normalized_review["title_href"] = review_route
            normalized_review["review_url"] = review_route
        if published_date:
            normalized_review["published_date"] = published_date
        if created_date:
            normalized_review["created_date"] = created_date

        language = self._clean_text(raw_review.get("language") or raw_review.get("originalLanguage") or "")
        if language:
            normalized_review["review_language"] = language

        helpful_votes = raw_review.get("helpfulVotes")
        if isinstance(helpful_votes, int):
            normalized_review["helpful_votes"] = helpful_votes

        user_profile_route = self._clean_text(((user_profile.get("route") or {}).get("url")))
        if user_profile_route:
            normalized_review["author_profile_url"] = user_profile_route

        trip_info = raw_review.get("tripInfo") or {}
        if isinstance(trip_info, dict):
            stay_date = self._clean_text(trip_info.get("stayDate") or "")
            trip_type = self._clean_text(trip_info.get("tripType") or "")
            if stay_date:
                normalized_review["stay_date"] = stay_date
            if trip_type:
                normalized_review["trip_type"] = trip_type

        image_urls = self._tripadvisor_graphql_photo_urls(raw_review.get("photos"))
        if image_urls:
            normalized_review["image_urls"] = image_urls

        owner_reply = self._tripadvisor_graphql_owner_reply(raw_review.get("mgmtResponse"))
        if owner_reply is not None:
            normalized_review["owner_reply"] = {
                "text": owner_reply["text"],
                "relative_time": owner_reply["relative_time"],
            }
            normalized_review["owner_reply_author_name"] = owner_reply.get("author_name", "")
            normalized_review["owner_reply_written_date"] = owner_reply.get("written_date", "")

        return normalized_review

    def _tripadvisor_graphql_photo_urls(self, raw_photos: Any) -> list[str]:
        if not isinstance(raw_photos, list):
            return []

        collected: list[str] = []
        seen: set[str] = set()
        for raw_photo in raw_photos:
            if not isinstance(raw_photo, dict):
                continue
            photo = raw_photo.get("photo") or {}
            if not isinstance(photo, dict):
                continue
            dynamic_size = photo.get("photoSizeDynamic") or {}
            if not isinstance(dynamic_size, dict):
                continue
            url_template = self._clean_text(dynamic_size.get("urlTemplate") or "")
            if not url_template:
                continue
            resolved_url = url_template.replace("{width}", "1200").replace("{height}", "1200")
            if not resolved_url or resolved_url in seen:
                continue
            seen.add(resolved_url)
            collected.append(resolved_url)
        return collected[:12]

    def _tripadvisor_graphql_owner_reply(self, raw_owner_reply: Any) -> dict[str, str] | None:
        if not isinstance(raw_owner_reply, dict):
            return None

        text = self._clean_text(
            raw_owner_reply.get("text")
            or raw_owner_reply.get("response")
            or raw_owner_reply.get("body")
            or ""
        )
        if not text:
            return None

        published_date = self._clean_text(raw_owner_reply.get("publishedDate") or "")
        created_date = self._clean_text(raw_owner_reply.get("createdDate") or "")
        written_date = published_date or created_date
        relative_time = self._tripadvisor_relative_time_from_iso_date(written_date) or written_date

        author_name = self._clean_text(
            ((raw_owner_reply.get("userProfile") or {}).get("displayName"))
            or raw_owner_reply.get("authorName")
            or ""
        )

        return {
            "text": text,
            "relative_time": relative_time,
            "written_date": written_date,
            "author_name": author_name,
        }

    async def _current_tripadvisor_graphql_review_page(
        self,
        *,
        timeout_ms: int,
    ) -> TripadvisorGraphqlReviewPageBatch | None:
        await self._install_tripadvisor_graphql_review_capture()
        expected_offset = await self._expected_tripadvisor_graphql_reviews_offset()
        self._last_tripadvisor_graphql_expected_offset = expected_offset

        deadline = monotonic() + (max(0, timeout_ms) / 1000.0)
        while monotonic() < deadline:
            batch = self._select_tripadvisor_graphql_review_batch(expected_offset=expected_offset)
            if batch is not None and batch.reviews:
                return batch
            page = self._require_page()
            await page.wait_for_timeout(90)

        return self._select_tripadvisor_graphql_review_batch(expected_offset=expected_offset)

    def _select_tripadvisor_graphql_review_batch(
        self,
        *,
        expected_offset: int | None,
    ) -> TripadvisorGraphqlReviewPageBatch | None:
        if expected_offset is not None:
            return self._tripadvisor_graphql_review_batches_by_offset.get(expected_offset)
        if not self._tripadvisor_graphql_review_batches_by_offset:
            return None
        return max(
            self._tripadvisor_graphql_review_batches_by_offset.values(),
            key=lambda item: item.captured_at_monotonic,
        )

    async def _expected_tripadvisor_graphql_reviews_offset(self) -> int:
        page = self._require_page()
        offset_from_url = self._reviews_offset_from_href(page.url)
        if offset_from_url is not None:
            return max(0, offset_from_url)

        snapshot = await self._reviews_pagination_snapshot()
        range_start = snapshot.get("range_start")
        if isinstance(range_start, int) and range_start > 0:
            return max(0, range_start - 1)

        current_page = snapshot.get("current_page")
        range_end = snapshot.get("range_end")
        if (
            isinstance(current_page, int)
            and current_page > 0
            and isinstance(range_end, int)
            and range_end > 0
        ):
            page_size = max(1, range_end // current_page)
            return max(0, (current_page - 1) * page_size)

        return 0

    def _project_tripadvisor_graphql_reviews_for_collection(
        self,
        *,
        reviews: list[dict[str, Any]],
        include_owner_reply: bool,
        include_image_urls: bool,
    ) -> list[dict[str, Any]]:
        projected_reviews: list[dict[str, Any]] = []
        for review in reviews:
            item = dict(review)
            if not include_owner_reply:
                item.pop("owner_reply", None)
                item.pop("owner_reply_author_name", None)
                item.pop("owner_reply_written_date", None)
            if not include_image_urls:
                item.pop("image_urls", None)
            projected_reviews.append(item)
        return projected_reviews

    def _tripadvisor_relative_time_from_iso_date(self, value: str) -> str:
        iso_value = self._clean_text(value)
        if not iso_value:
            return ""
        try:
            parsed_day = date.fromisoformat(iso_value[:10])
        except ValueError:
            return ""

        delta_days = max(0, (datetime.now(timezone.utc).date() - parsed_day).days)
        if delta_days == 0:
            return "Hace un momento"
        if delta_days == 1:
            return "Hace 1 día"
        if delta_days < 30:
            return f"Hace {delta_days} días"

        months = max(1, delta_days // 30)
        if months < 12:
            return "Hace 1 mes" if months == 1 else f"Hace {months} meses"

        years = max(1, delta_days // 365)
        return "Hace 1 año" if years == 1 else f"Hace {years} años"
