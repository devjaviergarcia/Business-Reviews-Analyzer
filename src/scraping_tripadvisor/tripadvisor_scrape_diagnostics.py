from __future__ import annotations

import html
import os
import re
from datetime import datetime, timezone
from typing import Any

from src.config import settings
from src.database import get_database
from src.scraping_tripadvisor.browser_scraper import TripadvisorScraper
from src.services.tripadvisor_session_service import TripadvisorSessionService


class TripadvisorScrapeDiagnostics:
    def __init__(
        self,
        *,
        diagnostics_collection_name: str,
        anti_bot_keywords: tuple[str, ...],
        anti_bot_strong_keywords: tuple[str, ...],
        anti_bot_captcha_companion_keywords: tuple[str, ...],
        anti_bot_robot_markers: tuple[str, ...],
        anti_bot_datadome_structural_markers: tuple[str, ...],
    ) -> None:
        self._diagnostics_collection_name = diagnostics_collection_name
        self._anti_bot_keywords = anti_bot_keywords
        self._anti_bot_strong_keywords = anti_bot_strong_keywords
        self._anti_bot_captcha_companion_keywords = anti_bot_captcha_companion_keywords
        self._anti_bot_robot_markers = anti_bot_robot_markers
        self._anti_bot_datadome_structural_markers = anti_bot_datadome_structural_markers

    async def ensure_session_available_for_relaunch(
        self,
        *,
        operation: str,
        job_id: str | None = None,
    ) -> None:
        session_service = TripadvisorSessionService()
        session_state = await session_service.ensure_available()
        if bool(session_state.get("availability_now")):
            return
        recovery_context = self.build_recovery_context(
            reason_code="tripadvisor_session_unavailable",
            session_state=session_state,
            user_reason=(
                "Tripadvisor session is not available; relaunching now will fail again."
            ),
        )
        operation_label = str(operation or "tripadvisor_operation").strip()
        job_suffix = f" (job_id={job_id})" if str(job_id or "").strip() else ""
        raise ValueError(
            f"Cannot execute {operation_label}{job_suffix}. "
            f"{recovery_context.get('human_message')}"
        )

    def resolve_effective_start_delay_seconds(self) -> float:
        fixed = max(0.0, float(settings.scraper_tripadvisor_start_delay_seconds))
        minimum = settings.scraper_tripadvisor_start_delay_min_seconds
        maximum = settings.scraper_tripadvisor_start_delay_max_seconds

        if minimum is None and maximum is None:
            return fixed

        lower = fixed if minimum is None else max(0.0, float(minimum))
        upper = fixed if maximum is None else max(0.0, float(maximum))
        if upper < lower:
            lower, upper = upper, lower
        if abs(upper - lower) < 1e-9:
            return lower
        import random

        return random.uniform(lower, upper)

    def resolve_profile_dir_hint(self) -> str:
        local_hint = str(os.getenv("SCRAPER_TRIPADVISOR_USER_DATA_DIR_LOCAL") or "").strip()
        if local_hint:
            return local_hint
        raw_profile_dir = str(settings.scraper_tripadvisor_user_data_dir or "").strip()
        if not raw_profile_dir:
            return "playwright-data-tripadvisor-worker-docker"
        if "worker" not in raw_profile_dir.lower():
            return "playwright-data-tripadvisor-worker-docker"
        if raw_profile_dir.startswith("/app/"):
            return raw_profile_dir.replace("/app/", "", 1)
        return raw_profile_dir

    def build_recovery_context(
        self,
        *,
        reason_code: str,
        session_state: dict[str, Any] | None,
        user_reason: str,
        stage: str | None = None,
        diagnostic_id: str | None = None,
    ) -> dict[str, Any]:
        state = session_state if isinstance(session_state, dict) else {}
        session_state_value = str(state.get("session_state") or "invalid").strip().lower() or "invalid"
        availability_now = bool(state.get("availability_now"))
        last_validation = str(state.get("last_validation_result") or "unknown").strip() or "unknown"
        session_cookie_expires_at = state.get("session_cookie_expires_at")
        last_human_intervention_at = state.get("last_human_intervention_at")
        last_error = str(state.get("last_error") or "").strip() or None
        profile_dir_hint = self.resolve_profile_dir_hint()
        recovery_commands = [
            "./scripts/tripadvisor_ctl.sh human",
            f"./scripts/tripadvisor_ctl.sh session-confirm {profile_dir_hint} true",
            "./scripts/tripadvisor_ctl.sh relaunch <job_id>",
            "./scripts/tripadvisor_ctl.sh relaunch <job_id> --force",
            "./scripts/tripadvisor_ctl.sh relaunch <job_id> --from-zero",
            "./scripts/tripadvisor_ctl.sh trace <job_id> 0",
        ]
        recovery_steps = [
            "Abre una sesión manual de TripAdvisor para resolver captcha/login.",
            "Cierra la ventana al terminar para que el proceso manual finalice.",
            "Confirma la sesión en backend para marcar availability_now=true.",
            "Relanza el job y revisa trazas en tiempo real.",
        ]
        reason_bits = [
            f"session_state={session_state_value}",
            f"availability_now={str(availability_now).lower()}",
            f"last_validation_result={last_validation}",
        ]
        if session_cookie_expires_at:
            reason_bits.append(f"session_cookie_expires_at={session_cookie_expires_at}")
        if last_error:
            reason_bits.append(f"last_error={last_error}")
        if stage:
            reason_bits.append(f"stage={stage}")
        if diagnostic_id:
            reason_bits.append(f"diagnostic_id={diagnostic_id}")
        reason_summary = "; ".join(reason_bits)
        human_message = (
            f"{user_reason} Motivo técnico: {reason_summary}. "
            "Acción requerida: ejecutar intervención humana de TripAdvisor. "
            f"Pasos: 1) {recovery_commands[0]} 2) {recovery_commands[1]} "
            f"3) {recovery_commands[2]} 4) {recovery_commands[3]}"
        )
        payload: dict[str, Any] = {
            "source": "tripadvisor",
            "reason_code": str(reason_code or "tripadvisor_action_required"),
            "reason": str(user_reason),
            "reason_summary": reason_summary,
            "human_message": human_message,
            "required_action": "manual_tripadvisor_intervention",
            "session_state": session_state_value,
            "availability_now": availability_now,
            "last_validation_result": last_validation,
            "session_cookie_expires_at": session_cookie_expires_at,
            "last_human_intervention_at": last_human_intervention_at,
            "last_error": last_error,
            "recovery_steps": recovery_steps,
            "recovery_commands": recovery_commands,
            "profile_dir_hint": profile_dir_hint,
        }
        if stage:
            payload["stage"] = stage
        if diagnostic_id:
            payload["diagnostic_id"] = diagnostic_id
        return payload

    async def record_failure_diagnostic(
        self,
        *,
        business_name: str,
        stage: str,
        scraper: TripadvisorScraper,
        error: str,
        diagnostic_type: str = "stage_error",
        timeout_seconds: int | None = None,
        elapsed_seconds: float | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        snapshot = await self.capture_snapshot(scraper=scraper)
        full_html = str(snapshot.get("html") or "")
        anti_bot_scan_text = self.extract_antibot_scan_text(full_html)
        max_html_length = 200_000
        html_truncated = len(full_html) > max_html_length
        stored_html = full_html[:max_html_length] if html_truncated else full_html
        bot_snippets = self.extract_keyword_context_snippets(
            full_html,
            keyword="bot",
            max_matches=8,
            context_chars=140,
        )
        anti_bot_matches = self.extract_anti_bot_keyword_matches(anti_bot_scan_text)
        anti_bot_detected, anti_bot_detection_rule = self.detect_antibot(
            html_text=full_html,
            keyword_matches=anti_bot_matches,
        )
        anti_bot_match_count = (
            sum(len(items) for items in anti_bot_matches.values()) if anti_bot_detected else 0
        )

        doc = {
            "source": "tripadvisor",
            "diagnostic_type": diagnostic_type,
            "business_name": business_name,
            "stage": stage,
            "timeout_seconds": int(timeout_seconds) if timeout_seconds is not None else None,
            "elapsed_seconds": float(elapsed_seconds) if elapsed_seconds is not None else None,
            "error": str(error or "").strip(),
            "page_url": str(snapshot.get("url") or ""),
            "page_title": str(snapshot.get("title") or ""),
            "html_snapshot": stored_html,
            "html_snapshot_length": len(full_html),
            "html_snapshot_truncated": bool(html_truncated),
            "keyword_matches": {
                "keyword": "bot",
                "count": len(bot_snippets),
                "snippets": bot_snippets,
            },
            "anti_bot": {
                "detected": anti_bot_detected,
                "detection_rule": anti_bot_detection_rule,
                "total_matches": anti_bot_match_count,
                "keywords": anti_bot_matches,
            },
            "capture_errors": list(snapshot.get("capture_errors") or []),
            "created_at": now,
            "updated_at": now,
        }

        diagnostics_collection = get_database()[self._diagnostics_collection_name]
        try:
            insert_result = await diagnostics_collection.insert_one(doc)
            diagnostic_id = str(insert_result.inserted_id)
            persist_error = None
        except Exception as exc:  # noqa: BLE001
            diagnostic_id = None
            persist_error = str(exc)

        return {
            "diagnostic_id": diagnostic_id,
            "persist_error": persist_error,
            "page_url": str(snapshot.get("url") or ""),
            "bot_match_count": len(bot_snippets),
            "anti_bot_detected": anti_bot_detected,
            "anti_bot_detection_rule": anti_bot_detection_rule,
            "anti_bot_match_count": anti_bot_match_count,
        }

    def detect_antibot(
        self,
        *,
        html_text: str,
        keyword_matches: dict[str, list[str]],
    ) -> tuple[bool, str]:
        html_text_lower = str(html_text or "").lower()

        robot_matches: list[str] = []
        for keyword in ("robot", "not a robot", "no soy un robot"):
            robot_matches.extend(keyword_matches.get(keyword) or [])
        robot_text_lower = " ".join(str(snippet or "") for snippet in robot_matches).lower()
        robot_marker_hits = sorted(
            {
                marker
                for marker in self._anti_bot_robot_markers
                if marker in html_text_lower or marker in robot_text_lower
            }
        )
        has_robot_word = bool(re.search(r"\brobot\b", robot_text_lower))
        has_robot_signal = bool(robot_matches) or bool(robot_marker_hits) or has_robot_word
        if not has_robot_signal:
            return False, "robot_keyword_missing"

        datadome_structure_hits = sorted(
            {
                marker
                for marker in self._anti_bot_datadome_structural_markers
                if marker in html_text_lower
            }
        )
        if datadome_structure_hits:
            return True, f"robot_with_datadome_structure:{','.join(datadome_structure_hits)}"

        explicit_challenge_markers = sorted(
            {
                marker
                for marker in (
                    "geo.captcha-delivery.com/captcha/",
                    "captcha/?initialcid=",
                    "ct.captcha-delivery.com/c.js",
                    "datadome captcha",
                )
                if marker in html_text_lower
            }
        )
        if explicit_challenge_markers:
            return True, f"explicit_challenge_markers:{','.join(explicit_challenge_markers)}"

        strong_keywords = {
            keyword
            for keyword in self._anti_bot_strong_keywords
            if keyword_matches.get(keyword)
        }
        if strong_keywords:
            return True, f"strong_keywords:{','.join(sorted(strong_keywords))}"

        captcha_matches = keyword_matches.get("captcha") or []
        if captcha_matches:
            captcha_text_lower = " ".join(str(snippet or "") for snippet in captcha_matches).lower()
            provider_markers = (
                "captcha-delivery.com",
                "datadome",
                "captcha/?initialcid=",
                "ct.captcha-delivery.com/c.js",
                "data-dd-captcha",
                "ddv1-captcha-container",
            )
            provider_hits = sorted(
                {
                    marker
                    for marker in provider_markers
                    if marker in html_text_lower or marker in captcha_text_lower
                }
            )
            if provider_hits:
                return True, f"captcha_provider_markers:{','.join(provider_hits)}"
            companions = [
                marker
                for marker in self._anti_bot_captcha_companion_keywords
                if marker in html_text_lower or marker in captcha_text_lower
            ]
            if companions:
                return True, f"captcha_with_companion:{','.join(sorted(set(companions)))}"
            return False, "captcha_without_companion_or_robot"

        return False, "no_strong_signal_with_robot"

    async def record_stage_timeout_diagnostic(
        self,
        *,
        business_name: str,
        stage: str,
        timeout_seconds: int,
        elapsed_seconds: float,
        scraper: TripadvisorScraper,
        error: str,
    ) -> dict[str, Any]:
        return await self.record_failure_diagnostic(
            business_name=business_name,
            stage=stage,
            scraper=scraper,
            error=error,
            diagnostic_type="stage_timeout",
            timeout_seconds=timeout_seconds,
            elapsed_seconds=elapsed_seconds,
        )

    async def capture_snapshot(
        self,
        *,
        scraper: TripadvisorScraper,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "url": "",
            "title": "",
            "html": "",
            "capture_errors": [],
        }
        page = None
        try:
            page = scraper.page
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"page_unavailable: {exc}")
            return payload

        try:
            payload["url"] = str(page.url or "")
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"url_read_failed: {exc}")
        try:
            payload["title"] = str(await page.title() or "")
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"title_read_failed: {exc}")
        try:
            main_html = str(await page.content() or "")
            payload["html"] = main_html
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"html_capture_failed: {exc}")
            main_html = ""

        try:
            frames = list(getattr(page, "frames", []) or [])
            frame_chunks: list[str] = []
            for index, frame in enumerate(frames):
                try:
                    if frame == page.main_frame:
                        continue
                except Exception:
                    pass
                try:
                    frame_url = str(getattr(frame, "url", "") or "")
                except Exception:
                    frame_url = ""
                try:
                    frame_html = str(await frame.content() or "")
                except Exception as exc:  # noqa: BLE001
                    payload["capture_errors"].append(
                        f"frame_html_capture_failed[{index}] ({frame_url}): {exc}"
                    )
                    continue
                if not frame_html:
                    continue
                escaped_url = html.escape(frame_url, quote=True)
                frame_chunks.append(
                    (
                        f"\n<!-- frame_snapshot index={index} url={escaped_url} -->\n"
                        f"{frame_html}\n"
                    )
                )
            if frame_chunks:
                payload["html"] = (
                    f"{main_html}\n<!-- frame_snapshots_begin -->"
                    f"{''.join(frame_chunks)}\n<!-- frame_snapshots_end -->"
                )
        except Exception as exc:  # noqa: BLE001
            payload["capture_errors"].append(f"frame_snapshot_capture_failed: {exc}")
        return payload

    def extract_anti_bot_keyword_matches(self, text: str) -> dict[str, list[str]]:
        matches: dict[str, list[str]] = {}
        for keyword in self._anti_bot_keywords:
            snippets = self.extract_keyword_context_snippets(
                text,
                keyword=keyword,
                max_matches=6,
                context_chars=140,
            )
            if snippets:
                matches[keyword] = snippets
        return matches

    def extract_antibot_scan_text(self, html_text: str) -> str:
        raw = str(html_text or "")
        if not raw:
            return ""
        without_embedded = re.sub(
            r"(?is)<(script|style|noscript|svg|iframe)[^>]*>.*?</\1>",
            " ",
            raw,
        )
        without_comments = re.sub(r"(?is)<!--.*?-->", " ", without_embedded)
        text_only = re.sub(r"(?is)<[^>]+>", " ", without_comments)
        normalized = html.unescape(text_only)
        return re.sub(r"\s+", " ", normalized).strip()

    def extract_keyword_context_snippets(
        self,
        text: str,
        *,
        keyword: str,
        max_matches: int = 8,
        context_chars: int = 120,
    ) -> list[str]:
        haystack = str(text or "")
        needle = str(keyword or "").strip()
        if not haystack or not needle:
            return []

        snippets: list[str] = []
        context_size = max(20, int(context_chars))
        limit = max(1, int(max_matches))
        word_pattern = re.compile(rf"\b{re.escape(needle)}\b", flags=re.IGNORECASE)

        for match in word_pattern.finditer(haystack):
            start = max(0, match.start() - context_size)
            end = min(len(haystack), match.end() + context_size)
            snippet = re.sub(r"\s+", " ", haystack[start:end]).strip()
            if snippet:
                snippets.append(snippet)
            if len(snippets) >= limit:
                return snippets

        has_symbol = bool(re.search(r"[^\w\s]", needle, flags=re.UNICODE))
        if not has_symbol:
            return snippets

        fallback_pattern = re.compile(re.escape(needle), flags=re.IGNORECASE)
        for match in fallback_pattern.finditer(haystack):
            start = max(0, match.start() - context_size)
            end = min(len(haystack), match.end() + context_size)
            snippet = re.sub(r"\s+", " ", haystack[start:end]).strip()
            if snippet:
                snippets.append(snippet)
            if len(snippets) >= limit:
                break
        return snippets
