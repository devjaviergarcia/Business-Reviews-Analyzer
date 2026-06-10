from __future__ import annotations

from typing import Any


class ScrapeBotDetectedError(RuntimeError):
    """Raised when an anti-bot challenge is detected during scraping."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}


class ScrapeNeedsHumanInterventionError(RuntimeError):
    """Raised when scraping must pause for human intervention."""

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.context = context or {}
