from __future__ import annotations

import re
import unicodedata
from typing import Any


class TripadvisorBrowserTextFacet:

    def _normalize_text(self, value: Any) -> str:
        text = self._clean_text(value)
        if not text:
            return ""
        normalized = unicodedata.normalize("NFKD", text)
        normalized = "".join(char for char in normalized if not unicodedata.combining(char))
        normalized = normalized.lower()
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""
        text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        return text
