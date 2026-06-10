from __future__ import annotations

from typing import Any


class _BaseSectionGenerator:
    def __init__(self, renderer: Any) -> None:
        self.renderer = renderer
