from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class TripadvisorSearchCandidate:
    title: str
    href: str
    score: float
