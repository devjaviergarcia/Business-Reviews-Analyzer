from __future__ import annotations

from pathlib import Path


class BusinessArtifactRuntime:
    def __init__(self, *, project_root: Path, artifacts_root: Path) -> None:
        self._project_root = project_root
        self._artifacts_root = artifacts_root

    def resolve_report_artifact_path(self, *, path: str) -> Path:
        raw_value = str(path or '').strip()
        if not raw_value:
            raise ValueError('Artifact path is required.')
        candidate_values: list[Path] = []
        as_path = Path(raw_value)
        if as_path.is_absolute():
            candidate_values.append(as_path)
        else:
            candidate_values.append(self._project_root / as_path)
        if raw_value.startswith('/app/'):
            candidate_values.append(self._project_root / raw_value.removeprefix('/app/'))
        checked_candidates: list[str] = []
        for candidate in candidate_values:
            try:
                resolved = candidate.expanduser().resolve(strict=True)
            except (FileNotFoundError, RuntimeError, OSError):
                checked_candidates.append(str(candidate))
                continue
            if not resolved.is_file():
                checked_candidates.append(str(resolved))
                continue
            try:
                resolved.relative_to(self._artifacts_root)
            except ValueError:
                checked_candidates.append(str(resolved))
                continue
            return resolved
        checked_text = ', '.join(checked_candidates[:4]) if checked_candidates else raw_value
        raise FileNotFoundError(f'Artifact not found or out of allowed scope: {checked_text}')
