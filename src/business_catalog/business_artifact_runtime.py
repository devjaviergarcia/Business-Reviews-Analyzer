from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
from urllib.parse import unquote, urlparse


class BusinessArtifactRuntime:
    def __init__(self, *, project_root: Path, artifacts_root: Path) -> None:
        self._project_root = project_root
        self._artifacts_root = artifacts_root

    def resolve_report_artifact_path(self, *, path: str) -> Path:
        raw_value = str(path or '').strip()
        if not raw_value:
            raise ValueError('Artifact path is required.')
        if raw_value.startswith('file://'):
            parsed = urlparse(raw_value)
            raw_value = unquote(parsed.path or '').strip()
            if not raw_value:
                raise ValueError('Artifact path is required.')
        candidate_values: list[Path] = []
        as_path = Path(raw_value)
        if as_path.is_absolute():
            candidate_values.append(as_path)
            remapped_artifacts_path = self._remap_foreign_artifact_path(as_path)
            if remapped_artifacts_path is not None:
                candidate_values.append(remapped_artifacts_path)
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

    def _remap_foreign_artifact_path(self, path: Path) -> Path | None:
        normalized_text = str(path).replace('\\', '/')
        marker = '/artifacts/'
        if marker not in normalized_text:
            return None
        suffix = normalized_text.split(marker, 1)[1].strip('/')
        if not suffix:
            return None
        return self._project_root / 'artifacts' / suffix

    def open_report_artifact_path(self, *, path: str) -> Path:
        resolved = self.resolve_report_artifact_path(path=path)
        opener = shutil.which('xdg-open')
        if not opener:
            raise RuntimeError("xdg-open is not available on this system.")
        subprocess.Popen(  # noqa: S603
            [opener, str(resolved)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        return resolved
