from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any


@dataclass(frozen=True)
class BrowserViewport:
    width: int
    height: int


@dataclass(frozen=True)
class BrowserProfile:
    profile_id: str
    user_agent: str
    viewport: BrowserViewport
    locale: str
    timezone_id: str
    accept_language: str
    navigator_languages: tuple[str, ...]
    navigator_platform: str
    color_scheme: str = "light"
    device_scale_factor: float = 1.0
    has_touch: bool = False
    is_mobile: bool = False
    browser_channel: str | None = None


_DESKTOP_ES_BROWSER_PROFILES: tuple[BrowserProfile, ...] = (
    BrowserProfile(
        profile_id="desktop_es_linux_01",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/137.0.0.0 Safari/537.36"
        ),
        viewport=BrowserViewport(width=1366, height=900),
        locale="es-ES",
        timezone_id="Europe/Madrid",
        accept_language="es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
        navigator_languages=("es-ES", "es", "en-US", "en"),
        navigator_platform="Linux x86_64",
    ),
    BrowserProfile(
        profile_id="desktop_es_linux_02",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/136.0.0.0 Safari/537.36"
        ),
        viewport=BrowserViewport(width=1440, height=960),
        locale="es-ES",
        timezone_id="Europe/Madrid",
        accept_language="es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
        navigator_languages=("es-ES", "es", "en-US", "en"),
        navigator_platform="Linux x86_64",
    ),
    BrowserProfile(
        profile_id="desktop_es_linux_03",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        viewport=BrowserViewport(width=1536, height=864),
        locale="es-ES",
        timezone_id="Europe/Madrid",
        accept_language="es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
        navigator_languages=("es-ES", "es", "en-US", "en"),
        navigator_platform="Linux x86_64",
    ),
)

_PROFILES_BY_ID = {profile.profile_id: profile for profile in _DESKTOP_ES_BROWSER_PROFILES}
_DEFAULT_PROFILE_ID = _DESKTOP_ES_BROWSER_PROFILES[0].profile_id


def list_browser_profiles() -> tuple[BrowserProfile, ...]:
    return _DESKTOP_ES_BROWSER_PROFILES


def resolve_browser_profile(profile_id: str | None = None) -> BrowserProfile:
    normalized_profile_id = str(profile_id or "").strip()
    if not normalized_profile_id:
        return _PROFILES_BY_ID[_DEFAULT_PROFILE_ID]
    profile = _PROFILES_BY_ID.get(normalized_profile_id)
    if profile is None:
        valid_ids = ", ".join(sorted(_PROFILES_BY_ID))
        raise ValueError(
            f"Unknown browser profile '{normalized_profile_id}'. Valid profiles: {valid_ids}"
        )
    return profile


def select_stable_browser_profile(
    *,
    source: str,
    stable_key: str | None,
    explicit_profile_id: str | None = None,
) -> BrowserProfile:
    if str(explicit_profile_id or "").strip():
        return resolve_browser_profile(explicit_profile_id)

    normalized_source = str(source or "generic").strip().lower() or "generic"
    normalized_key = str(stable_key or "").strip() or f"{normalized_source}:default"
    hash_value = sha256(f"{normalized_source}:{normalized_key}".encode("utf-8")).digest()
    selected_index = int.from_bytes(hash_value[:4], byteorder="big") % len(_DESKTOP_ES_BROWSER_PROFILES)
    return _DESKTOP_ES_BROWSER_PROFILES[selected_index]


def build_playwright_context_options(profile: BrowserProfile) -> dict[str, Any]:
    return {
        "viewport": {
            "width": profile.viewport.width,
            "height": profile.viewport.height,
        },
        "locale": profile.locale,
        "timezone_id": profile.timezone_id,
        "user_agent": profile.user_agent,
        "color_scheme": profile.color_scheme,
        "device_scale_factor": profile.device_scale_factor,
        "has_touch": profile.has_touch,
        "is_mobile": profile.is_mobile,
        "extra_http_headers": {
            "Accept-Language": profile.accept_language,
        },
    }


def build_browser_profile_stealth_script(profile: BrowserProfile) -> str:
    languages_json = ", ".join(f"'{item}'" for item in profile.navigator_languages)
    return f"""
        Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
        Object.defineProperty(navigator, 'language', {{ get: () => '{profile.locale}' }});
        Object.defineProperty(navigator, 'languages', {{ get: () => [{languages_json}] }});
        Object.defineProperty(navigator, 'plugins', {{ get: () => [1, 2, 3, 4] }});
        Object.defineProperty(navigator, 'platform', {{ get: () => '{profile.navigator_platform}' }});
        window.chrome = window.chrome || {{ runtime: {{}} }};
    """
