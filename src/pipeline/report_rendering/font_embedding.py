from __future__ import annotations

import base64
import re
from urllib.parse import urljoin
from urllib.request import Request, urlopen

_EMBEDDED_FONT_CSS_CACHE: str | None = None
_EMBEDDED_FONT_CSS_FAILED = False



def load_embedded_font_css() -> str:
    global _EMBEDDED_FONT_CSS_CACHE, _EMBEDDED_FONT_CSS_FAILED

    if _EMBEDDED_FONT_CSS_CACHE is not None:
        return _EMBEDDED_FONT_CSS_CACHE
    if _EMBEDDED_FONT_CSS_FAILED:
        return ""

    css_urls = [
        "https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&display=swap",
        "https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap",
    ]
    embedded_blocks: list[str] = []
    for css_url in css_urls:
        css_text = _download_text(css_url)
        if not css_text:
            continue
        embedded_blocks.append(_embed_font_urls(css_text=css_text, base_url=css_url))

    merged = "\n".join(block for block in embedded_blocks if block.strip())
    if merged.strip():
        _EMBEDDED_FONT_CSS_CACHE = merged
        return merged
    _EMBEDDED_FONT_CSS_FAILED = True
    return ""



def _embed_font_urls(*, css_text: str, base_url: str) -> str:
    pattern = re.compile(r"url\\((['\"]?)([^)'\"]+)\\1\\)")

    def _replace(match: re.Match[str]) -> str:
        raw_url = str(match.group(2) or "").strip()
        if not raw_url or raw_url.startswith("data:"):
            return match.group(0)
        resolved = urljoin(base_url, raw_url)
        binary = _download_binary(resolved)
        if not binary:
            return match.group(0)
        mime = _font_mime_from_url(resolved)
        encoded = base64.b64encode(binary).decode("ascii")
        return f'url("data:{mime};base64,{encoded}")'

    return pattern.sub(_replace, css_text)



def _download_text(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
            )
        },
    )
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310
            return response.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""



def _download_binary(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
            )
        },
    )
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310
            return bytes(response.read())
    except Exception:
        return b""



def _font_mime_from_url(value: str) -> str:
    clean = str(value).split("?", 1)[0].strip().lower()
    if clean.endswith(".woff2"):
        return "font/woff2"
    if clean.endswith(".woff"):
        return "font/woff"
    if clean.endswith(".ttf"):
        return "font/ttf"
    if clean.endswith(".otf"):
        return "font/otf"
    if clean.endswith(".eot"):
        return "application/vnd.ms-fontobject"
    return "font/woff2"
