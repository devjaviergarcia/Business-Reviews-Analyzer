#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from typing import Final

from playwright.async_api import Page, async_playwright

SECURITY_MARKER_SELECTORS: Final[tuple[str, ...]] = (
    ".captcha__robot",
    "[data-dd-captcha-robot]",
    "#ddv1-captcha-container",
    "#captcha__frame",
)

SECURITY_MARKER_TEXT: Final[tuple[str, ...]] = (
    "robot",
    "captcha",
    "not a robot",
    "no a un robot",
    "no soy un robot",
)


async def page_looks_like_security_challenge(page: Page) -> bool:
    # 1) DOM marker selectors on top-level document.
    for selector in SECURITY_MARKER_SELECTORS:
        try:
            if await page.locator(selector).count() > 0:
                return True
        except Exception:
            continue

    # 2) iframe/source markers visible from top-level wrapper.
    iframe_selectors = (
        "iframe[src*='captcha-delivery']",
        "iframe[src*='datadome']",
    )
    for selector in iframe_selectors:
        try:
            if await page.locator(selector).count() > 0:
                return True
        except Exception:
            continue

    # 3) Top-level text markers.
    try:
        text = (await page.locator("body").inner_text()).lower()
    except Exception:
        text = ""

    if any(marker in text for marker in SECURITY_MARKER_TEXT):
        return True

    # 4) Raw HTML markers from wrapper pages that load challenge in iframe.
    try:
        html_text = (await page.content()).lower()
    except Exception:
        html_text = ""
    wrapper_markers = (
        "captcha-delivery.com",
        "datadome",
        "ddv1-captcha-container",
        "data-dd-captcha",
        "robot",
        "captcha",
    )
    return any(marker in html_text for marker in wrapper_markers)


async def drag_local_slider(
    page: Page,
    *,
    slider_selector: str,
    container_selector: str,
    status_selector: str | None,
    expected_status: str | None,
    steps: int,
) -> str:
    slider = page.locator(slider_selector)
    container = page.locator(container_selector)
    await slider.wait_for(state="visible", timeout=12_000)
    await container.wait_for(state="visible", timeout=12_000)

    slider_box = await slider.bounding_box()
    container_box = await container.bounding_box()
    if not slider_box or not container_box:
        raise RuntimeError("Could not resolve slider/container bounding boxes.")

    start_x = slider_box["x"] + slider_box["width"] / 2
    start_y = slider_box["y"] + slider_box["height"] / 2
    target_x = container_box["x"] + container_box["width"] - slider_box["width"] / 2 - 6

    await page.mouse.move(start_x, start_y)
    await page.mouse.down()
    for idx in range(1, max(3, int(steps)) + 1):
        ratio = idx / max(3, int(steps))
        current_x = start_x + ((target_x - start_x) * ratio)
        await page.mouse.move(current_x, start_y)
        await page.wait_for_timeout(10)
    await page.mouse.up()

    if not status_selector:
        return ""

    status = page.locator(status_selector)
    await status.wait_for(state="visible", timeout=6_000)
    status_text = (await status.text_content() or "").strip()

    if expected_status and status_text != expected_status:
        raise RuntimeError(
            f"Slider drag finished but status is {status_text!r} (expected {expected_status!r})."
        )
    return status_text


async def run(args: argparse.Namespace) -> int:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=bool(args.headless))
        page = await browser.new_page()
        try:
            await page.goto(args.url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(int(args.settle_ms))

            if await page_looks_like_security_challenge(page):
                print(
                    "manual_required: detected security challenge markers "
                    "(robot/captcha/datadome)."
                )
                return 2

            status = await drag_local_slider(
                page,
                slider_selector=args.slider_selector,
                container_selector=args.container_selector,
                status_selector=args.status_selector if args.status_selector else None,
                expected_status=args.expected_status if args.expected_status else None,
                steps=int(args.steps),
            )
            if status:
                print(f"ok: slider_completed status={status!r}")
            else:
                print("ok: slider_drag_executed")
            return 0
        finally:
            await browser.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Local slider probe for non-security test pages. "
            "If robot/captcha markers are detected, it exits with manual_required."
        )
    )
    parser.add_argument("--url", required=True, help="Target URL, usually file:///... for local tests.")
    parser.add_argument("--slider-selector", default="#slider")
    parser.add_argument("--container-selector", default="#sliderContainer")
    parser.add_argument("--status-selector", default="#status")
    parser.add_argument("--expected-status", default="Completed")
    parser.add_argument("--steps", type=int, default=30)
    parser.add_argument("--settle-ms", type=int, default=1200)
    parser.add_argument("--headless", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = asyncio.run(run(args))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
