import argparse
import asyncio
import platform

from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Open headed Chromium and type text into the browser omnibox "
            "(address/search bar) one character at a time."
        )
    )
    parser.add_argument(
        "--text",
        default="tripadvisor.es",
        help="Text to type in Chromium omnibox (default: tripadvisor.es).",
    )
    parser.add_argument(
        "--channel",
        default="",
        help="Browser channel override: chrome | msedge | chromium (empty = bundled Chromium).",
    )
    parser.add_argument(
        "--char-delay-ms",
        type=int,
        default=140,
        help="Delay between characters in milliseconds (default: 140).",
    )
    parser.add_argument(
        "--before-type-ms",
        type=int,
        default=900,
        help="Initial wait before typing in milliseconds (default: 900).",
    )
    parser.add_argument(
        "--press-enter",
        action="store_true",
        help="Press Enter after typing.",
    )
    return parser.parse_args()


def _omnibox_shortcut() -> str:
    return "Meta+L" if platform.system() == "Darwin" else "Control+L"


async def _type_letter_by_letter(page: Page, text: str, char_delay_ms: int) -> None:
    await page.bring_to_front()
    await page.keyboard.press(_omnibox_shortcut())
    for char in text:
        await page.keyboard.type(char)
        await page.wait_for_timeout(max(0, char_delay_ms))


async def main() -> None:
    args = _parse_args()
    channel = (args.channel or "").strip() or None

    playwright: Playwright | None = None
    browser: Browser | None = None
    context: BrowserContext | None = None

    try:
        playwright = await async_playwright().start()

        launch_options = {
            "headless": False,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if channel:
            launch_options["channel"] = channel

        browser = await playwright.chromium.launch(**launch_options)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto("about:blank", wait_until="domcontentloaded")
        await page.wait_for_timeout(max(0, args.before_type_ms))
        await _type_letter_by_letter(page, args.text, args.char_delay_ms)

        if args.press_enter:
            await page.keyboard.press("Enter")

        print(f"Typed into omnibox: {args.text!r}")
        print("Close the browser window to exit.")

        while browser.is_connected():
            await asyncio.sleep(0.5)
    finally:
        try:
            if context is not None:
                await context.close()
        except Exception:
            pass
        if playwright is not None:
            await playwright.stop()


if __name__ == "__main__":
    asyncio.run(main())
