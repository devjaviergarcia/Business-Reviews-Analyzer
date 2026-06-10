from __future__ import annotations

from pathlib import Path


async def render_pdf_from_html(*, html_content: str, pdf_path: Path) -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            await page.set_content(html_content, wait_until="networkidle")
            await page.emulate_media(media="screen")
            await page.pdf(
                path=str(pdf_path),
                format="A4",
                print_background=True,
                margin={
                    "top": "12mm",
                    "bottom": "12mm",
                    "left": "10mm",
                    "right": "10mm",
                },
            )
        finally:
            await browser.close()
