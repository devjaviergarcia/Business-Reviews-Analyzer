import asyncio
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import settings
from src.scraper.google_maps import GoogleMapsScraper


async def main() -> None:
    scraper = GoogleMapsScraper(
        headless=False,
        slow_mo_ms=settings.scraper_slow_mo_ms,
        user_data_dir=settings.scraper_user_data_dir,
        browser_channel=settings.scraper_browser_channel,
        maps_url=settings.scraper_maps_url,
        timeout_ms=settings.scraper_timeout_ms,
        min_click_delay_ms=settings.scraper_min_click_delay_ms,
        max_click_delay_ms=settings.scraper_max_click_delay_ms,
        min_key_delay_ms=settings.scraper_min_key_delay_ms,
        max_key_delay_ms=settings.scraper_max_key_delay_ms,
        stealth_mode=settings.scraper_stealth_mode,
        harden_headless=settings.scraper_harden_headless,
        extra_chromium_args=settings.scraper_extra_chromium_args,
    )

    try:
        await scraper.start()
        page = scraper.page
        profile_path = scraper._resolve_user_data_dir()

        print(f"Profile path: {profile_path}")
        print(f"Current URL: {page.url}")
        print("If you are not signed in, sign in now in the opened browser window.")

        signin = page.get_by_role("link", name=re.compile("iniciar sesion|sign in", re.IGNORECASE)).first
        try:
            if await signin.count() > 0 and await signin.is_visible():
                await signin.click()
        except Exception:
            pass

        await asyncio.to_thread(
            input,
            "After completing Google login and loading Maps, press Enter here to continue...",
        )

        limited_view = await scraper._is_limited_maps_view()
        print(f"Limited view detected after login: {limited_view}")
        if limited_view:
            print("Login may not be completed in this profile yet. Repeat and verify your account is signed in.")
        else:
            print("Full view appears enabled. You can now run the smoke test.")
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
