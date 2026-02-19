from src.config import settings
from src.pipeline.llm_analyzer import ReviewLLMAnalyzer
from src.pipeline.preprocessor import ReviewPreprocessor
from src.scraper.google_maps import GoogleMapsScraper


class BusinessService:
    def __init__(self) -> None:
        self.scraper = GoogleMapsScraper(
            headless=settings.scraper_headless,
            slow_mo_ms=settings.scraper_slow_mo_ms,
            user_data_dir=settings.scraper_user_data_dir,
            browser_channel=settings.scraper_browser_channel,
            maps_url=settings.scraper_maps_url,
            timeout_ms=settings.scraper_timeout_ms,
            min_click_delay_ms=settings.scraper_min_click_delay_ms,
            max_click_delay_ms=settings.scraper_max_click_delay_ms,
            min_key_delay_ms=settings.scraper_min_key_delay_ms,
            max_key_delay_ms=settings.scraper_max_key_delay_ms,
        )
        self.preprocessor = ReviewPreprocessor()
        self.llm_analyzer = ReviewLLMAnalyzer()

    async def analyze_business(self, name: str, force: bool = False) -> dict:
        _ = (name, force)
        raise NotImplementedError("Business workflow orchestration will be implemented in day 7.")
