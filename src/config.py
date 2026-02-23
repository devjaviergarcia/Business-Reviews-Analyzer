from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Business Reviews Analyzer"
    app_env: str = "dev"
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    mongo_uri: str = "mongodb://localhost:27017"
    db_name: str = "business_reviews_analyzer"

    gemini_api_key: str = ""
    gemini_model: str = "gemini-1.5-flash"

    scraper_headless: bool = False
    scraper_incognito: bool = False
    scraper_slow_mo_ms: int = 50
    scraper_user_data_dir: str = "playwright-data"
    scraper_browser_channel: str = ""
    scraper_maps_url: str = "https://www.google.com/maps?hl=es"
    scraper_timeout_ms: int = 30000
    scraper_min_click_delay_ms: int = 3100
    scraper_max_click_delay_ms: int = 5200
    scraper_min_key_delay_ms: int = 90
    scraper_max_key_delay_ms: int = 260
    scraper_stealth_mode: bool = True
    scraper_harden_headless: bool = True
    scraper_extra_chromium_args: Annotated[list[str], NoDecode] = Field(default_factory=list)
    scraper_reviews_strategy: str = "scroll_copy"
    scraper_interactive_max_rounds: int = 40
    scraper_html_scroll_max_rounds: int = 0
    scraper_html_stable_rounds: int = 10
    scraper_html_scroll_min_interval_s: float = 1.0
    scraper_html_scroll_max_interval_s: float = 2.0
    analysis_reanalyze_default_batchers: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: [
            "latest_text",
            "balanced_rating",
            "low_rating_focus",
            "high_rating_focus",
        ]
    )
    analysis_reanalyze_batch_size: int = 30
    analysis_reanalyze_pool_size: int = 250
    worker_poll_seconds: int = 5

    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @field_validator("scraper_extra_chromium_args", mode="before")
    @classmethod
    def parse_scraper_extra_chromium_args(cls, value: object) -> object:
        if isinstance(value, str):
            return [arg.strip() for arg in value.split(",") if arg.strip()]
        return value

    @field_validator("analysis_reanalyze_default_batchers", mode="before")
    @classmethod
    def parse_analysis_reanalyze_default_batchers(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
