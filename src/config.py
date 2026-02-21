from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    scraper_slow_mo_ms: int = 50
    scraper_user_data_dir: str = "playwright-data"
    scraper_browser_channel: str = ""
    scraper_maps_url: str = "https://www.google.com/maps?hl=es"
    scraper_timeout_ms: int = 30000
    scraper_min_click_delay_ms: int = 3100
    scraper_max_click_delay_ms: int = 5200
    scraper_min_key_delay_ms: int = 90
    scraper_max_key_delay_ms: int = 260
    scraper_reviews_strategy: str = "interactive"

    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
