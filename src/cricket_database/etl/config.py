"""ETL configuration loaded from environment (.env).

Provides:
- Database DSN (MySQL, mysql+mysqlconnector)
- CricketArchive base URL
- Concurrency, rate limits, retry/backoff
- User-agent pool
- Playwright headless flag
- Source IDs (e.g., CricketArchive)
"""

from __future__ import annotations

from functools import lru_cache
from typing import List, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field, HttpUrl


# Load environment from .env if present
load_dotenv(override=False)


class DatabaseSettings(BaseModel):
    host: str = Field(default="127.0.0.1", alias="DB_HOST")
    port: int = Field(default=3306, alias="DB_PORT")
    name: str = Field(default="cricket_db", alias="DB_NAME")
    user: str = Field(default="cricket_user", alias="DB_USER")
    password: str = Field(default="", alias="DB_PASSWORD")

    @property
    def dsn(self) -> str:
        # mysql-connector-python driver
        return (
            f"mysql+mysqlconnector://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


class ScraperSettings(BaseModel):
    cricketarchive_base_url: HttpUrl = Field(
        default="https://cricketarchive.com", alias="CRICKETARCHIVE_BASE_URL"
    )
    concurrency: int = Field(default=4, alias="ETL_CONCURRENCY")
    # Conservative default: 1 request/sec
    rate_limit_rps: float = Field(default=1.0, alias="RATE_LIMIT_RPS")
    max_retries: int = Field(default=3, alias="MAX_RETRIES")
    backoff_base_seconds: float = Field(default=0.5, alias="BACKOFF_BASE_SECONDS")
    backoff_max_seconds: float = Field(default=8.0, alias="BACKOFF_MAX_SECONDS")
    user_agents_csv: Optional[str] = Field(default=None, alias="ETL_USER_AGENTS")
    # URL allow/block lists (comma-separated regex patterns)
    allowlist_csv: Optional[str] = Field(default=None, alias="ETL_ALLOWLIST")
    blocklist_csv: Optional[str] = Field(default=None, alias="ETL_BLOCKLIST")
    # Safety limit for new matches/pages per run
    max_new_matches: int = Field(default=50, alias="MAX_NEW_MATCHES")

    @property
    def user_agents(self) -> List[str]:
        if self.user_agents_csv:
            agents = [ua.strip() for ua in self.user_agents_csv.split(",") if ua.strip()]
            if agents:
                return agents
        # Sensible defaults; rotate to respect ToS and reduce blocks
        return [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        ]

    @property
    def allowlist(self) -> List[str]:
        if not self.allowlist_csv:
            return []
        return [p.strip() for p in self.allowlist_csv.split(",") if p.strip()]

    @property
    def blocklist(self) -> List[str]:
        if not self.blocklist_csv:
            return []
        return [p.strip() for p in self.blocklist_csv.split(",") if p.strip()]


class PlaywrightSettings(BaseModel):
    run_headless: bool = Field(default=True, alias="RUN_HEADLESS")


class SourcesSettings(BaseModel):
    cricketarchive_source_id: int = Field(default=1, alias="CRICKETARCHIVE_SOURCE_ID")


class ETLConfig(BaseModel):
    db: DatabaseSettings
    scraper: ScraperSettings
    playwright: PlaywrightSettings
    sources: SourcesSettings


@lru_cache(maxsize=1)
def get_etl_config() -> ETLConfig:
    """Return cached ETL configuration loaded from environment."""
    return ETLConfig(
        db=DatabaseSettings(),
        scraper=ScraperSettings(),
        playwright=PlaywrightSettings(),
        sources=SourcesSettings(),
    )


