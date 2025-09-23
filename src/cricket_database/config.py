"""Configuration management for the cricket database system."""

import os
from typing import Optional

from pydantic import BaseSettings, Field


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""
    
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=3306, env="DB_PORT")
    name: str = Field(default="cricket_db", env="DB_NAME")
    user: str = Field(default="cricket_user", env="DB_USER")
    password: str = Field(default="", env="DB_PASSWORD")
    
    @property
    def url(self) -> str:
        """Get database URL for SQLAlchemy."""
        return f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class ScraperSettings(BaseSettings):
    """Scraper configuration settings."""
    
    rate_limit: float = Field(default=1.0, env="SCRAPER_RATE_LIMIT")
    retry_attempts: int = Field(default=3, env="SCRAPER_RETRY_ATTEMPTS")
    timeout: int = Field(default=30, env="SCRAPER_TIMEOUT")
    user_agent: str = Field(default="CricketDataBot/1.0", env="SCRAPER_USER_AGENT")
    
    # Rate limiting
    max_requests_per_minute: int = Field(default=60, env="MAX_REQUESTS_PER_MINUTE")
    max_requests_per_hour: int = Field(default=1000, env="MAX_REQUESTS_PER_HOUR")


class DataQualitySettings(BaseSettings):
    """Data quality configuration settings."""
    
    enable_validation: bool = Field(default=True, env="ENABLE_DATA_VALIDATION")
    enable_duplicate_check: bool = Field(default=True, env="ENABLE_DUPLICATE_CHECK")
    batch_size: int = Field(default=1000, env="BATCH_SIZE")


class LoggingSettings(BaseSettings):
    """Logging configuration settings."""
    
    level: str = Field(default="INFO", env="LOG_LEVEL")
    file: Optional[str] = Field(default="logs/cricket_scraper.log", env="LOG_FILE")


class Settings(BaseSettings):
    """Main application settings."""
    
    database: DatabaseSettings = DatabaseSettings()
    scraper: ScraperSettings = ScraperSettings()
    data_quality: DataQualitySettings = DataQualitySettings()
    logging: LoggingSettings = LoggingSettings()
    
    # Data sources
    cricket_api_base_url: str = Field(default="https://api.cricket.com", env="CRICKET_API_BASE_URL")
    espn_cricket_base_url: str = Field(default="https://www.espncricinfo.com", env="ESPN_CRICKET_BASE_URL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
