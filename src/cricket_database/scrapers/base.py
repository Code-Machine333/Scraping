"""Base scraper class with common functionality."""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse

import httpx
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from ..config import settings


logger = logging.getLogger(__name__)


class ScrapingError(Exception):
    """Custom exception for scraping errors."""
    pass


class RateLimiter:
    """Rate limiter for HTTP requests."""
    
    def __init__(self, max_requests_per_minute: int = 60, max_requests_per_hour: int = 1000):
        self.max_requests_per_minute = max_requests_per_minute
        self.max_requests_per_hour = max_requests_per_hour
        self.minute_requests: List[float] = []
        self.hour_requests: List[float] = []
    
    async def wait_if_needed(self) -> None:
        """Wait if rate limit would be exceeded."""
        now = time.time()
        
        # Clean old requests
        self.minute_requests = [req_time for req_time in self.minute_requests if now - req_time < 60]
        self.hour_requests = [req_time for req_time in self.hour_requests if now - req_time < 3600]
        
        # Check minute limit
        if len(self.minute_requests) >= self.max_requests_per_minute:
            sleep_time = 60 - (now - self.minute_requests[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                await asyncio.sleep(sleep_time)
        
        # Check hour limit
        if len(self.hour_requests) >= self.max_requests_per_hour:
            sleep_time = 3600 - (now - self.hour_requests[0])
            if sleep_time > 0:
                logger.info(f"Hourly rate limit reached, sleeping for {sleep_time:.2f} seconds")
                await asyncio.sleep(sleep_time)
        
        # Record this request
        self.minute_requests.append(now)
        self.hour_requests.append(now)


class BaseScraper(ABC):
    """Base scraper class with common functionality."""
    
    def __init__(
        self,
        base_url: str,
        rate_limit: float = 1.0,
        retry_attempts: int = 3,
        timeout: int = 30,
        user_agent: Optional[str] = None,
        dry_run: bool = False
    ):
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.user_agent = user_agent or settings.scraper.user_agent
        self.dry_run = dry_run
        
        self.rate_limiter = RateLimiter(
            max_requests_per_minute=settings.scraper.max_requests_per_minute,
            max_requests_per_hour=settings.scraper.max_requests_per_hour
        )
        
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._setup_browser()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._cleanup_browser()
    
    async def _setup_browser(self) -> None:
        """Setup Playwright browser."""
        if self.dry_run:
            logger.info("Dry run mode - browser setup skipped")
            return
        
        playwright = await async_playwright().start()
        self._browser = await playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        self._context = await self._browser.new_context(
            user_agent=self.user_agent,
            viewport={'width': 1920, 'height': 1080}
        )
        self._page = await self._context.new_page()
        
        # Set default timeout
        self._page.set_default_timeout(self.timeout * 1000)
    
    async def _cleanup_browser(self) -> None:
        """Cleanup Playwright browser."""
        if self._page:
            await self._page.close()
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
    
    async def _make_request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        use_browser: bool = False
    ) -> Union[Dict[str, Any], str]:
        """Make HTTP request with rate limiting and retries."""
        if self.dry_run:
            logger.info(f"Dry run: Would make {method} request to {url}")
            return {}
        
        await self.rate_limiter.wait_if_needed()
        
        if use_browser and self._page:
            return await self._make_browser_request(url, method, headers, params, data)
        else:
            return await self._make_http_request(url, method, headers, params, data)
    
    async def _make_http_request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], str]:
        """Make HTTP request using httpx."""
        request_headers = {"User-Agent": self.user_agent}
        if headers:
            request_headers.update(headers)
        
        for attempt in range(self.retry_attempts):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=request_headers,
                        params=params,
                        json=data
                    )
                    response.raise_for_status()
                    
                    # Try to parse as JSON, fallback to text
                    try:
                        return response.json()
                    except ValueError:
                        return response.text
                        
            except Exception as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == self.retry_attempts - 1:
                    raise ScrapingError(f"Failed to make request to {url} after {self.retry_attempts} attempts: {e}")
                
                # Exponential backoff
                await asyncio.sleep(2 ** attempt)
    
    async def _make_browser_request(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> str:
        """Make request using Playwright browser."""
        if not self._page:
            raise ScrapingError("Browser not initialized")
        
        # Add query parameters to URL
        if params:
            from urllib.parse import urlencode
            url += "?" + urlencode(params)
        
        try:
            # Set headers if provided
            if headers:
                await self._page.set_extra_http_headers(headers)
            
            # Navigate to URL
            response = await self._page.goto(url, wait_until="networkidle")
            
            if not response or response.status >= 400:
                raise ScrapingError(f"HTTP {response.status if response else 'Unknown'} error for {url}")
            
            # Get page content
            content = await self._page.content()
            return content
            
        except Exception as e:
            raise ScrapingError(f"Browser request failed for {url}: {e}")
    
    def _build_url(self, path: str) -> str:
        """Build full URL from path."""
        return urljoin(self.base_url, path)
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    @abstractmethod
    async def scrape_teams(self) -> List[Dict[str, Any]]:
        """Scrape team data."""
        pass
    
    @abstractmethod
    async def scrape_players(self, team_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Scrape player data."""
        pass
    
    @abstractmethod
    async def scrape_matches(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        match_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Scrape match data."""
        pass
    
    @abstractmethod
    async def scrape_match_details(self, match_id: str) -> Dict[str, Any]:
        """Scrape detailed match data including ball-by-ball."""
        pass
    
    async def scrape_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Scrape all available data."""
        logger.info("Starting comprehensive data scraping")
        
        results = {}
        
        try:
            # Scrape teams
            logger.info("Scraping teams...")
            results["teams"] = await self.scrape_teams()
            
            # Scrape players
            logger.info("Scraping players...")
            results["players"] = await self.scrape_players()
            
            # Scrape recent matches
            logger.info("Scraping recent matches...")
            results["matches"] = await self.scrape_matches()
            
            logger.info("Scraping completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            raise ScrapingError(f"Comprehensive scraping failed: {e}")
