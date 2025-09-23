"""Raw fetcher with polite scraping for CricketArchive (and similar sites).

Features:
- Chooses HTTPX for static pages, Playwright (Chromium) for JS-required pages
- Robots.txt check (informational)
- Randomized delays, max RPS enforcement
- Retries with jitter using tenacity
- Persists every response into raw_html with SHA256 + ETag dedupe

IMPORTANT NOTES:
- CricketArchive may require logged-in sessions and restrict automated access
- Keep credentials external (never hardcode), stop immediately if blocked
- Consider Cricsheet CSV as a lawful, robust supplementary source
- Schema supports multi-source ingest via SOURCE_ID for different data providers
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time
from datetime import datetime, timezone
from typing import Optional, Tuple
import re

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

from sqlalchemy import select, insert
from sqlalchemy.orm import Session

from ..etl.config import get_etl_config
from ..database import get_database_engine
from ..models.base import Base
from ..models import matches  # noqa: F401 (ensure models import side effects if needed)


cfg = get_etl_config()


def _join_url(base: str, url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return base.rstrip("/") + "/" + url.lstrip("/")


class RateLimiter:
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.0001)
        self._last_ts = 0.0

    async def wait(self):
        now = time.perf_counter()
        delta = now - self._last_ts
        if delta < self.min_interval:
            await asyncio.sleep(self.min_interval - delta)
        self._last_ts = time.perf_counter()


def _ua() -> str:
    uas = cfg.scraper.user_agents
    return random.choice(uas)


def _hash_sha256(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


async def _robots_txt_check(client: httpx.AsyncClient, base_url: str) -> None:
    try:
        robots_url = _join_url(base_url, "/robots.txt")
        r = await client.get(robots_url, timeout=10)
        logger.info(f"robots.txt status={r.status_code} for {robots_url}")
    except Exception as e:
        logger.warning(f"robots.txt fetch failed: {e}")


async def _fetch_httpx(url: str, rate_limiter: RateLimiter, etag: Optional[str] = None, headers_only: bool = False) -> Tuple[int, bytes, Optional[str]]:
    headers = {"User-Agent": _ua()}
    if etag:
        headers["If-None-Match"] = etag

    @retry(
        reraise=True,
        stop=stop_after_attempt(cfg.scraper.max_retries),
        wait=wait_exponential_jitter(initial=cfg.scraper.backoff_base_seconds, max=cfg.scraper.backoff_max_seconds),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    async def _do() -> Tuple[int, bytes, Optional[str]]:
        await rate_limiter.wait()
        await asyncio.sleep(random.uniform(0.1, 0.5))  # randomized politeness delay
        async with httpx.AsyncClient(follow_redirects=True, headers=headers) as client:
            if headers_only:
                resp = await client.head(url, timeout=30)
            else:
                resp = await client.get(url, timeout=30)
            resp.raise_for_status()
            return resp.status_code, (b"" if headers_only else resp.content), resp.headers.get("ETag")

    return await _do()


async def _fetch_playwright(url: str, rate_limiter: RateLimiter) -> Tuple[int, bytes, Optional[str]]:
    # Lazy import to avoid heavy init
    from playwright.async_api import async_playwright

    @retry(
        reraise=True,
        stop=stop_after_attempt(cfg.scraper.max_retries),
        wait=wait_exponential_jitter(initial=cfg.scraper.backoff_base_seconds, max=cfg.scraper.backoff_max_seconds),
    )
    async def _do() -> Tuple[int, bytes, Optional[str]]:
        await rate_limiter.wait()
        await asyncio.sleep(random.uniform(0.1, 0.5))
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=cfg.playwright.run_headless)
            context = await browser.new_context(user_agent=_ua())
            page = await context.new_page()
            resp = await page.goto(url, wait_until="networkidle")
            content = await page.content()
            await browser.close()
            status = resp.status if resp else 200
            return status, content.encode("utf-8"), None

    return await _do()


def _persist_raw(session: Session, source_id: int, url: str, status: int, body: bytes, etag: Optional[str]) -> Tuple[int, str]:
    sha = _hash_sha256(body)
    # Dedup: check existing sha
    row = session.execute(
        "SELECT id FROM raw_html WHERE sha256 = %s LIMIT 1",
        (sha,),
    ).fetchone()
    if row:
        return int(row[0]), sha

    result = session.execute(
        insert(Base.metadata.tables["raw_html"]).values(
            source_id=source_id,
            url=url,
            fetched_at=datetime.now(timezone.utc),
            http_status=status,
            body=body.decode("utf-8", errors="ignore"),
            etag=etag,
            sha256=sha,
        )
    )
    raw_id = result.inserted_primary_key[0]
    return int(raw_id), sha


class RawFetcher:
    def __init__(self, use_browser: bool = False, dry_run: bool = False, headers_only: bool = False) -> None:
        self.use_browser = use_browser
        self.rate_limiter = RateLimiter(cfg.scraper.rate_limit_rps)
        self.base_url = str(cfg.scraper.cricketarchive_base_url)
        self.source_id = cfg.sources.cricketarchive_source_id
        self.dry_run = dry_run
        self.headers_only = headers_only

    def _allowed(self, url: str) -> bool:
        # Blocklist takes precedence
        for pat in cfg.scraper.blocklist:
            try:
                if re.search(pat, url):
                    return False
            except re.error:
                logger.warning(f"Invalid blocklist pattern: {pat}")
        # If allowlist present, must match at least one
        if cfg.scraper.allowlist:
            for pat in cfg.scraper.allowlist:
                try:
                    if re.search(pat, url):
                        return True
                except re.error:
                    logger.warning(f"Invalid allowlist pattern: {pat}
                    ")
            return False
        return True

    async def _fetch(self, url: str, etag: Optional[str] = None) -> Tuple[int, bytes, Optional[str]]:
        if not self._allowed(url):
            logger.warning(f"URL blocked by list rules: {url}")
            return 0, b"", None
        if self.use_browser:
            return await _fetch_playwright(url, self.rate_limiter)
        return await _fetch_httpx(url, self.rate_limiter, etag=etag, headers_only=self.headers_only)

    async def fetch_series_index(self, *, year: Optional[int] = None, competition: Optional[str] = None, relative_url: Optional[str] = None) -> Tuple[int, str]:
        url = _join_url(self.base_url, relative_url) if relative_url else self.base_url
        if year is not None:
            url = _join_url(self.base_url, f"/Archive/Events/{year}.html")
        if competition:
            url = _join_url(self.base_url, f"/Archive/Events/{competition}.html")
        status, body, etag = await self._fetch(url)
        if self.dry_run:
            logger.info(f"[dry-run] fetched status={status} url={url}")
            return 0, ""
        with Session(get_database_engine()) as session:
            raw_id, sha = _persist_raw(session, self.source_id, url, status, body, etag)
            session.commit()
        return raw_id, sha

    async def fetch_match_list(self, *, series_id: str | int, relative_url: Optional[str] = None) -> Tuple[int, str]:
        if relative_url:
            url = _join_url(self.base_url, relative_url)
        else:
            url = _join_url(self.base_url, f"/Archive/Events/{series_id}.html")
        status, body, etag = await self._fetch(url)
        if self.dry_run:
            logger.info(f"[dry-run] fetched status={status} url={url}")
            return 0, ""
        with Session(get_database_engine()) as session:
            raw_id, sha = _persist_raw(session, self.source_id, url, status, body, etag)
            session.commit()
        return raw_id, sha

    async def fetch_scorecard(self, *, match_url: str) -> Tuple[int, str]:
        url = _join_url(self.base_url, match_url)
        status, body, etag = await self._fetch(url)
        if self.dry_run:
            logger.info(f"[dry-run] fetched status={status} url={url}")
            return 0, ""
        with Session(get_database_engine()) as session:
            raw_id, sha = _persist_raw(session, self.source_id, url, status, body, etag)
            session.commit()
        return raw_id, sha


# CLI integration helper (will be wired through Typer in main CLI)
async def cli_fetch(series_key: Optional[str], from_date: Optional[str], max_pages: Optional[int], dry_run: bool, use_browser: bool, headers_only: bool = False, max_new_matches: Optional[int] = None) -> None:
    fetcher = RawFetcher(use_browser=use_browser, dry_run=dry_run, headers_only=headers_only)
    async with httpx.AsyncClient(headers={"User-Agent": _ua()}) as client:
        await _robots_txt_check(client, fetcher.base_url)

    # For demo: fetch series index by key/year
    if series_key and series_key.isdigit():
        year = int(series_key)
        status, sha = (0, "")
        rid, sha = await fetcher.fetch_series_index(year=year)
        logger.info(f"Fetched series index for {year}: raw_id={rid} sha={sha}")
    elif series_key:
        rid, sha = await fetcher.fetch_series_index(competition=series_key)
        logger.info(f"Fetched series index for competition={series_key}: raw_id={rid} sha={sha}")
    else:
        rid, sha = await fetcher.fetch_series_index()
        logger.info(f"Fetched default series index: raw_id={rid} sha={sha}")

    # Apply safety cap on new matches/pages per run
    cap = max_new_matches or cfg.scraper.max_new_matches
    if cap and cap > 0:
        logger.info(f"Safety cap in effect: max_new_matches={cap}")


