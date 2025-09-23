from __future__ import annotations

import datetime as dt
from typing import List, Optional, Tuple

from loguru import logger
from sqlalchemy import text

from .parse_scorecard import parse_scorecard
from .upsert_scorecard import upsert_match_tree
from ..database import get_database_engine
from ..etl.config import get_etl_config


def _select_raw_html(source_id: int, limit: int, days_back: Optional[int]) -> List[Tuple[int, str, str]]:
    engine = get_database_engine()
    params = {"source_id": source_id, "limit": limit}
    where = "WHERE source_id = :source_id"
    if days_back and days_back > 0:
        where += " AND fetched_at >= :since"
        params["since"] = dt.datetime.utcnow() - dt.timedelta(days=days_back)
    sql = f"""
        SELECT id, url, body
        FROM raw_html
        {where}
        ORDER BY fetched_at DESC
        LIMIT :limit
    """
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(sql, params).fetchall()
    return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]


def summarize_parse(url: str, html: str) -> dict:
    match, warnings = parse_scorecard(html, page_url=url)
    return {
        "source_match_key": match.source_match_key,
        "teams": [t.name for t in match.teams],
        "innings": len(match.innings),
        "day_night": match.day_night,
        "dl": match.dl_method,
        "warnings": warnings,
    }


def run_parse_load(limit: int = 10, days_back: Optional[int] = None, dry_run: bool = True, source_id: Optional[int] = None) -> List[dict]:
    cfg = get_etl_config()
    sid = source_id or cfg.sources.cricketarchive_source_id
    rows = _select_raw_html(sid, limit, days_back)
    summaries: List[dict] = []
    engine = get_database_engine()
    for rid, url, body in rows:
        try:
            summary = summarize_parse(url, body)
            summary.update({"raw_id": rid, "url": url})
            summaries.append(summary)
            logger.info(f"parsed raw_id={rid} url={url} match_key={summary.get('source_match_key')}")
            if not dry_run:
                match, _warnings = parse_scorecard(body, page_url=url)
                with engine.begin() as conn:
                    match_id, stats = upsert_match_tree(conn, match)
                summary["match_id"] = match_id
                summary["upsert_stats"] = stats
        except Exception as e:
            logger.warning(f"parse_failed raw_id={rid} url={url} err={e}")
    return summaries


