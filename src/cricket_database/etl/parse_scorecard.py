"""Robust HTML parser for cricket scorecards.

Converts raw HTML into structured Pydantic models with:
- Match details, teams, venue, officials
- Innings with batting/bowling entries
- Ball-by-ball deliveries when available
- Name normalization and source key extraction
- Graceful degradation for partial data

IMPORTANT NOTES:
- Use deterministic matching keys (source_match_key from URL/script) to avoid duplicates
- Keep player canonicalization conservative - never auto-merge without strong keys
- Record merge candidates for manual review in player_alias/team_alias tables
"""

from __future__ import annotations

import re
import unicodedata
from typing import List, Tuple, Optional

from loguru import logger
from lxml import html

from .models import (
    MatchModel,
    InningsModel,
    BattingEntry,
    BowlingEntry,
    FieldingEntry,
    Delivery,
    PlayerRef,
    TeamRef,
    VenueRef,
)


def _clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    # normalize unicode accents, collapse whitespace, strip
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _int_or_none(s: str) -> Optional[int]:
    s = s.strip() if s else s
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _float_or_none(s: str) -> Optional[float]:
    s = s.strip() if s else s
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_scorecard(html_text: str, page_url: Optional[str] = None) -> Tuple[MatchModel, List[str]]:
    """Parse a scorecard HTML page into MatchModel and warnings list.

    Resilient to missing bits; emits warnings rather than raising.
    """
    warnings: List[str] = []
    try:
        tree = html.fromstring(html_text)
    except Exception as e:
        logger.error(f"HTML parse error: {e}")
        raise

    match = MatchModel()

    # Extract source match key from URL when possible (e.g., .../Scorecards/12345.html)
    if page_url:
        m = re.search(r"(\d{4,})", page_url)
        if m:
            match.source_match_key = m.group(1)

    # Title/series/venue blocks (site-specific XPaths likely need tuning)
    try:
        title = tree.xpath('string(//title)')
        title = _clean_text(title)
        if title:
            match.aliases.append(title)
    except Exception as e:
        warnings.append(f"title_parse_failed: {e}")

    # Venue (very heuristic; adjust selectors as per actual DOM)
    try:
        venue_text = tree.xpath('string(//*[contains(@class, "venue")])') or ""
        venue_text = _clean_text(venue_text)
        if venue_text:
            match.venue = VenueRef(name=venue_text)
    except Exception as e:
        warnings.append(f"venue_parse_failed: {e}")

    # Teams
    try:
        team_nodes = tree.xpath('//h2[contains(@class, "team")]')
        teams: List[TeamRef] = []
        for tn in team_nodes[:2]:
            tname = _clean_text(tn.text_content())
            if tname:
                teams.append(TeamRef(name=tname))
        if teams:
            match.teams = teams
    except Exception as e:
        warnings.append(f"teams_parse_failed: {e}")

    # Toss/result/day-night/follow-on/DL (heuristic extraction)
    try:
        info_text = tree.xpath('string(//*[contains(@class, "match-info")])')
        info_text = _clean_text(info_text)
        if "day/night" in info_text.lower():
            match.day_night = True
        if "D/L" in info_text or "DLS" in info_text:
            match.dl_method = True
        # naive toss detection
        mtoss = re.search(r"Toss:\s*([^,]+),\s*(bat|bowl)", info_text, re.IGNORECASE)
        if mtoss:
            match.toss.winner = TeamRef(name=_clean_text(mtoss.group(1)))
            match.toss.decision = "bat" if mtoss.group(2).lower().startswith("bat") else "bowl"
    except Exception as e:
        warnings.append(f"meta_parse_failed: {e}")

    # Innings tables (selectors to be adapted to the provider)
    innings_blocks = tree.xpath('//div[contains(@class, "innings")]')
    for idx, block in enumerate(innings_blocks, start=1):
        try:
            header = _clean_text(block.xpath('string(.//h3)'))
            batting_team = TeamRef(name=_clean_text(header.split(" innings")[0])) if header else (match.teams[0] if match.teams else TeamRef(name="Unknown"))
            bowling_team = (match.teams[1] if match.teams and len(match.teams) > 1 else TeamRef(name="Unknown"))
            runs = _int_or_none(_clean_text(block.xpath('string(.//*[contains(@class, "score")])')))
            overs = _float_or_none(_clean_text(block.xpath('string(.//*[contains(@class, "overs")])')))
            wickets = None
            inn = InningsModel(
                innings_no=idx,
                batting_team=batting_team,
                bowling_team=bowling_team,
                runs=runs,
                wickets=wickets,
                overs=overs,
            )

            # Batting rows
            for tr in block.xpath('.//table[contains(@class, "batting")]//tr'):
                cells = [
                    _clean_text(x) for x in tr.xpath('./td//text()')
                    if _clean_text(x)
                ]
                if len(cells) >= 2 and not cells[0].lower().startswith("extras"):
                    name = cells[0]
                    how_out = cells[1] if len(cells) > 1 else None
                    runs = _int_or_none(cells[2]) if len(cells) > 2 else None
                    balls = _int_or_none(cells[3]) if len(cells) > 3 else None
                    fours = _int_or_none(cells[4]) if len(cells) > 4 else None
                    sixes = _int_or_none(cells[5]) if len(cells) > 5 else None
                    inn.batting.append(
                        BattingEntry(
                            player=PlayerRef(name=name),
                            runs=runs,
                            balls=balls,
                            fours=fours,
                            sixes=sixes,
                            how_out=how_out,
                        )
                    )

            # Bowling rows
            for tr in block.xpath('.//table[contains(@class, "bowling")]//tr'):
                cells = [
                    _clean_text(x) for x in tr.xpath('./td//text()')
                    if _clean_text(x)
                ]
                if len(cells) >= 5:
                    name = cells[0]
                    overs = _float_or_none(cells[1])
                    maidens = _int_or_none(cells[2])
                    runs_c = _int_or_none(cells[3])
                    wkts = _int_or_none(cells[4])
                    inn.bowling.append(
                        BowlingEntry(
                            player=PlayerRef(name=name),
                            overs=overs,
                            maidens=maidens,
                            runs=runs_c,
                            wickets=wkts,
                        )
                    )

            match.innings.append(inn)
        except Exception as e:
            warnings.append(f"innings_parse_failed_{idx}: {e}")

    return match, warnings


