"""Deterministic upsert functions for mapping MatchModel to normalized SQL schema.

Handles the complete match tree: countries → teams → players → venues → 
matches → match_teams → innings → batting/bowling/fielding → deliveries.

Uses INSERT ... ON DUPLICATE KEY UPDATE for idempotency.

IMPORTANT NOTES:
- Use deterministic matching keys (source_match_key) to avoid duplicate matches
- Keep player canonicalization conservative - never auto-merge without strong keys
- Record merge candidates for manual review in player_alias/team_alias tables
- Schema supports multi-source ingest via SOURCE_ID for different data providers
"""

from __future__ import annotations

from typing import Optional, Tuple

from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Connection

from .models import MatchModel, InningsModel, BattingEntry, BowlingEntry
from .config import get_etl_config


cfg = get_etl_config()


def _get_or_create_country(conn: Connection, name: Optional[str]) -> Optional[int]:
    if not name:
        return None
    row = conn.exec_driver_sql("SELECT id FROM countries WHERE name=%s", (name,)).fetchone()
    if row:
        return int(row[0])
    res = conn.exec_driver_sql("INSERT INTO countries(name) VALUES(%s)", (name,))
    return int(res.lastrowid)


def _get_or_create_team(conn: Connection, name: str, country_id: Optional[int]) -> int:
    row = conn.exec_driver_sql(
        "SELECT id FROM teams WHERE name=%s AND (country_id<=>%s)", (name, country_id)
    ).fetchone()
    if row:
        return int(row[0])
    res = conn.exec_driver_sql(
        "INSERT INTO teams(name, country_id) VALUES(%s, %s)", (name, country_id)
    )
    team_id = int(res.lastrowid)
    # emit alias
    try:
        conn.exec_driver_sql(
            "INSERT IGNORE INTO team_alias(team_id, alias, source_id) VALUES(%s,%s,%s)",
            (team_id, name, cfg.sources.cricketarchive_source_id),
        )
    except Exception:
        pass
    return team_id


def _get_or_create_player(conn: Connection, full_name: str, country_id: Optional[int]) -> int:
    row = conn.exec_driver_sql(
        "SELECT id FROM players WHERE full_name=%s AND (country_id<=>%s)", (full_name, country_id)
    ).fetchone()
    if row:
        return int(row[0])
    res = conn.exec_driver_sql(
        "INSERT INTO players(full_name, country_id) VALUES(%s, %s)", (full_name, country_id)
    )
    player_id = int(res.lastrowid)
    # emit alias
    try:
        conn.exec_driver_sql(
            "INSERT IGNORE INTO player_alias(player_id, alias, source_id) VALUES(%s,%s,%s)",
            (player_id, full_name, cfg.sources.cricketarchive_source_id),
        )
    except Exception:
        pass
    return player_id


def _get_or_create_venue(conn: Connection, name: str, country_id: Optional[int]) -> int:
    row = conn.exec_driver_sql(
        "SELECT id FROM venues WHERE name=%s AND (country_id<=>%s)", (name, country_id)
    ).fetchone()
    if row:
        return int(row[0])
    res = conn.exec_driver_sql(
        "INSERT INTO venues(name, country_id) VALUES(%s, %s)", (name, country_id)
    )
    return int(res.lastrowid)


def upsert_match_tree(conn: Connection, m: MatchModel) -> Tuple[int, dict]:
    """Upsert match and nested structures; returns (match_id, stats)."""
    stats = {"teams": 0, "players": 0, "innings": 0, "batting": 0, "bowling": 0}

    # Teams and venue (use provided country if present)
    team_ids = []
    for t in m.teams:
        if not t.name:
            continue
        tid = _get_or_create_team(conn, t.name, None)
        team_ids.append(tid)
        stats["teams"] += 1

    venue_id = None
    if m.venue and m.venue.name:
        venue_country_id = _get_or_create_country(conn, m.venue.country) if m.venue.country else None
        venue_id = _get_or_create_venue(conn, m.venue.name, venue_country_id)

    # Match
    # Determine format and dates (may be None)
    fmt = m.format or "Unknown"
    start_date = m.start_date or None
    end_date = m.end_date or None
    result_type = m.result.result_type if m.result else None

    # winner team id by name match
    winner_team_id = None
    if m.result and m.result.winner:
        wname = m.result.winner.name
        if wname:
            winner_team_id = _get_or_create_team(conn, wname, None)

    toss_winner_team_id = None
    if m.toss and m.toss.winner and m.toss.winner.name:
        toss_winner_team_id = _get_or_create_team(conn, m.toss.winner.name, None)

    toss_decision = m.toss.decision if m.toss and m.toss.decision else None

    # Optional series/season linkage
    series_id = None
    if m.series_name:
        season_id = None
        if m.start_date and len(m.start_date) >= 4:
            season_name = m.start_date[:4]
            row = conn.exec_driver_sql("SELECT id FROM seasons WHERE name=%s", (season_name,)).fetchone()
            if row:
                season_id = int(row[0])
            else:
                res_sea = conn.exec_driver_sql(
                    "INSERT INTO seasons(name, start_date) VALUES(%s,%s)", (season_name, m.start_date)
                )
                season_id = int(res_sea.lastrowid)
        row = conn.exec_driver_sql(
            "SELECT id FROM series WHERE name=%s AND (season_id<=>%s)", (m.series_name, season_id)
        ).fetchone()
        if row:
            series_id = int(row[0])
        else:
            res_ser = conn.exec_driver_sql(
                "INSERT INTO series(name, season_id) VALUES(%s,%s)", (m.series_name, season_id)
            )
            series_id = int(res_ser.lastrowid)

    # Upsert by source_match_key
    row = conn.exec_driver_sql(
        "SELECT id FROM matches WHERE source_match_key=%s", (m.source_match_key,)
    ).fetchone()
    if row:
        match_id = int(row[0])
        conn.exec_driver_sql(
            """
            UPDATE matches SET format=%s, start_date=%s, end_date=%s, venue_id=%s, series_id=%s,
                   result_type=%s, winner_team_id=%s, toss_winner_team_id=%s,
                   toss_decision=%s, day_night=%s, follow_on=%s, dl_method=%s
            WHERE id=%s
            """,
            (
                fmt, start_date, end_date, venue_id, series_id,
                result_type, winner_team_id, toss_winner_team_id,
                toss_decision, int(bool(m.day_night)), int(bool(m.follow_on)), int(bool(m.dl_method)),
                match_id,
            ),
        )
    else:
        res = conn.exec_driver_sql(
            """
            INSERT INTO matches(format, start_date, end_date, venue_id, series_id,
                                result_type, winner_team_id, toss_winner_team_id, toss_decision,
                                day_night, follow_on, dl_method, source_match_key)
            VALUES(%s,%s,%s,%s,NULL,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                fmt, start_date, end_date, venue_id,
                result_type, winner_team_id, toss_winner_team_id, toss_decision,
                int(bool(m.day_night)), int(bool(m.follow_on)), int(bool(m.dl_method)),
                m.source_match_key,
            ),
        )
        match_id = int(res.lastrowid)

    # Link match_teams
    for tid in team_ids[:2]:
        try:
            conn.exec_driver_sql(
                "INSERT IGNORE INTO match_teams(match_id, team_id, is_home) VALUES(%s,%s,%s)",
                (match_id, tid, 0),
            )
        except Exception:
            pass

    # Innings
    for inn in m.innings:
        stats["innings"] += 1
        batting_team_id = _get_or_create_team(conn, inn.batting_team.name, None)
        bowling_team_id = _get_or_create_team(conn, inn.bowling_team.name, None)
        # upsert by (match_id, innings_no)
        row = conn.exec_driver_sql(
            "SELECT id FROM innings WHERE match_id=%s AND innings_no=%s",
            (match_id, inn.innings_no),
        ).fetchone()
        if row:
            innings_id = int(row[0])
            conn.exec_driver_sql(
                """
                UPDATE innings SET batting_team_id=%s, bowling_team_id=%s, runs=%s, wickets=%s,
                       overs=%s, declared=%s, follow_on_enforced=%s
                WHERE id=%s
                """,
                (
                    batting_team_id, bowling_team_id, inn.runs, inn.wickets,
                    inn.overs, int(inn.declared), int(inn.follow_on_enforced), innings_id,
                ),
            )
        else:
            res = conn.exec_driver_sql(
                """
                INSERT INTO innings(match_id, innings_no, batting_team_id, bowling_team_id, runs, wickets, overs, declared, follow_on_enforced)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    match_id, inn.innings_no, batting_team_id, bowling_team_id,
                    inn.runs, inn.wickets, inn.overs, int(inn.declared), int(inn.follow_on_enforced),
                ),
            )
            innings_id = int(res.lastrowid)

        # Batting
        for be in inn.batting:
            stats["batting"] += 1
            pid = _get_or_create_player(conn, be.player.name, None)
            bowler_id = _get_or_create_player(conn, be.bowler.name, None) if be.bowler else None
            fielder_id = _get_or_create_player(conn, be.fielder.name, None) if be.fielder else None
            conn.exec_driver_sql(
                """
                INSERT INTO batting_innings(innings_id, player_id, position, runs, balls, minutes, fours, sixes, how_out, bowler_id, fielder_id)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE runs=VALUES(runs), balls=VALUES(balls), minutes=VALUES(minutes),
                    fours=VALUES(fours), sixes=VALUES(sixes), how_out=VALUES(how_out), bowler_id=VALUES(bowler_id), fielder_id=VALUES(fielder_id)
                """,
                (
                    innings_id, pid, be.position, be.runs, be.balls, be.minutes, be.fours, be.sixes,
                    be.how_out, bowler_id, fielder_id,
                ),
            )

        # Bowling
        for bw in inn.bowling:
            stats["bowling"] += 1
            pid = _get_or_create_player(conn, bw.player.name, None)
            conn.exec_driver_sql(
                """
                INSERT INTO bowling_innings(innings_id, player_id, overs, maidens, runs, wickets, wides, no_balls, econ)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE overs=VALUES(overs), maidens=VALUES(maidens), runs=VALUES(runs), wickets=VALUES(wickets),
                    wides=VALUES(wides), no_balls=VALUES(no_balls), econ=VALUES(econ)
                """,
                (
                    innings_id, pid, bw.overs, bw.maidens, bw.runs, bw.wickets, bw.wides, bw.no_balls, bw.econ,
                ),
            )

        # Deliveries
        for d in inn.deliveries:
            striker_id = _get_or_create_player(conn, d.striker.name, None)
            non_striker_id = _get_or_create_player(conn, d.non_striker.name, None)
            bowler_id = _get_or_create_player(conn, d.bowler.name, None)
            dismissal_id = _get_or_create_player(conn, d.dismissal_player.name, None) if d.dismissal_player else None
            conn.exec_driver_sql(
                """
                INSERT INTO deliveries(match_id, innings_id, over_no, ball_no, striker_id, non_striker_id, bowler_id,
                                       runs_off_bat, extras_bye, extras_legbye, extras_wide, extras_noball, extras_penalty,
                                       wicket_type, dismissal_player_id)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE runs_off_bat=VALUES(runs_off_bat), extras_bye=VALUES(extras_bye),
                    extras_legbye=VALUES(extras_legbye), extras_wide=VALUES(extras_wide), extras_noball=VALUES(extras_noball),
                    extras_penalty=VALUES(extras_penalty), wicket_type=VALUES(wicket_type), dismissal_player_id=VALUES(dismissal_player_id)
                """,
                (
                    match_id, innings_id, d.over_no, d.ball_no, striker_id, non_striker_id, bowler_id,
                    d.runs_off_bat, d.extras_bye, d.extras_legbye, d.extras_wide, d.extras_noball, d.extras_penalty,
                    d.wicket_type, dismissal_id,
                ),
            )

    return match_id, stats


