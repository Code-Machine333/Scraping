"""Legacy data migration helper.

Capabilities:
- Ingest existing raw Cricinfo tables into local staging tables
- Map/clean into canonical schema with best‑effort alias generation
- Generate reconciliation reports (CSV) for manual review
- Default dry‑run; use --commit to persist writes

Environment:
- CRICINFO_RO_DSN: SQLAlchemy URL to legacy Cricinfo (read-only)

IMPORTANT NOTES:
- Keep player canonicalization conservative - never auto-merge without strong keys
- Record merge candidates for manual review in player_alias/team_alias tables
- Schema supports multi-source ingest via SOURCE_ID for different data providers
- Consider Cricsheet CSV as a lawful, robust supplementary source
"""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, List, Tuple

import typer
from loguru import logger
from sqlalchemy import create_engine, text

from ..etl.config import get_etl_config


app = typer.Typer(no_args_is_help=True, help="Legacy migration utilities")

REPORTS_DIR = Path("docs/reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def _get_engines():
    cfg = get_etl_config()
    legacy_dsn = os.getenv("CRICINFO_RO_DSN")
    if not legacy_dsn:
        raise RuntimeError("CRICINFO_RO_DSN is not set")
    legacy_engine = create_engine(legacy_dsn)
    target_engine = create_engine(cfg.db.dsn)
    return legacy_engine, target_engine


def _create_staging_tables(conn):
    # Minimal staging schema (idempotent)
    conn.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS staging_players (
          legacy_player_id BIGINT,
          full_name VARCHAR(255),
          known_as VARCHAR(255),
          born_date DATE,
          country_name VARCHAR(255)
        ) ENGINE=InnoDB;
        """
    )
    conn.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS staging_teams (
          legacy_team_id BIGINT,
          name VARCHAR(255),
          country_name VARCHAR(255)
        ) ENGINE=InnoDB;
        """
    )
    conn.exec_driver_sql(
        """
        CREATE TABLE IF NOT EXISTS staging_matches (
          legacy_match_id BIGINT,
          format VARCHAR(32),
          start_date DATE,
          venue_name VARCHAR(255),
          home_team VARCHAR(255),
          away_team VARCHAR(255)
        ) ENGINE=InnoDB;
        """
    )


def _truncate_staging(conn):
    conn.exec_driver_sql("TRUNCATE TABLE staging_players")
    conn.exec_driver_sql("TRUNCATE TABLE staging_teams")
    conn.exec_driver_sql("TRUNCATE TABLE staging_matches")


def _ingest_legacy(legacy_engine, target_engine, commit: bool) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    with target_engine.begin() as tgt:
        _create_staging_tables(tgt)
        if commit:
            _truncate_staging(tgt)
        # Players
        with legacy_engine.connect() as src:
            rows = src.execute(text(
                """
                SELECT id as legacy_player_id,
                       full_name,
                       known_as,
                       born_date,
                       country as country_name
                FROM cricinfo_players
                """
            )).fetchall()
        counts["players_src"] = len(rows)
        if commit and rows:
            for r in rows:
                tgt.execute(
                    text(
                        """
                        INSERT INTO staging_players(legacy_player_id, full_name, known_as, born_date, country_name)
                        VALUES (:legacy_player_id, :full_name, :known_as, :born_date, :country_name)
                        """
                    ),
                    dict(r._mapping),
                )
        # Teams
        with legacy_engine.connect() as src:
            rows = src.execute(text(
                """
                SELECT id as legacy_team_id,
                       name,
                       country as country_name
                FROM cricinfo_teams
                """
            )).fetchall()
        counts["teams_src"] = len(rows)
        if commit and rows:
            for r in rows:
                tgt.execute(
                    text(
                        """
                        INSERT INTO staging_teams(legacy_team_id, name, country_name)
                        VALUES (:legacy_team_id, :name, :country_name)
                        """
                    ),
                    dict(r._mapping),
                )
        # Matches (minimal)
        with legacy_engine.connect() as src:
            rows = src.execute(text(
                """
                SELECT id as legacy_match_id,
                       format,
                       start_date,
                       venue_name,
                       home_team,
                       away_team
                FROM cricinfo_matches
                """
            )).fetchall()
        counts["matches_src"] = len(rows)
        if commit and rows:
            for r in rows:
                tgt.execute(
                    text(
                        """
                        INSERT INTO staging_matches(legacy_match_id, format, start_date, venue_name, home_team, away_team)
                        VALUES (:legacy_match_id, :format, :start_date, :venue_name, :home_team, :away_team)
                        """
                    ),
                    dict(r._mapping),
                )
    return counts


def _map_to_canonical(target_engine, commit: bool) -> Dict[str, int]:
    """Map staging to canonical minimal entities with aliasing.

    - Upsert countries, teams, players
    - Add aliases when names differ
    """
    stats: Dict[str, int] = {"countries": 0, "teams": 0, "players": 0, "aliases": 0}
    with target_engine.begin() as conn:
        # Countries from staging (distinct country_name)
        countries = conn.execute(text("SELECT DISTINCT country_name FROM staging_players WHERE country_name IS NOT NULL AND country_name<>'' "))
        for (country_name,) in countries:
            if not commit:
                continue
            conn.exec_driver_sql(
                """
                INSERT INTO countries(name)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE name=VALUES(name)
                """,
                (country_name,),
            )
            stats["countries"] += 1
        # Teams
        teams = conn.execute(text("SELECT name, country_name FROM staging_teams"))
        for name, country_name in teams:
            if not commit:
                continue
            country_id = conn.exec_driver_sql("SELECT id FROM countries WHERE name=%s LIMIT 1", (country_name,)).scalar()
            conn.exec_driver_sql(
                """
                INSERT INTO teams(name, country_id)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE country_id=VALUES(country_id)
                """,
                (name, country_id),
            )
            stats["teams"] += 1
        # Players
        players = conn.execute(text("SELECT full_name, known_as, born_date, country_name FROM staging_players"))
        for full_name, known_as, born_date, country_name in players:
            if not commit:
                continue
            country_id = conn.exec_driver_sql("SELECT id FROM countries WHERE name=%s LIMIT 1", (country_name,)).scalar()
            conn.exec_driver_sql(
                """
                INSERT INTO players(full_name, born_date, country_id)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE country_id=VALUES(country_id)
                """,
                (full_name, born_date, country_id),
            )
            stats["players"] += 1
            if known_as and known_as.strip() and known_as.strip() != full_name:
                pid = conn.exec_driver_sql("SELECT id FROM players WHERE full_name=%s LIMIT 1", (full_name,)).scalar()
                conn.exec_driver_sql(
                    """
                    INSERT INTO player_alias(player_id, alias_name)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE player_id=player_id
                    """,
                    (pid, known_as.strip()),
                )
                stats["aliases"] += 1
    return stats


def _write_csv(path: Path, headers: List[str], rows: List[Tuple]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow(list(r))


def _reconciliation_reports(target_engine):
    """Emit simple reports: players with same name diff DOB; unmatched venues from staging."""
    with target_engine.connect() as conn:
        dup_players = conn.exec_driver_sql(
            """
            SELECT p1.full_name, p1.born_date, p2.born_date
            FROM players p1
            JOIN players p2 ON p1.full_name=p2.full_name AND p1.id<p2.id
            WHERE p1.born_date IS NOT NULL AND p2.born_date IS NOT NULL AND p1.born_date<>p2.born_date
            LIMIT 500
            """
        ).fetchall()
        _write_csv(REPORTS_DIR / "legacy_dup_player_names.csv", ["full_name", "dob_1", "dob_2"], dup_players)

        unmatched_venues = conn.exec_driver_sql(
            """
            SELECT DISTINCT s.venue_name
            FROM staging_matches s
            LEFT JOIN venues v ON v.name = s.venue_name
            WHERE v.id IS NULL
            ORDER BY s.venue_name
            """
        ).fetchall()
        _write_csv(REPORTS_DIR / "legacy_unmatched_venues.csv", ["venue_name"], unmatched_venues)


@app.command()
def run(commit: bool = typer.Option(False, "--commit", help="Persist writes (otherwise dry-run)")):
    """Ingest legacy data to staging, map to canonical schema, and emit reports."""
    legacy_engine, target_engine = _get_engines()
    logger.info("Starting legacy ingestion (dry-run=%s)", not commit)
    counts = _ingest_legacy(legacy_engine, target_engine, commit)
    logger.info("Legacy rows: %s", counts)
    stats = _map_to_canonical(target_engine, commit)
    logger.info("Mapped stats: %s", stats)
    _reconciliation_reports(target_engine)
    logger.info("Reports written to %s", REPORTS_DIR)


if __name__ == "__main__":
    app()


