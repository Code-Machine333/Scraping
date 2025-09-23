from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from loguru import logger
from sqlalchemy import create_engine
from difflib import SequenceMatcher

from ..database import get_database_engine


@dataclass
class ReconcileConfig:
    cricinfo_dsn: str
    repo_root: Path


def load_config() -> ReconcileConfig:
    dsn = os.environ.get("CRICINFO_RO_DSN", "")
    if not dsn:
        raise RuntimeError("CRICINFO_RO_DSN not set in environment")
    root = Path(os.getcwd())
    return ReconcileConfig(cricinfo_dsn=dsn, repo_root=root)


def profile_old_schema(cfg: ReconcileConfig) -> Dict[str, int]:
    """Return row counts per table from old Cricinfo DB."""
    engine = create_engine(cfg.cricinfo_dsn, pool_pre_ping=True, future=True)
    counts: Dict[str, int] = {}
    with engine.connect() as conn:
        tables = conn.exec_driver_sql("SHOW TABLES").fetchall()
        for (tname,) in tables:
            try:
                c = conn.exec_driver_sql(f"SELECT COUNT(*) FROM `{tname}`").scalar()
                counts[str(tname)] = int(c)
            except Exception:
                counts[str(tname)] = -1
    return counts


def write_counts_report(counts: Dict[str, int], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "cricinfo_table_counts.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["table", "row_count"])
        for t, c in sorted(counts.items()):
            w.writerow([t, c])
    return out_path


def generate_missing_matches_sql(new_db_dsn: str, out_dir: Path) -> Path:
    """Placeholder: Generate a SQL script to backfill missing matches by comparing keys.

    In practice, you would compare against the new DB's `matches.source_match_key`. Here we emit a template.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_sql = out_dir / "2025_ensure_missing_matches_backfill.sql"
    content = """
-- Template SQL to backfill missing matches from CricketArchive
-- Compare source keys and insert new records safely
-- Fill in SELECTs specific to the old schema
START TRANSACTION;
-- INSERT INTO matches(...) SELECT ... FROM old_db.matches om LEFT JOIN new_db.matches nm ON nm.source_match_key = om.source_key WHERE nm.id IS NULL;
COMMIT;
"""
    out_sql.write_text(content.strip() + "\n", encoding="utf-8")
    return out_sql


def generate_duplicate_players_report(cfg: ReconcileConfig, out_dir: Path) -> Path:
    """Example: detect potential duplicate players by (full_name, born_date)."""
    engine = create_engine(cfg.cricinfo_dsn, pool_pre_ping=True, future=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "cricinfo_dup_players.csv"
    with engine.connect() as conn, out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["full_name", "born_date", "count"])
        sql = (
            "SELECT full_name, born_date, COUNT(*) c FROM players GROUP BY full_name, born_date HAVING c > 1 ORDER BY c DESC"
        )
        for row in conn.exec_driver_sql(sql).fetchall():
            w.writerow([row[0], str(row[1]) if row[1] is not None else "", int(row[2])])
    return out_path


def _norm_name(name: str) -> str:
    return " ".join(name.lower().strip().split())


def _fetch_old_players(cfg: ReconcileConfig) -> List[Tuple[str, Optional[str]]]:
    engine = create_engine(cfg.cricinfo_dsn, pool_pre_ping=True, future=True)
    with engine.connect() as conn:
        rows = conn.exec_driver_sql("SELECT full_name, born_date FROM players").fetchall()
    return [(str(r[0]), str(r[1]) if r[1] is not None else None) for r in rows]


def _fetch_new_players() -> List[Tuple[str, Optional[str]]]:
    engine = get_database_engine()
    with engine.connect() as conn:
        rows = conn.exec_driver_sql("SELECT full_name, NULL as born_date FROM players").fetchall()
    return [(str(r[0]), None) for r in rows]


def generate_player_mapping_candidates(cfg: ReconcileConfig, out_dir: Path, threshold: float = 0.9) -> Path:
    old_players = _fetch_old_players(cfg)
    new_players = _fetch_new_players()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "player_mapping_candidates.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["old_full_name", "old_born_date", "new_full_name", "sim_score"])
        new_norm = [(_norm_name(n), n, dob) for (n, dob) in new_players]
        for oname, odob in old_players:
            onorm = _norm_name(oname)
            best: Tuple[float, Optional[str]] = (0.0, None)
            for nnorm, nname, ndob in new_norm:
                score = SequenceMatcher(None, onorm, nnorm).ratio()
                if score > best[0]:
                    best = (score, nname)
            if best[0] >= threshold:
                w.writerow([oname, odob or "", best[1], f"{best[0]:.3f}"])
    return out_path


def _fetch_old_teams(cfg: ReconcileConfig) -> List[str]:
    engine = create_engine(cfg.cricinfo_dsn, pool_pre_ping=True, future=True)
    with engine.connect() as conn:
        rows = conn.exec_driver_sql("SELECT DISTINCT name FROM teams").fetchall()
    return [str(r[0]) for r in rows]


def _fetch_new_teams() -> List[str]:
    engine = get_database_engine()
    with engine.connect() as conn:
        rows = conn.exec_driver_sql("SELECT DISTINCT name FROM teams").fetchall()
    return [str(r[0]) for r in rows]


def generate_team_mapping_candidates(cfg: ReconcileConfig, out_dir: Path, threshold: float = 0.9) -> Path:
    old_teams = _fetch_old_teams(cfg)
    new_teams = _fetch_new_teams()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "team_mapping_candidates.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["old_team", "new_team", "sim_score"])
        new_norm = [(_norm_name(n), n) for n in new_teams]
        for oname in old_teams:
            onorm = _norm_name(oname)
            best: Tuple[float, Optional[str]] = (0.0, None)
            for nnorm, nname in new_norm:
                score = SequenceMatcher(None, onorm, nnorm).ratio()
                if score > best[0]:
                    best = (score, nname)
            if best[0] >= threshold:
                w.writerow([oname, best[1], f"{best[0]:.3f}"])
    return out_path


def reconcile_main(reports: List[str], threshold: float = 0.9) -> Dict[str, str]:
    cfg = load_config()
    reports_dir = cfg.repo_root / "docs" / "reports"
    migrations_dir = cfg.repo_root / "db" / "migrations"
    outputs: Dict[str, str] = {}

    if "counts" in reports:
        counts = profile_old_schema(cfg)
        path = write_counts_report(counts, reports_dir)
        outputs["counts"] = str(path)

    if "missing_matches" in reports:
        path = generate_missing_matches_sql("", migrations_dir)
        outputs["missing_matches"] = str(path)

    if "dup_players" in reports:
        path = generate_duplicate_players_report(cfg, reports_dir)
        outputs["dup_players"] = str(path)

    if "players_map" in reports:
        path = generate_player_mapping_candidates(cfg, reports_dir, threshold=threshold)
        outputs["players_map"] = str(path)

    if "teams_map" in reports:
        path = generate_team_mapping_candidates(cfg, reports_dir, threshold=threshold)
        outputs["teams_map"] = str(path)

    return outputs


