# Legacy Migration Guide

This guide explains how to migrate existing raw Cricinfo data into the new canonical schema.

## Overview

The script `src/cricket_database/etl/migrate_legacy.py` provides:
- Ingestion of legacy Cricinfo tables into local staging tables
- Best‑effort mapping into canonical entities (countries, teams, players) with alias emission
- Reconciliation reports (CSV) for manual review
- Dry‑run by default; use `--commit` to persist changes

## Prerequisites

- Set `CRICINFO_RO_DSN` to a read-only SQLAlchemy DSN for the legacy database, e.g.:
  - `mysql+mysqlconnector://user:pass@host:3306/cricinfo`
- Target database configured via `.env` and reachable

## Staging Schema

The migration script creates idempotent staging tables:
- `staging_players(legacy_player_id, full_name, known_as, born_date, country_name)`
- `staging_teams(legacy_team_id, name, country_name)`
- `staging_matches(legacy_match_id, format, start_date, venue_name, home_team, away_team)`

## Running the Migration

Dry-run (default):
```bash
python -m src.cricket_database.etl.migrate_legacy run
```

Persist writes:
```bash
python -m src.cricket_database.etl.migrate_legacy run --commit
```

The script:
1. Ingests legacy rows into staging tables
2. Maps countries, teams, players to canonical tables
3. Emits reconciliation reports to `docs/reports/`

## Reconciliation Reports

- `legacy_dup_player_names.csv`: same-name players with different DOB across canonical records
- `legacy_unmatched_venues.csv`: venue names present in staging but not found in canonical `venues`

Use these reports to:
- Merge or correct duplicate players
- Add or normalize missing venues, then re-run the script

## Safety and Idempotency

- The script is idempotent; it uses `INSERT ... ON DUPLICATE KEY UPDATE` for canonical upserts
- Staging tables are truncated only when `--commit` is passed
- Start with dry-run to review counts and reports, then re-run with `--commit`

## Extending the Mapping

Enhance `_ingest_legacy` with more source tables and `_map_to_canonical` with richer matching (e.g., fuzzy matching on names with thresholds) as needed.
