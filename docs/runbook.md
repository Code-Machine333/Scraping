# Operational Runbook

This runbook covers day-to-day and incident procedures for the cricket data platform.

## Backfill a New Season

1) Prepare environment
- Ensure `.env` is configured and DB reachable
- Apply migrations:
```bash
python -m src.cli migrate-sql
```

2) Discover and queue data
```bash
# Year example
python -m etl.cli discover-latest --since 2024-01-01
```

3) Fetch, parse, and load incrementally
```bash
# Dry-run to validate
python -m etl.cli refresh --since 2024-01-01 --dry-run

# Live run (remove --dry-run)
python -m etl.cli refresh --since 2024-01-01
```

4) Refresh analytics summaries
```bash
# Find season_id for the target season (via a simple SELECT or Adminer)
python -m src.cli refresh-season-all <SEASON_ID>
```

5) Verify
- Use Adminer to check `matches`, `innings`, and `deliveries` counts
- Run QA:
```bash
make qa
```

## Recover from a Failed Load

Symptoms: partial match load, constraint errors, or parser exceptions.

1) Identify failed item
- Check logs from `etl.cli load` or `refresh`
- Note `raw_html.id` or the `match_url`

2) Re-run parse/load for the single item in dry-run
```bash
python -m src.cricket_database.cli.main load --from-raw <RAW_ID> --no-dry-run
```
If using the incremental CLI cache flow, re-parse and then load:
```bash
python -m etl.cli parse --max-items 10
python -m etl.cli load --max-items 5
```

3) Database cleanup (rare)
- If a transaction failed mid-load, rows should have been rolled back
- If partial rows exist, delete affected `deliveries`/`innings`/`match_teams` for the `match_id`, then re-run load

4) Parser issues
- Save the raw HTML (`raw_html.body`) for debugging
- Add/adjust parser rules in `src/cricket_database/etl/parse_scorecard.py`
- Add a unit test to reproduce the failure

## Add a New Source (e.g., Cricsheet CSV) via SOURCE_ID

Goal: ingest another dataset into the same normalized schema using a unique `sources.id`.

1) Register the source
```sql
INSERT INTO sources (name, base_url) VALUES ('Cricsheet', 'https://cricsheet.org');
-- Note the assigned sources.id, or set a fixed id in dev
```

2) Configure environment
- Add `CRICSHEET_SOURCE_ID=<ID>` to `.env` (optional but recommended)

3) Implement an importer
- Create `src/cricket_database/etl/cricsheet_import.py` to parse CSV/JSON and map to `MatchModel`
- Reuse existing `transform.to_rows` + `load_rows` to upsert
- Ensure `source_keys` are populated deterministically using Cricsheet identifiers

4) Idempotency & aliases
- Emit `team_alias` and `player_alias` when names differ from canonical
- Use `INSERT ... ON DUPLICATE KEY UPDATE` loaders already implemented

5) Run importer
```bash
python -m src.cricket_database.etl.cricsheet_import import --path /data/cricsheet --source-id <ID>
```

6) QA and summaries
```bash
make qa
python -m src.cli refresh-season-all <SEASON_ID>
```

## Pause/Resume Cron and Adjust Rate Limits

### Pause/resume scheduled runs
- If using system cron:
  - Comment out the crontab line (`crontab -e`) to pause
  - Re-enable by uncommenting and saving
- If using our Typer scheduler (`src/cricket_database/cli/main.py schedule`), stop the process (Ctrl+C) to pause, re-run to resume

### Adjust rate limits and retries
- Edit `.env` and restart the process:
```
RATE_LIMIT_RPS=1.0        # requests per second (default conservative)
MAX_RETRIES=3
BACKOFF_BASE_SECONDS=0.5
BACKOFF_MAX_SECONDS=8.0
ETL_CONCURRENCY=4
```
- For emergency throttling, set `RATE_LIMIT_RPS=0.2` and relaunch fetch jobs

### Allow/Block lists
- To restrict what can be fetched:
```
ETL_ALLOWLIST=/Archive/.*
ETL_BLOCKLIST=.*\.(png|jpg|css|js)$
```
- Blocklist overrides; allowlist must match when set

## Verification Checklist (Post-ops)
- `make qa` shows no critical issues
- `docs/reports/explain_plans.json` exists and heavy queries are indexed
- Adminer quick checks:
  - `SELECT COUNT(*) FROM matches;`
  - `SELECT COUNT(*) FROM innings;`
  - `SELECT COUNT(*) FROM deliveries;`
- Last refresh durations: `python -m etl.cli metrics --json`
