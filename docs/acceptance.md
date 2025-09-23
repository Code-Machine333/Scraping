# Final Acceptance Checklist

- [ ] Repo builds and tests pass locally and in CI
- [ ] `make up` brings up MySQL and Adminer
- [ ] One real series fetched, parsed, loaded end-to-end
- [ ] Query library returns expected numbers on sample data
- [ ] QA checks produce no critical errors
- [ ] Re-run ETL is idempotent (no duplicate rows)
- [ ] Documentation is complete (README, runbook, legacy migration, queries)

## Verification Notes

- Build & Test: `make bootstrap && make test && make test-integration`
- Services Up: `make up` (or `docker compose up -d db adminer`), open Adminer at http://localhost:8080
- End-to-end:
  - `python -m etl.cli discover-latest --since 2024-01-01`
  - `python -m etl.cli refresh --since 2024-01-01`
- Queries: run files in `db/queries/` and validate totals
- QA: `make qa` should show 0 critical issues
- Idempotency: re-run refresh and confirm row counts unchanged
- Docs: ensure `README.md`, `docs/runbook.md`, `docs/legacy_migration.md`, and `db/queries/README.md` are present and accurate
