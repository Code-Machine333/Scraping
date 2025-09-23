from __future__ import annotations

import glob
import json
import re
from pathlib import Path
from typing import Dict

from sqlalchemy import create_engine, text

from ..etl.config import get_etl_config


PARAM_DEFAULTS: Dict[str, str] = {
    ":player_id": "1",
    ":format": "'ODI'",
    ":since_date": "NULL",
    ":until_date": "NULL",
    ":team_a_id": "1",
    ":team_b_id": "2",
    ":venue_id": "NULL",
    ":limit_n": "10",
    ":match_id": "NULL",
    ":team_id": "NULL",
    ":player_a_id": "NULL",
    ":player_b_id": "NULL",
    ":batsman_id": "1",
    ":bowler_id": "2",
}


def _subst_params(sql: str) -> str:
    # Replace :name with defaults in comments-style params
    for k, v in PARAM_DEFAULTS.items():
        sql = sql.replace(k, v)
    return sql


def explain_all() -> Dict[str, dict]:
    cfg = get_etl_config()
    engine = create_engine(cfg.db.dsn)
    results: Dict[str, dict] = {}
    for path in sorted(glob.glob("db/queries/*.sql")):
        raw = Path(path).read_text(encoding="utf-8")
        # Take the first SELECT statement in file
        m = re.search(r"SELECT[\s\S]*?;", raw, flags=re.IGNORECASE)
        if not m:
            continue
        sql = _subst_params(m.group(0))
        with engine.connect() as conn:
            try:
                row = conn.exec_driver_sql(f"EXPLAIN FORMAT=JSON {sql}").fetchone()
                plan = json.loads(row[0]) if row and row[0] else {}
            except Exception as e:
                plan = {"error": str(e)}
        results[path] = plan
    return results


if __name__ == "__main__":
    plans = explain_all()
    out = Path("docs/reports/explain_plans.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(plans, indent=2), encoding="utf-8")
    print(f"Wrote {out}")


