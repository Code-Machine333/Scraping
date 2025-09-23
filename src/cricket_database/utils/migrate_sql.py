"""SQL migration runner for applying .sql files in db/ddl in lexical order.

Uses SQLAlchemy engine (mysql+mysqlconnector) to execute raw SQL statements.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Tuple
import hashlib
import datetime as dt

from sqlalchemy import text
from sqlalchemy.engine import Engine


MIGRATIONS_TABLE = "schema_migrations"


def list_sql_files(migrations_dir: Path) -> List[Path]:
    """Return a sorted list of .sql files from the migrations directory."""
    files = [p for p in migrations_dir.glob("*.sql") if p.is_file()]
    return sorted(files, key=lambda p: p.name)


def read_sql_file(file_path: Path) -> str:
    """Read SQL file contents as a single string."""
    return file_path.read_text(encoding="utf-8")


def split_sql_batches(sql_text: str) -> Iterable[str]:
    """Yield executable SQL batches.

    - Keeps DELIMITER blocks intact by not splitting on semicolons inside them.
    - For typical schema files without custom delimiters, splits on semicolons.
    """
    # Simple heuristic: if we see DELIMITER, execute whole text as one batch
    upper = sql_text.upper()
    if "\nDELIMITER " in upper or upper.startswith("DELIMITER "):
        yield sql_text
        return

    buffer: List[str] = []
    for line in sql_text.splitlines(keepends=True):
        buffer.append(line)
        if line.strip().endswith(";"):
            batch = "".join(buffer).strip()
            if batch:
                yield batch
            buffer.clear()
    # trailing batch
    tail = "".join(buffer).strip()
    if tail:
        yield tail


def apply_sql_file(engine: Engine, file_path: Path) -> Tuple[str, int]:
    """Apply a single SQL file; returns (filename, statements_executed)."""
    sql_text = read_sql_file(file_path)
    executed = 0
    with engine.begin() as conn:
        for batch in split_sql_batches(sql_text):
            # Use exec_driver_sql to allow DDL and multiple dialect-specific statements
            conn.exec_driver_sql(batch)
            executed += 1
    return (file_path.name, executed)


def ensure_migrations_table(engine: Engine) -> None:
    """Create the migrations tracking table if it doesn't exist."""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        filename VARCHAR(255) NOT NULL,
        checksum CHAR(64) NOT NULL,
        applied_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_schema_migrations_filename (filename)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(create_sql)


def compute_checksum(text_value: str) -> str:
    return hashlib.sha256(text_value.encode("utf-8")).hexdigest()


def load_applied_migrations(engine: Engine) -> dict:
    with engine.begin() as conn:
        rows = conn.exec_driver_sql(
            f"SELECT filename, checksum FROM {MIGRATIONS_TABLE}"
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def record_migration(engine: Engine, filename: str, checksum: str) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"REPLACE INTO {MIGRATIONS_TABLE} (filename, checksum, applied_at) VALUES (%s, %s, %s)",
            (filename, checksum, dt.datetime.utcnow()),
        )


def migrate(
    engine: Engine,
    repo_root: Path | None = None,
    ddl_rel_path: str = "db/ddl",
    force_reapply: bool = False,
) -> List[Tuple[str, int, str]]:
    """Apply all SQL migrations in lexical order and return a summary list.

    Each tuple corresponds to (filename, statements_executed, status), where status is one of
    "applied", "skipped", or "reapplied" (when force_reapply=True and checksum changed).
    """
    root = repo_root or Path(os.getcwd())
    migrations_dir = root / ddl_rel_path
    if not migrations_dir.exists():
        raise FileNotFoundError(f"Migrations directory not found: {migrations_dir}")

    ensure_migrations_table(engine)
    applied = load_applied_migrations(engine)

    results: List[Tuple[str, int, str]] = []
    for file_path in list_sql_files(migrations_dir):
        fname = file_path.name
        sql_text = read_sql_file(file_path)
        checksum = compute_checksum(sql_text)
        if fname in applied and applied[fname] == checksum and not force_reapply:
            results.append((fname, 0, "skipped"))
            continue
        executed_count = 0
        if fname in applied and applied[fname] != checksum and not force_reapply:
            # Safety: do not silently reapply changed migration
            raise ValueError(
                f"Migration '{fname}' has changed since last apply. Use force_reapply=True to reapply."
            )
        _, executed_count = apply_sql_file(engine, file_path)
        record_migration(engine, fname, checksum)
        status = "reapplied" if fname in applied and applied[fname] != checksum else "applied"
        if fname in applied and applied[fname] == checksum and force_reapply:
            status = "reapplied"
        results.append((fname, executed_count, status))
    return results


