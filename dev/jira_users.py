from __future__ import annotations
import os
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from contextlib import contextmanager
from typing import Any, Dict, List, Tuple
from utils.logging_setup import setup_logging

# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_NAME = "JIRA_USER_PROFILES"
COMPANY_NAME = os.environ["COMPANY_NAME"]

MAIN_JIRA_SCHEMA = f"{COMPANY_NAME}_JIRA_"
READ_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")


# helpers
@contextmanager
def _db(path: Path, *, read_only: bool = False):
    conn = duckdb.connect(str(path), read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def _coerce(val: Any, *, lower: bool = False) -> str | None:
    if val is None:
        return None
    s = str(val)
    return s.lower() if lower else s


# schema (data definition language i.e DDL)
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    account_id       TEXT PRIMARY KEY,
    display_name     TEXT,
    email    TEXT
);
"""


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(DDL)
    log.info("Ensured table %s exists", TABLE_NAME)


# data fetch
SELECT_USERS = f"""
SELECT
    "ID"   AS account_id,
    "NAME" AS display_name,
    "EMAIL"AS email
FROM "{MAIN_JIRA_SCHEMA}"."USERS_SUMMARY"
WHERE "ID" IS NOT NULL;
"""


def _fetch_users(conn: duckdb.DuckDBPyConnection) -> List[Tuple[Any, ...]]:
    log.info("Querying %s.USERS_SUMMARY", MAIN_JIRA_SCHEMA)
    return conn.execute(SELECT_USERS).fetchall()


# record builder
def _build_records() -> List[Dict[str, Any]]:
    with _db(READ_DB, read_only=True) as conn:
        rows = _fetch_users(conn)
        log.info("Fetched %d JIRA users", len(rows))

    profiles: Dict[str, Dict[str, Any]] = {}

    for acc_id, disp, email in rows:
        key = _coerce(acc_id)
        if not key:
            log.debug("Skipping row without ID: %s", (acc_id, disp, email))
            continue

        # First-come wins; change to overwrite logic?
        profiles.setdefault(
            key,
            dict(
                account_id=key,
                display_name=_coerce(disp),
                email=_coerce(email, lower=True),
            ),
        )

    log.info("Built %d unique JIRA user profiles", len(profiles))
    return list(profiles.values())


# insert / upsert
UPSERT_SQL = f"""
INSERT INTO "{TABLE_NAME}" (
    account_id,
    display_name,
    email
) VALUES (?, ?, ?)
ON CONFLICT (account_id) DO UPDATE SET
    display_name  = excluded.display_name,
    email = excluded.email
"""


def _insert(records: List[Dict[str, Any]]) -> None:
    if not records:
        log.info("Nothing to insert.")
        return

    with _db(WRITE_DB) as conn:
        _ensure_table(conn)

        rows = [(r["account_id"], r["display_name"], r["email"]) for r in records]
        conn.executemany(UPSERT_SQL, rows)
        conn.commit()
        log.info("Upserted %d profiles into %s", len(rows), TABLE_NAME)


# entry point
def main() -> None:
    _insert(_build_records())


if __name__ == "__main__":
    log.info("Staging JIRA user profiles -> %s", TABLE_NAME)
    main()
    log.info("Done.")
