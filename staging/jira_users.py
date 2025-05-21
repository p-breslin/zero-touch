"""
Creates a standardized directory of JIRA user profiles in the staging database. The purpose is to provide a clean and accessible list of JIRA users for identity resolution and linking JIRA activities to individuals.

Steps
-----
1. Fetches user data from the USERS_SUMMARY table in the JIRA data source.
2. Processes these records (ensures uniqueness).
3. Upserts the records into a JIRA_USER_PROFILES table in the staging db.
"""

from __future__ import annotations
import os
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from typing import Any, Dict, List, Tuple
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_NAME = "JIRA_USER_PROFILES"
COMPANY_NAME = os.environ["COMPANY_NAME"]

MAIN_JIRA_SCHEMA = f"{COMPANY_NAME}_JIRA_"
READ_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

# Schema
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ACCOUNT_ID       TEXT PRIMARY KEY,
    DISPLAY_NAME     TEXT,
    EMAIL            TEXT
);
"""

SELECT_USERS = f"""
SELECT
    "ID"   AS ACCOUNT_ID,
    "NAME" AS DISPLAY_NAME,
    "EMAIL"AS EMAIL
FROM "{MAIN_JIRA_SCHEMA}"."USERS_SUMMARY"
WHERE "ID" IS NOT NULL;
"""

UPSERT_SQL = f"""
INSERT INTO "{TABLE_NAME}" (
    ACCOUNT_ID,
    DISPLAY_NAME,
    EMAIL
) VALUES (?, ?, ?)
ON CONFLICT (ACCOUNT_ID) DO UPDATE SET
    DISPLAY_NAME  = excluded.DISPLAY_NAME,
    EMAIL = excluded.EMAIL
"""


# Helpers ----------------------------------------------------------------------
def _coerce(val: Any, *, lower: bool = False) -> str | None:
    if val is None:
        return None
    s = str(val)
    return s.lower() if lower else s


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(DDL)
    log.info("Ensured table %s exists", TABLE_NAME)


def _fetch_users(conn: duckdb.DuckDBPyConnection) -> List[Tuple[Any, ...]]:
    log.info("Querying %s.USERS_SUMMARY", MAIN_JIRA_SCHEMA)
    return conn.execute(SELECT_USERS).fetchall()


# Record builder ---------------------------------------------------------------
def _build_records() -> List[Dict[str, Any]]:
    with db_manager(READ_DB, read_only=True) as conn:
        rows = _fetch_users(conn)
        log.info("Fetched %d JIRA users", len(rows))

    profiles: Dict[str, Dict[str, Any]] = {}

    for acc_id, disp, EMAIL in rows:
        key = _coerce(acc_id)
        if not key:
            log.debug("Skipping row without ID: %s", (acc_id, disp, EMAIL))
            continue

        # First-come wins; change to overwrite logic?
        profiles.setdefault(
            key,
            dict(
                ACCOUNT_ID=key,
                DISPLAY_NAME=_coerce(disp),
                EMAIL=_coerce(EMAIL, lower=True),
            ),
        )

    log.info("Built %d unique JIRA user profiles", len(profiles))
    return list(profiles.values())


# Insert -----------------------------------------------------------------------
def _insert(records: List[Dict[str, Any]]) -> None:
    if not records:
        log.info("Nothing to insert.")
        return

    with db_manager(WRITE_DB) as conn:
        _ensure_table(conn)

        rows = [(r["ACCOUNT_ID"], r["DISPLAY_NAME"], r["EMAIL"]) for r in records]
        conn.executemany(UPSERT_SQL, rows)
        conn.commit()
        log.info("Upserted %d profiles into %s", len(rows), TABLE_NAME)


# Entry point ------------------------------------------------------------------
def main() -> None:
    _insert(_build_records())


if __name__ == "__main__":
    log.info("Staging JIRA user profiles -> %s", TABLE_NAME)
    main()
    log.info("Done.")
