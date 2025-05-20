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

"""
This script creates a standardized directory of JIRA user profiles in the staging database. The purpose is to provide a clean and accessible list of JIRA users for identity resolution and linking JIRA activities to individuals.

    1. Fetches user data from the USERS_SUMMARY table in the JIRA data source. 
    2. Processes these records (ensures uniqueness).
    3. Upserts the records into a JIRA_USER_PROFILES table in the staging db. 
"""

# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_NAME = "JIRA_USER_PROFILES"
COMPANY_NAME = os.environ["COMPANY_NAME"]

MAIN_JIRA_SCHEMA = f"{COMPANY_NAME}_JIRA_"
READ_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")


# helpers
def _coerce(val: Any, *, lower: bool = False) -> str | None:
    if val is None:
        return None
    s = str(val)
    return s.lower() if lower else s


# schema (data definition language i.e DDL)
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    jira_account_id       TEXT PRIMARY KEY,
    jira_display_name     TEXT,
    jira_email_address    TEXT
);
"""


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(DDL)
    log.info("Ensured table %s exists", TABLE_NAME)


# data fetch
SELECT_USERS = f"""
SELECT
    "ID"   AS jira_account_id,
    "NAME" AS jira_display_name,
    "EMAIL"AS jira_email_address
FROM "{MAIN_JIRA_SCHEMA}"."USERS_SUMMARY"
WHERE "ID" IS NOT NULL;
"""


def _fetch_users(conn: duckdb.DuckDBPyConnection) -> List[Tuple[Any, ...]]:
    log.info("Querying %s.USERS_SUMMARY", MAIN_JIRA_SCHEMA)
    return conn.execute(SELECT_USERS).fetchall()


# record builder
def _build_records() -> List[Dict[str, Any]]:
    with db_manager(READ_DB, read_only=True) as conn:
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
                jira_account_id=key,
                jira_display_name=_coerce(disp),
                jira_email_address=_coerce(email, lower=True),
            ),
        )

    log.info("Built %d unique JIRA user profiles", len(profiles))
    return list(profiles.values())


# insert / upsert
UPSERT_SQL = f"""
INSERT INTO "{TABLE_NAME}" (
    jira_account_id,
    jira_display_name,
    jira_email_address
) VALUES (?, ?, ?)
ON CONFLICT (jira_account_id) DO UPDATE SET
    jira_display_name  = excluded.jira_display_name,
    jira_email_address = excluded.jira_email_address
"""


def _insert(records: List[Dict[str, Any]]) -> None:
    if not records:
        log.info("Nothing to insert.")
        return

    with db_manager(WRITE_DB) as conn:
        _ensure_table(conn)

        rows = [
            (r["jira_account_id"], r["jira_display_name"], r["jira_email_address"])
            for r in records
        ]
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
