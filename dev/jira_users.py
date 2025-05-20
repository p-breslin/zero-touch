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
    jira_account_id       TEXT PRIMARY KEY,
    jira_display_name     TEXT,
    jira_email_address    TEXT,
    jira_account_type     TEXT,
    is_active             BOOLEAN,
    jira_timezone         TEXT
);
"""


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(DDL)
    log.info("Ensured table %s exists", TABLE_NAME)


# data fetch
SELECT_USERS = f"""
SELECT
    "ACCOUNTID"   AS account_id,
    "DISPLAYNAME" AS display_name,
    "EMAILADDRESS"AS email_address,
    "ACCOUNTTYPE" AS account_type,
    "ACTIVE"      AS is_active,
    "TIMEZONE"    AS timezone
FROM "{MAIN_JIRA_SCHEMA}"."USERS"
WHERE "ACCOUNTID" IS NOT NULL;
"""


def _fetch_users(conn: duckdb.DuckDBPyConnection) -> List[Tuple[Any, ...]]:
    log.info("Querying %s.USERS", MAIN_JIRA_SCHEMA)
    return conn.execute(SELECT_USERS).fetchall()


# record builder
def _build_records() -> List[Dict[str, Any]]:
    with _db(READ_DB, read_only=True) as conn:
        rows = _fetch_users(conn)
        log.info("Fetched %d JIRA users", len(rows))

    profiles: Dict[str, Dict[str, Any]] = {}

    for acc_id, disp, email, acc_type, active, tz in rows:
        key = _coerce(acc_id)
        if not key:
            log.debug("Skipping row without ACCOUNTID: %s", (acc_id, disp, email))
            continue

        # First-come wins; change to overwrite logic?
        profiles.setdefault(
            key,
            dict(
                jira_account_id=key,
                jira_display_name=_coerce(disp),
                jira_email_address=_coerce(email, lower=True),
                jira_account_type=_coerce(acc_type),
                is_active=bool(active) if active is not None else None,
                jira_timezone=_coerce(tz),
            ),
        )

    log.info("Built %d unique JIRA user profiles", len(profiles))
    return list(profiles.values())


# insert / upsert
UPSERT_SQL = f"""
INSERT INTO "{TABLE_NAME}" (
    jira_account_id,
    jira_display_name,
    jira_email_address,
    jira_account_type,
    is_active,
    jira_timezone
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (jira_account_id) DO UPDATE SET
    jira_display_name  = excluded.jira_display_name,
    jira_email_address = excluded.jira_email_address,
    jira_account_type  = excluded.jira_account_type,
    is_active          = excluded.is_active,
    jira_timezone      = excluded.jira_timezone;
"""


def _insert(records: List[Dict[str, Any]]) -> None:
    if not records:
        log.info("Nothing to insert.")
        return

    with _db(WRITE_DB) as conn:
        _ensure_table(conn)

        rows = [
            (
                r["jira_account_id"],
                r["jira_display_name"],
                r["jira_email_address"],
                r["jira_account_type"],
                r["is_active"],
                r["jira_timezone"],
            )
            for r in records
        ]
        conn.executemany(UPSERT_SQL, rows)
        conn.commit()
        log.info("Upserted %d profiles into %s", len(rows), TABLE_NAME)


# entry point
def main() -> None:
    _insert(_build_records())


if __name__ == "__main__":
    log.info("Staging JIRA user profiles → %s", TABLE_NAME)
    main()
    log.info("Done.")
