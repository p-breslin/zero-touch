"""
Stage JIRA_ISSUES with full details for every JIRA key seen in GITHUB_COMMITS.

Steps
-----
1. Collect distinct keys from the GITHUB_COMMITS tables on the staging DB
2. Pull full issue blobs from the main JIRA DB
3. Enrich with account-email via JIRA_ACTIVE_USERS (staging DB)
4. Upsert into JIRA_ISSUES
"""

from __future__ import annotations
import os
import duckdb
import logging
from pathlib import Path
from typing import Set, Tuple
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging
from utils.helpers import db_manager, safe_json


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_JIRA = f"{COMPANY}_JIRA_"

STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
MAIN_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_NAME')}.duckdb")


T_COMMIT_KEYS = "GITHUB_COMMITS"
T_USER_PROF = "JIRA_ACTIVE_USERS"
T_TARGET = "JIRA_ISSUES"

COLS = (
    "ISSUE_KEY",
    "ASSIGNEE_DISPLAY_NAME",
    "REPORTER_DISPLAY_NAME",
    "ASSIGNEE_ACCOUNT_ID",
    "REPORTER_ACCOUNT_ID",
    "ASSIGNEE_EMAIL",
    "REPORTER_EMAIL",
    "INTERNAL_ISSUE_ID",
    "SUMMARY",
    "DESCRIPTION",
    "STATUS_NAME",
    "ISSUE_TYPE_NAME",
    "PROJECT_KEY",
    "PROJECT_NAME",
    "CREATED_TIMESTAMP",
    "UPDATED_TIMESTAMP",
    "RESOLUTION_NAME",
    "PRIORITY_NAME",
    "LABELS",
)

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET} (
    ISSUE_KEY TEXT PRIMARY KEY,
    ASSIGNEE_DISPLAY_NAME TEXT,
    REPORTER_DISPLAY_NAME TEXT,
    ASSIGNEE_ACCOUNT_ID TEXT,
    REPORTER_ACCOUNT_ID TEXT,
    ASSIGNEE_EMAIL TEXT,
    REPORTER_EMAIL TEXT,
    INTERNAL_ISSUE_ID BIGINT,
    SUMMARY TEXT,
    DESCRIPTION TEXT,
    STATUS_NAME TEXT,
    ISSUE_TYPE_NAME TEXT,
    PROJECT_KEY TEXT,
    PROJECT_NAME TEXT,
    CREATED_TIMESTAMP TIMESTAMP,
    UPDATED_TIMESTAMP TIMESTAMP,
    RESOLUTION_NAME TEXT,
    PRIORITY_NAME TEXT,
    LABELS VARCHAR[]
);
"""

UPSERT = f"""
INSERT INTO {T_TARGET} ({",".join(COLS)}) VALUES ({",".join("?" * len(COLS))})
ON CONFLICT(ISSUE_KEY) DO UPDATE SET
  {", ".join(f"{c}=excluded.{c}" for c in COLS[1:])};
"""


# Key collection ---------------------------------------------------------------
def _all_jira_keys(conn_stg) -> Set[str]:
    rows = conn_stg.execute(
        f"""
        SELECT EXTRACTED_JIRA_KEY FROM {T_COMMIT_KEYS}
        WHERE EXTRACTED_JIRA_KEY IS NOT NULL AND EXTRACTED_JIRA_KEY <> ''
        """
    ).fetchall()
    keys = {r[0] for r in rows if r[0]}
    log.info("Found %d distinct JIRA keys", len(keys))
    return keys


# Issue fetch / enrich ---------------------------------------------------------
def _issue_row(
    conn_main: duckdb.DuckDBPyConnection,
    conn_stg: duckdb.DuckDBPyConnection,
    key: str,
) -> Tuple | None:
    blob, internal_id = None, None
    row = conn_main.execute(
        f'SELECT "FIELDS","ID" FROM "{SCHEMA_JIRA}"."ISSUES" WHERE "KEY"=?;', (key,)
    ).fetchone()
    if not row:
        return None
    blob, internal_id = row
    f = safe_json(blob)

    def _profile(acct_id: str | None) -> Tuple[str | None, str | None]:
        if not acct_id:
            return None, None
        r = conn_stg.execute(
            f"SELECT DISPLAY_NAME, EMAIL FROM {T_USER_PROF} WHERE ID=?;",
            (acct_id,),
        ).fetchone()
        return r if r else (None, None)

    assignee = f.get("assignee") or {}
    reporter = f.get("reporter") or {}

    ass_disp, ass_email = _profile(assignee.get("accountId"))
    rep_disp, rep_email = _profile(reporter.get("accountId"))

    return (
        key,
        ass_disp or assignee.get("displayName"),
        rep_disp or reporter.get("displayName"),
        assignee.get("accountId"),
        reporter.get("accountId"),
        ass_email,
        rep_email,
        int(internal_id) if internal_id else None,
        f.get("summary"),
        f.get("description"),
        (f.get("status") or {}).get("name"),
        (f.get("issuetype") or {}).get("name"),
        (f.get("project") or {}).get("key"),
        (f.get("project") or {}).get("name"),
        f.get("created"),
        f.get("updated"),
        (f.get("resolution") or {}).get("name"),
        (f.get("priority") or {}).get("name"),
        f.get("labels") if isinstance(f.get("labels"), list) else None,
    )


# Pipeline ---------------------------------------------------------------------
def _stage():
    with db_manager(STG_DB) as stg, db_manager(MAIN_DB, read_only=True) as main:
        stg.execute(DDL)
        keys = _all_jira_keys(stg)
        if not keys:
            log.info("No keys -> nothing to stage.")
            return

        rows = []
        for k in keys:
            rec = _issue_row(main, stg, k)
            if rec:
                rows.append(rec)

        if not rows:
            log.info("No issue details fetched.")
            return

        stg.executemany(UPSERT, rows)
        stg.commit()
        log.info("Upserted %d rows into %s", len(rows), T_TARGET)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging JIRA issue details -> %s", T_TARGET)
    _stage()
    log.info("Done.")


if __name__ == "__main__":
    main()
