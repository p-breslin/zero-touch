from __future__ import annotations
import os
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Dict, List
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging
from utils.helpers import safe_json, db_manager

"""
This script stages JIRA issue details relevant to commit activity. Its purpose is to create a readily queryable table of JIRA issues that are directly linked to code commits.

    1. Identifies unique JIRA issue keys that have been previously extracted into the GITHUB_COMMITS table in the staging database. 
    2. Fetches detailed issue information (for each key) from the main JIRA ISSUES table and cross-references these account IDs with the JIRA_USER_PROFILES table in the staging DB.
    3. This enriched information is then upserted into a JIRA_ISSUES table in the staging database.
"""


# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY_NAME = os.environ["COMPANY_NAME"]

TABLE_ISSUES = "JIRA_ISSUES"
TABLE_COMMITS = "GITHUB_COMMITS"
TABLE_JIRA_USER_PROF = "JIRA_USER_PROFILES"  # user -> email (subset)

MAIN_JIRA_SCHEMA = f"{COMPANY_NAME}_JIRA_"

READ_DB_MAIN = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB_SUB = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")


# DuckDB management
DDL_ISSUES = f"""
CREATE TABLE IF NOT EXISTS {TABLE_ISSUES} (
    issue_key              TEXT PRIMARY KEY,
    assignee_display_name  TEXT,
    reporter_display_name  TEXT,
    assignee_account_id    TEXT,
    reporter_account_id    TEXT,
    assignee_email         TEXT,
    reporter_email         TEXT,
    internal_issue_id      BIGINT
);
"""


def _ensure_issues_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(DDL_ISSUES)
    log.info("Ensured table %s exists", TABLE_ISSUES)


# data fetch
SQL_KEYS = f"""
SELECT DISTINCT extracted_issue_key
FROM "{TABLE_COMMITS}"
WHERE extracted_issue_key IS NOT NULL AND extracted_issue_key <> '';
"""


def _get_jira_keys(conn_sub: duckdb.DuckDBPyConnection) -> List[str]:
    rows = conn_sub.execute(SQL_KEYS).fetchall()
    keys = [r[0] for r in rows]
    log.info("Found %d distinct JIRA keys from %s", len(keys), TABLE_COMMITS)
    return keys


SQL_ISSUE = f"""
SELECT "FIELDS" AS fields_blob, "ID" AS internal_id
FROM "{MAIN_JIRA_SCHEMA}"."ISSUES"
WHERE "KEY" = ?;
"""

SQL_EMAIL = f"""
SELECT jira_email_address
FROM {TABLE_JIRA_USER_PROF}
WHERE jira_account_id = ?;
"""


def _fetch_issue(
    conn_main: duckdb.DuckDBPyConnection,
    conn_sub: duckdb.DuckDBPyConnection,
    key: str,
) -> Dict[str, Any] | None:
    row = conn_main.execute(SQL_ISSUE, (key,)).fetchone()
    if not row:
        log.debug("No issue with key %s", key)
        return None

    fields = safe_json(row[0])
    internal_id = int(row[1]) if row[1] is not None else None
    assignee = fields.get("assignee", {})
    reporter = fields.get("reporter", {})

    aid = assignee.get("accountId")
    rid = reporter.get("accountId")

    aid_email = conn_sub.execute(SQL_EMAIL, (aid,)).fetchone()[0] if aid else None
    rid_email = conn_sub.execute(SQL_EMAIL, (rid,)).fetchone()[0] if rid else None

    return dict(
        issue_key=key,
        assignee_display_name=assignee.get("displayName"),
        reporter_display_name=reporter.get("displayName"),
        assignee_account_id=aid,
        reporter_account_id=rid,
        assignee_email=aid_email,
        reporter_email=rid_email,
        internal_issue_id=internal_id,
    )


# insert / upsert
UPSERT_ISSUES = f"""
INSERT INTO "{TABLE_ISSUES}" (
    issue_key,
    assignee_display_name,
    reporter_display_name,
    assignee_account_id,
    reporter_account_id,
    assignee_email,
    reporter_email,
    internal_issue_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(issue_key) DO UPDATE SET
    assignee_display_name = excluded.assignee_display_name,
    reporter_display_name = excluded.reporter_display_name,
    assignee_account_id   = excluded.assignee_account_id,
    reporter_account_id   = excluded.reporter_account_id,
    assignee_email        = excluded.assignee_email,
    reporter_email        = excluded.reporter_email,
    internal_issue_id     = excluded.internal_issue_id;
"""


def _upsert_issues(
    conn_sub: duckdb.DuckDBPyConnection, records: List[Dict[str, Any]]
) -> None:
    if not records:
        log.info("No issue records to upsert.")
        return

    rows = [
        (
            r["issue_key"],
            r["assignee_display_name"],
            r["reporter_display_name"],
            r["assignee_account_id"],
            r["reporter_account_id"],
            r["assignee_email"],
            r["reporter_email"],
            r["internal_issue_id"],
        )
        for r in records
    ]
    conn_sub.executemany(UPSERT_ISSUES, rows)
    conn_sub.commit()
    log.info("Upserted %d rows into %s", len(rows), TABLE_ISSUES)


# pipeline
def _build_issue_records(keys: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    with (
        db_manager(READ_DB_MAIN, read_only=True) as conn_main,
        db_manager(WRITE_DB_SUB) as conn_sub,
    ):
        _ensure_issues_table(conn_sub)

        for idx, key in enumerate(keys, 1):
            log.debug("Processing %s (%d/%d)", key, idx, len(keys))
            rec = _fetch_issue(conn_main, conn_sub, key)
            if rec:
                out.append(rec)

        _upsert_issues(conn_sub, out)
    return out


# entry point
def main() -> None:
    with db_manager(WRITE_DB_SUB, read_only=True) as conn_sub:
        keys = _get_jira_keys(conn_sub)

    if not keys:
        log.info("Nothing to do.")
        return

    _build_issue_records(keys)


if __name__ == "__main__":
    log.info("Staging linked JIRA issues → %s", TABLE_ISSUES)
    main()
    log.info("Done.")
