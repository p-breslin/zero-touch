from __future__ import annotations
import os
import logging
import datetime
from jira import JIRA
from pathlib import Path
from typing import List, Tuple
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_TARGET = "JIRA_ISSUES"
COMPANY = os.getenv("COMPANY_NAME")
STG_DB = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")


# DDL --------------------------------------------------------------------------
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


# JIRA API helper --------------------------------------------------------------
def _connect_jira() -> JIRA:
    """Reads JIRA server, username, token from env and returns a JIRA client."""
    server = os.getenv("JIRA_SERVER_URL")
    user = os.getenv("JIRA_USERNAME")
    token = os.getenv("JIRA_TOKEN")
    if not server or not user or not token:
        log.error(
            "Missing JIRA_SERVER_URL, JIRA_USERNAME, or JIRA_TOKEN in environment."
        )
        raise RuntimeError("JIRA credentials not set")
    return JIRA(server=server, basic_auth=(user, token))


def _fetch_issues_since(
    jira_client: JIRA, days: int = 90, batch_size: int = 100
) -> List[dict]:
    """
    Return a list of all issues updated in the last `days` days. Pages through JIRA using JQL "updated >= -{days}d".
    """
    jql = f"updated >= -{days}d ORDER BY updated ASC"
    start_at = 0
    all_issues: List[dict] = []

    while True:
        chunk = jira_client.search_issues(
            jql_str=jql,
            startAt=start_at,
            maxResults=batch_size,
            fields=[
                "summary",
                "description",
                "assignee",
                "reporter",
                "status",
                "issuetype",
                "project",
                "created",
                "updated",
                "resolution",
                "priority",
                "labels",
            ],
            expand=[],
        )
        if not chunk:
            break

        all_issues.extend(chunk)
        fetched = len(chunk)
        log.info("Fetched %d issues (start_at=%d).", fetched, start_at)
        if fetched < batch_size:
            break
        start_at += fetched

    log.info("Total issues fetched: %d", len(all_issues))
    return all_issues


def _issue_to_row(issue) -> Tuple:
    """Turn a jira.Issue into a tuple matching COLS order."""
    f = issue.fields

    # Helper to safely extract nested user fields:
    def _user_info(user_obj) -> Tuple[str | None, str | None, str | None]:
        """
        Returns (displayName, accountId, emailAddress) or (None, None, None) if no user.
        """
        if not user_obj:
            return None, None, None
        disp = getattr(user_obj, "displayName", None)
        acct = getattr(user_obj, "accountId", None)
        email = getattr(user_obj, "emailAddress", None)
        return disp, acct, email

    assignee_disp, assignee_acc, assignee_email = _user_info(f.assignee)
    reporter_disp, reporter_acc, reporter_email = _user_info(f.reporter)

    labels = f.labels if isinstance(f.labels, list) else None

    return (
        issue.key,  # ISSUE_KEY
        assignee_disp,  # ASSIGNEE_DISPLAY_NAME
        reporter_disp,  # REPORTER_DISPLAY_NAME
        assignee_acc,  # ASSIGNEE_ACCOUNT_ID
        reporter_acc,  # REPORTER_ACCOUNT_ID
        assignee_email,  # ASSIGNEE_EMAIL
        reporter_email,  # REPORTER_EMAIL
        int(issue.id) if issue.id else None,  # INTERNAL_ISSUE_ID
        f.summary,  # SUMMARY
        f.description,  # DESCRIPTION
        getattr(f.status, "name", None),  # STATUS_NAME
        getattr(f.issuetype, "name", None),  # ISSUE_TYPE_NAME
        getattr(f.project, "key", None),  # PROJECT_KEY
        getattr(f.project, "name", None),  # PROJECT_NAME
        _to_datetime(f.created),  # CREATED_TIMESTAMP
        _to_datetime(f.updated),  # UPDATED_TIMESTAMP
        getattr(f.resolution, "name", None),  # RESOLUTION_NAME
        getattr(f.priority, "name", None),  # PRIORITY_NAME
        labels,  # LABELS
    )


def _to_datetime(datestr: str | None) -> datetime.datetime | None:
    """
    Convert JIRA ISO-8601 string (e.g. '2025-05-10T14:20:30.000+0000')
    into a Python datetime object. Return None if datestr is None.
    """
    if not datestr:
        return None
    try:
        # strip timezone offset for simplicity; DuckDB will interpret as UTC
        # if you need exact timezone, consider dateutil.parser.parse(...)
        return datetime.datetime.fromisoformat(datestr.replace("Z", "+00:00"))
    except ValueError:
        # fallback: let DuckDB parse string automatically
        return None


# Main pipeline ----------------------------------------------------------------
def main():
    log.info("Starting live JIRA issue pull (last 90 days) -> %s", T_TARGET)
    jira_client = _connect_jira()

    # 1) Fetch from JIRA API
    issues = _fetch_issues_since(jira_client, days=90)

    # 2) Transform into rows
    rows: List[Tuple] = []
    for issue in issues:
        try:
            rec = _issue_to_row(issue)
            rows.append(rec)
        except Exception as e:
            log.warning("Skipping issue %s due to parsing error: %s", issue.key, e)

    if not rows:
        log.info("No JIRA issues found in the last 90 days -> nothing to upsert.")
        return

    # 3) Upsert into DuckDB
    with db_manager(STG_DB) as stg:
        stg.execute(DDL)
        stg.executemany(UPSERT, rows)
        stg.commit()
        log.info("Upserted %d rows into %s", len(rows), T_TARGET)

    log.info("Finished staging JIRA issue details.")


if __name__ == "__main__":
    main()
