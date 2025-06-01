from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging
from datetime import datetime, timedelta, timezone

"""
Stages Jira stories and links them to GitHub activity for cross-platform analysis.

Description
-----------
Extracts recent Jira stories and matches them to GitHub commits and pull requests using embedded Jira keys. Populates two staging tables: JIRA_STORIES for raw story metadata, and JIRA_GITHUB for enriched, joined records.

    1. Loads stories updated within the last 90 days from the ISSUES_SUMMARY_STATS source table.
    2. Writes structured story data to the JIRA_STORIES table, skipping duplicates.
    3. Joins stories to GitHub commits and PRs based on EXTRACTED_JIRA_KEY fields.
    4. Writes joined records to the JIRA_GITHUB table with contributor metadata from both systems.
"""


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_JIRA = f"{COMPANY}_JIRA_"

SRC_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_NAME')}.duckdb")
STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")

T_SRC = "ISSUES_SUMMARY_STATS"
T_STORIES = "JIRA_STORIES"
T_LINKS = "JIRA_GITHUB"

DAYS_BACK = 90

# JIRA_STORIES DDL -------------------------------------------------------------
DDL_STORIES = f"""
CREATE TABLE IF NOT EXISTS {T_STORIES} (
    ID              TEXT PRIMARY KEY,
    KEY             TEXT,
    EPIC_ID         TEXT,
    EPIC_KEY        TEXT,
    PROJECT_KEY     TEXT,
    PROJECT_NAME    TEXT,
    ISSUE_TYPE_NAME TEXT,
    CREATED_DATE    TIMESTAMP,
    UPDATED_DATE    TIMESTAMP,
    REPORTER_ID     TEXT,
    REPORTER_NAME   TEXT,
    REPORTER_EMAIL  TEXT,
    CREATOR_ID      TEXT,
    CREATOR_NAME    TEXT,
    CREATOR_EMAIL   TEXT,
    ASSIGNEE_ID     TEXT,
    ASSIGNEE_NAME   TEXT,
    ASSIGNEE_EMAIL  TEXT,
    SUMMARY         TEXT
);
"""

_COLS = (
    "ID",
    "KEY",
    "EPIC_ID",
    "EPIC_KEY",
    "PROJECT_KEY",
    "PROJECT_NAME",
    "ISSUE_TYPE_NAME",
    "CREATED_DATE",
    "UPDATED_DATE",
    "REPORTER_ID",
    "REPORTER_NAME",
    "REPORTER_EMAIL",
    "CREATOR_ID",
    "CREATOR_NAME",
    "CREATOR_EMAIL",
    "ASSIGNEE_ID",
    "ASSIGNEE_NAME",
    "ASSIGNEE_EMAIL",
    "SUMMARY",
)

_SELECT_LIST = """
    s."ID",
    s."KEY",
    CASE WHEN s."PARENT_ISSUE_TYPE" = 'Epic' THEN s."PARENT_ID"  END AS "EPIC_ID",
    CASE WHEN s."PARENT_ISSUE_TYPE" = 'Epic' THEN s."PARENT_KEY" END AS "EPIC_KEY",
    s."PROJECT_KEY",
    s."PROJECT_NAME",
    s."ISSUE_TYPE_NAME",
    s."CREATED_DATE",
    s."UPDATED_DATE",
    s."REPORTER_ID",
    s."REPORTER_NAME",
    s."REPORTER_EMAIL",
    s."CREATOR_ID",
    s."CREATOR_NAME",
    s."CREATOR_EMAIL",
    s."ASSIGNEE_ID",
    s."ASSIGNEE_NAME",
    s."ASSIGNEE_EMAIL",
    s."SUMMARY"
"""

# JIRA_GITHUB SQL --------------------------------------------------------------
SQL_CREATE_JIRA_GITHUB = f"""
CREATE OR REPLACE TABLE {T_LINKS} AS
SELECT
    JS.ID AS STORY_ID,
    JS.KEY AS STORY_KEY,
    JS.REPORTER_ID AS JIRA_REPORTER_ID,
    JS.REPORTER_NAME AS JIRA_REPORTER_NAME,
    JS.REPORTER_EMAIL AS JIRA_REPORTER_EMAIL,
    JS.CREATOR_ID AS JIRA_CREATOR_ID,
    JS.CREATOR_NAME AS JIRA_CREATOR_NAME,
    JS.CREATOR_EMAIL AS JIRA_CREATOR_EMAIL,
    JS.ASSIGNEE_ID AS JIRA_ASSIGNEE_ID,
    JS.ASSIGNEE_NAME AS JIRA_ASSIGNEE_NAME,
    JS.ASSIGNEE_EMAIL AS JIRA_ASSIGNEE_EMAIL,

    JS.* EXCLUDE (
        ID,
        KEY,
        REPORTER_ID, REPORTER_NAME, REPORTER_EMAIL,
        CREATOR_ID, CREATOR_NAME, CREATOR_EMAIL,
        ASSIGNEE_ID, ASSIGNEE_NAME, ASSIGNEE_EMAIL,
        ISSUE_TYPE_NAME, CREATED_DATE, UPDATED_DATE, SUMMARY
    ),

    COALESCE(GC.REPO, GP.REPO) AS REPO,

    GC.AUTHOR_ID AS GH_AUTHOR_ID,
    GC.AUTHOR_NAME AS GH_AUTHOR_NAME,
    GC.AUTHOR_EMAIL AS GH_AUTHOR_EMAIL,
    GC.AUTHOR_LOGIN AS GH_AUTHOR_LOGIN,
    GC.COMMITTER_ID AS GH_COMMITTER_ID,
    GC.COMMITTER_NAME AS GH_COMMITTER_NAME,
    GC.COMMITTER_EMAIL AS GH_COMMITTER_EMAIL,
    GC.COMMITTER_LOGIN AS GH_COMMITTER_LOGIN,

    GP.USER_ID AS PR_USER_ID,
    GP.USER_LOGIN AS PR_USER_LOGIN

FROM JIRA_STORIES JS

LEFT JOIN GITHUB_COMMITS GC
    ON JS.KEY = GC.EXTRACTED_JIRA_KEY
    AND GC.EXTRACTED_JIRA_KEY IS NOT NULL AND GC.EXTRACTED_JIRA_KEY != ''

LEFT JOIN GITHUB_PRS GP
    ON JS.KEY = GP.EXTRACTED_JIRA_KEY
    AND GP.EXTRACTED_JIRA_KEY IS NOT NULL AND GP.EXTRACTED_JIRA_KEY != ''

WHERE JS.KEY IN (
    SELECT EXTRACTED_JIRA_KEY FROM GITHUB_COMMITS
    WHERE EXTRACTED_JIRA_KEY IS NOT NULL AND EXTRACTED_JIRA_KEY != ''
    UNION
    SELECT EXTRACTED_JIRA_KEY FROM GITHUB_PRS
    WHERE EXTRACTED_JIRA_KEY IS NOT NULL AND EXTRACTED_JIRA_KEY != ''
);
"""


# Logic ------------------------------------------------------------------------
def _stage_jira_stories() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    with db_manager(SRC_DB, read_only=True) as src:
        rows = src.execute(
            f"""
            SELECT {_SELECT_LIST}
            FROM   "{SCHEMA_JIRA}"."{T_SRC}" s
            WHERE  s."ISSUE_TYPE_NAME" = 'Story'
              AND  s."UPDATED_DATE" >= ?
            ORDER  BY s."UPDATED_DATE" DESC
            """,
            (cutoff,),
        ).fetchall()

    if not rows:
        log.info("No Jira stories found in the last %d days.", DAYS_BACK)
        return

    records = [dict(zip(_COLS, row)) for row in rows]

    with db_manager(STG_DB) as conn:
        conn.execute(DDL_STORIES)
        conn.executemany(
            f"""INSERT INTO "{T_STORIES}" VALUES ({",".join("?" * len(_COLS))})
                ON CONFLICT (ID) DO NOTHING;""",
            [tuple(r[c] for c in _COLS) for r in records],
        )
        conn.commit()
        log.info("Inserted %d rows into %s", len(records), T_STORIES)


def _stage_jira_github_links() -> None:
    with db_manager(STG_DB) as conn:
        conn.execute(SQL_CREATE_JIRA_GITHUB)
        n = conn.execute(f'SELECT COUNT(*) FROM "{T_LINKS}"').fetchone()[0]
        log.info("%s refreshed — %d rows", T_LINKS, n)


# Main -------------------------------------------------------------------------
def main():
    log.info("Staging Jira stories for GitHub linking...")
    _stage_jira_stories()

    log.info("Staging Jira-GitHub links...")
    _stage_jira_github_links()

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
