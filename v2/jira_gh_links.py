from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging
from utils.helpers import db_manager

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

# SQL blocks -------------------------------------------------------------------
SQL_CREATE_JIRA_GITHUB_LINKS = """
CREATE OR REPLACE TABLE JIRA_GITHUB_LINKS AS
SELECT
    -- Renamed fields from JIRA_STORIES
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

    -- Include all other columns from JIRA_STORIES except excluded ones
    JS.* EXCLUDE (
        ID,
        KEY,
        REPORTER_ID, REPORTER_NAME, REPORTER_EMAIL,
        CREATOR_ID, CREATOR_NAME, CREATOR_EMAIL,
        ASSIGNEE_ID, ASSIGNEE_NAME, ASSIGNEE_EMAIL,
        ISSUE_TYPE_NAME, CREATED_DATE, UPDATED_DATE, SUMMARY
    ),

    -- Consolidated REPO column
    COALESCE(GC.REPO, GP.REPO) AS REPO,

    -- From GitHub Commits
    GC.AUTHOR_ID AS GH_AUTHOR_ID,
    GC.AUTHOR_NAME AS GH_AUTHOR_NAME,
    GC.AUTHOR_EMAIL AS GH_AUTHOR_EMAIL,
    GC.AUTHOR_LOGIN AS GH_AUTHOR_LOGIN,
    GC.COMMITTER_ID AS GH_COMMITTER_ID,
    GC.COMMITTER_NAME AS GH_COMMITTER_NAME,
    GC.COMMITTER_EMAIL AS GH_COMMITTER_EMAIL,
    GC.COMMITTER_LOGIN AS GH_COMMITTER_LOGIN,

    -- From GitHub PRs
    GP.USER_ID AS PR_USER_ID,
    GP.USER_LOGIN AS PR_USER_LOGIN

FROM JIRA_STORIES JS

LEFT JOIN GITHUB_COMMITS GC
    ON JS.KEY = GC.EXTRACTED_JIRA_KEY
    AND GC.EXTRACTED_JIRA_KEY IS NOT NULL
    AND GC.EXTRACTED_JIRA_KEY != ''

LEFT JOIN GITHUB_PRS GP
    ON JS.KEY = GP.EXTRACTED_JIRA_KEY
    AND GP.EXTRACTED_JIRA_KEY IS NOT NULL
    AND GP.EXTRACTED_JIRA_KEY != ''

WHERE JS.KEY IN (
    SELECT EXTRACTED_JIRA_KEY FROM GITHUB_COMMITS
    WHERE EXTRACTED_JIRA_KEY IS NOT NULL AND EXTRACTED_JIRA_KEY != ''
    UNION
    SELECT EXTRACTED_JIRA_KEY FROM GITHUB_PRS
    WHERE EXTRACTED_JIRA_KEY IS NOT NULL AND EXTRACTED_JIRA_KEY != ''
);
"""



# Runner -----------------------------------------------------------------------
def _execute(conn, sql: str, table: str) -> None:
    conn.execute(sql)
    n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    log.info("%s refreshed — %d rows", table, n)


def main() -> None:
    with db_manager(STG_DB) as conn:
        _execute(conn, SQL_CREATE_JIRA_GITHUB_LINKS, "JIRA_GITHUB_LINKS")
        log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
