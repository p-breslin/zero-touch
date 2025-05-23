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
SQL_CREATE_IDENTITIES = """
CREATE OR REPLACE TABLE IDENTITIES AS
SELECT
    NAME,
    JIRA_CREATOR_IDS,
    JIRA_REPORTER_IDS,
    JIRA_ASSIGNEE_IDS,
    GH_AUTHOR_IDS,
    GH_COMMITTER_IDS,
    PR_USER_IDS,
    GH_AUTHOR_EMAILS,
    GH_COMMITTER_EMAILS,
    JIRA_REPORTER_EMAILS,
    JIRA_CREATOR_EMAILS,
    JIRA_ASSIGNEE_EMAILS
FROM DATA_STAGING.main.MASTER_TABLE_UNIQUE;
"""

SQL_CREATE_PERSON_EPICS = """
CREATE OR REPLACE TABLE PERSON_EPICS AS
SELECT
    NAME,
    EPIC_KEYS,
    STORY_KEYS,
    PROJECT_NAMES
FROM DATA_STAGING.main.MASTER_TABLE_UNIQUE;
"""

SQL_CREATE_PERSON_ROLES = """
CREATE OR REPLACE TABLE PERSON_ROLES AS
SELECT
    MTU.NAME,
    CCS.COMMITTER_ROLE AS ROLE,
    CCS.COMMITTER_SKILLS AS SKILLS
FROM DATA_STAGING.main.MASTER_TABLE_UNIQUE MTU
LEFT JOIN COMMITTER_CODE_SUMMARIES CCS
    ON MTU.NAME = CCS.COMMITTER_NAME;
"""

SQL_CREATE_REPOS = """
CREATE OR REPLACE TABLE REPOS AS
WITH exploded AS (
    SELECT
        UNNEST(REPOS) AS REPO,
        NAME,
        UNNEST(EPIC_KEYS) AS EPIC_KEY,
        UNNEST(STORY_KEYS) AS STORY_KEY
    FROM DATA_STAGING.main.MASTER_TABLE_UNIQUE
)
SELECT
    e.REPO,
    RCS.REPO_LABEL,
    ARRAY_DISTINCT(LIST(e.EPIC_KEY)) AS EPICS,
    ARRAY_DISTINCT(LIST(e.STORY_KEY)) AS STORIES,
    ARRAY_DISTINCT(LIST(e.NAME)) AS PEOPLE
FROM exploded e
LEFT JOIN REPO_CODE_SUMMARIES RCS
    ON e.REPO = RCS.REPO
WHERE e.REPO IS NOT NULL
GROUP BY e.REPO, RCS.REPO_LABEL;
"""

SQL_CREATE_PERSON_COMBINED = """
CREATE OR REPLACE TABLE PERSON_COMBINED AS
SELECT
    MTU.NAME,
    CCS.COMMITTER_ROLE AS ROLE,
    CCS.COMMITTER_SKILLS AS SKILLS,
    MTU.EPIC_KEYS,
    MTU.STORY_KEYS,
    MTU.PROJECT_NAMES,
    MTU.REPOS
FROM DATA_STAGING.main.MASTER_TABLE_UNIQUE MTU
LEFT JOIN COMMITTER_CODE_SUMMARIES CCS
    ON MTU.NAME = CCS.COMMITTER_NAME;
"""


# Runner -----------------------------------------------------------------------
def _execute(conn, sql: str, table: str) -> None:
    conn.execute(sql)
    n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    log.info("%s refreshed — %d rows", table, n)


def main() -> None:
    with db_manager(STG_DB) as conn:
        _execute(conn, SQL_CREATE_IDENTITIES, "IDENTITIES")
        _execute(conn, SQL_CREATE_PERSON_EPICS, "PERSON_EPICS")
        _execute(conn, SQL_CREATE_PERSON_ROLES, "PERSON_ROLES")
        _execute(conn, SQL_CREATE_REPOS, "REPOS")
        _execute(conn, SQL_CREATE_PERSON_COMBINED, "PERSON_COMBINED")
        log.info("All person-derived tables created.")


if __name__ == "__main__":
    main()
