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
SQL_CREATE_BY_JIRA_CREATOR_NAME = """
CREATE OR REPLACE TABLE BY_JIRA_CREATOR_NAME AS
SELECT
    JIRA_CREATOR_NAME,
    ARRAY_DISTINCT(LIST(STORY_ID)) AS story_ids,
    ARRAY_DISTINCT(LIST(STORY_KEY)) AS story_keys,
    ARRAY_DISTINCT(LIST(REPO)) AS repos
FROM JIRA_GITHUB_LINKS
WHERE JIRA_CREATOR_NAME IS NOT NULL
GROUP BY JIRA_CREATOR_NAME;
"""

SQL_CREATE_BY_JIRA_REPORTER_NAME = """
CREATE OR REPLACE TABLE BY_JIRA_REPORTER_NAME AS
SELECT
    JIRA_REPORTER_NAME,
    ARRAY_DISTINCT(LIST(STORY_ID)) AS story_ids,
    ARRAY_DISTINCT(LIST(STORY_KEY)) AS story_keys,
    ARRAY_DISTINCT(LIST(REPO)) AS repos
FROM JIRA_GITHUB_LINKS
WHERE JIRA_REPORTER_NAME IS NOT NULL
GROUP BY JIRA_REPORTER_NAME;
"""

SQL_CREATE_BY_JIRA_ASSIGNEE_NAME = """
CREATE OR REPLACE TABLE BY_JIRA_ASSIGNEE_NAME AS
SELECT
    JIRA_ASSIGNEE_NAME,
    ARRAY_DISTINCT(LIST(STORY_ID)) AS story_ids,
    ARRAY_DISTINCT(LIST(STORY_KEY)) AS story_keys,
    ARRAY_DISTINCT(LIST(REPO)) AS repos
FROM JIRA_GITHUB_LINKS
WHERE JIRA_ASSIGNEE_NAME IS NOT NULL
GROUP BY JIRA_ASSIGNEE_NAME;
"""


# Runner -----------------------------------------------------------------------
def _execute(conn, sql: str, table: str) -> None:
    conn.execute(sql)
    n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    log.info("%s refreshed — %d rows", table, n)


def main() -> None:
    with db_manager(STG_DB) as conn:
        _execute(conn, SQL_CREATE_BY_JIRA_CREATOR_NAME, "BY_JIRA_CREATOR_NAME")
        _execute(conn, SQL_CREATE_BY_JIRA_REPORTER_NAME, "BY_JIRA_REPORTER_NAME")
        _execute(conn, SQL_CREATE_BY_JIRA_ASSIGNEE_NAME, "BY_JIRA_ASSIGNEE_NAME")
        log.info("All identity role tables created.")


if __name__ == "__main__":
    main()
