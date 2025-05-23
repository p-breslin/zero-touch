from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

# SQL blocks -------------------------------------------------------------------
SQL_CREATE_LINKED_IDENTITIES = """
CREATE OR REPLACE TABLE LINKED_IDENTITIES AS
SELECT
    JIRA_CREATOR_ID,

    -- Aggregate into lists, with deduplication
    ARRAY_DISTINCT(LIST(JIRA_CREATOR_NAME)) AS creator_names,
    ARRAY_DISTINCT(LIST(JIRA_CREATOR_EMAIL)) AS creator_emails,
    ARRAY_DISTINCT(LIST(JIRA_REPORTER_ID)) AS reporter_ids,
    ARRAY_DISTINCT(LIST(JIRA_REPORTER_NAME)) AS reporter_names,
    ARRAY_DISTINCT(LIST(JIRA_REPORTER_EMAIL)) AS reporter_emails,
    ARRAY_DISTINCT(LIST(JIRA_ASSIGNEE_ID)) AS assignee_ids,
    ARRAY_DISTINCT(LIST(JIRA_ASSIGNEE_NAME)) AS assignee_names,
    ARRAY_DISTINCT(LIST(JIRA_ASSIGNEE_EMAIL)) AS assignee_emails,
    ARRAY_DISTINCT(LIST(STORY_ID)) AS story_ids,
    ARRAY_DISTINCT(LIST(STORY_KEY)) AS story_keys,
    ARRAY_DISTINCT(LIST(REPO)) AS repos,

    -- GitHub identities (commits)
    ARRAY_DISTINCT(LIST(GH_AUTHOR_ID)) AS gh_author_ids,
    ARRAY_DISTINCT(LIST(GH_AUTHOR_LOGIN)) AS gh_author_logins,
    ARRAY_DISTINCT(LIST(GH_AUTHOR_EMAIL)) AS gh_author_emails,
    ARRAY_DISTINCT(LIST(GH_COMMITTER_ID)) AS gh_committer_ids,
    ARRAY_DISTINCT(LIST(GH_COMMITTER_LOGIN)) AS gh_committer_logins,
    ARRAY_DISTINCT(LIST(GH_COMMITTER_EMAIL)) AS gh_committer_emails,

    -- GitHub identities (PRs)
    ARRAY_DISTINCT(LIST(PR_USER_ID)) AS pr_user_ids,
    ARRAY_DISTINCT(LIST(PR_USER_LOGIN)) AS pr_user_logins

FROM JIRA_GITHUB_LINKS
WHERE JIRA_CREATOR_ID IS NOT NULL 
  AND JIRA_REPORTER_ID IS NOT NULL 
  AND JIRA_ASSIGNEE_ID IS NOT NULL
GROUP BY JIRA_CREATOR_ID;
"""


# Runner -----------------------------------------------------------------------
def _execute(conn, sql: str, table: str) -> None:
    conn.execute(sql)
    n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    log.info("%s refreshed — %d rows", table, n)


def main() -> None:
    with db_manager(STG_DB) as conn:
        _execute(conn, SQL_CREATE_LINKED_IDENTITIES, "LINKED_IDENTITIES")
        log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
