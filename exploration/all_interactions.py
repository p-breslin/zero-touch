from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
Constructs a unified table of all Jira and GitHub participants across linked stories.

Description
-----------
Generates the ALL_INTERACTIONS table by extracting role-based participants from Jira and GitHub and joining them against resolved identities. Normalizes contributor roles across platforms to support downstream analysis of collaboration patterns and user resolution status.

    1. Extracts contributors from JIRA_GITHUB by role: reporter, creator, assignee, author, committer, and PR user.
    2. Assigns each contributor a ROLE_TYPE and links them to story, epic, project, and repo context.
    3. Joins the result with RESOLVABLE_USERS to include resolved identities where available.
    4. Tags each row as RESOLVED or UNRESOLVED depending on whether identity resolution succeeded.

Removes duplicates by selecting distinct entries.
"""


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
T_USERS = "RESOLVABLE_USERS"
T_LINKS = "JIRA_GITHUB"
T_PERSON = "ALL_INTERACTIONS"


# SQL block --------------------------------------------------------------------
DDL = f"""
CREATE OR REPLACE TABLE {T_PERSON} AS
WITH role_expanded AS (
    -- JIRA roles
    SELECT DISTINCT
        JIRA_REPORTER_ID   AS JIRA_ID,
        JIRA_REPORTER_NAME AS JIRA_DISPLAY_NAME,
        JIRA_REPORTER_EMAIL AS JIRA_EMAIL,
        NULL               AS GITHUB_ID,
        NULL               AS GITHUB_DISPLAY_NAME,
        NULL               AS GITHUB_EMAIL,
        NULL               AS GITHUB_LOGIN,
        'JIRA_REPORTER'    AS ROLE_TYPE,
        STORY_ID, STORY_KEY, EPIC_ID, EPIC_KEY,
        PROJECT_KEY, PROJECT_NAME, REPO
    FROM {T_LINKS}
    WHERE JIRA_REPORTER_ID IS NOT NULL

    UNION ALL
    SELECT DISTINCT
        JIRA_CREATOR_ID,  JIRA_CREATOR_NAME,  JIRA_CREATOR_EMAIL,
        NULL, NULL, NULL, NULL,
        'JIRA_CREATOR',
        STORY_ID, STORY_KEY, EPIC_ID, EPIC_KEY,
        PROJECT_KEY, PROJECT_NAME, REPO
    FROM {T_LINKS}
    WHERE JIRA_CREATOR_ID IS NOT NULL

    UNION ALL
    SELECT DISTINCT
        JIRA_ASSIGNEE_ID, JIRA_ASSIGNEE_NAME, JIRA_ASSIGNEE_EMAIL,
        NULL, NULL, NULL, NULL,
        'JIRA_ASSIGNEE',
        STORY_ID, STORY_KEY, EPIC_ID, EPIC_KEY,
        PROJECT_KEY, PROJECT_NAME, REPO
    FROM {T_LINKS}
    WHERE JIRA_ASSIGNEE_ID IS NOT NULL

    -- GitHub roles
    UNION ALL
    SELECT DISTINCT
        NULL, NULL, NULL,
        GH_AUTHOR_ID, GH_AUTHOR_NAME, GH_AUTHOR_EMAIL, GH_AUTHOR_LOGIN,
        'GH_AUTHOR',
        STORY_ID, STORY_KEY, EPIC_ID, EPIC_KEY,
        PROJECT_KEY, PROJECT_NAME, REPO
    FROM {T_LINKS}
    WHERE GH_AUTHOR_ID IS NOT NULL

    UNION ALL
    SELECT DISTINCT
        NULL, NULL, NULL,
        GH_COMMITTER_ID, GH_COMMITTER_NAME, GH_COMMITTER_EMAIL, GH_COMMITTER_LOGIN,
        'GH_COMMITTER',
        STORY_ID, STORY_KEY, EPIC_ID, EPIC_KEY,
        PROJECT_KEY, PROJECT_NAME, REPO
    FROM {T_LINKS}
    WHERE GH_COMMITTER_ID IS NOT NULL

    UNION ALL
    SELECT DISTINCT
        NULL, NULL, NULL,
        PR_USER_ID, NULL, NULL, PR_USER_LOGIN,
        'PR_USER',
        STORY_ID, STORY_KEY, EPIC_ID, EPIC_KEY,
        PROJECT_KEY, PROJECT_NAME, REPO
    FROM {T_LINKS}
    WHERE PR_USER_ID IS NOT NULL
),
joined AS (
    SELECT
        -- Coalesce: prefer data from RESOLVABLE_USERS, else fall back to role-data
        COALESCE(u.JIRA_ID,      r.JIRA_ID)         AS JIRA_ID,
        COALESCE(u.JIRA_DISPLAY_NAME, r.JIRA_DISPLAY_NAME) AS JIRA_DISPLAY_NAME,
        COALESCE(u.JIRA_EMAIL,   r.JIRA_EMAIL)      AS JIRA_EMAIL,

        COALESCE(u.GITHUB_ID,    r.GITHUB_ID)       AS GITHUB_ID,
        COALESCE(u.GITHUB_DISPLAY_NAME, r.GITHUB_DISPLAY_NAME) AS GITHUB_DISPLAY_NAME,
        COALESCE(u.GITHUB_EMAIL, r.GITHUB_EMAIL)    AS GITHUB_EMAIL,
        COALESCE(u.GITHUB_LOGIN, r.GITHUB_LOGIN)    AS GITHUB_LOGIN,

        r.ROLE_TYPE,
        r.STORY_ID,
        r.STORY_KEY,
        r.EPIC_ID,
        r.EPIC_KEY,
        r.PROJECT_KEY,
        r.PROJECT_NAME,
        r.REPO,

        CASE
            WHEN u.JIRA_ID IS NULL AND u.GITHUB_ID IS NULL THEN 'UNRESOLVED'
            ELSE 'RESOLVED'
        END AS RESOLVE_STATUS
    FROM role_expanded r
    LEFT JOIN {T_USERS} u
      ON (u.JIRA_ID   = r.JIRA_ID   AND r.JIRA_ID   IS NOT NULL)
      OR (u.GITHUB_ID = r.GITHUB_ID AND r.GITHUB_ID IS NOT NULL)
)
SELECT DISTINCT * FROM joined;
"""


# Main logic -------------------------------------------------------------------
def main():
    with db_manager(STG_DB) as conn:
        log.info("Creating table: %s", T_PERSON)
        conn.execute(DDL)
        conn.commit()
        log.info("Table %s created successfully.", T_PERSON)


if __name__ == "__main__":
    main()
