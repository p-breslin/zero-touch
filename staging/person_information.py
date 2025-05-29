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

STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
T_INFERENCE = "DEVELOPER_INFERENCE"
T_INTERACTIONS = "ALL_INTERACTIONS"
T_PERSON = "PERSON_INFORMATION"

# SQL block --------------------------------------------------------------------
DDL = f"""
CREATE OR REPLACE TABLE {T_PERSON} AS
WITH  -- collect artefacts the person touched
artefacts AS (
    SELECT
        GITHUB_ID,
        ARRAY_AGG(DISTINCT STORY_KEY)     AS STORY_KEYS,
        ARRAY_AGG(DISTINCT STORY_ID)      AS STORY_IDS,
        ARRAY_AGG(DISTINCT EPIC_KEY)      AS EPIC_KEYS,
        ARRAY_AGG(DISTINCT EPIC_ID)       AS EPIC_IDS,
        ARRAY_AGG(DISTINCT PROJECT_KEY)   AS PROJECT_KEYS,
        ARRAY_AGG(DISTINCT PROJECT_NAME)  AS PROJECT_NAMES,
        ARRAY_AGG(DISTINCT REPO)          AS REPOS
    FROM {T_INTERACTIONS}
    GROUP BY GITHUB_ID
),

-- one record of basic identity pulled from interactions
identity_meta AS (
    SELECT
        GITHUB_ID,
        MAX(GITHUB_DISPLAY_NAME) AS GITHUB_DISPLAY_NAME,
        MAX(GITHUB_EMAIL)        AS GITHUB_EMAIL,
        MAX(GITHUB_LOGIN)        AS GITHUB_LOGIN,
        MAX(JIRA_ID)             AS JIRA_ID,
        MAX(JIRA_DISPLAY_NAME)   AS JIRA_DISPLAY_NAME,
        MAX(JIRA_EMAIL)          AS JIRA_EMAIL
    FROM {T_INTERACTIONS}
    GROUP BY GITHUB_ID
)

SELECT
    d.COMMITTER_ID                                   AS GITHUB_ID,

    /* fall-back to COMMITTER_NAME if no interaction record */
    COALESCE(im.GITHUB_DISPLAY_NAME, d.COMMITTER_NAME) AS GITHUB_DISPLAY_NAME,
    im.GITHUB_EMAIL,
    im.GITHUB_LOGIN,

    im.JIRA_ID,
    im.JIRA_DISPLAY_NAME,
    im.JIRA_EMAIL,

    d.ROLE,
    d.EXPERIENCE_LEVEL,
    d.SKILLS,

    /* ensure arrays are never NULL */
    COALESCE(a.STORY_KEYS,     ARRAY[]::TEXT[]) AS STORY_KEYS,
    COALESCE(a.STORY_IDS,      ARRAY[]::TEXT[]) AS STORY_IDS,
    COALESCE(a.EPIC_KEYS,      ARRAY[]::TEXT[]) AS EPIC_KEYS,
    COALESCE(a.EPIC_IDS,       ARRAY[]::TEXT[]) AS EPIC_IDS,
    COALESCE(a.PROJECT_KEYS,   ARRAY[]::TEXT[]) AS PROJECT_KEYS,
    COALESCE(a.PROJECT_NAMES,  ARRAY[]::TEXT[]) AS PROJECT_NAMES,
    COALESCE(a.REPOS,          ARRAY[]::TEXT[]) AS REPOS

FROM {T_INFERENCE}      d
LEFT JOIN identity_meta im ON im.GITHUB_ID = d.COMMITTER_ID
LEFT JOIN artefacts     a  ON a.GITHUB_ID  = d.COMMITTER_ID;
"""


# Entry point ------------------------------------------------------------------
def main() -> None:
    with db_manager(STG_DB) as conn:
        log.info("Creating aggregated %s table", T_PERSON)
        conn.execute(DDL)
        conn.commit()
        log.info("Table %s refreshed (one row per developer).", T_PERSON)


if __name__ == "__main__":
    main()
