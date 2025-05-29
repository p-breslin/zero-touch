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
T_USERS = "JIRA_ACTIVE_USERS"
T_STORIES = "JIRA_STORIES"
T_TARGET = "JIRA_USER_STORIES"


# SQL block --------------------------------------------------------------------
DDL = f"""
CREATE OR REPLACE TABLE {T_TARGET} AS
WITH links AS (
    SELECT
        u.JIRA_ID,
        u.JIRA_DISPLAY_NAME,
        u.JIRA_EMAIL,
        s.ID        AS STORY_ID,
        s.KEY       AS STORY_KEY,
        s.EPIC_ID,
        s.EPIC_KEY,
        s.PROJECT_KEY,
        s.PROJECT_NAME
    FROM {T_USERS}   u
    JOIN {T_STORIES} s
      ON   u.JIRA_ID = s.REPORTER_ID
       OR  u.JIRA_ID = s.CREATOR_ID
       OR  u.JIRA_ID = s.ASSIGNEE_ID
),
aggregated AS (
    SELECT
        JIRA_ID,
        MAX(JIRA_DISPLAY_NAME)          AS JIRA_DISPLAY_NAME,
        MAX(JIRA_EMAIL)                 AS JIRA_EMAIL,
        ARRAY_AGG(DISTINCT STORY_ID)    AS STORY_IDS,
        ARRAY_AGG(DISTINCT STORY_KEY)   AS STORY_KEYS,
        ARRAY_AGG(DISTINCT EPIC_ID)     AS EPIC_IDS,
        ARRAY_AGG(DISTINCT EPIC_KEY)    AS EPIC_KEYS,
        ARRAY_AGG(DISTINCT PROJECT_KEY) AS PROJECT_KEYS,
        ARRAY_AGG(DISTINCT PROJECT_NAME)AS PROJECT_NAMES
    FROM links
    GROUP BY JIRA_ID
)
SELECT * FROM aggregated;
"""


# Entry point ------------------------------------------------------------------
def main() -> None:
    with db_manager(STG_DB) as conn:
        log.info("Building %s", T_TARGET)
        conn.execute(DDL)
        conn.commit()
        log.info("%s created / refreshed.", T_TARGET)


if __name__ == "__main__":
    main()
