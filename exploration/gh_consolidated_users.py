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
T_COMMITS = "GITHUB_COMMITS"
T_PRS = "GITHUB_PRS"
T_TARGET = "CONSOLIDATED_GH_USERS"

# SQL block --------------------------------------------------------------------
DDL = f"""
CREATE OR REPLACE TABLE {T_TARGET} AS
WITH unioned AS (
    -- from commits: author
    SELECT
        AUTHOR_LOGIN          AS GITHUB_LOGIN,
        AUTHOR_ID             AS GITHUB_ID,
        AUTHOR_NAME           AS GITHUB_DISPLAY_NAME,
        AUTHOR_EMAIL          AS GITHUB_EMAIL
    FROM {T_COMMITS}
    WHERE AUTHOR_LOGIN IS NOT NULL

    UNION ALL
    -- from commits: committer
    SELECT
        COMMITTER_LOGIN,
        COMMITTER_ID,
        COMMITTER_NAME,
        COMMITTER_EMAIL
    FROM {T_COMMITS}
    WHERE COMMITTER_LOGIN IS NOT NULL

    UNION ALL
    -- from PR participants
    SELECT
        USER_LOGIN,
        USER_ID,
        /* PR rows don't store a name or email */
        NULL AS GITHUB_DISPLAY_NAME,
        NULL AS GITHUB_EMAIL
    FROM {T_PRS}
    WHERE USER_LOGIN IS NOT NULL
),
consolidated AS (
    SELECT
        GITHUB_LOGIN,

        -- first non-null ID (PR USER_ID fills blanks)
        COALESCE(
            MAX(GITHUB_ID) FILTER (WHERE GITHUB_ID IS NOT NULL),
            NULL
        ) AS GITHUB_ID,

        -- any non-null display name
        ANY_VALUE(GITHUB_DISPLAY_NAME) FILTER (WHERE GITHUB_DISPLAY_NAME IS NOT NULL)
            AS GITHUB_DISPLAY_NAME,

        -- first non-null email
        COALESCE(
            MAX(GITHUB_EMAIL) FILTER (WHERE GITHUB_EMAIL IS NOT NULL),
            NULL
        ) AS GITHUB_EMAIL
    FROM unioned
    GROUP BY GITHUB_LOGIN
)
SELECT
    GITHUB_ID,
    GITHUB_DISPLAY_NAME,
    GITHUB_LOGIN,
    GITHUB_EMAIL
FROM consolidated;
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
