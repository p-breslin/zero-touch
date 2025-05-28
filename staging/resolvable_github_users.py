"""
Creates a deduplicated list of GitHub users resolvable by USER_ID. Combines user identities found in commits and pull requests to unify identities. Note that this is based on committers.

Steps
-----
1. Query unique users from GITHUB_COMMITS (using COMMITTER_*)
2. Query unique users from GITHUB_PRS (using USER_*)
3. Normalize fields and unify into single record set
4. Upsert deduplicated records into RESOLVABLE_GITHUB_USERS table
"""

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

T_TARGET = "RESOLVABLE_GITHUB_USERS"
STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET} (
    GITHUB_ID               TEXT PRIMARY KEY,
    GITHUB_DISPLAY_NAME     TEXT,
    GITHUB_EMAIL            TEXT,
    GITHUB_LOGIN            TEXT
);
"""

UPSERT = """
WITH combined AS (
    SELECT
        COMMITTER_ID AS GITHUB_ID,
        COMMITTER_NAME AS GITHUB_DISPLAY_NAME,
        COMMITTER_EMAIL AS GITHUB_EMAIL,
        COMMITTER_LOGIN AS GITHUB_LOGIN
    FROM DATA_STAGING.main.GITHUB_COMMITS
    WHERE COMMITTER_ID IS NOT NULL

    UNION ALL

    SELECT
        USER_ID AS GITHUB_ID,
        NULL AS GITHUB_DISPLAY_NAME,
        NULL AS GITHUB_EMAIL,
        USER_LOGIN AS GITHUB_LOGIN
    FROM DATA_STAGING.main.GITHUB_PRS
    WHERE USER_ID IS NOT NULL
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY GITHUB_ID
               ORDER BY GITHUB_DISPLAY_NAME NULLS LAST, GITHUB_EMAIL NULLS LAST
           ) AS rk
    FROM combined
)
INSERT INTO RESOLVABLE_GITHUB_USERS (
    GITHUB_ID,
    GITHUB_DISPLAY_NAME,
    GITHUB_EMAIL,
    GITHUB_LOGIN
)
SELECT
    GITHUB_ID,
    GITHUB_DISPLAY_NAME,
    GITHUB_EMAIL,
    GITHUB_LOGIN
FROM ranked
WHERE rk = 1
ON CONFLICT (GITHUB_ID) DO UPDATE SET
    GITHUB_DISPLAY_NAME  = COALESCE(excluded.GITHUB_DISPLAY_NAME, RESOLVABLE_GITHUB_USERS.GITHUB_DISPLAY_NAME),
    GITHUB_EMAIL = COALESCE(excluded.GITHUB_EMAIL, RESOLVABLE_GITHUB_USERS.GITHUB_EMAIL),
    GITHUB_LOGIN = COALESCE(excluded.GITHUB_LOGIN, RESOLVABLE_GITHUB_USERS.GITHUB_LOGIN);
"""


# Pipeline ---------------------------------------------------------------------
def _stage():
    with db_manager(STG_DB) as conn:
        conn.execute(DDL)
        conn.execute(UPSERT)
        conn.commit()
        log.info("Upserted resolvable GitHub users into %s", T_TARGET)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Creating resolvable GitHub user view -> %s", T_TARGET)
    _stage()
    log.info("Done.")


if __name__ == "__main__":
    main()
