"""
Populates GITHUB_RESOLVABLE_USERS (staging DB) with all distinct author and committer identities from GITHUB_COMMITS. This prepares a unique reference table of GitHub identities for downstream mapping and enrichment.

Steps
-----
1. Read all rows from GITHUB_COMMITS.
2. Extract distinct author and committer IDs and identity fields.
3. Insert into GITHUB_RESOLVABLE_USERS (ON CONFLICT DO NOTHING).
"""

from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_GH = f"{COMPANY}_GITHUB_"

STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
T_COMMITS = "GITHUB_COMMITS"
T_USERS = "GITHUB_RESOLVABLE_USERS"

# DDL
DDL_USERS = f"""
CREATE TABLE IF NOT EXISTS {T_USERS} (
    ID          TEXT PRIMARY KEY,
    NAME        TEXT,
    EMAIL       TEXT,
    LOGIN       TEXT
);
"""


# Helpers ----------------------------------------------------------------------
def _distinct_identities(conn) -> List[Tuple[str, str, str, str]]:
    """Return unique identities from author and committer columns."""
    q = f"""
        WITH identities AS (
            SELECT DISTINCT
                AUTHOR_ID     AS ID,
                AUTHOR_NAME   AS NAME,
                AUTHOR_EMAIL  AS EMAIL,
                AUTHOR_LOGIN  AS LOGIN
            FROM {T_COMMITS}
            WHERE AUTHOR_ID IS NOT NULL

            UNION

            SELECT DISTINCT
                COMMITTER_ID     AS ID,
                COMMITTER_NAME   AS NAME,
                COMMITTER_EMAIL  AS EMAIL,
                COMMITTER_LOGIN  AS LOGIN
            FROM {T_COMMITS}
            WHERE COMMITTER_ID IS NOT NULL
        )
        SELECT * FROM identities;
    """
    return conn.execute(q).fetchall()


# Insert -----------------------------------------------------------------------
def _insert(records: List[Tuple[str, str, str, str]]):
    if not records:
        log.info("No user records to insert.")
        return

    with db_manager(STG_DB) as conn:
        conn.execute(DDL_USERS)
        conn.executemany(
            f"""
            INSERT INTO {T_USERS} (ID, NAME, EMAIL, LOGIN)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (ID) DO NOTHING;
            """,
            records,
        )
        conn.commit()
        log.info(
            "Inserted %d distinct GitHub user records into %s.", len(records), T_USERS
        )


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging distinct GitHub user identities from %s.", T_COMMITS)

    with db_manager(STG_DB, read_only=True) as conn:
        identities = _distinct_identities(conn)

    _insert(identities)
    log.info("Done staging GitHub users.")


if __name__ == "__main__":
    main()
