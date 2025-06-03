from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
Patches missing GitHub fields in MATCHED_USERS using PR_USERS_JIRA via JIRA_ID.

Description
-----------
Updates the `MATCHED_USERS` table by filling in `GITHUB_ID` and `GITHUB_LOGIN` fields for users whose GitHub identity is missing but whose `JIRA_ID` matches a record in the `PR_USERS_JIRA` table.

    1. Finds users in MATCHED_USERS with NULL `GITHUB_ID` and known `JIRA_ID`.
    2. Joins on `JIRA_ID` with PR_USERS_JIRA to retrieve GitHub identifiers.
    3. Updates `GITHUB_ID` and `GITHUB_LOGIN` fields in MATCHED_USERS from the matching records.

Ensures more complete cross-system identity resolution by backfilling GitHub metadata for JIRA-linked users.
"""

# Config -----------------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")
T_USERS = "MATCHED_USERS"
T_PRS = "PR_USERS_JIRA"


# Update logic -----------------------------------------------------------------
def patch_github_fields():
    with db_manager(DB_PATH) as conn:
        log.info(
            "Checking how many MATCHED_USERS rows need patching via PR_USERS_JIRA..."
        )

        # Count patchable rows
        count = conn.execute(f"""
            SELECT COUNT(*) FROM "{T_USERS}" u
            JOIN "{T_PRS}" j ON u.JIRA_ID = j.JIRA_ID
            WHERE u.GITHUB_ID IS NULL
        """).fetchone()[0]
        log.info("Found %d users to patch.", count)

        conn.execute(f"""
            UPDATE "{T_USERS}"
            SET
                GITHUB_ID = j.GITHUB_ID,
                GITHUB_LOGIN = j.GITHUB_LOGIN
            FROM "{T_PRS}" j
            WHERE
                "{T_USERS}".JIRA_ID = j.JIRA_ID
                AND "{T_USERS}".GITHUB_ID IS NULL;
        """).fetchone()

        conn.commit()
        log.info("Patch complete.")


# Entrypoint -------------------------------------------------------------------
def main():
    patch_github_fields()


if __name__ == "__main__":
    main()
