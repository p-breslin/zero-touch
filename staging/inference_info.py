import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
Enriches developer inference data with JIRA identity and authored issue context.

Description
-----------
Augments the INFERENCE_INFO table with additional identity and issue metadata by cross-referencing MATCHED_USERS and JIRA_ISSUES. Ensures required columns exist and populates them per developer.

    1. Adds DB_ID, JIRA_ID, and JIRA_ISSUES columns to INFERENCE_INFO if missing.
    2. Removes deprecated GITHUB_DISPLAY_NAME column if present.
    3. For each GITHUB_ID in INFERENCE_INFO:
        - Retrieves corresponding DB_ID and JIRA_ID from MATCHED_USERS.
        - Collects authored issues from JIRA_ISSUES using JIRA_ID.
        - Stores the issues in a JSON structure keyed by ISSUE_KEY.
    4. Updates the corresponding record in INFERENCE_INFO with enrichment data.
"""


# Configuration ---------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
T_INFERENCE = "INFERENCE_INFO"
T_USERS = "MATCHED_USERS"
T_ISSUES = "JIRA_ISSUES"


# Main logic -------------------------------------------------------------------
def enrich_inference_info():
    with db_manager(STG_DB) as conn:
        # Add column if it doesn't exist
        conn.execute(f"ALTER TABLE {T_INFERENCE} ADD COLUMN IF NOT EXISTS DB_ID TEXT")
        conn.execute(f"ALTER TABLE {T_INFERENCE} ADD COLUMN IF NOT EXISTS JIRA_ID TEXT")
        conn.execute(
            f"ALTER TABLE {T_INFERENCE} ADD COLUMN IF NOT EXISTS JIRA_ISSUES TEXT"
        )

        # Get all GITHUB_IDs from INFERENCE_INFO
        github_users = conn.execute(f"SELECT GITHUB_ID FROM {T_INFERENCE}").fetchall()

        for (github_id,) in github_users:
            # Match to DB_ID and JIRA_ID
            matched = conn.execute(
                f"""
                SELECT DB_ID, JIRA_ID FROM {T_USERS}
                WHERE GITHUB_ID = ?
                """,
                (github_id,),
            ).fetchone()

            if not matched:
                log.warning(f"No {T_USERS} entry for GITHUB_ID: {github_id}")
                continue

            db_id, jira_id = matched

            # Get all issues authored by the JIRA_ID
            issue_rows = conn.execute(
                f"""
                SELECT ISSUE_KEY, ISSUE_TYPE_NAME, SUMMARY, DESCRIPTION, PROJECT_KEY, PROJECT_NAME
                FROM {T_ISSUES}
                WHERE ASSIGNEE_ACCOUNT_ID = ?
                """,
                (jira_id,),
            ).fetchall()

            issues_by_key = {
                row[0]: {
                    "issue_type": row[1],
                    "summary": row[2],
                    "description": row[3],
                    "project_key": row[4],
                    "project_name": row[5],
                }
                for row in issue_rows
            }

            # Update the record in INFERENCE_INFO
            conn.execute(
                f"""
                UPDATE {T_INFERENCE}
                SET DB_ID = ?, JIRA_ID = ?, JIRA_ISSUES = ?
                WHERE GITHUB_ID = ?
                """,
                (db_id, jira_id, json.dumps(issues_by_key), github_id),
            )

        conn.commit()
        log.info("INFERENCE_INFO enrichment complete.")


if __name__ == "__main__":
    enrich_inference_info()
