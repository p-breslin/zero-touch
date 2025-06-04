from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
Patches or inserts GitHub fields in MATCHED_USERS by consuming PR_USERS_JIRA.

Logic:
  1. Load all rows from PR_USERS_JIRA.
  2. For each row, check if MATCHED_USERS has a row with the same JIRA_ID.
    - If row exists; update MATCHED_USERS with data from PR.
    - If no row exists: insert a new row into MATCHED_USERS PR data
"""

# Config -----------------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")
T_USERS = "MATCHED_USERS"
T_PRS = "PR_USERS_JIRA"


def patch_or_insert_from_pr():
    with db_manager(DB_PATH) as conn:
        # 1. Load PR_USERS_JIRA
        pr_df = conn.execute(
            f"""
            SELECT
                JIRA_ID,
                JIRA_DISPLAY_NAME,
                JIRA_EMAIL,
                GITHUB_LOGIN,
                GITHUB_ID
            FROM "{T_PRS}"
            """
        ).fetchdf()

        log.info("Loaded %d rows from %s", len(pr_df), T_PRS)

        for _, pr in pr_df.iterrows():
            jira_id = pr["JIRA_ID"]
            pr_github_id = pr["GITHUB_ID"]
            pr_github_login = pr["GITHUB_LOGIN"]
            pr_jira_display = pr["JIRA_DISPLAY_NAME"]
            pr_jira_email = pr["JIRA_EMAIL"]

            # 2a. Check if MATCHED_USERS already has this JIRA_ID
            existing = conn.execute(
                f"""
                SELECT
                    GITHUB_ID,
                    GITHUB_LOGIN,
                    COALESCE(GITHUB_ID_ALIAS, [])      AS existing_alias_ids,
                    COALESCE(GITHUB_LOGIN_ALIAS, [])   AS existing_alias_logins
                FROM "{T_USERS}"
                WHERE JIRA_ID = ?;
                """,
                (jira_id,),
            ).fetchall()

            if existing:
                # Row exists; unpack fields
                gh_id_existing, gh_login_existing, alias_ids, alias_logins = existing[0]

                if gh_id_existing is None:
                    # i) Fill in missing GITHUB_ID and GITHUB_LOGIN
                    conn.execute(
                        f"""
                        UPDATE "{T_USERS}"
                        SET
                            GITHUB_ID = ?,
                            GITHUB_LOGIN = ?
                        WHERE JIRA_ID = ?;
                        """,
                        (pr_github_id, pr_github_login, jira_id),
                    )
                    log.info(
                        "Patched MATCHED_USERS: set GITHUB_ID=%s, GITHUB_LOGIN=%s for JIRA_ID=%s",
                        pr_github_id,
                        pr_github_login,
                        jira_id,
                    )
                else:
                    # ii) If existing GITHUB_ID differs and new ID not aliased
                    if gh_id_existing != pr_github_id and pr_github_id not in alias_ids:
                        # Append to GITHUB_ID_ALIAS and GITHUB_LOGIN_ALIAS
                        conn.execute(
                            f"""
                            UPDATE "{T_USERS}"
                            SET
                                GITHUB_ID_ALIAS = array_append(COALESCE(GITHUB_ID_ALIAS, []), ?),
                                GITHUB_LOGIN_ALIAS = array_append(COALESCE(GITHUB_LOGIN_ALIAS, []), ?)
                            WHERE JIRA_ID = ?;
                            """,
                            (pr_github_id, pr_github_login, jira_id),
                        )
                        log.info(
                            "Appended alias for JIRA_ID=%s: added GITHUB_ID_ALIAS=%s, GITHUB_LOGIN_ALIAS=%s",
                            jira_id,
                            pr_github_id,
                            pr_github_login,
                        )
                    else:
                        # Either same ID or already aliased -> no change
                        log.debug(
                            "No update needed for JIRA_ID=%s (existing GITHUB_ID=%s, aliases=%s)",
                            jira_id,
                            gh_id_existing,
                            alias_ids,
                        )
            else:
                # 2b. No existing MATCHED_USERS row -> INSERT new row
                conn.execute(
                    f"""
                    INSERT INTO "{T_USERS}" (
                        JIRA_ID,
                        GITHUB_ID,
                        GITHUB_ID_ALIAS,
                        JIRA_DISPLAY_NAME,
                        JIRA_EMAIL,
                        GITHUB_DISPLAY_NAME,
                        GITHUB_EMAIL,
                        GITHUB_LOGIN,
                        GITHUB_DISPLAY_NAME_ALIAS,
                        GITHUB_EMAIL_ALIAS,
                        GITHUB_LOGIN_ALIAS,
                        MATCHING_METHOD,
                        MATCH_CONFIDENCE
                    )
                    VALUES (
                        ?, ?, ARRAY[]::TEXT[], ?, ?, NULL, NULL, ?, ARRAY[]::TEXT[], ARRAY[]::TEXT[], ARRAY[]::TEXT[], 'PATCH_PR', NULL
                    );
                    """,
                    (
                        jira_id,
                        pr_github_id,
                        pr_jira_display,
                        pr_jira_email,
                        pr_github_login,
                    ),
                )
                log.info(
                    "Inserted new MATCHED_USERS row for JIRA_ID=%s with GITHUB_ID=%s, GITHUB_LOGIN=%s",
                    jira_id,
                    pr_github_id,
                    pr_github_login,
                )

        conn.commit()
        log.info("patch_or_insert_from_pr: all done.")


# Entrypoint -------------------------------------------------------------------
def main():
    patch_or_insert_from_pr()


if __name__ == "__main__":
    main()
