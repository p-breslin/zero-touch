"""
Create or refresh PR_JIRA_ISSUE_LINKS in the stage DB by joining:

  - GITHUB_PRS  (has EXTRACTED_JIRA_KEY + participant roles)
  - JIRA_ISSUES (full issue details)

Primary key = (PR-internal-id, user_login, role, jira_key)
"""

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

DB_SUB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_PRS = "GITHUB_PRS"
T_JIRA = "JIRA_ISSUES"
T_LINKS = "PR_JIRA_ISSUE_LINKS"

COLS = (
    "GH_PR_INTERNAL_ID",  # 0
    "GH_REPO",
    "GH_ORG",
    "GH_USER_LOGIN",
    "GH_ROLE_IN_PR",
    "JIRA_ISSUE_KEY",  # 5
    "JIRA_ASSIGNEE_NAME",
    "JIRA_ASSIGNEE_ID",
    "JIRA_ASSIGNEE_EMAIL",
    "JIRA_REPORTER_NAME",
    "JIRA_REPORTER_ID",  # 10
    "JIRA_REPORTER_EMAIL",
    "JIRA_PROJECT_NAME",
    "JIRA_PROJECT_KEY",
    "JIRA_ISSUE_TYPE",
    "JIRA_SUMMARY",
    "JIRA_DESCRIPTION",  # 16
)

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_LINKS} (
    {COLS[0]} TEXT,
    {COLS[1]} TEXT,
    {COLS[2]} TEXT,
    {COLS[3]} TEXT,
    {COLS[4]} TEXT,
    {COLS[5]} TEXT,
    {COLS[6]} TEXT,
    {COLS[7]} TEXT,
    {COLS[8]} TEXT,
    {COLS[9]} TEXT,
    {COLS[10]} TEXT,
    {COLS[11]} TEXT,
    {COLS[12]} TEXT,
    {COLS[13]} TEXT,
    {COLS[14]} TEXT,
    {COLS[15]} TEXT,
    {COLS[16]} TEXT,
    PRIMARY KEY ({COLS[0]}, {COLS[3]}, {COLS[4]}, {COLS[5]})
);
"""

SELECT_LINK_ROWS = f"""
WITH src AS (
    SELECT
        prs.INTERNAL_ID            AS {COLS[0]},
        prs.REPO                   AS {COLS[1]},
        prs.ORG                    AS {COLS[2]},
        prs.USER_LOGIN             AS {COLS[3]},
        prs.ROLE_IN_PR             AS {COLS[4]},
        prs.EXTRACTED_JIRA_KEY     AS {COLS[5]},
        ji.ASSIGNEE_DISPLAY_NAME   AS {COLS[6]},
        ji.ASSIGNEE_ACCOUNT_ID     AS {COLS[7]},
        ji.ASSIGNEE_EMAIL          AS {COLS[8]},
        ji.REPORTER_DISPLAY_NAME   AS {COLS[9]},
        ji.REPORTER_ACCOUNT_ID     AS {COLS[10]},
        ji.REPORTER_EMAIL          AS {COLS[11]},
        ji.PROJECT_NAME            AS {COLS[12]},
        ji.PROJECT_KEY             AS {COLS[13]},
        ji.ISSUE_TYPE_NAME         AS {COLS[14]},
        ji.SUMMARY                 AS {COLS[15]},
        ji.DESCRIPTION             AS {COLS[16]},
        row_number() OVER (PARTITION BY ji.ISSUE_KEY
                           ORDER BY prs.INTERNAL_ID) AS rn
    FROM   {T_PRS} prs
    LEFT  JOIN {T_JIRA} ji
           ON prs.EXTRACTED_JIRA_KEY = ji.ISSUE_KEY
    WHERE  prs.EXTRACTED_JIRA_KEY IS NOT NULL
      AND  prs.EXTRACTED_JIRA_KEY <> ''
      AND  ji.ASSIGNEE_ACCOUNT_ID IS NOT NULL
      AND  ji.ASSIGNEE_ACCOUNT_ID = ji.REPORTER_ACCOUNT_ID
)
SELECT {", ".join(f"{c}" for c in COLS)}
FROM   src
WHERE  rn = 1
"""

# Build SET clause excluding PK columns ----------------------------------------
_pk_idx = {0, 3, 4, 5}
UPDATE_SET = ", ".join(
    f"{COLS[i]} = excluded.{COLS[i]}" for i in range(len(COLS)) if i not in _pk_idx
)

UPSERT = f"""
INSERT INTO {T_LINKS}
{SELECT_LINK_ROWS}
ON CONFLICT ({COLS[0]}, {COLS[3]}, {COLS[4]}, {COLS[5]})
DO UPDATE SET
    {UPDATE_SET};
"""


# Pipeline ---------------------------------------------------------------------
def stage_pr_jira_links() -> None:
    with db_manager(DB_SUB) as conn:
        conn.execute(DDL)

        before = conn.execute(f"SELECT COUNT(*) FROM {T_LINKS};").fetchone()[0]

        log.info("Populating %s …", T_LINKS)
        conn.execute(UPSERT)
        conn.commit()

        after = conn.execute(f"SELECT COUNT(*) FROM {T_LINKS};").fetchone()[0]

    log.info("Upserted %d rows; table now has %d", after - before, after)


# Entry point ------------------------------------------------------------------
if __name__ == "__main__":
    stage_pr_jira_links()
