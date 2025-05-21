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

DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
T_LINKS = "PR_JIRA_ISSUE_LINKS"
T_ATTR = "PERSON_ATTRIBUTES"

COLS = (
    "ASSIGNEE_JIRA_ACCOUNT_ID",
    "NAME",
    "EMAIL",
    "ASSOCIATED_JIRA_PROJECT_NAMES",  # array
    "ASSOCIATED_GITHUB_REPOS",  # array
    "ROLE",
)

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_ATTR} (
    {COLS[0]} TEXT PRIMARY KEY,
    {COLS[1]} TEXT,
    {COLS[2]} TEXT,
    {COLS[3]} VARCHAR[],
    {COLS[4]} VARCHAR[],
    {COLS[5]} TEXT
);
"""

SELECT_AGG = f"""
WITH acts AS (
  SELECT
    JIRA_ASSIGNEE_ID         AS id,
    NULLIF(TRIM(JIRA_ASSIGNEE_NAME ),'') AS name,
    NULLIF(TRIM(JIRA_ASSIGNEE_EMAIL),'') AS email,
    NULLIF(TRIM(JIRA_PROJECT_NAME  ),'') AS project,
    NULLIF(TRIM(GH_REPO           ),'') AS repo
  FROM {T_LINKS}
  WHERE JIRA_ASSIGNEE_ID IS NOT NULL
), agg AS (
    SELECT
        id,
        MIN(name )  FILTER (WHERE name  IS NOT NULL) AS name,
        MIN(email)  FILTER (WHERE email IS NOT NULL) AS email,
        list_distinct(list(project) FILTER (WHERE project IS NOT NULL)) AS projects,
        list_distinct(list(repo)    FILTER (WHERE repo    IS NOT NULL)) AS repos
    FROM acts
    GROUP BY id
)
SELECT id, name, email, projects, repos, NULL AS role
FROM   agg
"""

UPDATE_SET = ", ".join(f"{c}=excluded.{c}" for c in COLS[1:5])

UPSERT = f"""
INSERT INTO {T_ATTR} ({",".join(COLS)})
{SELECT_AGG}
ON CONFLICT ({COLS[0]}) DO UPDATE SET {UPDATE_SET};
"""


# Pipeline ---------------------------------------------------------------------
def run() -> None:
    with db_manager(DB) as conn:
        conn.execute(DDL)
        conn.execute(
            f'ALTER TABLE {T_ATTR} ADD COLUMN IF NOT EXISTS "{COLS[3]}" VARCHAR[];'
        )
        conn.execute(
            f'ALTER TABLE {T_ATTR} ADD COLUMN IF NOT EXISTS "{COLS[4]}" VARCHAR[];'
        )
        before = conn.execute(f"SELECT COUNT(*) FROM {T_ATTR};").fetchone()[0]
        log.info("Upserting PERSON_ATTRIBUTES ...")
        conn.execute(UPSERT)
        conn.commit()
        after = conn.execute(f"SELECT COUNT(*) FROM {T_ATTR};").fetchone()[0]
    log.info("PERSON_ATTRIBUTES now %d rows (delta %d)", after, after - before)


if __name__ == "__main__":
    run()
