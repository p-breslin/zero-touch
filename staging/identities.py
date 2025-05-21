"""
Populate PERSON_ATTRIBUTES (stage DB) with one row per JIRA assignee:
  - NAME:   first non-null assignee name (alpha)
  - EMAIL:  first non-null assignee email (alpha)
  - TEAM:   first non-null JIRA project name (alpha)
  - REPO:   first non-null GitHub repo (alpha)
  - ROLE:   left NULL (filled later by role-inference script)

Source = PR_JIRA_ISSUE_LINKS
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

T_LINKS = "PR_JIRA_ISSUE_LINKS"
T_ATTR = "PERSON_ATTRIBUTES"

COLS = (
    "ASSIGNEE_JIRA_ACCOUNT_ID",  # PK 0
    "NAME",
    "EMAIL",
    "TEAM",
    "REPO",
    "ROLE",
)

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_ATTR} (
    {COLS[0]} TEXT PRIMARY KEY,
    {COLS[1]} TEXT,
    {COLS[2]} TEXT,
    {COLS[3]} TEXT,
    {COLS[4]} TEXT,
    {COLS[5]} TEXT
);
"""

# Use MIN() (alphabetic first) on NULL-stripped values
SELECT_AGG = f"""
WITH acts AS (
    SELECT
        JIRA_ASSIGNEE_ID    AS id,
        NULLIF(TRIM(JIRA_ASSIGNEE_NAME ) ,'') AS name,
        NULLIF(TRIM(JIRA_ASSIGNEE_EMAIL),'') AS email,
        NULLIF(TRIM(JIRA_PROJECT_NAME  ),'') AS project,
        NULLIF(TRIM(GH_REPO           ),'') AS repo
    FROM {T_LINKS}
    WHERE JIRA_ASSIGNEE_ID IS NOT NULL AND JIRA_ASSIGNEE_ID <> ''
), agg AS (
    SELECT
        id,
        MIN(name   ) FILTER (WHERE name   IS NOT NULL) AS name,
        MIN(email  ) FILTER (WHERE email  IS NOT NULL) AS email,
        MIN(project) FILTER (WHERE project IS NOT NULL) AS team,
        MIN(repo   ) FILTER (WHERE repo   IS NOT NULL) AS repo
    FROM acts
    GROUP BY id
)
SELECT id, name, email, team, repo, NULL AS role
FROM   agg
"""

NON_PK_SET = ", ".join(f"{c}=excluded.{c}" for c in COLS[1:5])  # keep ROLE as-is

UPSERT = f"""
INSERT INTO {T_ATTR} ({",".join(COLS)})
{SELECT_AGG}
ON CONFLICT ({COLS[0]}) DO UPDATE SET {NON_PK_SET};
"""


# Pipeline ---------------------------------------------------------------------
def populate_person_basics() -> None:
    with db_manager(DB_SUB) as conn:
        conn.execute(DDL)
        before = conn.execute(f"SELECT COUNT(*) FROM {T_ATTR};").fetchone()[0]
        log.info("Upserting basic person attributes …")
        conn.execute(UPSERT)
        conn.commit()
        after = conn.execute(f"SELECT COUNT(*) FROM {T_ATTR};").fetchone()[0]

    log.info("PERSON_ATTRIBUTES now %d rows (∆ %d this run)", after, after - before)


# Entry point ------------------------------------------------------------------
if __name__ == "__main__":
    populate_person_basics()
