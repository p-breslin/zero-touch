"""
Stage Jira Storie issues that were updated in the last N days and attach its Epic (ID, KEY).
    - Source table (primary DB): <COMPANY>_JIRA_.ISSUES_SUMMARY_STATS
    - Target table (staging DB): JIRA_STORIES

Logic
-----
Source:  <COMPANY>_JIRA_.ISSUES_SUMMARY_STATS
Filter:  ISSUE_TYPE_NAME = 'Story' AND UPDATED_DATE >= now-N days
For each row:
      IF PARENT_ISSUE_TYPE = 'Epic'
           EPIC_ID  = PARENT_ID
           EPIC_KEY = PARENT_KEY
      ELSE
           EPIC_ID  = NULL
           EPIC_KEY = NULL
Target:  JIRA_STORIES (staging DB) — upsert on primary key ID
"""

from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from typing import Any, Dict, List, Tuple
from utils.logging_setup import setup_logging
from datetime import datetime, timedelta, timezone


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_JIRA = f"{COMPANY}_JIRA_"

SRC_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_SRC = "ISSUES_SUMMARY_STATS"
T_TARGET = "JIRA_STORIES"

DAYS_BACK = 90

# DDL
DDL_STORIES = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET} (
    ID              TEXT PRIMARY KEY,
    KEY             TEXT,
    EPIC_ID         TEXT,
    EPIC_KEY        TEXT,
    PROJECT_KEY     TEXT,
    PROJECT_NAME    TEXT,
    ISSUE_TYPE_NAME TEXT,
    CREATED_DATE    TIMESTAMP,
    UPDATED_DATE    TIMESTAMP,
    REPORTER_ID     TEXT,
    REPORTER_NAME   TEXT,
    REPORTER_EMAIL  TEXT,
    CREATOR_ID      TEXT,
    CREATOR_NAME    TEXT,
    CREATOR_EMAIL   TEXT,
    ASSIGNEE_ID     TEXT,
    ASSIGNEE_NAME   TEXT,
    ASSIGNEE_EMAIL  TEXT,
    SUMMARY         TEXT
);
"""

_COLS = (
    "ID",
    "KEY",
    "EPIC_ID",
    "EPIC_KEY",
    "PROJECT_KEY",
    "PROJECT_NAME",
    "ISSUE_TYPE_NAME",
    "CREATED_DATE",
    "UPDATED_DATE",
    "REPORTER_ID",
    "REPORTER_NAME",
    "REPORTER_EMAIL",
    "CREATOR_ID",
    "CREATOR_NAME",
    "CREATOR_EMAIL",
    "ASSIGNEE_ID",
    "ASSIGNEE_NAME",
    "ASSIGNEE_EMAIL",
    "SUMMARY",
)

# Explicit select list with conditional Epic mapping
_SELECT_LIST = """
    s."ID",
    s."KEY",
    CASE WHEN s."PARENT_ISSUE_TYPE" = 'Epic' THEN s."PARENT_ID"  END AS "EPIC_ID",
    CASE WHEN s."PARENT_ISSUE_TYPE" = 'Epic' THEN s."PARENT_KEY" END AS "EPIC_KEY",
    s."PROJECT_KEY",
    s."PROJECT_NAME",
    s."ISSUE_TYPE_NAME",
    s."CREATED_DATE",
    s."UPDATED_DATE",
    s."REPORTER_ID",
    s."REPORTER_NAME",
    s."REPORTER_EMAIL",
    s."CREATOR_ID",
    s."CREATOR_NAME",
    s."CREATOR_EMAIL",
    s."ASSIGNEE_ID",
    s."ASSIGNEE_NAME",
    s."ASSIGNEE_EMAIL",
    s."SUMMARY"
"""


# Helpers ----------------------------------------------------------------------
def _recent_story_rows() -> List[Tuple]:
    """Return Story rows updated within the last N days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    with db_manager(SRC_DB, read_only=True) as src:
        return src.execute(
            f"""
            SELECT {_SELECT_LIST}
            FROM   "{SCHEMA_JIRA}"."{T_SRC}" s
            WHERE  s."ISSUE_TYPE_NAME" = 'Story'
              AND  s."UPDATED_DATE"    >= ?
            ORDER  BY s."UPDATED_DATE" DESC
            """,
            (cutoff,),
        ).fetchall()


# Insert -----------------------------------------------------------------------
def _insert(records: List[Dict[str, Any]]):
    if not records:
        log.info("No new stories to insert.")
        return
    with db_manager(STG_DB) as conn:
        conn.execute(DDL_STORIES)
        conn.executemany(
            f"""INSERT INTO "{T_TARGET}" VALUES ({",".join("?" * len(_COLS))})
                ON CONFLICT (ID) DO NOTHING;""",
            [tuple(r[c] for c in _COLS) for r in records],
        )
        conn.commit()
        log.info("Inserted %d rows into %s", len(records), T_TARGET)


# Entry point ------------------------------------------------------------------
def main():
    rows = _recent_story_rows()
    recs = [dict(zip(_COLS, row)) for row in rows]
    log.info(
        "Built %d Story records updated in the last %d days.", len(recs), DAYS_BACK
    )
    _insert(recs)
    log.info("Done staging Jira stories.")


if __name__ == "__main__":
    main()
