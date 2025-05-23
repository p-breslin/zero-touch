"""
Stage Jira Epics that are referenced by any Story in JIRA_STORIES.
    - Source table (primary DB): <COMPANY>_JIRA_.ISSUES_SUMMARY_STATS
    - Target table (staging DB): JIRA_EPICS
"""

from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from typing import Dict, List, Any
from utils.helpers import db_manager
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_JIRA = f"{COMPANY}_JIRA_"

SRC_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_SRC = "ISSUES_SUMMARY_STATS"
T_TARGET = "JIRA_EPICS"

# DDL
DDL_EPICS = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET} (
    ID              TEXT PRIMARY KEY,
    KEY             TEXT,
    PROJECT_KEY     TEXT,
    PROJECT_NAME    TEXT,
    ISSUE_TYPE_NAME TEXT,
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
    "PROJECT_KEY",
    "PROJECT_NAME",
    "ISSUE_TYPE_NAME",
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
_SELECT_LIST = ", ".join(f's."{c}"' for c in _COLS)


# Record builder ---------------------------------------------------------------
def _build_records() -> List[Dict[str, Any]]:
    with db_manager(STG_DB, read_only=True) as conn:
        # Get all distinct EPIC_IDs and EPIC_KEYs from stories
        epic_refs = conn.execute(
            "SELECT DISTINCT EPIC_ID, EPIC_KEY FROM JIRA_STORIES WHERE EPIC_ID IS NOT NULL"
        ).fetchall()

    if not epic_refs:
        log.info("No epic references found in JIRA_STORIES.")
        return []

    with db_manager(SRC_DB, read_only=True) as conn:
        # Use a temp table for join
        conn.execute("CREATE TEMP TABLE tmp_epics(id TEXT, key TEXT);")
        conn.executemany("INSERT INTO tmp_epics VALUES (?, ?);", epic_refs)

        rows = conn.execute(
            f"""
            SELECT {_SELECT_LIST}
            FROM   "{SCHEMA_JIRA}"."{T_SRC}" s
            JOIN   tmp_epics e
              ON   s.ID = e.id OR s.KEY = e.key
            WHERE  s.ISSUE_TYPE_NAME = 'Epic'
            """
        ).fetchall()

        conn.execute("DROP TABLE tmp_epics;")

    records = [dict(zip(_COLS, row)) for row in rows]
    log.info("Built %d epic records based on linked stories.", len(records))
    return records


# Insert -----------------------------------------------------------------------
def _insert(records: List[Dict[str, Any]]):
    if not records:
        return

    with db_manager(STG_DB) as conn:
        conn.execute(DDL_EPICS)
        conn.executemany(
            f"""
            INSERT INTO "{T_TARGET}" VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            ) ON CONFLICT (ID) DO NOTHING;
            """,
            [tuple(r[c] for c in _COLS) for r in records],
        )
        conn.commit()
        log.info("Inserted %d rows into %s", len(records), T_TARGET)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging Jira epics referenced by stories in JIRA_STORIES.")
    _insert(_build_records())
    log.info("Done staging Jira epics.")


if __name__ == "__main__":
    main()
