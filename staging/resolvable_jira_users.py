"""
Creates a deduplicated list of JIRA users resolvable by display name. Each entry includes the user's display name, account ID, and email address, using the most representative record per name found in JIRA_ISSUES.

Steps
-----
1. Query distinct ASSIGNEE_DISPLAY_NAMEs from JIRA_ISSUES (staging DB)
2. For each name, pick a representative account ID and email
3. Insert or update records in the RESOLVABLE_JIRA_USERS table (staging DB)
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

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_JIRA = f"{COMPANY}_JIRA_"
STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_SOURCE = "JIRA_ISSUES"
T_TARGET = "RESOLVABLE_JIRA_USERS"

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET} (
    JIRA_DISPLAY_NAME   TEXT PRIMARY KEY,
    JIRA_ID             TEXT,
    JIRA_EMAIL          TEXT
);
"""

UPSERT = f"""
INSERT INTO {T_TARGET} (
    JIRA_DISPLAY_NAME,
    JIRA_ID,
    JIRA_EMAIL
)
SELECT DISTINCT ON (ASSIGNEE_DISPLAY_NAME)
    ASSIGNEE_DISPLAY_NAME,
    ASSIGNEE_ACCOUNT_ID,
    ASSIGNEE_EMAIL
FROM {T_SOURCE}
WHERE ASSIGNEE_DISPLAY_NAME IS NOT NULL
ON CONFLICT (JIRA_DISPLAY_NAME) DO UPDATE SET
    JIRA_ID = excluded.JIRA_ID,
    JIRA_EMAIL = excluded.JIRA_EMAIL;
"""


# Pipeline ---------------------------------------------------------------------
def _stage():
    with db_manager(STG_DB) as conn:
        conn.execute(DDL)
        conn.execute(UPSERT)
        conn.commit()
        log.info("Upserted deduplicated JIRA users into %s", T_TARGET)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Creating resolvable JIRA user view -> %s", T_TARGET)
    _stage()
    log.info("Done.")


if __name__ == "__main__":
    main()
