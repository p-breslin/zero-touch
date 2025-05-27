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

STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

# SQL blocks -------------------------------------------------------------------
SQL_CREATE_IDENTITIES = """
CREATE OR REPLACE TABLE ALL_IDENTITIES AS
SELECT
    NAME,
    JIRA_CREATOR_IDS,
    JIRA_REPORTER_IDS,
    JIRA_ASSIGNEE_IDS,
    GH_AUTHOR_IDS,
    GH_COMMITTER_IDS,
    PR_USER_IDS,
    GH_AUTHOR_EMAILS,
    GH_COMMITTER_EMAILS,
    JIRA_REPORTER_EMAILS,
    JIRA_CREATOR_EMAILS,
    JIRA_ASSIGNEE_EMAILS
FROM DATA_STAGING.main.MASTER_TABLE;
"""


# Runner -----------------------------------------------------------------------
def _execute(conn, sql: str, table: str) -> None:
    conn.execute(sql)
    n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    log.info("%s refreshed — %d rows", table, n)


def main() -> None:
    with db_manager(STG_DB) as conn:
        _execute(conn, SQL_CREATE_IDENTITIES, "ALL_IDENTITIES")
        log.info("All person-derived tables created.")


if __name__ == "__main__":
    main()
