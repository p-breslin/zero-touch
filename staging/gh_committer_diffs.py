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

# SQL block --------------------------------------------------------------------
SQL_CREATE_COMMITTER_DIFFS = """
CREATE OR REPLACE TABLE COMMITTER_DIFFS AS
WITH annotated AS (
    SELECT
        COMMITTER_ID,
        COMMITTER_NAME,
        CODE_TEXT AS CODE_DIFF
    FROM GITHUB_COMMIT_FILES
    WHERE COMMITTER_ID IS NOT NULL
      AND CODE_TEXT IS NOT NULL
      AND CODE_TEXT != ''
)
SELECT
    COMMITTER_ID,
    ANY_VALUE(COMMITTER_NAME) AS COMMITTER_NAME,
    STRING_AGG(
        CODE_DIFF,
        '\n\n--- Diff associated with new commit ---\n\n'
    ) AS AGGREGATED_DIFFS
FROM annotated
GROUP BY COMMITTER_ID;
"""


# Runner -----------------------------------------------------------------------
def _execute(conn, sql: str, table: str) -> None:
    conn.execute(sql)
    n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    log.info("%s refreshed — %d rows", table, n)


def main() -> None:
    with db_manager(STG_DB) as conn:
        _execute(conn, SQL_CREATE_COMMITTER_DIFFS, "COMMITTER_DIFFS")
        log.info("Committer diff code aggregation complete.")


if __name__ == "__main__":
    main()
