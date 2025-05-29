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
T_DIFFS = "GITHUB_DIFFS"

# SQL block --------------------------------------------------------------------
SQL_CREATE_COMMITTER_DIFFS = f"""
CREATE OR REPLACE TABLE COMMITTER_DIFFS AS
WITH annotated AS (
    SELECT
        COMMITTER_ID,
        COMMITTER_NAME,
        COMMIT_TIMESTAMP,
        CODE_TEXT AS CODE_DIFF
    FROM {T_DIFFS}
    WHERE COMMITTER_ID IS NOT NULL
      AND CODE_TEXT IS NOT NULL
      AND CODE_TEXT != ''
),

first_diffs AS (
    SELECT
        COMMITTER_ID,
        CODE_DIFF AS FIRST_DIFF
    FROM annotated
    WHERE (COMMITTER_ID, COMMIT_TIMESTAMP) IN (
        SELECT COMMITTER_ID, MIN(COMMIT_TIMESTAMP)
        FROM annotated
        GROUP BY COMMITTER_ID
    )
)

SELECT
    a.COMMITTER_ID,
    ANY_VALUE(a.COMMITTER_NAME) AS COMMITTER_NAME,
    COUNT(*) AS DIFF_COUNTS,
    STRING_AGG(
        a.CODE_DIFF,
        '\n\n--- Diff associated with new commit ---\n\n'
    ) AS AGGREGATED_DIFFS,
    ANY_VALUE(f.FIRST_DIFF) AS FIRST_DIFF
FROM annotated a
LEFT JOIN first_diffs f ON f.COMMITTER_ID = a.COMMITTER_ID
GROUP BY a.COMMITTER_ID;
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
