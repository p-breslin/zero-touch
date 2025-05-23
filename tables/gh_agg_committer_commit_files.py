from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging
from utils.helpers import db_manager

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

# SQL block (no truncation, includes COMMITTER_NAME, removes role/skills) -----
SQL_CREATE_COMMITTER_CODE_SUMMARIES = """
CREATE OR REPLACE TABLE COMMITTER_CODE_SUMMARIES AS
WITH annotated AS (
    SELECT
        COMMITTER_ID,
        COMMITTER_NAME,
        CODE_TEXT AS code_diff
    FROM GITHUB_COMMIT_FILES
    WHERE COMMITTER_ID IS NOT NULL
      AND CODE_TEXT IS NOT NULL
      AND CODE_TEXT != ''
)
SELECT
    COMMITTER_ID,
    ANY_VALUE(COMMITTER_NAME) AS COMMITTER_NAME,
    STRING_AGG(
        code_diff,
        '\n\n--- Code change associated with new commit ---\n\n'
    ) AS AGGREGATED_CODE
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
        _execute(conn, SQL_CREATE_COMMITTER_CODE_SUMMARIES, "COMMITTER_CODE_SUMMARIES")
        log.info("Committer code aggregation complete.")


if __name__ == "__main__":
    main()
