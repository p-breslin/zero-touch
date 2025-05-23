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

# SQL block (truncates if over 50 associated commits) --------------------------
SQL_CREATE_AGG_COMMIT_FILES = """
CREATE OR REPLACE TABLE REPO_CODE_SUMMARIES AS
WITH annotated AS (
    SELECT
        REPO,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY REPO) > 50
            THEN LEFT(CODE_TEXT, 10000)
            ELSE CODE_TEXT
        END AS maybe_truncated_code
    FROM GITHUB_COMMIT_FILES
    WHERE CODE_TEXT IS NOT NULL AND CODE_TEXT != ''
)
SELECT
    REPO,
    STRING_AGG(
        maybe_truncated_code,
        '\n\n--- Code change associated with new commit ---\n\n'
    ) AS aggregated_code,
    NULL::TEXT AS REPO_LABEL  -- placeholder for future inference
FROM annotated
GROUP BY REPO;
"""


# Runner -----------------------------------------------------------------------
def _execute(conn, sql: str, table: str) -> None:
    conn.execute(sql)
    n = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    log.info("%s refreshed — %d rows", table, n)


def main() -> None:
    with db_manager(STG_DB) as conn:
        _execute(conn, SQL_CREATE_AGG_COMMIT_FILES, "REPO_CODE_SUMMARIES")
        log.info("Code aggregation by repo complete.")


if __name__ == "__main__":
    main()
