from __future__ import annotations
import os
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
Aggregates and annotates GitHub commit diffs per committer into a unified table.

Description
-----------
Generates the COMMITTER_DIFFS table by processing raw GitHub diff data from the GITHUB_DIFFS source table. Groups diffs by committer, combines multiple diffs into a single formatted string, and records metadata and the committer's earliest code change.

    1. Filters out null or empty CODE_TEXT and null COMMITTER_ID entries.
    2. Orders commit diffs by descending COMMIT_TIMESTAMP (most recent first).
    3. Aggregates commit diffs into a single block until character length cap is reached.
    4. Captures how many file diffs each committer contributed and total character length.
    5. Extracts first diff content based on earliest COMMIT_TIMESTAMP.
"""

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)
STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
T_DIFFS = "GITHUB_DIFFS"
T_AGG = "COMMITTER_DIFFS"
CHAR_CAP = 100_000

# SQL block --------------------------------------------------------------------
SQL_ORDERED_DIFFS = f"""
SELECT
    COMMITTER_ID,
    COMMITTER_NAME,
    COMMIT_TIMESTAMP,
    COMMIT_SHA,
    REPO,
    FILE_PATH,
    CODE_TEXT AS CODE_DIFF
FROM {T_DIFFS}
WHERE COMMITTER_ID IS NOT NULL
  AND CODE_TEXT IS NOT NULL
  AND CODE_TEXT != ''
ORDER BY COMMITTER_ID, COMMIT_TIMESTAMP DESC
"""

SQL_FIRST_DIFFS = f"""
SELECT
    COMMITTER_ID,
    CODE_TEXT AS FIRST_DIFF
FROM {T_DIFFS}
WHERE COMMITTER_ID IS NOT NULL
  AND CODE_TEXT IS NOT NULL
  AND CODE_TEXT != ''
  AND (COMMITTER_ID, COMMIT_TIMESTAMP) IN (
      SELECT COMMITTER_ID, MIN(COMMIT_TIMESTAMP)
      FROM {T_DIFFS}
      WHERE COMMITTER_ID IS NOT NULL AND CODE_TEXT IS NOT NULL AND CODE_TEXT != ''
      GROUP BY COMMITTER_ID
  )
"""


# Aggregation Logic ------------------------------------------------------------
def aggregate_diffs(df: pd.DataFrame, first_diffs: dict, char_cap: int):
    grouped = defaultdict(list)
    for row in df.itertuples(index=False):
        grouped[row.COMMITTER_ID].append(row)

    results = []

    for committer_id, diffs in grouped.items():
        aggregated_blocks = []
        total_chars = 0
        count = 0
        committer_name = None

        for diff in diffs:
            block = f"""--- START OF COMMIT: {diff.COMMIT_SHA} ---
--- TIMESTAMP: {diff.COMMIT_TIMESTAMP} ---
--- REPO: {diff.REPO} ---
--- FILE_PATH: {diff.FILE_PATH} ---

{diff.CODE_DIFF}

--- END OF COMMIT: {diff.COMMIT_SHA} ---
"""
            block_len = len(block)
            total_chars += block_len
            aggregated_blocks.append(block)
            count += 1
            committer_name = diff.COMMITTER_NAME
            if total_chars > char_cap:
                break

        aggregated_text = "\n\n### NEXT CODE CHANGE ###\n\n".join(aggregated_blocks)
        results.append(
            {
                "COMMITTER_ID": committer_id,
                "COMMITTER_NAME": committer_name,
                "DIFF_COUNTS": count,
                "AGGREGATED_DIFFS": aggregated_text,
                "AGGREGATED_DIFF_LENGTH": len(aggregated_text),
                "FIRST_DIFF": first_diffs.get(committer_id, ""),
            }
        )

    return results


# Runner -----------------------------------------------------------------------
def main() -> None:
    with db_manager(STG_DB) as conn:
        df = conn.execute(SQL_ORDERED_DIFFS).fetchdf()
        first_df = conn.execute(SQL_FIRST_DIFFS).fetchdf()
        first_diffs = {
            row["COMMITTER_ID"]: row["FIRST_DIFF"] for _, row in first_df.iterrows()
        }

        rows = aggregate_diffs(df, first_diffs, CHAR_CAP)

        conn.execute(f"DROP TABLE IF EXISTS {T_AGG}")
        conn.execute(f"""
            CREATE TABLE {T_AGG} (
                COMMITTER_ID TEXT,
                COMMITTER_NAME TEXT,
                DIFF_COUNTS INTEGER,
                AGGREGATED_DIFFS TEXT,
                AGGREGATED_DIFF_LENGTH INTEGER,
                FIRST_DIFF TEXT
            )
        """)
        conn.register("agg_results", pd.DataFrame(rows))
        conn.execute(f"""
            INSERT INTO {T_AGG}
            SELECT * FROM agg_results
        """)
        log.info("Committer diff code aggregation complete — %d rows", len(rows))


if __name__ == "__main__":
    main()
