from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from agno.agent import RunResponse
from typing import List, Tuple, Optional

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from models import PreprocessedDiffOutput
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging

"""
Preprocesses aggregated code diffs using an AI agent.

Description
-----------
Analyzes commit diffs from developers and converts them into structured contribution data using agents.

    1. Loads committer IDs, names, and aggregated diffs that have not yet been preprocessed.
    2. Sends each code diff to an agent for structured parsing into a structured response model.
    3. Serializes the contributions as a JSON string and inserts them into the database.
    4. Limits the number of committers and controls concurrency with a semaphore.
"""


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_COMMITTER_DIFFS_SOURCE = "COMMITTER_DIFFS"
T_PREPROCESSED_OUTPUT = "PREPROCESSED_DIFFS"
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
LIMIT = int(os.getenv("PREPROCESS_LIMIT", 1000))
CONCUR = int(os.getenv("PREPROCESS_CONCURRENCY", 10))
AGENT_KEY = "Diff_Preprocessor"


# SQL helpers ------------------------------------------------------------------
def _load_committers_for_preprocessing(
    conn, limit: int
) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Load committers with their aggregated diffs for preprocessing. Avoids reprocessing committers already in T_PREPROCESSED_OUTPUT.
    """
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}

    if T_PREPROCESSED_OUTPUT in tables:
        q = f"""
            SELECT COMMITTER_ID, COMMITTER_NAME, AGGREGATED_DIFFS
            FROM   {T_COMMITTER_DIFFS_SOURCE}
            WHERE  COMMITTER_ID NOT IN (SELECT COMMITTER_ID FROM {T_PREPROCESSED_OUTPUT})
              AND AGGREGATED_DIFFS IS NOT NULL AND AGGREGATED_DIFFS != ''
            LIMIT  {limit};
        """
    else:
        q = f"""
            SELECT COMMITTER_ID, COMMITTER_NAME, AGGREGATED_DIFFS
            FROM   {T_COMMITTER_DIFFS_SOURCE}
            WHERE  AGGREGATED_DIFFS IS NOT NULL AND AGGREGATED_DIFFS != ''
            LIMIT  {limit};
        """
    return conn.execute(q).fetchall()


def _insert_preprocessed_contributions(
    conn,
    rows: List[
        Tuple[
            str,  # COMMITTER_ID
            Optional[str],  # COMMITTER_NAME
            Optional[str],  # JSON str of PreprocessedDiffOutput.contributions
        ]
    ],
):
    if not rows:
        log.debug("No preprocessed contribution rows to insert.")
        return

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_PREPROCESSED_OUTPUT} (
            COMMITTER_ID TEXT PRIMARY KEY,
            COMMITTER_NAME TEXT,
            STRUCTURED_CONTRIBUTIONS TEXT -- Storing as JSON string
        );
    """)

    conn.executemany(
        f"""
        INSERT INTO {T_PREPROCESSED_OUTPUT} (
            COMMITTER_ID, COMMITTER_NAME, STRUCTURED_CONTRIBUTIONS
        ) VALUES (?, ?, ?)
        ON CONFLICT DO NOTHING;
        """,
        rows,
    )
    conn.commit()
    log.info("Inserted preprocessed contribution data for %d committers", len(rows))


# Agent logic ------------------------------------------------------------------
async def _preprocess_diffs(
    committer_id: str, committer_name: Optional[str], aggregated_diffs: Optional[str]
) -> Tuple[
    str,  # COMMITTER_ID
    Optional[str],  # COMMITTER_NAME
    Optional[str],  # JSON str of PreprocessedDiffOutput.contributions
]:
    if not aggregated_diffs or not aggregated_diffs.strip():
        log.warning(f"Committer {committer_id} has no aggregated_diffs to preprocess.")
        return committer_id, committer_name, None

    try:
        agent = build_agent(AGENT_KEY)
        log.debug(
            f"Preprocessing diffs for committer {committer_id}. AGGREGATED_DIFFS snippet:\n{aggregated_diffs[:500]}"
        )

        resp: RunResponse = await agent.arun(message=aggregated_diffs)

        if resp and isinstance(resp.content, PreprocessedDiffOutput):
            output_model: PreprocessedDiffOutput = resp.content
            contributions_json = output_model.model_dump_json()

            log.info(
                "Diff preprocessing complete for committer %s. Number of structured contributions: %d",
                committer_id,
                len(output_model.contributions),
            )
            return (
                committer_id,
                committer_name,
                contributions_json,
            )

        log.warning(
            "Committer %s diff preprocessing returned no valid structured output. Response type: %s",
            committer_id,
            type(resp.content) if resp else "None",
        )
        return committer_id, committer_name, None

    except Exception as exc:
        log.error(
            "Diff preprocessing agent error for committer %s: %s",
            committer_id,
            exc,
            exc_info=True,
        )
        return committer_id, committer_name, None


# Concurrency helper -----------------------------------------------------------
async def _bounded_preprocess_diffs(
    row: Tuple[str, Optional[str], Optional[str]], sem: asyncio.Semaphore
) -> Tuple[
    str,
    Optional[str],
    Optional[
        str
    ],  # COMMITTER_ID, COMMITTER_NAME, STRUCTURED_CONTRIBUTIONS (JSON string)
]:
    committer_id, committer_name, diffs_data = row
    async with sem:
        return await _preprocess_diffs(
            committer_id, committer_name, diffs_data if diffs_data else None
        )


# Main async -------------------------------------------------------------------
async def _run_preprocessing():
    with db_manager(DB_PATH) as conn:
        rows_to_process = _load_committers_for_preprocessing(conn, LIMIT)
        if not rows_to_process:
            log.info("No new committers need diff preprocessing.")
            return

        log.info("Starting preprocessing for %d committers.", len(rows_to_process))
        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_preprocess_diffs(row, sem) for row in rows_to_process]
        results = await asyncio.gather(*tasks)

        # Filter out None results before inserting
        valid_results = [
            res for res in results if res is not None and res[2] is not None
        ]
        _insert_preprocessed_contributions(conn, valid_results)


# Entry point ------------------------------------------------------------------
def main():
    log.info(
        "Starting committer diff preprocessing script (limit=%d, concur=%d)",
        LIMIT,
        CONCUR,
    )
    asyncio.run(_run_preprocessing())
    log.info("Finished committer diff preprocessing.")


if __name__ == "__main__":
    main()
