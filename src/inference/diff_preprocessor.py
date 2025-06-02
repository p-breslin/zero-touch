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
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging
from models import (
    GeneratedCommitSummary,
    PreprocessedCommitSummary,
    PreprocessedDiffOutput,
)

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
def _load_commits_for_user(conn, committer_id: str) -> List[dict]:
    q = """
        SELECT COMMIT_MESSAGE, DIFF, FILE_ADDITIONS, FILE_DELETIONS, FILE_PATH
        FROM COMMIT_DIFFS
        WHERE COMMITTER_ID = ?
          AND DIFF IS NOT NULL AND DIFF != ''
        ORDER BY COMMIT_TIMESTAMP DESC;
    """
    rows = conn.execute(q, (committer_id,)).fetchall()
    commits = {}
    for msg, diff, add, del_, path in rows:
        key = (msg, diff)
        if key not in commits:
            commits[key] = {
                "commit_message": msg,
                "diff": diff,
                "file_paths": set(),
                "loc_added": 0,
                "loc_removed": 0,
            }
        commits[key]["file_paths"].add(path)
        commits[key]["loc_added"] += add
        commits[key]["loc_removed"] += del_
    return list(commits.values())


def extract_path_roots(paths: set[str]) -> List[str]:
    return list({p.split("/")[0] for p in paths if "/" in p})


def get_review_comment_count(conn, user_id: str) -> int:
    q = """
        SELECT COUNT(*) FROM REVIEW_COMMENTS
        WHERE json_extract_string(USER, '$.id') = ?
    """
    result = conn.execute(q, (user_id,)).fetchone()
    return result[0] if result else 0


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
async def _summarize_commit(
    commit_msg: str, diff_text: str
) -> Optional[GeneratedCommitSummary]:
    try:
        agent = build_agent(AGENT_KEY)
        message = f"=== Commit message ===\n{commit_msg}\n\n=== Diff ===\n{diff_text}"
        resp: RunResponse = await agent.arun(message=message)

        if resp and isinstance(resp.content, GeneratedCommitSummary):
            return resp.content
        else:
            log.warning("Unexpected or empty response from commit summarizer.")
            return None
    except Exception as exc:
        log.error("Error summarizing commit: %s", exc, exc_info=True)
        return None


async def _preprocess_diffs(
    committer_id: str, committer_name: Optional[str], _: Optional[str]
) -> Tuple[str, Optional[str], Optional[str]]:
    try:
        with db_manager(DB_PATH) as conn:
            raw_commits = _load_commits_for_user(conn, committer_id)
            pr_review_comments = get_review_comment_count(conn, committer_id)

        if not raw_commits:
            log.warning(f"No commits found for committer {committer_id}.")
            return committer_id, committer_name, None

        sem = asyncio.Semaphore(CONCUR)

        async def _task(commit):
            async with sem:
                agent_summary = await _summarize_commit(
                    commit["commit_message"], commit["diff"]
                )
                if not agent_summary:
                    return None
                return PreprocessedCommitSummary(
                    commit_message=commit["commit_message"],
                    summary=agent_summary.summary,
                    key_changes=agent_summary.key_changes,
                    langs=agent_summary.langs,
                    frameworks=agent_summary.frameworks,
                    loc_added=commit["loc_added"],
                    loc_removed=commit["loc_removed"],
                    file_count=len(commit["file_paths"]),
                    path_roots=extract_path_roots(commit["file_paths"]),
                )

        tasks = [_task(commit) for commit in raw_commits]
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r is not None]

        if not valid:
            log.warning(f"No valid commit summaries for committer {committer_id}.")
            return committer_id, committer_name, None

        output = PreprocessedDiffOutput(
            last_90d_commits=len(valid),
            pr_review_comments=pr_review_comments,
            commits=valid,
        )
        return committer_id, committer_name, output.model_dump_json()

    except Exception as exc:
        log.error(
            "User-level diff preprocessing failed for %s: %s",
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
