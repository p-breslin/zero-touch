"""
Targets commits already staged, typically those associated with recently processed Pull Requests. Extracts the first JIRA issue key from GitHub commit messages and writes it into GITHUB_COMMITS.EXTRACTED_JIRA_KEY column.

Steps
-----
1. Read commits whose EXTRACTED_JIRA_KEY is NULL.
2. Feed COMMIT_MESSAGE to the Agent.
3. Batch-update the EXTRACTED_JIRA_KEY column.
"""

from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple
from agno.agent import RunResponse

from models import IssueKey
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_COMMITS = "GITHUB_COMMITS"
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
LIMIT = int(os.getenv("COMMIT_KEY_PROCESS_LIMIT", 1000))
CONCUR = int(os.getenv("COMMIT_KEY_CONCURRENCY_LIMIT", 100))
AGENT_KEY = "Issue_Key_Inference"


# SQL helpers ------------------------------------------------------------------
def _load_commits(conn, limit: int) -> List[Tuple[str, str | None]]:
    """
    Queries the GITHUB_COMMITS table in the staging DB. Selects COMMIT_SHA and COMMIT_MESSAGE for commits where EXTRACTED_JIRA_KEY is currently NULL, up to a specified limit. Returns these records as a list of tuples.
    """
    q = f"""
        SELECT COMMIT_SHA, COMMIT_MESSAGE
        FROM   {T_COMMITS}
        WHERE  EXTRACTED_JIRA_KEY IS NULL
        LIMIT  {limit};
    """
    return conn.execute(q).fetchall()


def _update_commit_keys(conn, rows: List[Tuple[str | None, str]]):
    """
    Takes a list of tuples (extracted JIRA key, COMMIT_SHA). Executes a batch UPDATE statement on the GITHUB_COMMITS table, setting the EXTRACTED_JIRA_KEY for each corresponding COMMIT_SHA.
    """
    if not rows:
        log.debug("No commit key updates to apply.")
        return

    remapped_rows = [(key, sha) for sha, key in rows]
    conn.executemany(
        f"""UPDATE {T_COMMITS} SET EXTRACTED_JIRA_KEY = ? WHERE COMMIT_SHA = ?;""",
        remapped_rows,  # Expects (key, sha)
    )
    conn.commit()
    log.info("Updated EXTRACTED_JIRA_KEY for %d commit rows", len(remapped_rows))


# Agent logic ------------------------------------------------------------------
async def _extract_key(
    commit_sha: str, commit_message: str | None
) -> Tuple[str, str | None]:  # Returns (commit_sha, extracted_key | None)
    """
    Builds an Agent instance and gives it a commit message. Processes the agent's response and returns a tuple containing the original commit_sha and the extracted JIRA key string (or None if no key is found/error).
    """
    if not commit_message or not commit_message.strip():
        log.debug(f"Commit {commit_sha} has no message to scan.")
        return commit_sha, None

    try:
        agent = build_agent(AGENT_KEY)
        resp: RunResponse = await agent.arun(message=commit_message)
        if resp and isinstance(resp.content, IssueKey):
            key = resp.content.key
            return commit_sha, key.strip() if key and key.strip() else None

        log.info("Commit %s produced no key", commit_sha)
        return commit_sha, None
    except Exception as exc:
        log.error("Agent error for commit %s: %s", commit_sha, exc, exc_info=True)
        return commit_sha, None


# Concurrency helper -----------------------------------------------------------
async def _bounded_extract(
    row: Tuple[str, str | None, str | None], sem: asyncio.Semaphore
) -> Tuple[str, str | None]:
    """
    Helper function to respect the concurrency semaphore.
    """
    commit_sha, commit_message = row
    async with sem:
        return await _extract_key(commit_sha, commit_message)


# Main async -------------------------------------------------------------------
async def _run():
    """
    Connects to the staging DB, fetches commits needing key extraction, and concurrently processes these commits. Collects all the results and updates the database in a single batch.
    """
    with db_manager(DB_PATH) as conn:
        commits = _load_commits(conn, LIMIT)
        if not commits:
            log.info("No commits need key extraction.")
            return

        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_extract(row, sem) for row in commits]
        updates = await asyncio.gather(*tasks)
        _update_commit_keys(conn, updates)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Extracting JIRA keys from commits (limit=%d, concur=%d)", LIMIT, CONCUR)
    asyncio.run(_run())
    log.info("Finished commit key extraction")


if __name__ == "__main__":
    main()
