"""
Extracts JIRA issue keys from GitHub Pull Request titles and bodies.

    1. Fetches PRs from the GITHUB_PRS table in the staging DB
       where EXTRACTED_JIRA_KEY is NULL.
    2. Concatenates PR TITLE and BODY to form input text for the Agent.
    3. Invokes an Agent to extract the first JIRA key.
    4. Updates the EXTRACTED_JIRA_KEY column in the GITHUB_PRS table for the processed PR.
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

DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
T_PRS = "GITHUB_PRS"
LIMIT = int(os.getenv("PR_KEY_PROCESS_LIMIT", 1000))
CONCUR = int(os.getenv("PR_KEY_CONCURRENCY_LIMIT", 100))
AGENT_KEY = "PR_Issue_Key_Inference"


# SQL helpers ------------------------------------------------------------------
def _load_prs(conn, limit: int) -> List[Tuple[str, str, str]]:
    """
    Queries the GITHUB_PRS table in the staging DB. Delects the INTERNAL_ID, TITLE, and BODY for PRs where the EXTRACTED_JIRA_KEY is currently NULL, up to a specified limit, and returns these records as a list of tuples.
    """
    q = f"""
        SELECT INTERNAL_ID, TITLE, BODY
        FROM   {T_PRS}
        WHERE  EXTRACTED_JIRA_KEY IS NULL
        LIMIT  {limit};
    """
    return conn.execute(q).fetchall()


def _update_keys(conn, rows: List[Tuple[str, str]]):
    """
    Takes a list of tuples (extracted JIRA key and a PR INTERNAL_ID). Executes a batch UPDATE statement on the GITHUB_PRS table in the staging DB, setting the EXTRACTED_JIRA_KEY for each corresponding INTERNAL_ID.
    """
    if not rows:
        return

    conn.executemany(
        f"""UPDATE {T_PRS} SET EXTRACTED_JIRA_KEY = ? WHERE INTERNAL_ID = ?;""",
        rows,
    )
    conn.commit()
    log.info("Updated %d PR rows with extracted keys", len(rows))


# Agent logic ------------------------------------------------------------------
async def _extract_key(
    pr_id: str, title: str | None, body: str | None
) -> Tuple[str, str | None]:
    """
    Builds an Agent insatnce and gives it a concatenated PR title and body. Processeses the agent's response and returns a tuple containing the original pr_id and the extracted JIRA key string (or an empty string if no key is found or an error occurs).
    """
    txt = "\n\n".join(p for p in (title, body) if p).strip()
    if not txt:
        return pr_id, None  # no key found

    try:
        agent = build_agent(AGENT_KEY)
        resp: RunResponse = await agent.arun(message=txt)
        if resp and isinstance(resp.content, IssueKey):
            key = resp.content.key
            return pr_id, key.strip() if key and key.strip() else None

        log.info("PR %s produced no key", pr_id)
        return pr_id, None
    except Exception as exc:
        log.error("Agent error on PR %s: %s", pr_id, exc, exc_info=True)
        return pr_id, None


# Concurrency helper -----------------------------------------------------------
async def _bounded_extract(
    row: Tuple[str, str | None, str | None], sem: asyncio.Semaphore
) -> Tuple[str, str | None]:
    """
    Helper function to respect the concurrency semaphore.
    """
    pr_id, title, body = row  # unpack (INTERNAL_ID, TITLE, BODY)
    async with sem:
        return await _extract_key(pr_id, title, body)


# Main async -------------------------------------------------------------------
async def _run():
    """
    Connects to the staging DB, fetches PRs needing key extraction, and concurrently processes these PRs. Collects all the results and updates the database in a single batch.
    """
    with db_manager(DB_PATH) as conn:
        prs = _load_prs(conn, LIMIT)
        if not prs:
            log.info("No PRs need key extraction.")
            return

        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_extract(row, sem) for row in prs]
        updates = await asyncio.gather(*tasks)
        _update_keys(conn, updates)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Extracting JIRA keys from PRs (limit=%d, concur=%d)", LIMIT, CONCUR)
    asyncio.run(_run())
    log.info("Finished PR key extraction")


if __name__ == "__main__":
    main()
