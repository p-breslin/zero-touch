"""
Infer a functional role from each JIRA issues summary + description and store it in PERSON_ATTRIBUTES.

Steps
-----
1. Pull issues from JIRA_ISSUES that lack a role entry.
2. Feed Summary and Description to Agent.
3. Upsert into PERSON_ATTRIBUTES.
"""

from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple
from agno.agent import RunResponse

from models import InferredRole
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_ISSUES = "JIRA_ISSUES"
T_ATTRIB = "PERSON_ATTRIBUTES"

LIMIT = int(os.getenv("JIRA_ROLE_PROCESS_LIMIT", 1000))
CONCUR = int(os.getenv("JIRA_ROLE_CONCURRENCY_LIMIT", 100))
AGENT_KEY = "Role_Inference"


# SQL helpers ------------------------------------------------------------------
def _load_issues(conn, limit: int) -> List[Tuple[str, str | None, str | None, str]]:
    """
    Returns (issue_key, summary, description, assignee_id) for issues whose
    assignee has ROLE IS NULL in PERSON_ATTRIBUTES.
    """
    return conn.execute(
        f"""
        SELECT ji.ISSUE_KEY, ji.SUMMARY, ji.DESCRIPTION, ji.ASSIGNEE_ACCOUNT_ID
        FROM   {T_ISSUES} ji
        JOIN   {T_ATTRIB} pa
               ON ji.ASSIGNEE_ACCOUNT_ID = pa.ASSIGNEE_JIRA_ACCOUNT_ID
        WHERE  pa.ROLE IS NULL
          AND  (ji.SUMMARY IS NOT NULL OR ji.DESCRIPTION IS NOT NULL)
        LIMIT  {limit};
        """
    ).fetchall()


def _upsert_roles(conn, rows: List[Tuple[str, str | None]]):
    """
    rows: (assignee_id, role|None).  Updates ROLE or inserts new row if missing.
    """
    if not rows:
        return
    conn.executemany(
        f"""
        INSERT INTO {T_ATTRIB} (ASSIGNEE_JIRA_ACCOUNT_ID, ROLE)
        VALUES (?, ?)
        ON CONFLICT (ASSIGNEE_JIRA_ACCOUNT_ID) DO UPDATE SET
            ROLE = excluded.ROLE;
        """,
        rows,
    )
    conn.commit()
    log.info("Upserted %d ROLE values", len(rows))


# Agent logic ------------------------------------------------------------------
async def _infer_role(summary: str | None, description: str | None) -> str | None:
    text = "\n\n".join(p for p in (summary, description) if p).strip()
    if not text:
        return None
    agent = build_agent(AGENT_KEY)
    resp: RunResponse = await agent.arun(message=text)
    if resp and isinstance(resp.content, InferredRole):
        role = resp.content.role
        return role.strip() if role and role.strip() else None
    return None


# Worker with semaphore --------------------------------------------------------
async def _bounded(issue_row, sem: asyncio.Semaphore):
    key, summ, desc, assignee = issue_row
    async with sem:
        role = await _infer_role(summ, desc)
        return assignee, role  # (id, role|None)


# Main async -------------------------------------------------------------------
async def _pipeline():
    with db_manager(DB_PATH) as conn:
        issues = _load_issues(conn, LIMIT)
        if not issues:
            log.info("No assignees need role inference.")
            return

        sem = asyncio.Semaphore(CONCUR)
        results = await asyncio.gather(*[_bounded(r, sem) for r in issues])
        _upsert_roles(conn, results)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Inferring ROLE for JIRA assignees (limit=%d, concur=%d)", LIMIT, CONCUR)
    asyncio.run(_pipeline())
    log.info("Finished ROLE inference.")


if __name__ == "__main__":
    main()
