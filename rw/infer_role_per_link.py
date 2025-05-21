"""
Infer a functional role for each row in PR_JIRA_ISSUE_LINKS and store the result
in TEMP_ROLE_INFERENCES_PER_LINK (one row per link).

Later, aggregate script will pick the most-frequent role per assignee.

"""

from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from typing import List, Tuple
from dotenv import load_dotenv
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

DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_LINKS = "PR_JIRA_ISSUE_LINKS"
T_TEMP = "TEMP_ROLE_INFERENCES_PER_LINK"

LIMIT = int(os.getenv("LINK_ROLE_PROCESS_LIMIT", 1000))
CONCUR = int(os.getenv("LINK_ROLE_CONCURRENCY_LIMIT", 50))
AGENT_KEY = "Role_Inference"

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_TEMP} (
    LINK_PK_HASH TEXT PRIMARY KEY,
    ASSIGNEE_ID  TEXT,
    INFERRED_ROLE TEXT
);
"""


# Helpers ----------------------------------------------------------------------
SQL_NEW_LINKS = f"""
WITH src AS (
    SELECT
        md5(
          GH_PR_INTERNAL_ID || '|' || GH_USER_LOGIN || '|' ||
          GH_ROLE_IN_PR     || '|' || JIRA_ISSUE_KEY
        ) AS h,
        JIRA_ASSIGNEE_ID AS aid,
        JIRA_SUMMARY     AS summ,
        JIRA_DESCRIPTION AS descr
    FROM {T_LINKS}
    WHERE JIRA_ASSIGNEE_ID IS NOT NULL
)
SELECT h, aid, summ, descr
FROM   src
LEFT JOIN {T_TEMP} t ON src.h = t.LINK_PK_HASH
WHERE  t.LINK_PK_HASH IS NULL
LIMIT  {LIMIT};
"""


def _fetch_unprocessed(conn) -> List[Tuple[str, str, str | None, str | None]]:
    return conn.execute(SQL_NEW_LINKS).fetchall()


# Agent logic ------------------------------------------------------------------
async def _infer_role(
    link_hash: str, assignee: str, summary: str | None, description: str | None
) -> Tuple[str, str, str | None]:
    text = "\n\n".join(p for p in (summary, description) if p).strip()
    if not text:
        return link_hash, assignee, None
    try:
        agent = build_agent(AGENT_KEY)
        resp: RunResponse = await agent.arun(message=text)
        if resp and isinstance(resp.content, InferredRole):
            role = resp.content.role
            role = role.strip() if role and role.strip() else None
            return link_hash, assignee, role
        return link_hash, assignee, None
    except Exception as exc:
        log.error("Agent error for link %s: %s", link_hash, exc, exc_info=True)
        return link_hash, assignee, None


# Bounded helper ---------------------------------------------------------------
async def bounded_infer(
    sem: asyncio.Semaphore, row: Tuple[str, str, str | None, str | None]
) -> Tuple[str, str, str | None]:
    h, aid, summ, descr = row
    async with sem:
        return await _infer_role(h, aid, summ, descr)


# Async pipeline ---------------------------------------------------------------
async def main_async():
    with db_manager(DB) as conn:
        conn.execute(DDL)
        rows = _fetch_unprocessed(conn)
        if not rows:
            log.info("No new link rows to process.")
            return

        sem = asyncio.Semaphore(CONCUR)
        tasks = [bounded_infer(sem, r) for r in rows]
        results = await asyncio.gather(*tasks)

        conn.executemany(
            f"INSERT OR REPLACE INTO {T_TEMP} VALUES (?, ?, ?);",
            results,
        )
        conn.commit()
        log.info("Upserted %d link-level role rows", len(results))


# Entry point ------------------------------------------------------------------
def main():
    log.info("Inferring role per link (limit=%d, concur=%d)", LIMIT, CONCUR)
    asyncio.run(main_async())
    log.info("Finished link-level role inference.")


if __name__ == "__main__":
    main()
