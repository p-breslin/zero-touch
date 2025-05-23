from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple
from agno.agent import RunResponse

from models import CommitterInfo
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_COMMITTERS = "COMMITTER_CODE_SUMMARIES"
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
LIMIT = int(os.getenv("COMMITTER_INFER_LIMIT", 1000))
CONCUR = int(os.getenv("COMMITTER_INFER_CONCURRENCY", 100))
AGENT_KEY = "Committer_Info_Inference"
N_CHARS = 40000  # truncate code text to ~10000 tokens


# SQL helpers ------------------------------------------------------------------
def _load_committers(conn, limit: int) -> List[Tuple[str, str | None]]:
    """
    Selects COMMITTER_ID and AGGREGATED_CODE for those without a role.
    """
    q = f"""
        SELECT COMMITTER_ID, AGGREGATED_CODE
        FROM   {T_COMMITTERS}
        WHERE  COMMITTER_ROLE IS NULL
        LIMIT  {limit};
    """
    return conn.execute(q).fetchall()


def _update_committer_info(conn, rows: List[Tuple[str | None, str, str]]):
    """
    Updates COMMITTER_ROLE and COMMITTER_SKILLS for each COMMITTER_ID.
    """
    if not rows:
        log.debug("No committer info updates to apply.")
        return

    conn.execute(
        f"ALTER TABLE {T_COMMITTERS} ADD COLUMN IF NOT EXISTS COMMITTER_ROLE TEXT;"
    )
    conn.execute(
        f"ALTER TABLE {T_COMMITTERS} ADD COLUMN IF NOT EXISTS COMMITTER_SKILLS TEXT;"
    )

    remapped_rows = [
        (role, skills, committer_id) for committer_id, role, skills in rows
    ]
    conn.executemany(
        f"""UPDATE {T_COMMITTERS}
            SET COMMITTER_ROLE = ?, COMMITTER_SKILLS = ?
            WHERE COMMITTER_ID = ?;""",
        remapped_rows,
    )
    conn.commit()
    log.info("Updated committer info for %d rows", len(remapped_rows))


# Agent logic ------------------------------------------------------------------
async def _extract_info(
    committer_id: str, code: str | None
) -> Tuple[str, str | None, str | None]:
    """
    Returns (COMMITTER_ID, ROLE, SKILLS as comma-separated string)
    """
    if not code or not code.strip():
        log.debug(f"Committer {committer_id} has no code to analyze.")
        return committer_id, None, None

    try:
        agent = build_agent(AGENT_KEY)
        log.debug(f"\n\nNew agent instance. Code snippet:\n{code[:500]}")
        resp: RunResponse = await agent.arun(code=code)
        if resp and isinstance(resp.content, CommitterInfo):
            role = resp.content.role
            skills = ", ".join(resp.content.skills) if resp.content.skills else None
            log.info("Role and skills inferred")
            return committer_id, role.strip() if role else None, skills
        log.info("Committer %s returned no role/skills", committer_id)
        return committer_id, None, None
    except Exception as exc:
        log.error("Agent error for committer %s: %s", committer_id, exc, exc_info=True)
        return committer_id, None, None


# Concurrency helper -----------------------------------------------------------
async def _bounded_extract(
    row: Tuple[str, str | None], sem: asyncio.Semaphore
) -> Tuple[str, str | None, str | None]:
    committer_id, code = row
    async with sem:
        return await _extract_info(committer_id, code[:N_CHARS])


# Main async -------------------------------------------------------------------
async def _run():
    with db_manager(DB_PATH) as conn:
        rows = _load_committers(conn, LIMIT)
        if not rows:
            log.info("No committers need role/skills inference.")
            return

        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_extract(row, sem) for row in rows]
        updates = await asyncio.gather(*tasks)
        _update_committer_info(conn, updates)


# Entry point ------------------------------------------------------------------
def main():
    log.info(
        "Inferring roles and skills for committers (limit=%d, concur=%d)", LIMIT, CONCUR
    )
    asyncio.run(_run())
    log.info("Finished committer role/skills inference")


if __name__ == "__main__":
    main()
