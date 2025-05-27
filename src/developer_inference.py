from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple, Optional
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

T_COMMITTERS = "COMMITTER_DIFFS"
T_INFER = "DEVELOPER_INFERENCE"
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
LIMIT = int(os.getenv("COMMITTER_INFER_LIMIT", 1000))
CONCUR = int(os.getenv("COMMITTER_INFER_CONCURRENCY", 100))
AGENT_KEY = "Committer_Info_Inference"
N_CHARS = 40000  # truncate code text to ~10000 tokens


# SQL helpers ------------------------------------------------------------------
def _load_committers(
    conn, limit: int
) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Load committers with their name and code for inference, avoiding duplicates if DEVELOPER_INFERENCE exists.
    """
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}

    if T_INFER in tables:
        q = f"""
            SELECT COMMITTER_ID, COMMITTER_NAME, AGGREGATED_DIFFS
            FROM   {T_COMMITTERS}
            WHERE  COMMITTER_ID NOT IN (SELECT COMMITTER_ID FROM {T_INFER})
            LIMIT  {limit};
        """
    else:
        q = f"""
            SELECT COMMITTER_ID, COMMITTER_NAME, AGGREGATED_DIFFS
            FROM   {T_COMMITTERS}
            LIMIT  {limit};
        """

    return conn.execute(q).fetchall()


def _insert_committer_analysis(
    conn,
    rows: List[
        Tuple[
            str,
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
            Optional[str],
        ]
    ],
):
    if not rows:
        log.debug("No committer inference rows to insert.")
        return

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_INFER} (
            COMMITTER_ID TEXT PRIMARY KEY,
            COMMITTER_NAME TEXT,
            ANALYSIS TEXT,
            ROLE TEXT,
            EXPERIENCE_LEVEL TEXT,
            SKILLS TEXT,
            JUSTIFICATION TEXT,
            NOTES TEXT
        );
    """)

    conn.executemany(
        f"""
        INSERT INTO {T_INFER} (
            COMMITTER_ID, COMMITTER_NAME, ANALYSIS, ROLE,
            EXPERIENCE_LEVEL, SKILLS, JUSTIFICATION, NOTES
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING;
        """,
        rows,
    )
    conn.commit()
    log.info("Inserted developer inference data for %d committers", len(rows))


# Agent logic ------------------------------------------------------------------
async def _extract_info(
    committer_id: str, committer_name: Optional[str], code: Optional[str]
) -> Tuple[
    str,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    if not code or not code.strip():
        log.debug(f"Committer {committer_id} has no code to analyze.")
        return committer_id, committer_name, None, None, None, None, None, None

    try:
        agent = build_agent(AGENT_KEY)
        log.debug(f"\n\nNew agent instance. Code snippet:\n{code[:500]}")
        resp: RunResponse = await agent.arun(code=code)
        if resp and isinstance(resp.content, CommitterInfo):
            info: CommitterInfo = resp.content
            log.info("Developer inference complete for committer %s", committer_id)
            return (
                committer_id,
                committer_name,
                info.analysis,
                info.role,
                info.experience_level,
                ", ".join(info.skills) if info.skills else None,
                info.justification,
                info.notes,
            )
        log.info("Committer %s returned no inference", committer_id)
        return committer_id, committer_name, None, None, None, None, None, None
    except Exception as exc:
        log.error("Agent error for committer %s: %s", committer_id, exc, exc_info=True)
        return committer_id, committer_name, None, None, None, None, None, None


# Concurrency helper -----------------------------------------------------------
async def _bounded_extract(
    row: Tuple[str, Optional[str], Optional[str]], sem: asyncio.Semaphore
) -> Tuple[
    str,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    committer_id, committer_name, code = row
    async with sem:
        return await _extract_info(
            committer_id, committer_name, code[:N_CHARS] if code else None
        )


# Main async -------------------------------------------------------------------
async def _run():
    with db_manager(DB_PATH) as conn:
        rows = _load_committers(conn, LIMIT)
        if not rows:
            log.info("No committers need inference.")
            return

        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_extract(row, sem) for row in rows]
        results = await asyncio.gather(*tasks)
        _insert_committer_analysis(conn, results)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Inferring developer info (limit=%d, concur=%d)", LIMIT, CONCUR)
    asyncio.run(_run())
    log.info("Finished developer inference")


if __name__ == "__main__":
    main()
