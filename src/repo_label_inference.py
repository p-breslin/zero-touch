from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple
from agno.agent import RunResponse

from models import RepoLabel
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_REPOS = "REPO_CODE_SUMMARIES"
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
LIMIT = int(os.getenv("REPO_LABEL_PROCESS_LIMIT", 1000))
CONCUR = int(os.getenv("REPO_LABEL_CONCURRENCY_LIMIT", 100))
AGENT_KEY = "Repo_Label_Inference"
N_CHARS = 40000  # truncate code text to ~10000 tokens


# SQL helpers ------------------------------------------------------------------
def _load_repos(conn, limit: int) -> List[Tuple[str, str | None]]:
    """
    Selects REPO and aggregated_code where REPO_LABEL is NULL.
    """
    q = f"""
        SELECT REPO, AGGREGATED_CODE
        FROM   {T_REPOS}
        WHERE  REPO_LABEL IS NULL
        LIMIT  {limit};
    """
    return conn.execute(q).fetchall()


def _update_repo_labels(conn, rows: List[Tuple[str | None, str]]):
    """
    Updates REPO_LABEL for each REPO.
    """
    if not rows:
        log.debug("No repo label updates to apply.")
        return

    remapped_rows = [(label, repo) for repo, label in rows]
    conn.execute(
        f"ALTER TABLE {T_REPOS} ADD COLUMN IF NOT EXISTS REPO_LABEL TEXT;"
    )
    conn.executemany(
        f"""UPDATE {T_REPOS} SET REPO_LABEL = ? WHERE REPO = ?;""",
        remapped_rows,
    )
    conn.commit()
    log.info("Updated REPO_LABEL for %d repos", len(remapped_rows))


# Agent logic ------------------------------------------------------------------
async def _extract_label(repo: str, code: str | None) -> Tuple[str, str | None]:
    if not code or not code.strip():
        log.debug(f"Repo {repo} has no code to infer from.")
        return repo, None

    try:
        agent = build_agent(AGENT_KEY)
        resp: RunResponse = await agent.arun(code=code)
        if resp and isinstance(resp.content, RepoLabel):
            label = resp.content.label
            return repo, label.strip() if label and label.strip() else None

        log.info("Repo %s produced no label", repo)
        return repo, None
    except Exception as exc:
        log.error("Agent error for repo %s: %s", repo, exc, exc_info=True)
        return repo, None


# Concurrency wrapper ----------------------------------------------------------
async def _bounded_extract(
    row: Tuple[str, str | None], sem: asyncio.Semaphore
) -> Tuple[str, str | None]:
    repo, code = row
    async with sem:
        return await _extract_label(repo, code[:N_CHARS])


# Main async -------------------------------------------------------------------
async def _run():
    with db_manager(DB_PATH) as conn:
        rows = _load_repos(conn, LIMIT)
        if not rows:
            log.info("No repos need label inference.")
            return

        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_extract(row, sem) for row in rows]
        updates = await asyncio.gather(*tasks)
        _update_repo_labels(conn, updates)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Inferring labels for repos (limit=%d, concur=%d)", LIMIT, CONCUR)
    asyncio.run(_run())
    log.info("Finished repo label inference")


if __name__ == "__main__":
    main()