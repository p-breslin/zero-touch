from __future__ import annotations
import os
import duckdb
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple
from agno.agent import RunResponse
from contextlib import contextmanager

from scripts.paths import DATA_DIR
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging

# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_COMMITS = "GITHUB_COMMITS"
DB_SUBSET = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")

PROCESS_LIMIT = int(os.getenv("PROCESS_LIMIT", 1000))
CONCURRENCY_LIMIT = int(os.getenv("CONCURRENCY_LIMIT", 100))


# DB helpers
@contextmanager
def _db(path: Path, *, read_only: bool = False):
    conn = duckdb.connect(str(path), read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def _fetch_commits(
    conn: duckdb.DuckDBPyConnection, limit: int
) -> List[Tuple[str, str]]:
    rows = conn.execute(
        f"""
        SELECT commit_sha, commit_message
        FROM "{TABLE_COMMITS}"
        WHERE extracted_issue_key IS NULL
        LIMIT {limit};
        """
    ).fetchall()
    log.info("Fetched %d commits needing extraction", len(rows))
    return rows


def _batch_update(
    conn: duckdb.DuckDBPyConnection, updates: List[Tuple[str, str]]
) -> None:
    if not updates:
        return

    conn.executemany(
        f"""
        UPDATE "{TABLE_COMMITS}"
        SET extracted_issue_key = ?
        WHERE commit_sha = ?;
        """,
        updates,  # (key, sha)
    )
    conn.commit()
    log.info("Wrote %d extracted keys to DB", len(updates))


# Agent runner
async def _extract_key(sha: str, message: str) -> Tuple[str, str | None]:
    agent = build_agent("Issue_Key_Agent")
    if not agent:
        log.error("Agent build failed for SHA %s", sha)
        return sha, None

    try:
        resp: RunResponse = await agent.arun(message=message)
        key: str | None = (
            getattr(resp.content, "key", None) if resp and resp.content else None
        )
        key = key.strip() if key else ""  # ''->processed/no-match, None->error
        log.debug("SHA %s -> key '%s'", sha, key)
        return sha, key
    except Exception as exc:
        log.error("Agent raised for SHA %s: %s", sha, exc, exc_info=True)
        return sha, None


# Orchestration
async def _process_commits() -> None:
    # open DB once for the whole run
    with _db(DB_SUBSET) as conn:
        commits = _fetch_commits(conn, PROCESS_LIMIT)

        if not commits:
            log.info("Nothing to do.")
            return

        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        results: List[Tuple[str, str | None]] = []

        async def _wrap(sha: str, msg: str):
            async with semaphore:
                return await _extract_key(sha, msg)

        tasks = [_wrap(sha, msg) for sha, msg in commits]

        for coro in asyncio.as_completed(tasks):
            sha, key = await coro
            if key is not None:  # skip rows where agent errored
                results.append((key, sha))  # INSERT order = (key, sha)

            # flush in batches of CONCURRENCY_LIMIT to keep mem low
            if len(results) >= CONCURRENCY_LIMIT:
                _batch_update(conn, results)
                results.clear()

        # final flush
        _batch_update(conn, results)


# entry point
def main() -> None:
    log.info(
        "Extracting JIRA keys (limit=%d, concurrency=%d)",
        PROCESS_LIMIT,
        CONCURRENCY_LIMIT,
    )
    asyncio.run(_process_commits())
    log.info("Finished.")


if __name__ == "__main__":
    main()
