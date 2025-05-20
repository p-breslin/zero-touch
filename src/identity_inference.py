from __future__ import annotations
import os
import json
import duckdb
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Dict, List
from contextlib import contextmanager
from agno.agent import Agent, RunResponse

from scripts.paths import DATA_DIR
from models import IdentityInference
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging
from tools.jira_lookup_tools import build_jira_lookup_tools


# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_SUBSET = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")

SRC_SIGNALS = "GITHUB_IDENTITY_SIGNALS"
SRC_JIRA_USERS = "JIRA_USER_PROFILES"
TGT_RESOLVED = "RESOLVED_PERSON_LINKS"

LIMIT = int(os.getenv("PERSON_MATCH_PROCESS_LIMIT", 500))
CONCURRENT = int(os.getenv("PERSON_MATCH_CONCURRENCY_LIMIT", 10))


# helpers
@contextmanager
def _db(path: Path, *, read_only: bool = False):
    c = duckdb.connect(str(path), read_only=read_only)
    try:
        yield c
    finally:
        c.close()


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TGT_RESOLVED} (
            link_id                   UUID PRIMARY KEY DEFAULT uuid(),
            signal_fingerprint        TEXT NOT NULL,
            github_user_id            TEXT,
            github_login              TEXT,
            git_name                  TEXT,
            git_email                 TEXT,
            github_profile_name       TEXT,
            github_profile_email      TEXT,
            matched_jira_account_id   TEXT,
            matched_jira_display_name TEXT,
            matched_jira_email        TEXT,
            match_type                TEXT,
            match_confidence          FLOAT,
            match_reasoning           TEXT,
            agent_notes               TEXT,
            FOREIGN KEY (signal_fingerprint)
              REFERENCES {SRC_SIGNALS}(signal_fingerprint)
        );
        """
    )


def _pending_signals(
    conn: duckdb.DuckDBPyConnection, limit: int
) -> List[Dict[str, Any]]:
    cur = conn.execute(
        f"""
        SELECT signal_fingerprint, github_user_id, github_login,
               git_name, git_email, github_profile_name, github_profile_email
        FROM "{SRC_SIGNALS}"
        WHERE signal_fingerprint NOT IN (
            SELECT DISTINCT signal_fingerprint FROM {TGT_RESOLVED}
        )
        LIMIT {limit};
        """
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# JIRA helper tools (email / name search)
def _search_by_email(
    conn: duckdb.DuckDBPyConnection, email: str
) -> List[Dict[str, Any]]:
    if not email:
        return []
    cur = conn.execute(
        f"""SELECT jira_account_id, jira_display_name, jira_email_address
            FROM {SRC_JIRA_USERS}
            WHERE jira_email_address = ?;""",
        (email.lower(),),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _search_by_name(
    conn: duckdb.DuckDBPyConnection, q: str, limit: int = 5
) -> List[Dict[str, Any]]:
    if not q:
        return []
    cur = conn.execute(
        f"""SELECT jira_account_id, jira_display_name, jira_email_address
            FROM {SRC_JIRA_USERS}
            WHERE jira_display_name ILIKE ?
            LIMIT ?;""",
        (f"%{q}%", limit),
    )
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# agent runner
async def _run_agent(agent: Agent, signal: Dict[str, Any]) -> IdentityInference:
    payload = json.dumps(
        {
            "github_user_id": signal["github_user_id"],
            "github_login": signal["github_login"],
            "git_name": signal["git_name"],
            "git_email": signal["git_email"],
            "github_profile_name": signal["github_profile_name"],
            "github_profile_email": signal["github_profile_email"],
        }
    )
    try:
        resp: RunResponse = await agent.arun(message=payload)
        if resp and isinstance(resp.content, IdentityInference):
            return resp.content
        raise TypeError("Unexpected agent response")
    except Exception as exc:
        log.error(
            "Agent failed for %s: %s", signal["signal_fingerprint"], exc, exc_info=True
        )
        return IdentityInference(
            github_user_id=signal["github_user_id"],
            github_login=signal["github_login"],
            git_name=signal["git_name"],
            git_email=signal["git_email"],
            github_profile_name=signal["github_profile_name"],
            github_profile_email=signal["github_profile_email"],
            matched_jira_profiles=[],
            notes=f"Agent error: {exc}",
        )


# batch insert
def _insert_links(
    conn: duckdb.DuckDBPyConnection, outs: List[IdentityInference]
) -> None:
    if not outs:
        return
    _ensure_table(conn)

    rows, fps = [], set()
    for out in outs:
        fp = "|".join(
            str(v or "none")
            for v in (
                out.github_user_id,
                out.github_login,
                out.git_email,
                out.git_name,
                out.github_profile_email,
                out.github_profile_name,
            )
        )
        if not out.matched_jira_profiles:  # no match
            rows.append(
                (
                    fp,
                    out.github_user_id,
                    out.github_login,
                    out.git_name,
                    out.git_email,
                    out.github_profile_name,
                    out.github_profile_email,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    out.notes,
                )
            )
        else:
            for m in out.matched_jira_profiles:
                rows.append(
                    (
                        fp,
                        out.github_user_id,
                        out.github_login,
                        out.git_name,
                        out.git_email,
                        out.github_profile_name,
                        out.github_profile_email,
                        m.jira_account_id,
                        m.jira_display_name,
                        m.jira_email_address,
                        m.match_type,
                        m.confidence,
                        m.reasoning,
                        out.notes,
                    )
                )
        fps.add(fp)

    # replace (delete+insert) to avoid dupes on re-run
    conn.execute(
        f"DELETE FROM {TGT_RESOLVED} WHERE signal_fingerprint IN ({','.join(['?'] * len(fps))});",
        list(fps),
    )
    conn.executemany(
        f"""
        INSERT INTO {TGT_RESOLVED} (
            signal_fingerprint, github_user_id, github_login,
            git_name, git_email, github_profile_name, github_profile_email,
            matched_jira_account_id, matched_jira_display_name, matched_jira_email,
            match_type, match_confidence, match_reasoning, agent_notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )
    conn.commit()
    log.info("Inserted %d rows for %d fingerprints", len(rows), len(fps))


# main async pipeline
async def _pipeline() -> None:
    with _db(DB_SUBSET) as conn:
        signals = _pending_signals(conn, LIMIT)
        if not signals:
            log.info("Nothing to resolve.")
            return

        # Build the Jira lookup toolkit bound to this live connection
        jira_tools = build_jira_lookup_tools(conn)

        # Pass the toolkit to the agent builder
        agent = build_agent("Identity_Agent", tools=[jira_tools])
        if not agent:
            log.error("Could not build agent.")
            return

        sem = asyncio.Semaphore(CONCURRENT)
        outs: List[IdentityInference] = []

        async def _worker(sig: Dict[str, Any]):
            async with sem:
                return await _run_agent(agent, sig)

        tasks = [_worker(s) for s in signals]
        for idx, coro in enumerate(asyncio.as_completed(tasks), 1):
            outs.append(await coro)
            if idx % CONCURRENT == 0:
                log.info("Processed %d/%d", idx, len(tasks))

        _insert_links(conn, outs)


# entry point
def main() -> None:
    log.info("Identity-resolution: limit=%d, concurrency=%d", LIMIT, CONCURRENT)
    asyncio.run(_pipeline())
    log.info("Done")


if __name__ == "__main__":
    main()
