from __future__ import annotations
import os
import json
import duckdb
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from agno.agent import Agent, RunResponse
from typing import Any, Dict, List, Tuple
from pydantic import TypeAdapter, ValidationError

from scripts.paths import DATA_DIR
from models import IdentityInference
from utils.helpers import db_manager
from utils.logging_setup import setup_logging
from src.agents.agent_builder import build_agent
from src.tools.jira_lookup_tools import build_jira_lookup_tools

"""
This script focuses on resolving the identities of actors (authors and committers) involved in GitHub commits. Its purpose is to specifically link individuals performing code changes (commits) to their JIRA identities.

    1. Fetches distinct commit actor details from the GITHUB_COMMITS table in the staging DB.
    2. Enriches these with GitHub profile information from the main USERS_SUMMARY table, and then asynchronously calls an Agent for each unique actor signal. 
    3. The agent attempts to match this signal to JIRA user profiles. The resulting inferred links (including match type, confidence, reasoning, and any agent notes) are then upserted into a RESOLVED_PERSON_LINKS table in the staging DB. 
"""


# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
DB_SUB = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")
DB_MAIN = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")

T_COMMITS = "GITHUB_COMMITS"
T_USERSUMMARY = "USERS_SUMMARY"
SCHEMA_GH = f"{COMPANY}_GITHUB_"

T_RESOLVED = "RESOLVED_PERSON_LINKS"

LIMIT = int(os.getenv("RESOLVE_COMMITS_LIMIT", 50))
CONCURRENT = int(os.getenv("RESOLVE_CONCURRENCY_LIMIT", 10))

_ID_ADAPTER = TypeAdapter(IdentityInference)


# helpers
def _ensure_resolved_table(conn):
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {T_RESOLVED} (
            link_id UUID PRIMARY KEY DEFAULT uuid(),
            src_type TEXT NOT NULL,                -- e.g. COMMIT
            src_ref  TEXT NOT NULL,                -- commit_sha
            actor_role TEXT NOT NULL,              -- AUTHOR / COMMITTER
            github_user_id  TEXT,
            github_login    TEXT,
            git_name        TEXT,
            git_email       TEXT,
            gh_profile_name TEXT,
            gh_profile_email TEXT,
            jira_id     TEXT,
            jira_name   TEXT,
            jira_email  TEXT,
            match_type  TEXT,
            confidence  FLOAT,
            reasoning   TEXT,
            notes       TEXT,
            UNIQUE(src_type, src_ref, actor_role, github_user_id, github_login, git_email, jira_id)
        );
        """
    )


# data fetch
def _pending_commit_actors(
    sub: duckdb.DuckDBPyConnection,
    main: duckdb.DuckDBPyConnection,
    limit: int,
) -> List[Dict[str, Any]]:
    # load USERS_SUMMARY into dicts for quick enrichment
    u_by_id, u_by_login = {}, {}
    summary_rows = main.execute(
        f'''
    SELECT "ID","LOGIN","NAME","EMAIL"
    FROM "{SCHEMA_GH}"."{T_USERSUMMARY}";
    '''
    ).fetchall()
    for uid, login, name, email in summary_rows:
        entry = {"profile_name": name, "profile_email": email}
        if uid:
            u_by_id[str(uid)] = entry | {"id": uid, "login": login}
        if login:
            u_by_login[str(login)] = entry | {"id": uid, "login": login}

    rows = sub.execute(
        f"""
        SELECT commit_sha,
               'AUTHOR'    AS role, author_id, author_login, author_name, author_email
        FROM   {T_COMMITS}
        UNION ALL
        SELECT commit_sha,
               'COMMITTER', committer_id, committer_login, committer_name, committer_email
        FROM   {T_COMMITS}
        LIMIT {limit};
        """
    ).fetchall()

    actors: List[Dict[str, Any]] = []
    seen = set()
    for sha, role, gid, glog, gname, gemail in rows:
        key = (sha, role, gid or glog or gemail)
        if key in seen:
            continue
        seen.add(key)

        summary = u_by_id.get(str(gid)) if gid else None
        if summary is None and glog:
            summary = u_by_login.get(str(glog))

        if summary:
            prof_name = summary["profile_name"]
            prof_email = summary["profile_email"]
        else:
            prof_name = prof_email = None

        actors.append(
            {
                "sha": sha,
                "role": role,
                "github_user_id": str(gid) if gid else None,
                "github_login": str(glog) if glog else None,
                "git_name": gname,
                "git_email": str(gemail).lower() if gemail else None,
                "gh_profile_name": prof_name,
                "gh_profile_email": str(prof_email).lower() if prof_email else None,
            }
        )
    return actors


# agent runner
async def _call_agent(agent: Agent, sig: Dict[str, Any]) -> IdentityInference:
    """Return a valid model; never raise."""
    payload = json.dumps(
        {
            "github_user_id": sig["github_user_id"],
            "github_login": sig["github_login"],
            "git_name": sig["git_name"],
            "git_email": sig["git_email"],
            "github_profile_name": sig["gh_profile_name"],
            "github_profile_email": sig["gh_profile_email"],
        }
    )

    try:
        resp: RunResponse = await agent.arun(message=payload)
        content = resp.content if resp else None

        if isinstance(content, IdentityInference):
            return content

        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                log.info("Unparsable string for %s:%s → stub", sig["sha"], sig["role"])
                raise ValueError

        if isinstance(content, dict):
            return _ID_ADAPTER.validate_python(content)

        raise ValueError
    except (ValidationError, ValueError, Exception) as exc:
        # graceful stub
        return IdentityInference(
            github_user_id=sig["github_user_id"],
            github_login=sig["github_login"],
            git_name=sig["git_name"],
            git_email=sig["git_email"],
            github_profile_name=sig["gh_profile_name"],
            github_profile_email=sig["gh_profile_email"],
            matched_jira_profiles=[],
            notes=f"agent-error: {exc}",
        )


# batch upsert
def _upsert(conn: duckdb.DuckDBPyConnection, rows: List[Tuple]):
    if not rows:
        return
    conn.execute(
        f"DELETE FROM {T_RESOLVED} WHERE (src_type,src_ref,actor_role) IN ({','.join(['(?,?,?)'] * len(rows))});",
        [item for r in rows for item in r[:3]],  # flatten first three cols
    )
    conn.executemany(
        f"""
        INSERT INTO {T_RESOLVED} (
            src_type, src_ref, actor_role,
            github_user_id, github_login, git_name, git_email,
            gh_profile_name, gh_profile_email,
            jira_id, jira_name, jira_email,
            match_type, confidence, reasoning, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )
    conn.commit()
    log.info("Upserted %d rows into %s", len(rows), T_RESOLVED)


# pipeline
async def _pipeline(sub: duckdb.DuckDBPyConnection, main: duckdb.DuckDBPyConnection):
    actors = _pending_commit_actors(sub, main, LIMIT)
    if not actors:
        log.info("No commit actors to process.")
        return

    tools = build_jira_lookup_tools(sub)
    agent = build_agent(
        "Identity_Agent",
        tools=[tools],
    )

    sem = asyncio.Semaphore(CONCURRENT)
    rows = []

    async def _worker(a: Dict[str, Any]):
        async with sem:
            inf = await _call_agent(agent, a)
            first_match = (
                inf.matched_jira_profiles[0] if inf.matched_jira_profiles else None
            )
            rows.append(
                (
                    "COMMIT",
                    a["sha"],
                    a["role"],  # src_type/ref/role
                    inf.github_user_id,
                    inf.github_login,
                    inf.git_name,
                    inf.git_email,
                    inf.github_profile_name,
                    inf.github_profile_email,
                    first_match.jira_account_id if first_match else None,
                    first_match.jira_display_name if first_match else None,
                    first_match.jira_email_address if first_match else None,
                    first_match.match_type if first_match else None,
                    first_match.confidence if first_match else None,
                    first_match.reasoning if first_match else None,
                    inf.notes,
                )
            )

    await asyncio.gather(*[_worker(a) for a in actors])
    _ensure_resolved_table(sub)
    _upsert(sub, rows)


# entry point
def main() -> None:
    log.info(
        "Commit-actor identity resolution: limit=%d, concurrency=%d", LIMIT, CONCURRENT
    )
    with db_manager(DB_SUB) as sub, db_manager(DB_MAIN, read_only=True) as main:
        asyncio.run(_pipeline(sub, main))
    log.info("Done.")


if __name__ == "__main__":
    main()
