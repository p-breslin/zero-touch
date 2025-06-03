from __future__ import annotations
import os
import json
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from agno.agent import RunResponse
from typing import List, Tuple, Optional

from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging
from src.agents.agent_builder import build_agent
from models import InferenceOutput, DeveloperInfo
from utils.helpers import db_manager, validate_output

"""
Infers developer profiles from structured commit summaries using AI agents.

Description
-----------
Generates high-level developer profiles based on structured summaries of commit activity. Parses prior outputs from commit-level diff preprocessing and uses an AI agent to infer role, experience level, skills, and justification. Results are stored in the DEVELOPER_INFERENCE table.

    1. Loads structured commit summaries from INFERENCE_INFO.
    2. Sends structured input to an agent to extract DeveloperInfo attributes.
    4. Builds a row with DB_ID, display name, inferred role, experience level, skillset, analysis, and justification.
    5. Inserts results into DEVELOPER_INFERENCE (one row per developer).
"""


# Config -----------------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_INPUT = "INFERENCE_INFO"
T_OUTPUT = "DEVELOPER_INFERENCE"
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

LIMIT = int(os.getenv("PROFILE_INFER_LIMIT", 100))
CONCUR = int(os.getenv("PROFILE_INFER_CONCURRENCY", 5))
# AGENT_KEY = "Developer_Inference"
AGENT_KEY = "Developer_Inference_gemini"


# Helpers ----------------------------------------------------------------------
def pydantic_to_gemini(output_model: InferenceOutput) -> str:
    return json.dumps(output_model.model_dump(), ensure_ascii=False, indent=None)


# Load committers with data ----------------------------------------------------
def _load_committers_with_diff_outputs(conn, limit: int) -> List[Tuple[str, str, str]]:
    """
    Loads (DB_ID, JIRA_DISPLAY_NAME, SUMMARIES) by joining INFERENCE_INFO and MATCHED_USERS.
    """
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    where_clause = "WHERE i.SUMMARIES IS NOT NULL"

    if T_OUTPUT in tables:
        where_clause += f" AND u.DB_ID NOT IN (SELECT DB_ID FROM {T_OUTPUT})"

    q = f"""
        SELECT u.DB_ID, u.JIRA_DISPLAY_NAME, i.SUMMARIES
        FROM {T_INPUT} i
        JOIN MATCHED_USERS u ON i.GITHUB_ID = u.GITHUB_ID
        {where_clause}
        LIMIT {limit};
    """
    return conn.execute(q).fetchall()


# Write output to database -----------------------------------------------------
def _insert_inferred_profiles(
    conn, rows: List[Tuple[str, str, str, str, str, str, str]]
):
    if not rows:
        log.debug("No developer profiles to insert.")
        return

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_OUTPUT} (
            DB_ID TEXT PRIMARY KEY,
            JIRA_DISPLAY_NAME TEXT,
            ROLE TEXT,
            EXPERIENCE_LEVEL TEXT,
            SKILLS TEXT,
            ANALYSIS TEXT,
            JUSTIFICATION TEXT
        );
    """)

    conn.executemany(
        f"""
        INSERT INTO {T_OUTPUT} (
            DB_ID, JIRA_DISPLAY_NAME, ROLE, EXPERIENCE_LEVEL, SKILLS, ANALYSIS, JUSTIFICATION
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING;
    """,
        rows,
    )

    conn.commit()
    log.info("Inserted developer profiles for %d users", len(rows))


# Inference logic --------------------------------------------------------------
async def _infer_profile(
    db_id: str, display_name: str, summaries_json: str
) -> Optional[Tuple[str, str, str, str, str, str, str]]:
    try:
        parsed = InferenceOutput.model_validate_json(summaries_json)
        if not parsed or not parsed.commits:
            log.warning(f"Empty commit list for {display_name}")
            return None

        agent = build_agent(AGENT_KEY)

        if "gemini" in AGENT_KEY:
            prompt = pydantic_to_gemini(parsed)
            resp: RunResponse = await agent.arun(prompt)
            info = validate_output(resp.content, DeveloperInfo)

        else:
            resp: RunResponse = await agent.arun(structured_json_input=parsed)
            info = resp.content if isinstance(resp.content, DeveloperInfo) else None

        if info:
            skills_str = ", ".join(info.skills) if info.skills else ""
            log.info(
                "Profile for %s: %s (%s)",
                display_name,
                info.role,
                info.experience_level,
            )
            return (
                db_id,
                display_name,
                info.role,
                info.experience_level,
                skills_str,
                info.analysis,
                info.justification,
            )
        else:
            log.warning(f"No valid DeveloperInfo returned for {display_name}")
            return (
                db_id,
                display_name,
                info.role,
                info.experience_level,
                skills_str,
                info.analysis,
                info.justification,
            )
    except Exception as exc:
        log.error("Inference failed for %s: %s", display_name, exc, exc_info=True)
        return None


# Concurrency wrapper ----------------------------------------------------------
async def _bounded_infer(
    row: Tuple[str, str, str], sem: asyncio.Semaphore
) -> Optional[Tuple[str, str, str, str, str, str, str]]:
    db_id, jira_display_name, summaries_json = row
    async with sem:
        return await _infer_profile(db_id, jira_display_name, summaries_json)


# Async main logic -------------------------------------------------------------
async def _run_inference():
    with db_manager(DB_PATH) as conn:
        rows = _load_committers_with_diff_outputs(conn, LIMIT)
        if not rows:
            log.info("No committers need profile inference.")
            return

        log.info("Inferring profiles for %d developers", len(rows))
        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_infer(row, sem) for row in rows]
        results = await asyncio.gather(*tasks)

        valid = [r for r in results if r]
        _insert_inferred_profiles(conn, valid)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Starting developer inference (limit=%d, concur=%d)", LIMIT, CONCUR)
    asyncio.run(_run_inference())
    log.info("Done.")


if __name__ == "__main__":
    main()
