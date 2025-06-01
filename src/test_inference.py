from __future__ import annotations
import os
import json
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from agno.agent import RunResponse
from typing import Any, List, Dict, Tuple, Optional

from models import DeveloperInfo
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging

"""
Infers developer profiles from structured contribution data (diffs that have been processed/summarized by a prior LLM process) using an AI agent.

Description
-----------
Analyzes structured contribution records for each committer and infers high-level developer profile attributes including role, experience level, skills, justification, and a summary analysis.

    1. Loads structured contributions as JSON strings for committers.
    2. Parses and validates contribution data.
    3. Invokes agents to infer developer characteristics from contribution history.
    4. Converts agent output to structured fields and inserts the results into the target table.
"""


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_PREPROCESSED_SOURCE = "PREPROCESSED_DIFFS"
T_INFERENCE_OUTPUT = "DEVELOPER_PROFILE_INFERENCE"
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
LIMIT = int(os.getenv("PROFILE_INFER_LIMIT", 1000))
CONCUR = int(os.getenv("PROFILE_INFER_CONCURRENCY", 50))
AGENT_KEY = "Developer_Inference"


# SQL helpers ------------------------------------------------------------------
def _load_committers_with_structured_contributions(
    conn, limit: int
) -> List[
    Tuple[str, Optional[str], Optional[str]]
]:  # COMMITTER_ID, COMMITTER_NAME, STRUCTURED_CONTRIBUTIONS (JSON string)
    """
    Load committers with their name and structured contribution data for inference, avoiding duplicates if T_INFERENCE_OUTPUT exists.
    """
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}

    if T_INFERENCE_OUTPUT in tables:
        q = f"""
            SELECT COMMITTER_ID, COMMITTER_NAME, STRUCTURED_CONTRIBUTIONS
            FROM   {T_PREPROCESSED_SOURCE}
            WHERE  COMMITTER_ID NOT IN (SELECT COMMITTER_ID FROM {T_INFERENCE_OUTPUT})
              AND STRUCTURED_CONTRIBUTIONS IS NOT NULL AND STRUCTURED_CONTRIBUTIONS != '{{""contributions"": []}}' AND STRUCTURED_CONTRIBUTIONS != '[]'
            LIMIT  {limit};
        """
    else:
        q = f"""
            SELECT COMMITTER_ID, COMMITTER_NAME, STRUCTURED_CONTRIBUTIONS
            FROM   {T_PREPROCESSED_SOURCE}
            WHERE  STRUCTURED_CONTRIBUTIONS IS NOT NULL AND STRUCTURED_CONTRIBUTIONS != '{{""contributions"": []}}' AND STRUCTURED_CONTRIBUTIONS != '[]'
            LIMIT  {limit};
        """
    return conn.execute(q).fetchall()


def _insert_developer_profile_analysis(
    conn,
    rows: List[
        Tuple[
            str,  # COMMITTER_ID
            Optional[str],  # COMMITTER_NAME
            Optional[str],  # ANALYSIS
            Optional[str],  # ROLE
            Optional[str],  # EXPERIENCE_LEVEL
            Optional[str],  # SKILLS (comma-separated string)
            Optional[str],  # JUSTIFICATION
        ]
    ],
):
    if not rows:
        log.debug("No developer profile inference rows to insert.")
        return

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_INFERENCE_OUTPUT} (
            COMMITTER_ID TEXT PRIMARY KEY,
            COMMITTER_NAME TEXT,
            ANALYSIS TEXT,
            ROLE TEXT,
            EXPERIENCE_LEVEL TEXT,
            SKILLS TEXT,
            JUSTIFICATION TEXT
        );
    """)

    conn.executemany(
        f"""
        INSERT INTO {T_INFERENCE_OUTPUT} (
            COMMITTER_ID, COMMITTER_NAME, ANALYSIS, ROLE,
            EXPERIENCE_LEVEL, SKILLS, JUSTIFICATION
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING;
        """,
        rows,
    )
    conn.commit()
    log.info("Inserted developer profile inference data for %d committers", len(rows))


# Agent logic ------------------------------------------------------------------
async def _infer_developer_profile(
    committer_id: str,
    committer_name: Optional[str],
    structured_contributions_json_str: Optional[str],
) -> Tuple[
    str,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    default_return = (
        committer_id,
        committer_name,
        None,
        None,
        None,
        None,
        None,
    )

    try:
        # Parse the JSON string from the database into a dictionary
        parsed_input_data: Dict[str, Any] = json.loads(
            structured_contributions_json_str
        )

        # Extract the actual list of contributions
        contributions_list: List[Dict[str, Any]] = parsed_input_data.get(
            "contributions", []
        )
        if not contributions_list:
            log.warning(
                f"Committer {committer_id} has an empty 'contributions' list or malformed JSON after parsing."
            )
            return default_return

        agent = build_agent(AGENT_KEY)
        log.debug(
            f"Inferring profile for committer {committer_id}. Number of contributions: {len(contributions_list)}"
        )

        resp: RunResponse = await agent.arun(structured_json_input=contributions_list)
        if resp and isinstance(resp.content, DeveloperInfo):
            info: DeveloperInfo = resp.content
            log.info(
                "Developer profile inference complete for committer %s. Role: %s, Level: %s",
                committer_id,
                info.role,
                info.experience_level,
            )
            skills_str = ", ".join(info.skills) if info.skills else None
            return (
                committer_id,
                committer_name,
                info.analysis,
                info.role,
                info.experience_level,
                skills_str,
                info.justification,
            )

        log.warning(
            "Committer %s profile inference returned no valid DeveloperInfo object. Response type: %s",
            committer_id,
            type(resp.content) if resp else "None",
        )
        return default_return

    except json.JSONDecodeError as json_err:
        log.error(
            "Failed to parse structured_contributions_json for committer %s: %s. JSON string: %s",
            committer_id,
            json_err,
            structured_contributions_json_str[:500],
        )
        return default_return
    except Exception as exc:
        log.error(
            "Developer profile inference agent error for committer %s: %s",
            committer_id,
            exc,
            exc_info=True,
        )
        return default_return


# Concurrency helper -----------------------------------------------------------
async def _bounded_infer_profile(
    row: Tuple[str, Optional[str], Optional[str]], sem: asyncio.Semaphore
) -> Tuple[
    str,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    committer_id, committer_name, contributions_json_str = row
    async with sem:
        return await _infer_developer_profile(
            committer_id, committer_name, contributions_json_str
        )


# Main async -------------------------------------------------------------------
async def _run_inference():
    with db_manager(DB_PATH) as conn:
        rows_to_infer = _load_committers_with_structured_contributions(conn, LIMIT)
        if not rows_to_infer:
            log.info("No committers need profile inference based on preprocessed data.")
            return

        log.info(
            "Starting developer profile inference for %d committers.",
            len(rows_to_infer),
        )
        sem = asyncio.Semaphore(CONCUR)
        tasks = [_bounded_infer_profile(row, sem) for row in rows_to_infer]
        results = await asyncio.gather(*tasks)

        valid_results = [
            res for res in results if res is not None and res[2] is not None
        ]  # Check if analysis is not None
        _insert_developer_profile_analysis(conn, valid_results)


# Entry point ------------------------------------------------------------------
def main():
    log.info(
        "Starting developer profile inference script (limit=%d, concur=%d)",
        LIMIT,
        CONCUR,
    )
    asyncio.run(_run_inference())
    log.info("Finished developer profile inference.")


if __name__ == "__main__":
    main()
