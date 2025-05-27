"""
Runs the Identity_Inference agent against rows in the ALL_IDENTITIES table, storing consolidated entities and ambiguous links back to the staging database.
"""

from __future__ import annotations
import os
import json
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from agno.agent import RunResponse
from typing import List, Dict, Tuple, Any, Sequence

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging
from models import IdentityInference, ConsolidatedEntity, AmbiguousLink


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
T_IDENTITIES = "ALL_IDENTITIES"
T_CONSOLIDATED = "CONSOLIDATED_IDENTITIES"
T_AMBIGUOUS = "AMBIGUOUS_LINKS"
AGENT_KEY = "Identity_Inference"


# Helpers ------------------------------------------------------------------
def _load_identity_rows(conn) -> List[Dict[str, Any]]:
    """Fetches table rows from the database."""
    q = f"SELECT * FROM {T_IDENTITIES};"
    log.debug("Loading identities with query: %s", q)
    rows = conn.execute(q).fetchdf()
    return rows.to_dict("records")


def _sanitize_for_json(obj: Any) -> Any:
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if hasattr(obj, "tolist"):
        return obj.tolist()
    return obj


# Table-creation & update helpers ----------------------------------------------
def _ensure_output_tables(conn):
    """Creates output tables if they don't already exist."""

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {T_CONSOLIDATED} (
            canonical_name         TEXT UNIQUE,
            original_names         TEXT,
            all_jira_ids           TEXT,
            all_github_ids         TEXT,
            all_pr_user_ids        TEXT,
            all_emails             TEXT,
            is_bot_or_system       BOOLEAN,
            notes                  TEXT
        );
        """
    )

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {T_AMBIGUOUS} (
            entity1_identifiers    TEXT,
            entity2_identifiers    TEXT,
            reason_for_ambiguity   TEXT
        );
        """
    )
    conn.commit()


def _update_consolidated_entities(conn, entities: Sequence[ConsolidatedEntity]):
    if not entities:
        log.info("No consolidated entities to upsert.")
        return

    rows: List[Tuple[Any, ...]] = []
    for e in entities:
        rows.append(
            (
                e.canonical_name,
                ",".join(sorted(e.original_names_user_names)),
                ",".join(sorted(e.all_jira_ids)),
                ",".join(sorted(e.all_github_ids)),
                ",".join(sorted(e.all_pr_user_ids)),
                ",".join(sorted(e.all_emails)),
                e.is_bot_or_system,
                e.notes or "",
            )
        )

    conn.executemany(
        f"""
        INSERT INTO {T_CONSOLIDATED} (
            canonical_name, original_names, all_jira_ids, all_github_ids,
            all_pr_user_ids, all_emails, is_bot_or_system, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(canonical_name) DO UPDATE SET
            original_names      = EXCLUDED.original_names,
            all_jira_ids        = EXCLUDED.all_jira_ids,
            all_github_ids      = EXCLUDED.all_github_ids,
            all_pr_user_ids     = EXCLUDED.all_pr_user_ids,
            all_emails          = EXCLUDED.all_emails,
            is_bot_or_system    = EXCLUDED.is_bot_or_system,
            notes               = EXCLUDED.notes;
        """,
        rows,
    )
    conn.commit()
    log.info("Upserted %d consolidated entities", len(rows))


def _update_ambiguous_links(conn, links: Sequence[AmbiguousLink]):
    if not links:
        log.info("No ambiguous links to insert.")
        return

    rows: List[Tuple[str, str, str]] = []
    for l in links:
        rows.append(
            (
                json.dumps(l.entity1_identifiers),
                json.dumps(l.entity2_identifiers),
                l.reason_for_ambiguity,
            )
        )

    conn.executemany(
        f"INSERT INTO {T_AMBIGUOUS} (entity1_identifiers, entity2_identifiers, reason_for_ambiguity) VALUES (?, ?, ?);",
        rows,
    )
    conn.commit()
    log.info("Inserted %d ambiguous links", len(rows))


# Agent logic ------------------------------------------------------------------
async def _infer_identities(records: List[Dict[str, Any]]) -> IdentityInference | None:
    """Runs the agent on the given records."""
    if not records:
        log.warning("No identity records supplied to the agent.")
        return None

    try:
        # Passing the records as JSON so the agent sees explicit column names
        agent = build_agent(AGENT_KEY)
        sanitized_records = _sanitize_for_json(records)
        payload = json.dumps(sanitized_records)
        resp: RunResponse = await agent.arun(message=payload)
        if resp and isinstance(resp.content, IdentityInference):
            return resp.content
        log.error("Agent response did not conform to IdentityInference model.")
    except Exception as exc:
        log.exception("Identity inference agent error: %s", exc)
    return None


# Runner -----------------------------------------------------------------------
async def _run():
    with db_manager(DB_PATH) as conn:
        _ensure_output_tables(conn)

        records = _load_identity_rows(conn)
        if not records:
            log.info("No identities found to process.")
            return

    inference = await _infer_identities(records)
    if inference is None:
        log.warning("No inference generated; aborting update.")
        return

    # Re-open connection for the update phase
    with db_manager(DB_PATH) as conn:
        _update_consolidated_entities(conn, inference.consolidated_entities)
        _update_ambiguous_links(conn, inference.ambiguous_links)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Running identity inference against %s", DB_PATH.name)
    asyncio.run(_run())
    log.info("Finished identity inference run")


if __name__ == "__main__":
    main()
