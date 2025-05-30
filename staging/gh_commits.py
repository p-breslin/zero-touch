"""
Populates GITHUB_COMMITS (staging DB) with all commit records from the last 90 days. Purpose is to prepare them for JIRA issue-key extraction and subsequent analysis.

Steps
-----
1. Read all rows from COMMITS (source DB) whose COMMIT_TIMESTAMP >= cutoff.
2. Flatten + insert into GITHUB_COMMITS (ON CONFLICT DO NOTHING)
"""

from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from typing import Any, Dict, List, Tuple
from datetime import datetime, timedelta, timezone


from utils.logging_setup import setup_logging
from utils.helpers import db_manager, safe_json


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_GH = f"{COMPANY}_GITHUB_"

SRC_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_COMMITS = "GITHUB_COMMITS"
DAYS_BACK = 90  # how far back in time to fetch commits

# DDL
DDL_COMMITS = f"""
CREATE TABLE IF NOT EXISTS {T_COMMITS} (
    ORG                TEXT,
    REPO               TEXT,
    COMMIT_SHA         TEXT PRIMARY KEY,
    COMMIT_TIMESTAMP   TIMESTAMP,
    AUTHOR_ID          TEXT,
    AUTHOR_NAME        TEXT,
    AUTHOR_EMAIL       TEXT,
    AUTHOR_LOGIN       TEXT,
    COMMITTER_ID       TEXT,
    COMMITTER_NAME     TEXT,
    COMMITTER_EMAIL    TEXT,
    COMMITTER_LOGIN    TEXT,
    COMMIT_MESSAGE     TEXT,
    EXTRACTED_JIRA_KEY TEXT
);
"""


# Helpers ----------------------------------------------------------------------
def _recent_commit_rows(conn, since: datetime) -> List[Tuple[Any, ...]]:
    """Return rows (sha, org, repo, commit_blob, author_blob, committer_blob, ts)."""
    q = f"""
        SELECT "SHA","ORG","REPO","COMMIT","AUTHOR","COMMITTER","COMMIT_TIMESTAMP"
        FROM "{SCHEMA_GH}"."COMMITS"
        WHERE "COMMIT_TIMESTAMP" >= ?
        ORDER BY "COMMIT_TIMESTAMP" DESC;
    """
    return conn.execute(q, (since,)).fetchall()


# Record builder ---------------------------------------------------------------
def _build_records() -> List[Dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    with db_manager(SRC_DB, read_only=True) as conn:
        rows = _recent_commit_rows(conn, cutoff)

    if not rows:
        log.info("No commits in the last %d days.", DAYS_BACK)
        return []

    records: List[Dict[str, Any]] = []
    for sha, org, repo, c_blob, a_blob, cm_blob, ts in rows:
        commit = safe_json(c_blob) or {}
        author_j = safe_json(a_blob) or {}
        comm_j = safe_json(cm_blob) or {}

        author_m = commit.get("author") or {}
        comm_m = commit.get("committer") or {}

        records.append(
            dict(
                ORG=org,
                REPO=repo,
                COMMIT_SHA=sha,
                COMMIT_TIMESTAMP=ts,
                AUTHOR_ID=str(author_j.get("id")) if author_j.get("id") else None,
                AUTHOR_NAME=author_m.get("name"),
                AUTHOR_EMAIL=author_m.get("email"),
                AUTHOR_LOGIN=author_j.get("login"),
                COMMITTER_ID=str(comm_j.get("id")) if comm_j.get("id") else None,
                COMMITTER_NAME=comm_m.get("name"),
                COMMITTER_EMAIL=comm_m.get("email"),
                COMMITTER_LOGIN=comm_j.get("login"),
                COMMIT_MESSAGE=commit.get("message"),
                EXTRACTED_JIRA_KEY=None,
            )
        )
    log.info("Built %d commit records (cutoff %s).", len(records), cutoff.isoformat())
    return records


# Insert -----------------------------------------------------------------------
def _insert(records: List[Dict[str, Any]]):
    if not records:
        return

    with db_manager(STG_DB) as conn:
        conn.execute(DDL_COMMITS)
        conn.executemany(
            f"""
            INSERT INTO "{T_COMMITS}" VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            ) ON CONFLICT (COMMIT_SHA) DO NOTHING;
            """,
            [
                (
                    r["ORG"],
                    r["REPO"],
                    r["COMMIT_SHA"],
                    r["COMMIT_TIMESTAMP"],
                    r["AUTHOR_ID"],
                    r["AUTHOR_NAME"],
                    r["AUTHOR_EMAIL"],
                    r["AUTHOR_LOGIN"],
                    r["COMMITTER_ID"],
                    r["COMMITTER_NAME"],
                    r["COMMITTER_EMAIL"],
                    r["COMMITTER_LOGIN"],
                    r["COMMIT_MESSAGE"],
                    r["EXTRACTED_JIRA_KEY"],
                )
                for r in records
            ],
        )
        conn.commit()
        log.info("Inserted %d rows into %s", len(records), T_COMMITS)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging commit data from the last %d days.", DAYS_BACK)
    _insert(_build_records())
    log.info("Done staging commits.")


if __name__ == "__main__":
    main()
