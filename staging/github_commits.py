"""
Populates GITHUB_COMMITS (staging DB) with commit records only for Pull Requests that were recently staged in GITHUB_PRS. Purpose is to prepare it for JIRA issue key extraction and subsequent analysis.

Pipeline
--------
1. Read distinct (NUMBER, ORG, REPO) from GITHUB_PRS    [staging DB]
2. Fetch SHAs for those PRs from PULL_REQUEST_COMMITS   [source DB]
3. Pull full commit blobs for those SHAs from COMMITS   [source DB]
4. Flatten + insert into GITHUB_COMMITS (ON CONFLICT DO NOTHING)
"""

from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from typing import Any, Dict, List, Set, Tuple

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
T_STAGED_PRS = "GITHUB_PRS"

# DDL
DDL_COMMITS = f"""
CREATE TABLE IF NOT EXISTS {T_COMMITS} (
    COMMIT_SHA          TEXT PRIMARY KEY,
    COMMIT_TIMESTAMP    TIMESTAMP,
    EXTRACTED_ISSUE_KEY TEXT,
    AUTHOR_ID           TEXT,
    AUTHOR_NAME         TEXT,
    AUTHOR_EMAIL        TEXT,
    AUTHOR_LOGIN        TEXT,
    COMMITTER_ID        TEXT,
    COMMITTER_NAME      TEXT,
    COMMITTER_EMAIL     TEXT,
    COMMITTER_LOGIN     TEXT,
    COMMIT_MESSAGE      TEXT
);
"""


# Helpers ----------------------------------------------------------------------
def _pr_keys(conn) -> List[Tuple[int, str, str]]:
    """
    Queries the GITHUB_PRS table in the staging DB. Retrieves and returns a list of distinct tuples, where each tuple contains the NUMBER, ORG, and REPO identifying a recently staged pull request.
    """
    q = f"""SELECT DISTINCT "NUMBER","ORG","REPO" FROM "{T_STAGED_PRS}";"""
    return conn.execute(q).fetchall()


def _shas_for_prs(conn, keys: List[Tuple[int, str, str]]) -> Set[str]:
    """
    Takes a list of PR identifiers (number, org, repo) and queries the PULL_REQUEST_COMMITS table in source DB. Finds all distinct commit SHAs associated with any of the provided PRs and returns them as a set.
    """
    if not keys:
        return set()

    value_placeholders = ",".join(["(?, ?, ?)"] * len(keys))
    params = [v for triple in keys for v in triple]

    q = f"""
        WITH pr_keys (PULL_NUMBER, ORG, REPO) AS (
            VALUES {value_placeholders}
        )
        SELECT DISTINCT prc."SHA"
        FROM "{SCHEMA_GH}"."PULL_REQUEST_COMMITS" AS prc
        JOIN pr_keys pk
          ON prc."PULL_NUMBER" = pk.PULL_NUMBER
         AND prc."ORG"         = pk.ORG
         AND prc."REPO"        = pk.REPO;
    """
    rows = conn.execute(q, params).fetchall()
    return {r[0] for r in rows}


def _commit_rows(conn, shas: Set[str]) -> List[Tuple[Any, ...]]:
    """
    Takes a set of commit SHAs and queries the main COMMITS table in the source DB. Retrieves the full details (SHA, commit JSON blob, author JSON blob, committer JSON blob, and commit timestamp) for each of these specific commits, ordered by timestamp.
    """
    if not shas:
        return []

    placeholders = ",".join("?" * len(shas))
    q = f"""
        SELECT "SHA","COMMIT","AUTHOR","COMMITTER","COMMIT_TIMESTAMP"
        FROM "{SCHEMA_GH}"."COMMITS"
        WHERE "SHA" IN ({placeholders})
        ORDER BY "COMMIT_TIMESTAMP" DESC;
    """
    return conn.execute(q, list(shas)).fetchall()


# Record builder ---------------------------------------------------------------
def _build_records() -> List[Dict[str, Any]]:
    """
    1. Gets recently staged PR keys from staging DB.
    2. Pulls corresponding commit SHAs from source DB.
    3. Fetches commit rows and transform them into dicts ready for insertion.
    """
    with db_manager(STG_DB, read_only=True) as conn:
        pr_keys = _pr_keys(conn)

    if not pr_keys:
        log.info("No staged PRs -> nothing to stage for commits.")
        return []

    with db_manager(SRC_DB, read_only=True) as src:
        sha_set = _shas_for_prs(src, pr_keys)
        rows = _commit_rows(src, sha_set)

    records: List[Dict[str, Any]] = []
    for sha, commit_blob, author_blob, committer_blob, ts in rows:
        commit = safe_json(commit_blob) or {}
        author_j = safe_json(author_blob) or {}
        comm_j = safe_json(committer_blob) or {}

        author_m = commit.get("author") or {}
        comm_m = commit.get("committer") or {}

        records.append(
            dict(
                COMMIT_SHA=sha,
                COMMIT_TIMESTAMP=ts,
                EXTRACTED_ISSUE_KEY=None,
                AUTHOR_ID=str(author_j.get("id")) if author_j.get("id") else None,
                AUTHOR_NAME=author_m.get("name"),
                AUTHOR_EMAIL=author_m.get("email"),
                AUTHOR_LOGIN=author_j.get("login"),
                COMMITTER_ID=str(comm_j.get("id")) if comm_j.get("id") else None,
                COMMITTER_NAME=comm_m.get("name"),
                COMMITTER_EMAIL=comm_m.get("email"),
                COMMITTER_LOGIN=comm_j.get("login"),
                COMMIT_MESSAGE=commit.get("message"),
            )
        )
    log.info("Built %d commit records from %d SHAs.", len(records), len(rows))
    return records


# Insert -----------------------------------------------------------------------
def _insert(records: List[Dict[str, Any]]):
    """
    Takes the list of processed commit records (from _build_records) and inserts them into the GITHUB_COMMITS table in the staging DB.
    """
    if not records:
        return
    with db_manager(STG_DB) as conn:
        conn.execute(DDL_COMMITS)

        conn.executemany(
            f"""
            INSERT INTO "{T_COMMITS}" VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            ) ON CONFLICT (COMMIT_SHA) DO NOTHING;
            """,
            [
                (
                    r["COMMIT_SHA"],
                    r["COMMIT_TIMESTAMP"],
                    r["EXTRACTED_ISSUE_KEY"],
                    r["AUTHOR_ID"],
                    r["AUTHOR_NAME"],
                    r["AUTHOR_EMAIL"],
                    r["AUTHOR_LOGIN"],
                    r["COMMITTER_ID"],
                    r["COMMITTER_NAME"],
                    r["COMMITTER_EMAIL"],
                    r["COMMITTER_LOGIN"],
                    r["COMMIT_MESSAGE"],
                )
                for r in records
            ],
        )
        conn.commit()
        log.info("Inserted %d rows into %s", len(records), T_COMMITS)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging commit data related to recently staged PRs.")
    _insert(_build_records())
    log.info("Done staging commits for PRs.")


if __name__ == "__main__":
    main()
