"""
Extracts information about user involvement in GitHub Pull Requests within a specified time window. The purpose is to create a focused table linking users to PRs and their specific roles.

    1. Fetches recent PR data from the PULL_REQUESTS table and actual reviewer data from the REVIEWS table in the main GitHub data source, filtered by the PR's last update time.
    2. Identifies distinct user roles (AUTHOR, ASSIGNEE, REQUESTED_REVIEWER, ACTUAL_REVIEWER) for each PR, along with user IDs and logins.
    3. Inserts this data into a GITHUB_PRS table in the staging database.
"""

from __future__ import annotations
import os
import json
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Dict, List, Tuple, Set

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DEFAULT_DAYS_TO_FETCH = 90
READ_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA = f"{COMPANY}_GITHUB_"
T_TARGET = "GITHUB_PRS"
COLS = (
    "INTERNAL_ID",
    "NUMBER",
    "ORG",
    "REPO",
    "USER_ID",
    "USER_LOGIN",
    "ROLE_IN_PR",
    "TITLE",
    "BODY",
    "CREATED_AT",
    "UPDATED_AT",
    "MERGED_AT",
    "CLOSED_AT",
    "STATE",
    "EXTRACTED_JIRA_KEY",
)
DDL = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET} (
    INTERNAL_ID  TEXT,
    NUMBER       INTEGER,
    ORG          TEXT,
    REPO         TEXT,
    USER_ID      TEXT,
    USER_LOGIN   TEXT,
    ROLE_IN_PR   TEXT,
    TITLE        TEXT,
    BODY         TEXT,
    CREATED_AT   TIMESTAMP,
    UPDATED_AT   TIMESTAMP,
    MERGED_AT    TIMESTAMP,
    CLOSED_AT    TIMESTAMP,
    STATE        TEXT,
    EXTRACTED_JIRA_KEY TEXT,
    PRIMARY KEY (INTERNAL_ID, USER_ID, ROLE_IN_PR)
);
"""


# Helpers ----------------------------------------------------------------------
def _json_to_user(blob: Any) -> Tuple[str | None, str | None]:
    """
    Parses a data blob (JSON str or Python dict representing a GitHub user) to extract the user's numerical ID and login name, returning them as a tuple.
    """
    if not blob:
        return None, None
    if isinstance(blob, str):
        try:
            blob = json.loads(blob)
        except json.JSONDecodeError:
            return None, None
    if isinstance(blob, dict):
        return str(blob.get("id")) if blob.get("id") else None, blob.get("login")
    return None, None


def _safe_json_list(blob: str | None) -> List[Any]:
    """
    Attempts to parse a JSON string that is expected to represent a list.
    """
    if not blob:
        return []
    try:
        data = json.loads(blob)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


# Source fetches ---------------------------------------------------------------
def _recent_pr_rows(conn: duckdb.DuckDBPyConnection, days: int) -> List[Tuple]:
    """
    Queries the source GitHub PULL_REQUESTS table in the source DB. Retrieves raw data for PRs that have been updated within the specified number of days from the current date and returns these records as a list of tuples.
    """
    q = f"""
      SELECT "ID","NUMBER","ORG","REPO","USER","ASSIGNEE",
             "ASSIGNEES","REQUESTED_REVIEWERS","TITLE","BODY",
             "CREATED_AT","UPDATED_AT","MERGED_AT","CLOSED_AT","STATE"
      FROM   "{SCHEMA}"."PULL_REQUESTS"
      WHERE  "UPDATED_AT" >= (CURRENT_DATE - INTERVAL '{days} days')
    """
    return conn.execute(q).fetchall()


def _reviewer_rows(
    conn: duckdb.DuckDBPyConnection, pr_keys: List[Tuple[int, str, str]]
) -> List[Tuple[int, str, str, Any]]:
    """
    Fetches actual reviewer info for a given list of PRs (identified by pr_keys containing their number, org, and reoo). Creates a temp table with these keys, then joins it with the source GitHub REVIEWS table to efficiently retrieve the PR identifiers and the user object of each reviewer.
    """
    if not pr_keys:
        return []

    conn.execute("CREATE TEMP TABLE tmp_pr(id INT, org TEXT, repo TEXT);")
    conn.executemany("INSERT INTO tmp_pr VALUES (?,?,?);", pr_keys)
    rows = conn.execute(
        f"""
        SELECT r."PULL_NUMBER", r."ORG", r."REPO", r."USER"
        FROM   "{SCHEMA}"."REVIEWS" r
        JOIN   tmp_pr t
          ON   r."PULL_NUMBER" = t.id AND r."ORG" = t.org AND r."REPO" = t.repo
        """
    ).fetchall()
    conn.execute("DROP TABLE tmp_pr;")
    return rows


# Record builder ---------------------------------------------------------------
def _build_records(days: int) -> List[Dict[str, Any]]:
    """
    Fetches recent PRs. Iterates through these PRs in a two-pass process:
        1. Processes each PR to extract info for users in roles like AUTHOR, ASSIGNEE, REQUESTED_REVIEWER. Creates a base dictionary for each PR.
        2. Fetches actual reviewers and adds these user-role records.
    Returns a list of dictionaries, each representing a unique user-PR-role relationship.
    """
    with db_manager(READ_DB, read_only=True) as src:
        prs = _recent_pr_rows(src, days)

        records: List[Dict[str, Any]] = []
        seen: Set[Tuple[str, str, str, str]] = set()  # (internal_id,user_id,role)

        base_lookup: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

        # pass 1 – author / assignee / requested
        for (
            pr_id,
            num,
            org,
            repo,
            author,
            assignee,
            assignees,
            requested,
            title,
            body,
            created,
            updated,
            merged,
            closed,
            state,
        ) in prs:
            base = dict(
                INTERNAL_ID=str(pr_id),
                NUMBER=num,
                ORG=org,
                REPO=repo,
                TITLE=title,
                BODY=body,
                CREATED_AT=created,
                UPDATED_AT=updated,
                MERGED_AT=merged,
                CLOSED_AT=closed,
                STATE=state,
                EXTRACTED_JIRA_KEY=None,
            )
            base_lookup[(num, org, repo)] = base

            for blob, role in (
                (author, "AUTHOR"),
                (assignee, "ASSIGNEE"),
            ):
                uid, login = _json_to_user(blob)
                if uid and login and (pr_id, uid, role) not in seen:
                    seen.add((pr_id, uid, role))
                    records.append(
                        {
                            **base,
                            "USER_ID": uid,
                            "USER_LOGIN": login,
                            "ROLE_IN_PR": role,
                        }
                    )

            # list assignees
            for blob_item in _safe_json_list(assignees):
                uid, login = _json_to_user(blob_item)
                key = (pr_id, uid, "ASSIGNEE")
                if uid and login and key not in seen:
                    seen.add(key)
                    records.append(
                        {
                            **base,
                            "USER_ID": uid,
                            "USER_LOGIN": login,
                            "ROLE_IN_PR": "ASSIGNEE",
                        }
                    )

            # requested reviewers
            for blob_item in _safe_json_list(requested):
                uid, login = _json_to_user(blob_item)
                key = (pr_id, uid, "REQUESTED_REVIEWER")
                if uid and login and key not in seen:
                    seen.add(key)
                    records.append(
                        {
                            **base,
                            "USER_ID": uid,
                            "USER_LOGIN": login,
                            "ROLE_IN_PR": "REQUESTED_REVIEWER",
                        }
                    )

        # pass 2 – actual reviewers via REVIEWS table
        rev_rows = _reviewer_rows(src, list(base_lookup.keys()))
        for pull_num, org, repo, rev_blob in rev_rows:
            uid, login = _json_to_user(rev_blob)
            base = base_lookup[(pull_num, org, repo)]
            key = (base["INTERNAL_ID"], uid, "ACTUAL_REVIEWER")
            if uid and login and key not in seen:
                seen.add(key)
                records.append(
                    {
                        **base,
                        "USER_ID": uid,
                        "USER_LOGIN": login,
                        "ROLE_IN_PR": "ACTUAL_REVIEWER",
                    }
                )

    log.info("Built %d PR-participant records", len(records))
    return records


# Insert -----------------------------------------------------------------------
def _insert(recs: List[Dict[str, Any]]):
    """
    Takes the list of processed records (from _build_records) and inserts them into the GITHUB_PRS table in the staging DB.
    """
    if not recs:
        log.info("Nothing new to insert.")
        return

    with db_manager(WRITE_DB) as conn:
        conn.execute(DDL)
        conn.executemany(
            f"""INSERT INTO "{T_TARGET}" ({",".join(COLS)}) VALUES ({",".join("?" * len(COLS))})
                ON CONFLICT (INTERNAL_ID, USER_ID, ROLE_IN_PR) DO NOTHING;""",
            [tuple(r[c] for c in COLS) for r in recs],
        )
        conn.commit()
        log.info("Inserted %d rows -> %s", len(recs), T_TARGET)


# Entry point ------------------------------------------------------------------
def main(days: int = 90):
    log.info("Staging PR participants (updated ≤ %d days)", days)
    _insert(_build_records(days))
    log.info("Done.")


if __name__ == "__main__":
    main(days=DEFAULT_DAYS_TO_FETCH)
