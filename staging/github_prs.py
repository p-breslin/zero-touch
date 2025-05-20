import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
This script extracts information about user involvement in GitHub Pull Requests within a specified time window The purpose is to create a focused table linking users to PRs and their specific roles.

    1. Fetches recent PR data from the PULL_REQUESTS table and actual reviewer data from the REVIEWS table in the main GitHub data source, filtered by the PR's last update time. 
    2. Identifies distinct user roles (AUTHOR, ASSIGNEE, REQUESTED_REVIEWER, ACTUAL_REVIEWER) for each PR, along with user IDs and logins. 
    3. Inserts this data into a GITHUB_PRS table in the staging database. 
"""

# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_NAME = "GITHUB_PRS"
DEFAULT_DAYS_TO_FETCH = 90
COMPANY_NAME = os.environ["COMPANY_NAME"]

READ_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")


# helpers
def _json_to_user(blob: Any) -> Tuple[str | None, str | None]:
    """
    Returns (id, login) from a GitHub-user blob (dict or str).
        Takes a data blob (could be a JSON str or dict representing a GitHub user) and attempts to parse it to extract and return the user's numerical ID and login name as a tuple. Handles cases where input is None or malformed JSON.
    """
    if not blob:
        return None, None
    if isinstance(blob, str):
        try:
            blob = json.loads(blob)
        except json.JSONDecodeError:
            log.debug("Invalid JSON for user blob: %s", blob)
            return None, None
    if isinstance(blob, dict):
        uid = blob.get("id")
        login = blob.get("login")
        return (str(uid) if uid is not None else None, login)
    return None, None


def _add_record(
    dest: List[Dict[str, Any]],
    base: Dict[str, Any],
    user_id_val: str,
    user_login_val: str,
    role_in_pr_val: str,
) -> None:
    """
    Appends a new dictionary (representing a single row for the GITHUB_PRS table) to the destination list. It combines a base dictionary (containing common PR details) with specific user information (user_id_val, user_login_val) and their role_in_pr_val.
    """
    dest.append(
        {
            **base,
            "USER_ID": user_id_val,
            "USER_LOGIN": user_login_val,
            "ROLE_IN_PR": role_in_pr_val,
        }
    )


# DuckDB management
def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Connects to the target DuckDB database and executes a CREATE TABLE IF NOT EXISTS SQL statement to ensure that the GITHUB_PRS table exists with the correct schema.
    """
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            INTERNAL_ID         TEXT,
            NUMBER              INTEGER,
            ORG                 TEXT,
            REPO                TEXT,
            USER_ID             TEXT,
            USER_LOGIN          TEXT,
            ROLE_IN_PR          TEXT,
            TITLE               TEXT,
            BODY                TEXT,
            CREATED_AT          TIMESTAMP,
            UPDATED_AT          TIMESTAMP,
            MERGED_AT           TIMESTAMP,
            CLOSED_AT           TIMESTAMP,
            STATE               TEXT,
            EXTRACTED_JIRA_KEY  TEXT,
            PRIMARY KEY (INTERNAL_ID, USER_ID, ROLE_IN_PR)
        );
        """
    )
    log.info("Ensured table %s exists", TABLE_NAME)


def _fetch_recent_prs(conn: duckdb.DuckDBPyConnection, days_limit: int) -> List[Tuple]:
    """
    Queries the source GitHub PULL_REQUESTS table to retrieve details for PRs that have been updated within the specified days_limit. Orders them by update time and returns a list of tuples, each representing a raw PR record.
    """
    query = f"""
        SELECT
            "ID",
            "NUMBER",
            "ORG",
            "REPO",
            "USER",
            "ASSIGNEE",
            "ASSIGNEES",
            "REQUESTED_REVIEWERS",
            "TITLE",
            "BODY", 
            "CREATED_AT",
            "UPDATED_AT",
            "MERGED_AT",
            "CLOSED_AT",
            "STATE"
        FROM "{COMPANY_NAME}_GITHUB_"."PULL_REQUESTS"
        WHERE "UPDATED_AT" >= (current_date - INTERVAL '{days_limit} days')
        ORDER BY "UPDATED_AT" DESC
    """
    log.info(f"Fetching PRs updated in the last {days_limit} days.")
    return conn.execute(query).fetchall()


def _fetch_reviewers(
    conn: duckdb.DuckDBPyConnection, pr_keys: List[Tuple[int, str, str]]
) -> List[Tuple]:
    """
    Queries the source GitHub REVIEWS table to find users who actually submitted reviews for a given list of PRs (identified by their number, organization, and repo name). Returns a list of tuples containing the PR identifiers and the user object of the reviewer.
    """
    if not pr_keys:
        return []
    placeholders = " OR ".join(
        ['( "PULL_NUMBER" = ? AND "ORG" = ? AND "REPO" = ? )'] * len(pr_keys)
    )
    params = [item for key in pr_keys for item in key]  # Flatten list of tuples
    query = f"""
        SELECT "PULL_NUMBER", "ORG", "REPO", "USER"
        FROM "{COMPANY_NAME}_GITHUB_"."REVIEWS"
        WHERE {placeholders}
    """
    return conn.execute(query, params).fetchall()


# core logic
def _build_records(days_to_fetch: int = DEFAULT_DAYS_TO_FETCH) -> List[Dict[str, Any]]:
    """
    1. Fetches recent PRs using _fetch_recent_prs. For each PR, it extracts information for various user roles.
    2. Fetches actual reviewers for these PRs using _fetch_reviewers.
    3. Compiles all this information into a list of dictionaries, where each dictionary represents a unique user-PR-role relationship.

    """
    with db_manager(READ_DB, read_only=True) as conn:
        prs = _fetch_recent_prs(conn, days_to_fetch)
        log.info(
            "Fetched %d pull requests updated in the last %d days",
            len(prs),
            days_to_fetch,
        )

        records: List[Dict[str, Any]] = []
        pr_key_to_base: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

        for (
            pr_id,
            number,
            org,
            repo,
            author_blob,
            assignee_blob,
            assignees_blob,
            requested_blob,
            title,
            body,
            created_at,
            updated_at,
            merged_at,
            closed_at,
            state,
        ) in prs:
            base = {
                "INTERNAL_ID": str(pr_id),
                "NUMBER": number,
                "ORG": org,
                "REPO": repo,
                "TITLE": title,
                "BODY": body,
                "CREATED_AT": created_at,
                "UPDATED_AT": updated_at,
                "MERGED_AT": merged_at,
                "CLOSED_AT": closed_at,
                "STATE": state,
                "EXTRACTED_JIRA_KEY": None,  # Placeholder for later enrichment
            }
            pr_key_to_base[(number, org, repo)] = base

            # Author
            uid, login = _json_to_user(author_blob)
            if uid and login:
                _add_record(records, base, uid, login, "AUTHOR")

            # Single assignee
            uid, login = _json_to_user(assignee_blob)
            if uid and login:
                _add_record(records, base, uid, login, "ASSIGNEE")

            # Multiple assignees
            for blob_item in json.loads(assignees_blob or "[]"):
                uid, login = _json_to_user(blob_item)
                if uid and login:
                    # Avoid adding duplicate assignee entries if already captured by single assignee
                    duplicate = any(
                        r["INTERNAL_ID"] == base["INTERNAL_ID"]
                        and r["USER_ID"] == uid
                        and r["ROLE_IN_PR"] == "ASSIGNEE"
                        for r in records
                    )
                    if not duplicate:
                        _add_record(records, base, uid, login, "ASSIGNEE")

            # Requested reviewers
            for blob_item in json.loads(requested_blob or "[]"):
                uid, login = _json_to_user(blob_item)
                if uid and login:
                    _add_record(records, base, uid, login, "REQUESTED_REVIEWER")

        # Actual reviewers
        if pr_key_to_base:  # Only fetch reviewers if there are PRs
            reviewers = _fetch_reviewers(conn, list(pr_key_to_base.keys()))
            log.info("Fetched %d review entries for the recent PRs", len(reviewers))

            for pull_number, org_rev, repo_rev, reviewer_blob in reviewers:
                uid, login = _json_to_user(reviewer_blob)
                if not (uid and login):
                    continue
                base_for_reviewer = pr_key_to_base.get((pull_number, org_rev, repo_rev))
                if base_for_reviewer:
                    _add_record(
                        records, base_for_reviewer, uid, login, "ACTUAL_REVIEWER"
                    )
        else:
            log.info("No PRs found to fetch reviewers for.")

        return records


def _insert_records(records: List[Dict[str, Any]]) -> None:
    """
    Takes the list of processed records (from _build_records) and inserts them into the GITHUB_PRS table in the staging database. Handles duplicates.
    """
    if not records:
        log.info("No records to insert into %s.", TABLE_NAME)
        return

    with db_manager(WRITE_DB) as conn:
        _ensure_table(conn)
        rows = [
            (
                r["INTERNAL_ID"],
                r["NUMBER"],
                r["ORG"],
                r["REPO"],
                r["USER_ID"],
                r["USER_LOGIN"],
                r["ROLE_IN_PR"],
                r["TITLE"],
                r["BODY"],
                r["CREATED_AT"],
                r["UPDATED_AT"],
                r["MERGED_AT"],
                r["CLOSED_AT"],
                r["STATE"],
                r["EXTRACTED_JIRA_KEY"],
            )
            for r in records
        ]
        conn.executemany(
            f"""
            INSERT INTO "{TABLE_NAME}" VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT (INTERNAL_ID, USER_ID, ROLE_IN_PR) DO NOTHING;
            """,
            rows,
        )
        conn.commit()
        log.info("Inserted %d records into %s", len(rows), TABLE_NAME)


def main(days_to_fetch: int = DEFAULT_DAYS_TO_FETCH) -> None:
    log.info(
        f"Starting GITHUB_PRS data staging for PRs updated in the last {days_to_fetch} days."
    )
    records = _build_records(days_to_fetch)
    _insert_records(records)
    log.info("GITHUB_PRS data staging completed.")


if __name__ == "__main__":
    main()
