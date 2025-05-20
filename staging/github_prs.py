import os
import json
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Dict, List, Tuple

import duckdb
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# configuration
TABLE_NAME = "GITHUB_PRS"
COMPANY_NAME = os.environ["COMPANY_NAME"]

READ_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")


# helpers
def _json_to_user(blob: Any) -> Tuple[str | None, str | None]:
    """Return (id, login) from a GitHub-user blob (dict or str)."""
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
    github_id: str,
    login: str,
    role: str,
) -> None:
    dest.append(
        {
            **base,
            "github_user_id": github_id,
            "github_user_login": login,
            "role_in_pr": role,
        }
    )


@contextmanager
def _db(path: Path, *, read_only: bool = False):
    conn = duckdb.connect(path, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


# DuckDB management
def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            pr_internal_id TEXT,
            pr_number      INTEGER,
            org_name       TEXT,
            repo_name      TEXT,
            github_user_id TEXT,
            github_user_login TEXT,
            role_in_pr     TEXT,
            pr_title       TEXT,
            pr_created_at  TIMESTAMP,
            pr_updated_at  TIMESTAMP,
            pr_merged_at   TIMESTAMP,
            pr_closed_at   TIMESTAMP,
            pr_state       TEXT,
            extracted_jira_key_from_pr TEXT,
            PRIMARY KEY (pr_internal_id, github_user_id, role_in_pr)
        );
        """
    )
    log.info("Ensured table %s exists", TABLE_NAME)


def _fetch_recent_prs(conn: duckdb.DuckDBPyConnection, limit: int) -> List[Tuple]:
    return conn.execute(
        f"""
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
            "CREATED_AT",
            "UPDATED_AT",
            "MERGED_AT",
            "CLOSED_AT",
            "STATE"
        FROM "{COMPANY_NAME}_GITHUB_"."PULL_REQUESTS"
        ORDER BY "UPDATED_AT" DESC
        LIMIT {limit}
        """
    ).fetchall()


def _fetch_reviewers(
    conn: duckdb.DuckDBPyConnection, pr_keys: List[Tuple[int, str, str]]
) -> List[Tuple]:
    if not pr_keys:
        return []
    placeholders = " OR ".join(
        ['( "PULL_NUMBER" = ? AND "ORG" = ? AND "REPO" = ? )'] * len(pr_keys)
    )
    params = [item for key in pr_keys for item in key]
    return conn.execute(
        f"""
        SELECT "PULL_NUMBER", "ORG", "REPO", "USER"
        FROM "{COMPANY_NAME}_GITHUB_"."REVIEWS"
        WHERE {placeholders}
        """,
        params,
    ).fetchall()


# core logic
def _build_records(limit: int = 1000) -> List[Dict[str, Any]]:
    with _db(READ_DB, read_only=True) as conn:
        prs = _fetch_recent_prs(conn, limit)
        log.info("Fetched %d pull requests", len(prs))

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
            created_at,
            updated_at,
            merged_at,
            closed_at,
            state,
        ) in prs:
            base = {
                "pr_internal_id": str(pr_id),
                "pr_number": number,
                "org_name": org,
                "repo_name": repo,
                "pr_title": title,
                "pr_created_at": created_at,
                "pr_updated_at": updated_at,
                "pr_merged_at": merged_at,
                "pr_closed_at": closed_at,
                "pr_state": state,
                "extracted_jira_key_from_pr": None,
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
            for blob in json.loads(assignees_blob or "[]"):
                uid, login = _json_to_user(blob)
                if uid and login:
                    duplicate = any(
                        r["pr_internal_id"] == base["pr_internal_id"]
                        and r["github_user_id"] == uid
                        and r["role_in_pr"] == "ASSIGNEE"
                        for r in records
                    )
                    if not duplicate:
                        _add_record(records, base, uid, login, "ASSIGNEE")

            # Requested reviewers
            for blob in json.loads(requested_blob or "[]"):
                uid, login = _json_to_user(blob)
                if uid and login:
                    _add_record(records, base, uid, login, "REQUESTED_REVIEWER")

        # Actual reviewers
        reviewers = _fetch_reviewers(conn, list(pr_key_to_base.keys()))
        log.info("Fetched %d review entries", len(reviewers))

        for pull_number, org, repo, reviewer_blob in reviewers:
            uid, login = _json_to_user(reviewer_blob)
            if not (uid and login):
                continue
            base = pr_key_to_base.get((pull_number, org, repo))
            if base:
                _add_record(records, base, uid, login, "ACTUAL_REVIEWER")

        return records


def _insert_records(records: List[Dict[str, Any]]) -> None:
    with _db(WRITE_DB) as conn:
        _ensure_table(conn)
        rows = [
            (
                r["pr_internal_id"],
                r["pr_number"],
                r["org_name"],
                r["repo_name"],
                r["github_user_id"],
                r["github_user_login"],
                r["role_in_pr"],
                r["pr_title"],
                r["pr_created_at"],
                r["pr_updated_at"],
                r["pr_merged_at"],
                r["pr_closed_at"],
                r["pr_state"],
                r["extracted_jira_key_from_pr"],
            )
            for r in records
        ]
        conn.executemany(
            f"""
            INSERT INTO "{TABLE_NAME}" VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT (pr_internal_id, github_user_id, role_in_pr) DO NOTHING;
            """,
            rows,
        )
        conn.commit()
        log.info("Inserted %d records into %s", len(rows), TABLE_NAME)


def main(limit: int = 1000) -> None:
    records = _build_records(limit)
    _insert_records(records)


if __name__ == "__main__":
    main()
