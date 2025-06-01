"""
Create / refresh GITHUB_PRS for every pull request that contains at least one commit already staged in GITHUB_COMMITS.

Steps
-----
1. Read COMMIT_SHA list from staging.GITHUB_COMMITS.
2. In the source DB, map SHAs -> (PULL_NUMBER, ORG, REPO) via PULL_REQUEST_COMMITS.
3. Pull PR details + reviews for those PR keys.
4. Explode into user-role rows and insert into staging.GITHUB_PRS.
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

SRC_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA = f"{COMPANY}_GITHUB_"

T_COMMITS_SRC = "PULL_REQUESTS"
T_PRCMT_SRC = "PULL_REQUEST_COMMITS"
T_REVIEWS_SRC = "REVIEWS"

T_COMMITS_STG = "GITHUB_COMMITS"
T_TARGET = "GITHUB_PRS"

COLS = (
    "INTERNAL_ID",
    "NUMBER",
    "COMMIT_SHA",
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
    COMMIT_SHA   TEXT,
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
    if not blob:
        return []
    try:
        data = json.loads(blob)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def _commit_shas() -> List[str]:
    """All SHAs currently in staging.GITHUB_COMMITS."""
    with db_manager(STG_DB, read_only=True) as conn:
        return [
            r[0]
            for r in conn.execute(
                f'SELECT COMMIT_SHA FROM "{T_COMMITS_STG}"'
            ).fetchall()
        ]


def _pr_rows_for_shas(src: duckdb.DuckDBPyConnection, shas: List[str]) -> List[Tuple]:
    """
    Return PR rows (same shape as old _recent_pr_rows) for all PRs that
    contain any of *shas*.
    """
    if not shas:
        return []

    src.execute("CREATE TEMP TABLE tmp_sha(SHA TEXT);")
    src.executemany("INSERT INTO tmp_sha VALUES (?);", [(s,) for s in shas])

    q = f"""
      SELECT pr."ID","NUMBER", pc."SHA", pr."ORG", pr."REPO", pr."USER","ASSIGNEE",
             pr."ASSIGNEES","REQUESTED_REVIEWERS",
             pr."TITLE","BODY",
             pr."CREATED_AT","UPDATED_AT","MERGED_AT","CLOSED_AT","STATE"
      FROM   "{SCHEMA}"."{T_COMMITS_SRC}"        AS pr
      JOIN   "{SCHEMA}"."{T_PRCMT_SRC}"          AS pc
            ON pc."PULL_NUMBER" = pr."NUMBER"
           AND pc."ORG"         = pr."ORG"
           AND pc."REPO"        = pr."REPO"
      JOIN   tmp_sha ts ON ts.SHA = pc."SHA"
      GROUP  BY ALL
    """
    rows = src.execute(q).fetchall()
    src.execute("DROP TABLE tmp_sha;")
    return rows


def _reviewer_rows(
    src: duckdb.DuckDBPyConnection,
    pr_keys: List[Tuple[int, str, str]],
) -> List[Tuple[int, str, str, Any]]:
    if not pr_keys:
        return []

    src.execute("CREATE TEMP TABLE tmp_pr(id INT, org TEXT, repo TEXT);")
    src.executemany("INSERT INTO tmp_pr VALUES (?,?,?);", pr_keys)

    rows = src.execute(
        f"""
        SELECT r."PULL_NUMBER", r."ORG", r."REPO", r."USER"
        FROM   "{SCHEMA}"."{T_REVIEWS_SRC}" r
        JOIN   tmp_pr t
          ON   r."PULL_NUMBER" = t.id AND r."ORG" = t.org AND r."REPO" = t.repo
        """
    ).fetchall()
    src.execute("DROP TABLE tmp_pr;")
    return rows


# Record builder ---------------------------------------------------------------
def _build_records() -> List[Dict[str, Any]]:
    shas = _commit_shas()
    if not shas:
        log.info("No commits in staging → nothing to do.")
        return []

    with db_manager(SRC_DB, read_only=True) as src:
        prs = _pr_rows_for_shas(src, shas)

        records: List[Dict[str, Any]] = []
        seen: Set[Tuple[str, str, str]] = set()  # (internal_id,user_id,role)
        base_lookup: Dict[Tuple[int, str, str], Dict[str, Any]] = {}

        # pass 1 – author / assignee / requested
        for (
            pr_id,
            num,
            sha,
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
                COMMIT_SHA=sha,
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

        # pass 2 – ACTUAL_REVIEWER via REVIEWS
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

    log.info("Built %d PR-participant records from %d SHAs", len(records), len(shas))
    return records


# Insert -----------------------------------------------------------------------
def _insert(recs: List[Dict[str, Any]]):
    if not recs:
        log.info("Nothing new to insert.")
        return

    with db_manager(STG_DB) as conn:
        conn.execute(DDL)
        conn.executemany(
            f"""INSERT INTO "{T_TARGET}" ({", ".join(COLS)}) VALUES ({", ".join("?" * len(COLS))})
                ON CONFLICT (INTERNAL_ID, USER_ID, ROLE_IN_PR) DO NOTHING;""",
            [tuple(r[c] for c in COLS) for r in recs],
        )
        conn.commit()
        log.info("Inserted %d rows into %s", len(recs), T_TARGET)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging PR participants linked to staged commits.")
    _insert(_build_records())
    log.info("Done.")


if __name__ == "__main__":
    main()
