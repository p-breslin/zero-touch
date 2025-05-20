from __future__ import annotations

import os
import json
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from contextlib import contextmanager
from typing import Any, Dict, List, Tuple
from utils.logging_setup import setup_logging

# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_NAME = "GITHUB_COMMITS"
COMPANY_NAME = os.environ["COMPANY_NAME"]

READ_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
WRITE_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")


# helpers
def _safe_json(blob: Any) -> Dict[str, Any]:
    """Return a dict; never None."""
    if not blob:
        return {}
    if isinstance(blob, dict):
        return blob
    if isinstance(blob, (str, bytes, bytearray)):
        try:
            return json.loads(blob)
        except json.JSONDecodeError:
            log.debug("Bad JSON blob ignored: %s", blob)
    return {}


@contextmanager
def _db(path: Path, *, read_only: bool = False):
    conn = duckdb.connect(path, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


# DuckDB management
DDL_COMMITS = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    commit_sha            TEXT PRIMARY KEY,
    commit_timestamp      TIMESTAMP,
    extracted_issue_key   TEXT,
    author_id             TEXT,
    author_name           TEXT,
    author_email          TEXT,
    author_login          TEXT,
    committer_id          TEXT,
    committer_name        TEXT,
    committer_email       TEXT,
    committer_login       TEXT,
    commit_message        TEXT
);
"""


def _ensure_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(DDL_COMMITS)
    log.info("Ensured table %s exists", TABLE_NAME)


def _fetch_commits(
    conn: duckdb.DuckDBPyConnection, limit: int
) -> List[Tuple[Any, ...]]:
    return conn.execute(
        f"""
        SELECT
            "SHA",
            "COMMIT",
            "AUTHOR",
            "COMMITTER",
            "COMMIT_TIMESTAMP"
        FROM "{COMPANY_NAME}_GITHUB_"."COMMITS"
        ORDER BY "COMMIT_TIMESTAMP" DESC
        LIMIT {limit}
        """
    ).fetchall()


# core logic
def _build_records(limit: int = 1000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    with _db(READ_DB, read_only=True) as conn:
        rows = _fetch_commits(conn, limit)
        log.info("Fetched %d commits", len(rows))

        for sha, commit_blob, author_blob, committer_blob, ts in rows:
            commit_info = _safe_json(commit_blob)
            author_json = _safe_json(author_blob)
            committer_json = _safe_json(committer_blob)

            # Author/committer details stored twice in GitHub’s payload:
            author_meta = commit_info.get("author", {})
            committer_meta = commit_info.get("committer", {})

            out.append(
                dict(
                    commit_sha=sha,
                    commit_timestamp=ts,
                    extracted_issue_key=None,  # downstream enrichment
                    author_id=(author_json or {}).get("id"),
                    author_login=(author_json or {}).get("login"),
                    author_name=author_meta.get("name"),
                    author_email=author_meta.get("email"),
                    committer_id=(committer_json or {}).get("id"),
                    committer_login=(committer_json or {}).get("login"),
                    committer_name=committer_meta.get("name"),
                    committer_email=committer_meta.get("email"),
                    commit_message=commit_info.get("message"),
                )
            )
    return out


def _insert(records: List[Dict[str, Any]]) -> None:
    if not records:
        log.info("No records to insert.")
        return

    with _db(WRITE_DB) as conn:
        _ensure_table(conn)

        rows = [
            (
                r["commit_sha"],
                r["commit_timestamp"],
                r["extracted_issue_key"],
                r["author_id"],
                r["author_name"],
                r["author_email"],
                r["author_login"],
                r["committer_id"],
                r["committer_name"],
                r["committer_email"],
                r["committer_login"],
                r["commit_message"],
            )
            for r in records
        ]

        conn.executemany(
            f"""
            INSERT INTO "{TABLE_NAME}" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (commit_sha) DO NOTHING;
            """,
            rows,
        )
        conn.commit()
        log.info("Inserted %d records into %s", len(rows), TABLE_NAME)


def main(limit: int = 1000) -> None:
    _insert(_build_records(limit))


if __name__ == "__main__":
    main()
