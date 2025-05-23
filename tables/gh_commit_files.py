from __future__ import annotations

import os
import base64
import logging
from pathlib import Path
from github import Github
from dotenv import load_dotenv
from collections import defaultdict
from typing import Dict, List, Tuple, Set

from concurrent.futures import ThreadPoolExecutor, as_completed
from github.GithubException import GithubException, UnknownObjectException

from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging
from utils.helpers import db_manager

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SRC_STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

T_COMMITS = "GITHUB_COMMITS"
T_COMMIT_FILES = "GITHUB_COMMIT_FILES"

MAX_WORKERS = 10
_BLOB_CACHE: Dict[str, str] = {}

_GH_TOKEN = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
_G = Github(_GH_TOKEN, per_page=100, retry=3)

# DDL --------------------------------------------------------------------------
DDL_COMMIT_FILES = f"""
CREATE TABLE IF NOT EXISTS {T_COMMIT_FILES} (
    ORG             TEXT,
    REPO            TEXT,
    COMMIT_SHA      TEXT,
    COMMITTER_ID    TEXT,
    COMMITTER_NAME  TEXT,
    FILE_PATH       TEXT,
    CODE_TEXT       TEXT,
    PRIMARY KEY (ORG, REPO, COMMIT_SHA, FILE_PATH)
);
"""


# Helpers ----------------------------------------------------------------------
def _commits_missing_files(conn) -> List[Tuple[str, str, str, str, str]]:
    q = f"""
        SELECT ORG, REPO, COMMIT_SHA, COMMITTER_ID, COMMITTER_NAME
        FROM "{T_COMMITS}" c
        WHERE NOT EXISTS (
            SELECT 1 FROM "{T_COMMIT_FILES}" f
            WHERE f.ORG = c.ORG
              AND f.REPO = c.REPO
              AND f.COMMIT_SHA = c.COMMIT_SHA
        );
    """
    return conn.execute(q).fetchall()


def _fetch_blob(repo_obj, blob_sha: str) -> str | None:
    if blob_sha in _BLOB_CACHE:
        return _BLOB_CACHE[blob_sha]
    try:
        blob = repo_obj.get_git_blob(blob_sha)
        text = base64.b64decode(blob.content).decode(errors="replace")
        _BLOB_CACHE[blob_sha] = text
        return text
    except GithubException as exc:
        log.error("Blob %s error: %s", blob_sha, exc)
        return None


def _files_for_commit(repo_obj, sha: str) -> List[Tuple[str, str]]:
    try:
        commit = repo_obj.get_commit(sha)
    except UnknownObjectException:
        log.warning("Commit not found: %s@%s", repo_obj.full_name, sha)
        return []
    except GithubException as exc:
        log.error("Commit fetch error %s@%s: %s", repo_obj.full_name, sha, exc)
        return []

    out: List[Tuple[str, str]] = []
    for f in commit.files:
        if getattr(f, "status", "") == "removed":
            continue
        patch = getattr(f, "patch", None)  # this is the unified diff
        if patch is not None:
            out.append((f.filename, patch))
    return out


# Core logic -------------------------------------------------------------------
def _insert_file_rows(rows: List[Tuple[str, str, str, str, str, str, str]]):
    if not rows:
        return
    with db_manager(SRC_STG_DB) as conn:
        conn.execute(DDL_COMMIT_FILES)
        conn.executemany(
            f'INSERT INTO "{T_COMMIT_FILES}" VALUES (?,?,?,?,?,?,?)'
            " ON CONFLICT DO NOTHING;",
            rows,
        )
        conn.commit()
        log.info("Inserted %d code blobs into %s", len(rows), T_COMMIT_FILES)


def main():
    with db_manager(SRC_STG_DB) as conn:
        conn.execute(DDL_COMMIT_FILES)
        todo = _commits_missing_files(conn)

    if not todo:
        log.info("No new commits require file-blob staging.")
        return

    per_repo: Dict[Tuple[str, str], Set[Tuple[str, str, str]]] = defaultdict(set)
    for org, repo, sha, committer_id, committer_name in todo:
        per_repo[(org, repo)].add((sha, committer_id, committer_name))

    staged_rows: List[Tuple[str, str, str, str, str, str, str]] = []
    log.info(
        "Fetching code blobs for %d commits across %d repos …", len(todo), len(per_repo)
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {}
        for (org, repo), commit_infos in per_repo.items():
            try:
                log.info("Searching repo: %s", repo)
                repo_obj = _G.get_repo(f"{org}/{repo}")
            except UnknownObjectException:
                log.warning("Repo not found: %s/%s", org, repo)
                continue
            except GithubException as exc:
                log.error("Repo fetch error %s/%s: %s", org, repo, exc)
                continue

            for sha, committer_id, committer_name in commit_infos:
                futures[pool.submit(_files_for_commit, repo_obj, sha)] = (
                    org,
                    repo,
                    sha,
                    committer_id,
                    committer_name,
                )

        for fut in as_completed(futures):
            org, repo, sha, committer_id, committer_name = futures[fut]
            try:
                for path, code in fut.result():
                    staged_rows.append(
                        (org, repo, sha, committer_id, committer_name, path, code)
                    )
                    log.info("Appended code from repo: %s", repo)
            except Exception as exc:
                log.error("Worker error for %s/%s@%s: %s", org, repo, sha, exc)

    _insert_file_rows(staged_rows)
    log.info(
        "Done staging %d blobs (%d unique blobs cached).",
        len(staged_rows),
        len(_BLOB_CACHE),
    )


if __name__ == "__main__":
    main()
