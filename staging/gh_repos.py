from __future__ import annotations

import os
import logging
from pathlib import Path
from github import Github
from dotenv import load_dotenv
from typing import List, Tuple
from datetime import datetime, timedelta, timezone
from github.GithubException import GithubException, UnknownObjectException

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
T_REPO_FILES = "REPO_FILES"

_GH_TOKEN = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
ORG_NAME = os.getenv("GITHUB_ORG_NAME")
MAX_REPOS = int(os.getenv("MAX_REPOS_TO_SNAPSHOT", 500))  # api rate limit
_G = Github(_GH_TOKEN, per_page=100, retry=3)

# DDL --------------------------------------------------------------------------
DDL_REPO_FILES = f"""
CREATE TABLE IF NOT EXISTS {T_REPO_FILES} (
    ORG                 TEXT,
    REPO                TEXT,
    BRANCH              TEXT,
    COMMIT_SHA          TEXT,
    FILE_PATH           TEXT,
    DIR_PATH            TEXT,
    FILE_NAME           TEXT,
    EXTENSION           TEXT,
    FILE_SIZE           INTEGER,
    SNAPSHOT_TIMESTAMP  TIMESTAMP,
    LAST_PUSHED_AT      TIMESTAMP,
    PRIMARY KEY (ORG, REPO, COMMIT_SHA, FILE_PATH)
);
"""


# Helpers ----------------------------------------------------------------------
def _parse_file_path(path: str) -> Tuple[str, str, str]:
    dir_path = os.path.dirname(path)
    file_name = os.path.basename(path)
    _, ext = os.path.splitext(file_name)
    return dir_path, file_name, ext


def _collect_file_tree(
    org: str, repo_name: str, pushed_at: datetime, branch: str = None
) -> List[Tuple]:
    try:
        repo = _G.get_repo(f"{org}/{repo_name}")
        if branch is None:
            branch = repo.default_branch
        commit = repo.get_branch(branch).commit
        tree = repo.get_git_tree(commit.sha, recursive=True)
        timestamp = datetime.now(timezone.utc)

        rows = []
        for entry in tree.tree:
            if entry.type != "blob":
                continue
            dir_path, file_name, ext = _parse_file_path(entry.path)
            rows.append(
                (
                    org,
                    repo_name,
                    branch,
                    commit.sha,
                    entry.path,
                    dir_path,
                    file_name,
                    ext,
                    entry.size,
                    timestamp,
                    pushed_at,
                )
            )
        log.info("Collected %d files from %s/%s@%s", len(rows), org, repo_name, branch)
        return rows

    except UnknownObjectException:
        log.warning("Repo not found: %s/%s", org, repo_name)
    except GithubException as exc:
        log.error("GitHub API error for %s/%s: %s", org, repo_name, exc)
    except Exception as exc:
        log.exception(
            "Unexpected error fetching repo tree for %s/%s: %s", org, repo_name, exc
        )

    return []


def _get_org_repos(org_name: str) -> List[Tuple[str, str, datetime]]:
    try:
        org = _G.get_organization(org_name)
        repos = org.get_repos()
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        active_repos = [
            (org_name, repo.name, repo.pushed_at)
            for repo in repos
            if repo.pushed_at and repo.pushed_at >= cutoff
        ]
        return active_repos[:MAX_REPOS]
    except Exception as exc:
        log.exception("Failed to fetch repositories for org %s: %s", org_name, exc)
        return []


# Main -------------------------------------------------------------------------
def snapshot_org_repos(org_name: str):
    all_rows = []
    repo_targets = _get_org_repos(org_name)
    log.info("Discovered %d active repositories in org %s", len(repo_targets), org_name)

    for org, repo, pushed_at in repo_targets:
        log.info("Snapshotting repo: %s/%s (last pushed: %s)", org, repo, pushed_at)
        rows = _collect_file_tree(org, repo, pushed_at)
        all_rows.extend(rows)

    if not all_rows:
        log.info("No file data collected.")
        return

    with db_manager(STG_DB) as conn:
        conn.execute(DDL_REPO_FILES)
        conn.executemany(
            f"""INSERT INTO {T_REPO_FILES} VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            ) ON CONFLICT DO NOTHING;""",
            all_rows,
        )
        conn.commit()
        log.info("Inserted snapshot of %d files into %s", len(all_rows), T_REPO_FILES)


# Entry ------------------------------------------------------------------------
def main():
    if not ORG_NAME:
        raise EnvironmentError("GITHUB_ORG_NAME must be set in your .env file")
    snapshot_org_repos(ORG_NAME)


if __name__ == "__main__":
    main()
