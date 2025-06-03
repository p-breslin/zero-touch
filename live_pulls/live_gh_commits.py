from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from typing import Any, Dict, List
from datetime import datetime, timedelta, timezone

from utils.helpers import db_manager
from utils.logging_setup import setup_logging

from github import Github, Auth
from github.Repository import Repository
from github.GithubException import GithubException


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_GH = f"{COMPANY}_GITHUB_"
DB = Path(DATA_DIR, f"{os.environ['LIVE_DB_NAME']}.duckdb")

T_COMMITS = "GITHUB_COMMITS"
DAYS_BACK = 90  # how far back in time to fetch commits

_GH_TOKEN = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
_G = Github(auth=Auth.Token(_GH_TOKEN), per_page=100, retry=3)


# DDL --------------------------------------------------------------------------
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


# Record builder ---------------------------------------------------------------
def _build_records() -> List[Dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    org_name = os.getenv("GITHUB_ORG_NAME")
    records: List[Dict[str, Any]] = []

    try:
        org = _G.get_organization(org_name)
        repos: List[Repository] = list(org.get_repos(type="all"))
    except GithubException as exc:
        log.error("Error fetching repos from org %s: %s", org_name, exc)
        return []

    log.info("Fetched %d repos in org %s.", len(repos), org_name)

    for repo in repos:
        try:
            for commit in repo.get_commits(since=cutoff):
                c = commit.commit  # Git commit data
                a = commit.author  # GitHub user (may be None)
                m = commit.committer  # GitHub user (may be None)

                # Raw author/committer blobs from Git data
                a_git = c.author or {}
                m_git = c.committer or {}

                commit_ts = a_git.date or m_git.date
                commit_ts = commit_ts.replace(tzinfo=None) if commit_ts else None

                records.append(
                    dict(
                        ORG=org.login,
                        REPO=repo.name,
                        COMMIT_SHA=commit.sha,
                        COMMIT_TIMESTAMP=commit_ts,
                        AUTHOR_ID=str(a.id) if a and a.id else None,
                        AUTHOR_NAME=a_git.name,
                        AUTHOR_EMAIL=a_git.email,
                        AUTHOR_LOGIN=a.login if a else None,
                        COMMITTER_ID=str(m.id) if m and m.id else None,
                        COMMITTER_NAME=m_git.name,
                        COMMITTER_EMAIL=m_git.email,
                        COMMITTER_LOGIN=m.login if m else None,
                        COMMIT_MESSAGE=c.message,
                        EXTRACTED_JIRA_KEY=None,
                    )
                )
        except GithubException as exc:
            log.warning("Failed to fetch commits from %s: %s", repo.full_name, exc)
        except Exception:
            log.exception("Unexpected error in repo %s", repo.full_name)

    log.info(
        "Built %d commit records from live GitHub API (cutoff: %s)",
        len(records),
        cutoff.isoformat(),
    )
    return records


# Insert -----------------------------------------------------------------------
def _insert(records: List[Dict[str, Any]]):
    if not records:
        return

    with db_manager(DB) as conn:
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
