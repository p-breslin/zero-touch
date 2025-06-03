from __future__ import annotations
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Set

from dotenv import load_dotenv
from github import Github, Auth
from github.Repository import Repository
from github.GithubException import GithubException

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging


# Config -----------------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")
T_TARGET = "GITHUB_PRS"
DAYS_BACK = 90
ORG_NAME = os.getenv("GITHUB_ORG_NAME")

_GH_TOKEN = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
_G = Github(auth=Auth.Token(_GH_TOKEN), per_page=100, retry=3)

DDL = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET} (
    INTERNAL_ID        TEXT,
    NUMBER             INTEGER,
    HEAD_COMMIT_SHA    TEXT,
    BASE_COMMIT_SHA    TEXT,
    MERGE_COMMIT_SHA   TEXT,
    HEAD_LABEL         TEXT,
    BASE_LABEL         TEXT,
    ORG                TEXT,
    REPO               TEXT,
    HEAD_REPO          TEXT,
    COMMIT_COUN        TEXT,
    ADDITIONS          TEXT,
    DELETIONS          TEXT,
    CHANGED_FILES      TEXT,
    USER_ID            TEXT,
    USER_LOGIN         TEXT,
    ROLE_IN_PR         TEXT,
    TITLE              TEXT,
    BODY               TEXT,
    CREATED_AT         TIMESTAMP,
    UPDATED_AT         TIMESTAMP,
    MERGED_AT          TIMESTAMP,
    CLOSED_AT          TIMESTAMP,
    STATE              TEXT,
    EXTRACTED_JIRA_KEY TEXT,
    MERGED_BY_ID       TEXT,
    MERGED_BY_LOGIN    TEXT,
    PRIMARY KEY (INTERNAL_ID, USER_ID, ROLE_IN_PR)
);
"""

COLS = (
    "INTERNAL_ID",
    "NUMBER",
    "HEAD_COMMIT_SHA",
    "BASE_COMMIT_SHA",
    "MERGE_COMMIT_SHA",
    "HEAD_LABEL",
    "BASE_LABEL",
    "ORG",
    "REPO",
    "HEAD_REPO",
    "COMMIT_COUNT",
    "ADDITIONS",
    "DELETIONS",
    "CHANGED_FILES",
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
    "MERGED_BY_ID",
    "MERGED_BY_LOGIN",
)


# Helpers ----------------------------------------------------------------------
def _build_records() -> List[Dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    records: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str, str]] = set()

    try:
        org = _G.get_organization(ORG_NAME)
        repos: List[Repository] = list(org.get_repos(type="sources"))
    except GithubException as exc:
        log.error("Error fetching org '%s': %s", ORG_NAME, exc)
        return []

    for repo in repos:
        if repo.archived or repo.fork:
            continue
        try:
            for pr in repo.get_pulls(state="all", sort="updated", direction="desc"):
                if pr.updated_at < since:
                    break

                merged_by_id = str(pr.merged_by.id) if pr.merged_by else None
                merged_by_login = pr.merged_by.login if pr.merged_by else None
                head_repo = pr.head.repo.full_name if pr.head.repo else None

                base = dict(
                    INTERNAL_ID=str(pr.id),
                    NUMBER=pr.number,
                    HEAD_COMMIT_SHA=pr.head.sha,
                    BASE_COMMIT_SHA=pr.base.sha,
                    MERGE_COMMIT_SHA=pr.merge_commit_sha,
                    HEAD_LABEL=pr.head.label,
                    BASE_LABEL=pr.base.label,
                    ORG=ORG_NAME,
                    REPO=repo.name,
                    HEAD_REPO=head_repo,
                    COMMIT_COUNT=pr.commits,
                    ADDITIONS=pr.additions,
                    DELETIONS=pr.deletions,
                    CHANGED_FILES=pr.changed_files,
                    TITLE=pr.title,
                    BODY=pr.body,
                    CREATED_AT=pr.created_at,
                    UPDATED_AT=pr.updated_at,
                    MERGED_AT=pr.merged_at,
                    CLOSED_AT=pr.closed_at,
                    STATE=pr.state,
                    EXTRACTED_JIRA_KEY=None,
                    MERGED_BY_ID=merged_by_id,
                    MERGED_BY_LOGIN=merged_by_login,
                )

                def add(uid, login, role):
                    key = (base["INTERNAL_ID"], uid, role)
                    if uid and login and key not in seen:
                        seen.add(key)
                        records.append(
                            {
                                **base,
                                "USER_ID": str(uid),
                                "USER_LOGIN": login,
                                "ROLE_IN_PR": role,
                            }
                        )

                # Author
                if pr.user:
                    add(pr.user.id, pr.user.login, "AUTHOR")

                # Assignee (single + list)
                if pr.assignee:
                    add(pr.assignee.id, pr.assignee.login, "ASSIGNEE")
                for a in pr.assignees:
                    add(a.id, a.login, "ASSIGNEE")

                # Requested reviewers
                for r in pr.requested_reviewers:
                    add(r.id, r.login, "REQUESTED_REVIEWER")

                # Actual reviewers via reviews
                try:
                    for review in pr.get_reviews():
                        if review.user:
                            add(review.user.id, review.user.login, "ACTUAL_REVIEWER")
                except GithubException:
                    log.warning(
                        "Failed to fetch reviews for PR %d in %s",
                        pr.number,
                        repo.full_name,
                    )

        except GithubException as exc:
            log.warning("Skipping repo %s due to API error: %s", repo.full_name, exc)
        except Exception:
            log.exception("Unexpected error in repo %s", repo.full_name)

    log.info("Built %d PR-participant records from %s repos", len(records), len(repos))
    return records


def _insert(records: List[Dict[str, Any]]):
    if not records:
        return

    with db_manager(DB) as conn:
        conn.execute(DDL)
        conn.executemany(
            f"""
            INSERT INTO "{T_TARGET}" ({", ".join(COLS)}) VALUES ({", ".join("?" * len(COLS))})
            ON CONFLICT (INTERNAL_ID, USER_ID, ROLE_IN_PR) DO NOTHING;
            """,
            [tuple(r[c] for c in COLS) for r in records],
        )
        conn.commit()
        log.info("Inserted %d rows into %s", len(records), T_TARGET)


# Entry ------------------------------------------------------------------------
def main():
    log.info("Staging recent PRs and participants from live GitHub API.")
    _insert(_build_records())
    log.info("Done staging PRs.")


if __name__ == "__main__":
    main()
