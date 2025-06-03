from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse
from datetime import datetime, timezone
from typing import List, Optional, Set, Tuple

from github import Github, Auth
from github.File import File as GHFile
from github.Commit import Commit as GHCommit
from github.Repository import Repository as GHRepository
from github.GithubException import (
    GithubException,
    RateLimitExceededException,
    UnknownObjectException,
)

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
Collects ALL GitHub commit diffs and metadata per user for downstream analysis.

Description
-----------
Fetches and stages GitHub commit diffs and file-level metadata for each known contributor. Stores structured diff records in the GITHUB_DIFFS table, limited by user and per-repo commit counts.

    1. Loads contributor GitHub logins and display names from MATCHED_USERS.
    2. Retrieves all repos in the org, sorted by most recently pushed.
    3. For each contributor, fetches new commits from each repo.
    4. For each commit:
        a. Extracts the full commit message.
        b. Extracts per-file patches and change statistics (adds, dels, tot).
    5. Stages structured per-file diff records with message and metadata.
    6. Limits insertions to MAX_DIFFS_PER_USER per contributor.
"""


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY_ORG_NAME: str = os.getenv("GITHUB_ORG_NAME")
# DB_PATH = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")

T_TARGET_DIFFS = "GITHUB_DIFFS"
T_SOURCE_USERS = "MATCHED_USERS"

# Configuration ---------------------------------------------------------------
MAX_DIFFS_PER_USER: int = int(os.getenv("MAX_DIFFS_PER_USER", 200))
MAX_COMMITS_PER_REPO: int = int(os.getenv("MAX_COMMITS_PER_REPO", 200))
MAX_WORKERS_REPO_FETCH: int = int(os.getenv("MAX_WORKERS_REPO_FETCH", 5))
MAX_WORKERS_COMMIT_FETCH: int = int(os.getenv("MAX_WORKERS_COMMIT_FETCH", 10))

_GH_TOKEN = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
_G = Github(auth=Auth.Token(_GH_TOKEN), per_page=100, retry=3)


# DDL --------------------------------------------------------------------------
DDL_COMMIT_FILES = f"""
CREATE TABLE IF NOT EXISTS {T_TARGET_DIFFS} (
    ORG              TEXT,
    REPO             TEXT,
    COMMIT_SHA       TEXT,
    COMMIT_TIMESTAMP TIMESTAMP,
    COMMITTER_ID     TEXT,
    COMMITTER_LOGIN  TEXT,
    COMMITTER_NAME   TEXT,
    COMMIT_MESSAGE   TEXT,
    FILE_PATH        TEXT,
    FILE_ADDITIONS   INTEGER,
    FILE_DELETIONS   INTEGER,
    FILE_CHANGES     INTEGER,
    DIFF             TEXT,
    PRIMARY KEY (ORG, REPO, COMMIT_SHA, FILE_PATH)
);
"""


# Helpers ----------------------------------------------------------------------
def _get_committers_to_fetch(
    conn, limit: Optional[int] = None
) -> List[Tuple[str, Optional[str]]]:
    """Return (login, display_name) pairs that still need diff collection."""
    query = f"""
        SELECT DISTINCT GITHUB_LOGIN, GITHUB_DISPLAY_NAME
        FROM "{T_SOURCE_USERS}"
        WHERE GITHUB_LOGIN IS NOT NULL AND GITHUB_LOGIN != ''
        """
    if limit:
        query += f" LIMIT {limit}"

    users = conn.execute(query).fetchall()
    log.info("Loaded %d committers to process.", len(users))
    return users


def _get_existing_commits_for_user(conn, committer_id: str) -> Set[str]:
    """Return SHAs already stored for committer_id to avoid re-processing."""
    query = f"""
        SELECT DISTINCT COMMIT_SHA
        FROM "{T_TARGET_DIFFS}"
        WHERE COMMITTER_ID = ?
    """
    return {row[0] for row in conn.execute(query, [committer_id]).fetchall()}


# GitHub API calls -------------------------------------------------------------
def _get_diff_for_file(file_obj: GHFile) -> Optional[str]:
    """Return patch text for `file_obj`, or `None` if not applicable."""
    # If the file was deleted, GitHub still lists it but there is no patch
    if getattr(file_obj, "status", "") == "removed":
        return None
    # PyGithub stores the diff text in the `patch` attribute
    return getattr(file_obj, "patch", None)


def _get_repos_for_org(org_name: str) -> List[GHRepository]:
    """Fetch all repos for org. Returns an empty list on failure."""
    try:
        org = _G.get_organization(org_name)
        repos = list(org.get_repos(type="all"))
        log.info("%s: %d repositories fetched.", org_name, len(repos))
        return repos
    except UnknownObjectException:
        log.warning("Organisation '%s' not found or token lacks permissions.", org_name)
    except RateLimitExceededException:
        log.error("Rate limit exceeded while listing repos for '%s'.", org_name)
        raise  # escalate - upstream will decide how to proceed
    except GithubException as exc:
        log.error("Error fetching repos for '%s': %s", org_name, exc)
    return []


def _fetch_commits_for_user_in_repo(
    repo_obj: GHRepository,
    committer_login: str,
    existing_commit_shas: Set[str],
) -> List[GHCommit]:
    """Return new commits by committer_login in repo_obj (capped)."""
    commits: List[GHCommit] = []
    try:
        for commit in repo_obj.get_commits(author=committer_login):  # paginated
            if commit.sha in existing_commit_shas:
                continue
            commits.append(commit)
            if len(commits) >= MAX_COMMITS_PER_REPO:  # per-repo guard
                break
        log.info(
            "%-40s | %s | %d new commits",
            repo_obj.full_name,
            committer_login,
            len(commits),
        )
    except RateLimitExceededException:
        log.error(
            "Rate limit while fetching commits for %s in %s.",
            committer_login,
            repo_obj.full_name,
        )
        raise
    except GithubException as exc:
        if "Git Repository is empty" in str(exc):
            log.info("%-40s | %s | repo empty.", repo_obj.full_name, committer_login)
        else:
            log.error(
                "Error fetching commits for %s in %s: %s",
                committer_login,
                repo_obj.full_name,
                exc,
            )
    return commits


# Database operations ----------------------------------------------------------
def _insert_diff_rows(
    rows: List[
        Tuple[
            str,  # ORG
            str,  # REPO
            str,  # COMMIT_SHA
            datetime,  # COMMIT_TIMESTAMP
            str,  # COMMITTER_ID
            str,  # COMMITTER_LOGIN
            str,  # COMMITTER_NAME
            str,  # COMMIT_MESSAGE
            str,  # FILE_PATH
            int,  # FILE_ADDITIONS
            int,  # FILE_DELETIONS
            int,  # FILE_CHANGES
            str,  # DIFF
        ]
    ],
) -> None:
    """Batch-insert rows, ignoring conflicts."""
    if not rows:
        return

    with db_manager(DB_PATH) as conn:
        conn.execute(DDL_COMMIT_FILES)
        conn.executemany(
            f"""
            INSERT INTO "{T_TARGET_DIFFS}" (
                ORG, REPO, COMMIT_SHA, COMMIT_TIMESTAMP,
                COMMITTER_ID, COMMITTER_LOGIN, COMMITTER_NAME, COMMIT_MESSAGE,
                FILE_PATH, FILE_ADDITIONS, FILE_DELETIONS, FILE_CHANGES,
                DIFF
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(ORG, REPO, COMMIT_SHA, FILE_PATH) DO NOTHING;
            """,
            rows,
        )
        conn.commit()
    log.info("Inserted/updated %d diffs into %s", len(rows), T_TARGET_DIFFS)


# Core logic -------------------------------------------------------------------
def process_committer(
    committer_login: str,
    committer_name: Optional[str],
    repos: List[GHRepository],
) -> None:
    """Stage diffs for committer_login until MAX_DIFFS_PER_USER is reached."""
    staged_rows: List[
        Tuple[str, str, str, datetime, str, str, str, str, str, int, int, int, str]
    ] = []
    diff_cap = MAX_DIFFS_PER_USER
    diff_cnt = 0

    # look-up existing commit SHAs
    with db_manager(DB_PATH) as conn:
        existing = _get_existing_commits_for_user(conn, committer_login)

    # process repos newest-pushed first to bias toward recent work
    repos.sort(
        key=lambda r: r.pushed_at or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    for repo in repos:
        if diff_cnt >= diff_cap:
            break

        try:
            commits = _fetch_commits_for_user_in_repo(repo, committer_login, existing)
        except RateLimitExceededException:
            return
        except Exception as exc:
            log.error(
                "%s -> unexpected error for %s: %s",
                repo.full_name,
                committer_login,
                exc,
                exc_info=True,
            )
            continue

        commits.sort(
            key=lambda c: c.commit.author.date if c.commit.author else datetime.min,
            reverse=True,
        )

        for commit in commits:
            if diff_cnt >= diff_cap:
                break
            try:
                details = commit.commit
                ts = details.author.date.astimezone(timezone.utc).replace(tzinfo=None)

                # org / repo derivation
                if commit.repository:
                    org_name = commit.repository.owner.login
                    repo_name = commit.repository.name
                else:
                    slug = (
                        urlparse(commit.html_url).path.lstrip("/").split("/commit")[0]
                    )
                    org_name, repo_name = slug.split("/", 1)

                author = commit.author or ""
                committer_id = str(author.id) or ""
                committer_login_actual = author.login or ""

                for file_obj in commit.files or []:
                    if diff_cnt >= diff_cap:
                        break
                    patch = _get_diff_for_file(file_obj)
                    if patch:
                        additions = getattr(file_obj, "additions", 0)
                        deletions = getattr(file_obj, "deletions", 0)
                        changes = getattr(file_obj, "changes", additions + deletions)

                        staged_rows.append(
                            (
                                org_name,
                                repo_name,
                                commit.sha,
                                ts,
                                committer_id,
                                committer_login_actual,
                                committer_name or author.name if author else "",
                                details.message,
                                file_obj.filename,
                                additions,
                                deletions,
                                changes,
                                patch,
                            )
                        )
                        diff_cnt += 1
            except RateLimitExceededException:
                log.error(
                    "Rate limit while processing commit %s for %s.",
                    commit.sha,
                    committer_login,
                )
                break
            except GithubException as exc:
                log.error("GitHub error on commit %s: %s", commit.sha, exc)
            except Exception:
                log.exception("Unexpected error on commit %s", commit.sha)

    _insert_diff_rows(staged_rows)
    log.info("%s -> staged %d/%d diffs.", committer_login, diff_cnt, diff_cap)


# Entry point ------------------------------------------------------------------
def main() -> None:
    """Entry point."""
    with db_manager(DB_PATH) as conn:
        conn.execute(DDL_COMMIT_FILES)
        committers = _get_committers_to_fetch(conn)

    if not committers:
        log.info("No committers to process - exiting.")
        return

    org_repos = _get_repos_for_org(COMPANY_ORG_NAME)
    if not org_repos:
        log.warning("No repositories returned for %s - exiting.", COMPANY_ORG_NAME)
        return

    for login, name in committers:
        try:
            process_committer(login, name, org_repos)
            # time.sleep(1)  # for API rates if needed
        except RateLimitExceededException:
            log.critical("Global rate-limit reached - stopping early.")
            break
        except Exception:
            log.exception("Unhandled error processing %s", login)

    log.info("Diff collection complete.")


if __name__ == "__main__":
    main()
