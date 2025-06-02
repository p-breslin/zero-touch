from __future__ import annotations
import os
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
from agno.agent import RunResponse
from typing import List, Tuple, Optional, Dict, Any

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging
from src.agents.agent_builder import build_agent
from models import (
    GeneratedCommitSummary,
    PreprocessedCommitSummary,
    IssueInfo,
    PreprocessedDiffOutput,
)

"""
Preprocesses individual commit diffs into structured summaries using AI agents.

Description
-----------
Generates structured summaries of individual commits for each committer by analyzing commit message and diff content. Pulls commits directly from the GITHUB_DIFFS table instead of relying on aggregated diffs. Each commit is summarized into change description, technologies used, and metadata. Results are stored in the INFERENCE_INFO table as JSON strings.

    1. Loads distinct committers from GITHUB_DIFFS who are not yet in INFERENCE_INFO.
    2. For each committer, retrieves their individual commit messages and diffs.
    3. Sends each commit to an gent for summarization.
    4. Builds a PreprocessedDiffOutput object containing summaries, key changes, languages, and file metadata.
    5. Serializes the structured output and stores it in the database.

Includes developer review activity by counting authored comments in REVIEW_COMMENTS. Supports asynchronous execution with concurrency limits.
"""


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

T_COMMITTER_DIFFS_SOURCE = "GITHUB_DIFFS"
T_OUTPUT = "INFERENCE_INFO"
T_USERS = "MATCHED_USERS"
T_ISSUES = "JIRA_ISSUES"

STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
MAIN_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_NAME')}.duckdb")

LIMIT = int(os.getenv("PREPROCESS_LIMIT", 1000))
CONCUR = int(os.getenv("PREPROCESS_CONCURRENCY", 10))
AGENT_KEY = "Diff_Preprocessor"


# Database operations ----------------------------------------------------------
def _load_commits_for_user(conn, user_id: str) -> List[Dict[str, Any]]:
    q = """
        SELECT COMMIT_SHA, COMMIT_MESSAGE, COMMIT_TIMESTAMP, FILE_PATH, DIFF, FILE_ADDITIONS, FILE_DELETIONS
        FROM GITHUB_DIFFS
        WHERE COMMITTER_ID = ?
        ORDER BY COMMIT_TIMESTAMP DESC
    """
    rows = conn.execute(q, (user_id,)).fetchall()

    commits: Dict[str, Dict[str, Any]] = {}

    for sha, msg, ts, path, diff, additions, deletions in rows:
        if sha not in commits:
            commits[sha] = {
                "commit_sha": sha,
                "message": msg,
                "timestamp": ts,
                "file_paths": set(),
                "diffs": [],
                "additions": 0,
                "deletions": 0,
            }

        commits[sha]["file_paths"].add(path)
        commits[sha]["diffs"].append(diff)
        commits[sha]["additions"] += additions
        commits[sha]["deletions"] += deletions

    return list(commits.values())


def _insert_inference_info(
    conn,
    rows: List[Tuple[str, Optional[str], Optional[str], Optional[str]]],
):
    """
    Inserts commit summaries into INFERENCE_INFO table. Each row is (GITHUB_ID and SUMMARIES as JSON string).
    """
    if not rows:
        log.debug("No inference info rows to insert.")
        return

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {T_OUTPUT} (
            GITHUB_ID TEXT PRIMARY KEY,
            JIRA_ID TEXT,
            DB_ID TEXT,
            SUMMARIES TEXT
        );
    """)

    conn.executemany(
        f"""
        INSERT INTO {T_OUTPUT} (GITHUB_ID, JIRA_ID, DB_ID, SUMMARIES)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (GITHUB_ID) DO UPDATE SET
            SUMMARIES = excluded.SUMMARIES,
            JIRA_ID = excluded.JIRA_ID,
            DB_ID = excluded.DB_ID;
        """,
        rows,
    )
    conn.commit()
    log.info("Inserted inference info for %d users", len(rows))


def get_review_comment_count(conn, user_id: str) -> int:
    """
    Returns the number of review comments authored by the given user,
    using the REVIEW_COMMENTS table from the MAIN_DB.
    """
    q = """
        SELECT COUNT(*) FROM XFLOW_DEV_GITHUB_.REVIEW_COMMENTS
        WHERE json_extract_string(USER, '$.id') = ?
    """
    with db_manager(MAIN_DB) as conn:
        result = conn.execute(q, (user_id,)).fetchone()
        return result[0] if result else 0


def _load_committers_for_preprocessing(
    conn, limit: int
) -> List[Tuple[str, Optional[str]]]:
    """
    Loads distinct committers from GITHUB_DIFFS who have not yet been preprocessed.
    """
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}

    if T_OUTPUT in tables:
        q = f"""
            SELECT DISTINCT COMMITTER_ID
            FROM   {T_COMMITTER_DIFFS_SOURCE}
            WHERE  COMMITTER_ID NOT IN (SELECT COMMITTER_ID FROM {T_OUTPUT})
              AND COMMITTER_ID IS NOT NULL
            LIMIT  {limit};
        """
    else:
        q = f"""
            SELECT DISTINCT COMMITTER_ID
            FROM   {T_COMMITTER_DIFFS_SOURCE}
            WHERE  COMMITTER_ID IS NOT NULL
            LIMIT  {limit};
        """
    return conn.execute(q).fetchall()


# Agent logic ------------------------------------------------------------------
async def _summarize_commit(
    commit_msg: str, diff_text: str
) -> Optional[GeneratedCommitSummary]:
    try:
        agent = build_agent(AGENT_KEY)
        message = f"=== Commit message ===\n{commit_msg}\n\n=== Diff ===\n{diff_text}"
        resp: RunResponse = await agent.arun(message=message)

        if resp and isinstance(resp.content, GeneratedCommitSummary):
            return resp.content
        else:
            log.warning("Unexpected or empty response from commit summarizer.")
            return None
    except Exception as exc:
        log.error("Error summarizing commit: %s", exc, exc_info=True)
        return None


async def _preprocess_diffs(
    committer_id: str,
    stg_conn,
    main_conn,
    sem: asyncio.Semaphore,
) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    try:
        raw_commits = _load_commits_for_user(stg_conn, committer_id)
        if not raw_commits:
            log.warning(f"No commits found for committer {committer_id}.")
            return committer_id, None, None, None

        # Get number of PR review comments
        pr_review_comments = get_review_comment_count(main_conn, committer_id)

        # Enrich with associated JIRA issues
        matched = stg_conn.execute(
            f"""
            SELECT DB_ID, JIRA_ID FROM {T_USERS}
            WHERE GITHUB_ID = ?
            """,
            (committer_id,),
        ).fetchone()

        associated_issues: Dict[str, IssueInfo] = {}

        if matched:
            _, jira_id = matched
            issue_rows = stg_conn.execute(
                """
                SELECT ISSUE_KEY, ISSUE_TYPE_NAME, SUMMARY, DESCRIPTION, PROJECT_KEY, PROJECT_NAME
                FROM JIRA_ISSUES
                WHERE ASSIGNEE_ACCOUNT_ID = ?
                """,
                (jira_id,),
            ).fetchall()

            for row in issue_rows:
                issue_key = row[0]
                associated_issues[issue_key] = IssueInfo(
                    issue_type=row[1],
                    summary=row[2],
                    description=row[3],
                    project_key=row[4],
                    project_name=row[5],
                )
        else:
            log.warning(f"No MATCHED_USERS entry for committer: {committer_id}")

        # Budgeted filtering logic
        MAX_TOTAL_DIFF_CHARS = 100000
        filtered_commits = []
        char_sum_so_far = 0

        total_commit_count = len(raw_commits)
        for commit in raw_commits:
            diff_str = "\n".join(commit["diffs"])
            diff_len = len(diff_str)

            if (
                filtered_commits
                and char_sum_so_far + diff_len > MAX_TOTAL_DIFF_CHARS * 1.5
            ):
                log.info(
                    "Char budget used: %d / %d", char_sum_so_far, MAX_TOTAL_DIFF_CHARS
                )
                break

            commit["joined_diff"] = diff_str
            filtered_commits.append(commit)
            char_sum_so_far += diff_len

        if not filtered_commits:
            log.warning(f"No commits within budget for committer {committer_id}.")
            return committer_id, None, None, None

        async def _task(commit):
            async with sem:
                agent_summary = await _summarize_commit(
                    commit["message"], commit["joined_diff"]
                )
                if not agent_summary:
                    return None
                log.info("An agent returned a Commit Summary. Prcocessing..")
                return PreprocessedCommitSummary(
                    commit_message=commit["message"],
                    summary=agent_summary.summary,
                    key_changes=agent_summary.key_changes,
                    langs=agent_summary.langs,
                    frameworks=agent_summary.frameworks,
                    loc_added=commit["additions"],
                    loc_removed=commit["deletions"],
                    file_count=len(commit["file_paths"]),
                    file_path=commit["file_paths"],
                )

        tasks = [_task(commit) for commit in filtered_commits]
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r is not None]

        if not valid:
            log.warning(f"No valid commit summaries for committer {committer_id}.")
            return committer_id, None, None, None

        log.info("Perparing the Preprocessed Diff Output..")
        output = PreprocessedDiffOutput(
            last_90d_commits=total_commit_count,
            pr_review_comments=pr_review_comments,
            associated_issues=associated_issues,
            commits=valid,
        )
        return committer_id, matched[1], matched[0], output.model_dump_json()

    except Exception as exc:
        log.error(
            "User-level diff preprocessing failed for %s: %s",
            committer_id,
            exc,
            exc_info=True,
        )
        return committer_id, None, None, None


# Concurrency helper -----------------------------------------------------------
async def _bounded_preprocess_diffs(
    row: Tuple[str], stg_conn, main_conn, sem: asyncio.Semaphore
) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    (committer_id,) = row
    return await _preprocess_diffs(committer_id, stg_conn, main_conn, sem)


# Main async -------------------------------------------------------------------
async def _run_preprocessing():
    with db_manager(STG_DB) as stg_conn, db_manager(MAIN_DB) as main_conn:
        rows_to_process = _load_committers_for_preprocessing(stg_conn, LIMIT)
        if not rows_to_process:
            log.info("No new committers need diff preprocessing.")
            return

        log.info("Starting preprocessing for %d committers.", len(rows_to_process))
        global_sem = asyncio.Semaphore(CONCUR)
        tasks = [
            _bounded_preprocess_diffs((row[0],), stg_conn, main_conn, global_sem)
            for row in rows_to_process
        ]
        results = await asyncio.gather(*tasks)

        # Filter out None and empty results
        valid_results = [res for res in results if res and res[3]]
        _insert_inference_info(stg_conn, valid_results)


# Entry point ------------------------------------------------------------------
def main():
    log.info(
        "Collecting relevant information for user inference (limit=%d, concur=%d)",
        LIMIT,
        CONCUR,
    )
    asyncio.run(_run_preprocessing())
    log.info("Finished.")


if __name__ == "__main__":
    main()


async def test_single_user():
    with db_manager(STG_DB) as stg_conn, db_manager(MAIN_DB) as main_conn:
        global_sem = asyncio.Semaphore(CONCUR)
        results = await _preprocess_diffs("49854264", stg_conn, main_conn, global_sem)
        print(results)
        valid_results = [res for res in results if res and res[3]]
        _insert_inference_info(stg_conn, [valid_results])


# asyncio.run(test_single_user())
