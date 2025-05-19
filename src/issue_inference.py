import os
import duckdb
import logging
import asyncio
from dotenv import load_dotenv
from agno.agent import RunResponse

from scripts.paths import DATA_DIR
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# Initialize set-up
try:
    db_path = DATA_DIR / f"{os.getenv('DUCKDB_SUBSET_NAME')}.duckdb"
    conn = duckdb.connect(database=str(db_path), read_only=False)
    log.info("Connected to DuckDB database.")
    try:
        agent = build_agent(agent_key="Issue_Key_Agent")
        log.info("Agent built successfully.")

    except Exception as e:
        log.error(f"Failed to build Agent: {e}")
        exit()
except Exception as e:
    log.error(f"Failed to connect to DuckDB at {db_path}: {e}")
    exit()


def fetch_commits(conn, limit=None):
    """
    Fetches commit_sha and commit_message from the data table where extracted_issue_key is NULL.
    """
    query = """
    SELECT commit_sha, commit_message 
    FROM "GITHUB_COMMITS"
    WHERE extracted_issue_key IS NULL
    """
    if limit:
        query += f" LIMIT {limit}"

    try:
        results = conn.execute(query).fetchall()
        log.info(f"Fetched {len(results)} commits to process for JIRA key extraction.")
        return results
    except Exception as e:
        log.error(f"Error fetching commits to process: {e}")
        return []


def update_extracted_keys(conn, updates: list):
    """
    Updates the extracted_issue_key for a batch of commits.
    """
    log.info(f"Preparing to update {len(updates)} records in the database.")
    try:
        conn.begin()  # Start transaction
        for commit_sha, extracted_key in updates:
            conn.execute(
                """
                UPDATE "GITHUB_COMMITS"
                SET extracted_issue_key = ?
                WHERE commit_sha = ?;
                """,
                (extracted_key, commit_sha),
            )
        conn.commit()  # Commit all updates for the batch
        log.info(f"Batch database update of {len(updates)} records successful.")
    except Exception as e:
        log.error(f"Error during batch database update: {e}")
        conn.rollback()


async def run_agent(commit_sha: str, commit_message: str):
    """
    Asynchronously runs the Agent for a single commit message. Returns a tuple: (commit_sha, extracted_key_string_or_empty)
    """
    log.info(f"Processing SHA: {commit_sha}.")
    try:
        resp: RunResponse = await agent.arun(message=commit_message)

        extracted_key = None
        if resp and resp.content:
            if isinstance(resp.content, str) and resp.content.strip():
                extracted_key = resp.content.strip()
                log.info(f"Extracted key: {extracted_key}. SHA: {commit_sha}")
            else:
                log.info(f"Agent found no key or empty content for SHA: {commit_sha}")
                extracted_key = ""
        else:
            log.warning(
                f"Agent returned no content for SHA: {commit_sha}. Marking as no key found."
            )
            extracted_key = ""
        return commit_sha, extracted_key

    except Exception as e:
        log.error(f"Error running agent for SHA {commit_sha}: {e}", exc_info=True)
        return commit_sha, None  # Return None for key to indicate an error


async def process_commits(process_limit=100, concurrency_limit=10):
    """
    Fetches unprocessed commits, runs the Agents in parallel, and collects results for batch database update.
    """
    try:
        commits_data = fetch_commits(conn, limit=process_limit)

        if not commits_data:
            log.info("No commits found requiring JIRA key extraction.")
            return

        tasks = []
        for commit_sha, commit_message in commits_data:
            tasks.append(run_agent(commit_sha, commit_message))

        results = []

        # Process tasks in chunks to respect concurrency_limit
        for i in range(0, len(tasks), concurrency_limit):
            batch_tasks = tasks[i : i + concurrency_limit]
            log.info(
                f"Running a batch of {len(batch_tasks)} agent tasks concurrently..."
            )
            batch_results = await asyncio.gather(*batch_tasks)
            results.extend(batch_results)
            log.info(f"Completed batch {i // concurrency_limit + 1}.")

            # Filter out None results from tasks that failed before DB update:
            # If key is None from run_agent: an error occurred
            # If key is '': the agent processed it and found nothing
            updates = [(sha, key) for sha, key in results if key is not None]

            if updates:
                update_extracted_keys(conn, updates)
            else:
                log.info("No agent results to update for this run.")

    finally:
        conn.close()


if __name__ == "__main__":
    try:
        COMMITS_TO_PROCESS = os.getenv("PROCESS_LIMIT", 1000)
        CONCURRENT_AGENTS = os.getenv("CONCURRENCY_LIMIT", 100)

        asyncio.run(
            process_commits(
                process_limit=int(COMMITS_TO_PROCESS),
                concurrency_limit=int(CONCURRENT_AGENTS),
            )
        )
    except Exception as e:
        log.error(f"An error occurred in the main execution block: {e}", exc_info=True)
    finally:
        log.info("JIRA key extraction script finished.")
