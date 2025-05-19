import os
import json
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_NAME = "GITHUB_COMMITS"
COMPANY_NAME = os.getenv("COMPANY_NAME")
DUCKDB_READ_PATH = Path(DATA_DIR / f"{os.getenv('DUCKDB_NAME')}.duckdb")
DUCKDB_WRITE_PATH = Path(DATA_DIR / f"{os.getenv('DUCKDB_SUBSET_NAME')}.duckdb")
conn_read = duckdb.connect(database=DUCKDB_READ_PATH, read_only=True)


def populate_database(conn_write):
    """Creates and populates a table for the commit details."""
    commits_table = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        commit_sha TEXT PRIMARY KEY,
        commit_timestamp TIMESTAMP,
        author_id TEXT,
        author_name TEXT,
        author_email TEXT,
        author_login TEXT,
        commit_message TEXT,
        extracted_issue_key TEXT,
    );
    """
    conn_write.execute(commits_table)
    log.info(f"{TABLE_NAME} table created.")


def get_commits(N=1000):
    """Query for the Most Recent GitHub Commit."""

    query_recent_commit = f"""
    SELECT
        "SHA" AS sha,
        "COMMIT" AS commit_blob,
        "AUTHOR" AS author_blob,
        "COMMIT_TIMESTAMP" AS commit_date
    FROM "{COMPANY_NAME}_GITHUB_"."COMMITS"
    ORDER BY "COMMIT_TIMESTAMP" DESC
    LIMIT {N};
    """
    recent_commits = conn_read.execute(query_recent_commit).fetchall()

    records = []
    for row_tuple in recent_commits:
        sha, commit_blob_str, author_blob_str, commit_date_ts = row_tuple

        try:
            commit_details = (
                json.loads(commit_blob_str)
                if isinstance(commit_blob_str, str)
                else commit_blob_str
            )
            author_details = (
                json.loads(author_blob_str)
                if isinstance(author_blob_str, str)
                else author_blob_str
            )
            author_info = commit_details.get("author", None)

            record = {
                "commit_sha": sha,
                "commit_timestamp": commit_date_ts,
                "author_id": author_details.get("id")
                if isinstance(author_details, dict)
                else None,
                "author_login": author_details.get("login")
                if isinstance(author_details, dict)
                else None,
                "author_name": author_info.get("name")
                if isinstance(author_info, dict)
                else None,
                "author_email": author_info.get("email")
                if isinstance(author_info, dict)
                else None,
                "commit_message": commit_details.get("message"),
                "extracted_issue_key": None,
            }
            records.append(record)
        except Exception as e:
            log.error(f"Error processing commit SHA {sha}: {e}. Skipping this commit.")
            N -= 1
            continue

    log.info(f"GitHub commit details obtained for {N} commits.")

    conn_write = None
    try:
        # Initialize table
        conn_write = duckdb.connect(database=DUCKDB_WRITE_PATH, read_only=False)
        populate_database(conn_write)

        # Prepare data as list of tuples in the correct column order
        insert_data_tuples = [
            (
                r["commit_sha"],
                r["commit_timestamp"],
                r["author_id"],
                r["author_name"],
                r["author_email"],
                r["author_login"],
                r["commit_message"],
                r["extracted_issue_key"],
            )
            for r in records
        ]
        # ON CONFLICT DO NOTHING to avoid errors if a commit is re-processed
        conn_write.executemany(
            f"""
            INSERT INTO "{TABLE_NAME}" VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (commit_sha) DO NOTHING;
        """,
            insert_data_tuples,
        )

        conn_write.commit()
        log.info(f"Inserted {len(records)} records into {TABLE_NAME}.")

    except Exception as e:
        log.error(f"Error during database write operation: {e}", exc_info=True)
        if conn_write:
            conn_write.rollback()
    finally:
        if conn_write:
            conn_write.close()


if __name__ == "__main__":
    try:
        get_commits(N=1000)
    finally:
        if conn_read:
            conn_read.close()
            log.info("Connections closed; script complete.")
