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

TABLE_NAME = "JIRA_ISSUES"
COMPANY_NAME = os.getenv("COMPANY_NAME")
DUCKDB_READ_PATH = Path(DATA_DIR / f"{os.getenv('DUCKDB_NAME')}.duckdb")
DUCKDB_WRITE_PATH = Path(DATA_DIR / f"{os.getenv('DUCKDB_SUBSET_NAME')}.duckdb")

conn_read = duckdb.connect(database=DUCKDB_READ_PATH, read_only=True)
conn_write = duckdb.connect(database=DUCKDB_WRITE_PATH, read_only=False)


def create_issues_table(conn_write):
    """Creates and populates a table for the commit details."""
    issues_table = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        issue_key TEXT PRIMARY KEY,
        assignee_display_name TEXT,
        reporter_display_name TEXT,
        assignee_account_id TEXT,
        reporter_account_id TEXT,
        internal_issue_id BIGINT,
    );
    """
    conn_write.execute(issues_table)
    log.info(f"{TABLE_NAME} table created.")


def jira_keys_from_commits(conn):
    """
    Fetches distinct, non-empty, non-null extracted_issue_key from the processed GitHub commits table.
    """
    query = """
    SELECT DISTINCT extracted_issue_key
    FROM "GITHUB_COMMITS"
    WHERE extracted_issue_key IS NOT NULL AND extracted_issue_key != '';
    """
    try:
        results = conn.execute(query).fetchall()
        # fetchall returns a list of tuples, e.g., [('DNS-123',), ('PROJ-456',)]
        keys = [row[0] for row in results]
        log.info(
            f"Found {len(keys)} distinct JIRA issue keys to process from GitHub commits."
        )
        return keys
    except Exception as e:
        log.error(f"Error fetching JIRA keys from 'GITHUB_COMMITS' table: {e}")
        return []


def fetch_issue_details(conn, issue_key: str):
    """
    Fetches details for a single JIRA issue from the main JIRA ISSUES table. Returns a dictionary of the required fields or None if not found/error.
    """
    query = f"""
    SELECT 
        "FIELDS" AS fields_blob,
        "ID" AS internal_id 
    FROM "{COMPANY_NAME}_JIRA_"."ISSUES"
    WHERE "KEY" = ?;
    """
    try:
        issue_data_row = conn.execute(query, (issue_key,)).fetchone()
        if not issue_data_row:
            log.warning(f"No JIRA issue found with Key: {issue_key}")
            return None

        fields_blob_str = issue_data_row[0]
        jira_numeric_id = (
            int(issue_data_row[1]) if issue_data_row[1] is not None else None
        )

        fields_details = (
            json.loads(fields_blob_str)
            if isinstance(fields_blob_str, str)
            else fields_blob_str
        )
        if not isinstance(fields_details, dict):
            log.error(
                f"FIELDS blob for JIRA key {issue_key} is not a valid dictionary."
            )
            return None

        assignee_info = fields_details.get("assignee")
        reporter_info = fields_details.get("reporter")

        issue_details_map = {
            "issue_key": issue_key,
            "assignee_display_name": assignee_info.get("displayName")
            if isinstance(assignee_info, dict)
            else None,
            "reporter_display_name": reporter_info.get("displayName")
            if isinstance(reporter_info, dict)
            else None,
            "assignee_account_id": assignee_info.get("accountId")
            if isinstance(assignee_info, dict)
            else None,
            "reporter_account_id": reporter_info.get("accountId")
            if isinstance(reporter_info, dict)
            else None,
            "internal_issue_id": jira_numeric_id,
        }
        return issue_details_map

    except Exception as e:
        log.error(
            f"Error fetching or parsing JIRA issue details: Key = {issue_key}: {e}"
        )
        return None


def populate_linked_jira_issues_table(conn, jira_issue_records: list):
    """
    Inserts or updates records into the JIRA_ISSUES_LINKED_FROM_GIT table.
    """
    if not jira_issue_records:
        log.info("No JIRA issue records to insert/update.")
        return

    log.info(
        f"Preparing to insert/update {len(jira_issue_records)} records into 'JIRA_ISSUES'."
    )

    insert_sql = """
    INSERT INTO "JIRA_ISSUES" (
        issue_key, 
        assignee_display_name, 
        reporter_display_name, 
        assignee_account_id, 
        reporter_account_id, 
        internal_issue_id
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(issue_key) DO NOTHING
    """
    data_tuples = [
        (
            r["issue_key"],
            r["assignee_display_name"],
            r["reporter_display_name"],
            r["assignee_account_id"],
            r["reporter_account_id"],
            r["internal_issue_id"],
        )
        for r in jira_issue_records
    ]

    try:
        conn.begin()
        conn.executemany(insert_sql, data_tuples)
        conn.commit()
        log.info(
            f"Successfully inserted/updated {len(data_tuples)} records in 'JIRA_ISSUES'."
        )
    except Exception as e:
        log.error(f"Error during batch database insert/update for JIRA issues: {e}")
        conn.rollback()


if __name__ == "__main__":
    try:
        create_issues_table(conn_write)
        distinct_keys = jira_keys_from_commits(conn_write)

        if distinct_keys:
            jira_issue_details_list = []
            processed_count = 0
            for key in distinct_keys:
                processed_count += 1
                log.info(
                    f"Fetching JIRA details for key: {key} ({processed_count}/{len(distinct_keys)})"
                )
                details = fetch_issue_details(conn_read, key)
                if details:
                    jira_issue_details_list.append(details)

            # Populate the new JIRA issues table in the subset database
            populate_linked_jira_issues_table(conn_write, jira_issue_details_list)
        else:
            log.info("No JIRA keys found in processed GitHub commits.")

    finally:
        if conn_read:
            conn_read.close()
            log.info("Read-only main JIRA data source connection closed.")
        if conn_write:
            conn_write.close()
            log.info("Read/write subset database connection closed.")
        log.info("Script to populate linked JIRA issues finished.")
