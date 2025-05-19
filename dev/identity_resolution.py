import os
import json
import pprint
import duckdb
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR

load_dotenv()

COMPANY_NAME = os.getenv("COMPANY_NAME")
DUCKDB_PATH = Path(DATA_DIR / "MELTANO_DATABASE.duckdb")
conn = duckdb.connect(database=DUCKDB_PATH, read_only=True)


def pp_json(data, title="JSON Data", indent=2):
    """Helper to pretty print (PP) JSON blobs."""
    print(f"\n--- {title} ---")
    if isinstance(data, str):
        try:
            parsed_data = json.loads(data)
            print(json.dumps(parsed_data, indent=indent, sort_keys=False))
        except json.JSONDecodeError:
            print(data)
    elif data is None:
        print("None")
    else:
        # If it's already a Python dict/list (e.g., from DuckDB JSON extension)
        print(json.dumps(data, indent=indent, sort_keys=False))


def github_commit_details(verbose=False, N=10):
    """Query for the Most Recent GitHub Commit."""

    print("=" * 20 + " Retrieving Most Recent GitHub Commit " + "=" * 20)

    query_recent_commit = f"""
    SELECT
        "COMMIT" AS commit_blob,
        "AUTHOR" AS author_blob,
        "COMMITTER" AS commiter_blob,
        "COMMIT_TIMESTAMP" AS commit_date,
        "SHA" as sha
    FROM "{COMPANY_NAME}_GITHUB_"."COMMITS"
    ORDER BY "COMMIT_TIMESTAMP" DESC
    LIMIT {N};
    """
    recent_commit_data = conn.execute(query_recent_commit).fetchall()

    data = {}
    if recent_commit_data:
        for i in range(N):
            commit_blob = recent_commit_data[i][0]
            author_blob = recent_commit_data[i][1]
            commiter_blob = recent_commit_data[i][2]
            commit_date = recent_commit_data[i][3]
            sha = recent_commit_data[i][4]

            # Most recent commit timestamp (from column)
            print(f"\nTimestamp: {commit_date}")

            if verbose:
                pp_json(commit_blob, "COMMIT blob")
                pp_json(author_blob, "AUTHOR blob (from COMMIT row)")
                pp_json(commiter_blob, "COMMITTER blob (from COMMIT row)")

            # Extracting specific fields for easier manual inspection
            commit_details = (
                json.loads(commit_blob) if isinstance(commit_blob, str) else commit_blob
            )
            author_details = (
                json.loads(author_blob) if isinstance(author_blob, str) else author_blob
            )

            commit_message = commit_details.get("message", "N/A")
            print(f"Commit Message: {commit_message}")

            if verbose:
                print("\n--- Extracted GitHub Commit Details ---")
                print(
                    f"  Author Name (COMMIT blob): {commit_details.get('author', {}).get('name', 'N/A')}"
                )
                print(
                    f"  Author Email (COMMIT blob): {commit_details.get('author', {}).get('email', 'N/A')}"
                )
                print(
                    f"  Timestamp (COMMIT blob committer.date): {commit_details.get('committer', {}).get('date', 'N/A')}"
                )
                print(f"  Login (AUTHOR blob): {author_details.get('login', 'N/A')}")
                print(f"  ID (AUTHOR blob): {author_details.get('id', 'N/A')}")

            data[sha] = {
                "date": commit_date,
                "id": author_details.get("id", "N/A"),
                "author": commit_details.get("author", {}).get("name", "N/A"),
                "email": commit_details.get("author", {}).get("email", "N/A"),
                "login": author_details.get("login", "N/A"),
                "message": commit_message,
            }
        return data
    else:
        print("No commits found in XFLOW_GITHUB_.COMMITS.")
        return  # Exit if no commit data


def jira_issue_details(jira_id):
    """Query JIRA Issue (Based on Manually Identified JIRA ID)."""

    print("=" * 20 + " Retrieving JIRA Issue for Key " + "=" * 20)

    query_jira_issue = f"""
    SELECT
        "KEY" as issue_key,
        "FIELDS" as fields_blob,
        "ID" as issue_id
    FROM "{COMPANY_NAME}_JIRA_"."ISSUES"
    WHERE "KEY" = '{jira_id}';
    """
    jira_issue_data = conn.execute(query_jira_issue).fetchone()

    if jira_issue_data:
        issue_key_ret = jira_issue_data[0]
        fields_blob = jira_issue_data[1]
        issue_id = jira_issue_data[2]

        print(f"Found JIRA Issue - ID: {issue_id}, Key: {issue_key_ret}")
        pp_json(fields_blob, "Full JIRA Issue FIELDS JSON Blob")

        try:
            fields_details = (
                json.loads(fields_blob) if isinstance(fields_blob, str) else fields_blob
            )
            assignee_info = fields_details.get("assignee")
            reporter_info = fields_details.get("reporter")

            print("\n--- Extracted JIRA Issue Details ---")
            if assignee_info and isinstance(assignee_info, dict):
                account_id = assignee_info.get("accountId", "N/A")
                print(
                    f"  Assignee DisplayName: {assignee_info.get('displayName', 'N/A')}"
                )
                print(f"  Assignee AccountID: {account_id}")
            else:
                print(
                    "  Assignee: Not assigned or not found in FIELDS blob as expected."
                )

            if reporter_info and isinstance(reporter_info, dict):
                reporter_id = reporter_info.get("accountId", "N/A")
                print(
                    f"  Reporter DisplayName: {reporter_info.get('displayName', 'N/A')}"
                )
                print(f"  Reporter AccountID: {reporter_id}")
            else:
                print("  Reporter: Not found in FIELDS blob as expected.")
        except Exception as e:
            print(f"Error parsing JIRA FIELDS JSON for extraction: {e}")
    else:
        print(f"No JIRA issue found with Key: {jira_id}")
        return


def jira_user_id(user_ids):
    """Query JIRA User(s)."""

    print("\n" + "=" * 20 + " Retrieving JIRA User Details " + "=" * 20)
    for account_id in user_ids:
        print(f"\nQuerying for JIRA User with AccountID: {account_id}")
        query_jira_user = f"""
        SELECT
            "ID",
            "NAME",
            "EMAIL",
        FROM "{COMPANY_NAME}_JIRA_"."USERS_SUMMARY"
        WHERE "ID" = '{account_id}';
        """
        jira_user_data = conn.execute(query_jira_user).fetchone()

        if jira_user_data:
            acc_id, display_name, email = jira_user_data
            print(f"  JIRA User AccountID: {acc_id}")
            print(f"  Name: {display_name}")
            print(f"  Email Address: {email}")
        else:
            print(f"  No JIRA user found with AccountID: {account_id}")


if __name__ == "__main__":
    try:
        data = github_commit_details(verbose=False, N=5)
        # jira_issue_details(jira_id="DNS-15653")
        # jira_user_id(user_ids=["600a966de2a1350069856d47"])

        pprint.pp(data, indent=2, width=200)
    finally:
        conn.close()
