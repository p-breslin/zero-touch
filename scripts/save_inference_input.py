import os
import json
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager

load_dotenv()
LIVE_DB_NAME = os.getenv("LIVE_DB_NAME")
db_path = Path(DATA_DIR) / f"{LIVE_DB_NAME}.duckdb"
T_INFERENCE = "INFERENCE_INFO"
T_DEVELOPER = "DEVELOPER_INFERENCE"
T_NEW = "DEV_INF_SHEET"


def format_record(summaries_json: str) -> str:
    """
    Given the raw JSON string from the SUMMARIES column, parse it and return a human-readable representation.
    """
    data = json.loads(summaries_json)
    lines = []

    # Top‐level stats
    lines.append(f"Last 90-day Commits: {data.get('last_90d_commits', 0)}")
    lines.append(f"PR Review Comments: {data.get('pr_review_comments', 0)}")
    lines.append("")

    # Associated issues (if any)
    associated_issues = data.get("associated_issues", {})
    if associated_issues:
        lines.append("Associated Issues:")
        for issue_key, info in associated_issues.items():
            issue_type = info.get("issue_type", "Unknown")
            project_key = info.get("project_key", "")
            project_name = info.get("project_name", "")
            lines.append(
                f"  • {issue_key} [{issue_type}] (Project: {project_key} - {project_name})"
            )
            if info.get("description"):
                lines.append(f"      Description: {info['description']}")
        lines.append("")

    # Commits array
    commits = data.get("commits", [])
    if commits:
        lines.append("Commits:")
        for idx, commit in enumerate(commits, start=1):
            lines.append(f"  Commit {idx}:")
            repos = commit.get("repos", [])
            if repos:
                lines.append(f"    Repos: {', '.join(repos)}")

            message = commit.get("commit_message", "")
            if message:
                lines.append(f"    Message: {message}")

            summary = commit.get("summary", "")
            if summary:
                lines.append(f"    Summary: {summary}")

            key_changes = commit.get("key_changes", [])
            if key_changes:
                lines.append("    Key Changes:")
                for change in key_changes:
                    lines.append(f"      - {change}")

            langs = commit.get("langs", [])
            if langs:
                lines.append(f"    Languages: {', '.join(langs)}")

            frameworks = commit.get("frameworks", [])
            if frameworks:
                lines.append(f"    Frameworks: {', '.join(frameworks)}")

            loc_added = commit.get("loc_added", 0)
            loc_removed = commit.get("loc_removed", 0)
            lines.append(f"    LOC Added: {loc_added}")
            lines.append(f"    LOC Removed: {loc_removed}")

            file_count = commit.get("file_count", 0)
            lines.append(f"    File Count: {file_count}")

            file_paths = commit.get("file_path", [])
            if file_paths:
                lines.append("    File Paths:")
                for path in file_paths:
                    lines.append(f"      - {path}")

            lines.append("")  # blank line between commits

    return "\n".join(lines).rstrip()


def main():
    with db_manager(db_path) as conn:
        # 1. Fetch column info for DEVELOPER_INFERENCE
        cols_info = conn.execute(f"PRAGMA table_info({T_DEVELOPER})").fetchall()
        if not cols_info:
            raise RuntimeError(
                f"Table '{T_DEVELOPER}' does not exist or has no columns."
            )

        columns = [row[1] for row in cols_info]  # second field is column name
        types = [row[2] for row in cols_info]  # third field is data type

        # 2. Drop DEV_INF_SHEET if it exists
        conn.execute(f"DROP TABLE IF EXISTS {T_NEW}")

        # 3. Create DEV_INF_SHEET with same schema plus a 'INFERENCE_INPUT' column
        create_cols = ", ".join(f"{col} {dtype}" for col, dtype in zip(columns, types))
        create_cols += ", INFERENCE_INPUT TEXT"
        conn.execute(f"CREATE TABLE {T_NEW} ({create_cols})")

        # 4. Select all rows from DEVELOPER_INFERENCE, ordered by UUID
        rows = conn.execute(f"SELECT * FROM {T_DEVELOPER} ORDER BY UUID").fetchall()

        # 5. For each row, fetch the corresponding SUMMARIES JSON and format it
        for row in rows:
            row_dict = dict(zip(columns, row))
            uuid = row_dict.get("UUID")
            summaries_row = conn.execute(
                f"SELECT SUMMARIES FROM {T_INFERENCE} WHERE UUID = ?", (uuid,)
            ).fetchone()
            summaries_json = summaries_row[0] if summaries_row else ""
            readable = format_record(summaries_json) if summaries_json else ""

            # 6. Build INSERT statement dynamically
            placeholders = ", ".join("?" for _ in (columns + ["INFERENCE_INPUT"]))
            insert_cols = ", ".join(columns + ["INFERENCE_INPUT"])
            values = list(row) + [readable]
            conn.execute(
                f"INSERT INTO {T_NEW} ({insert_cols}) VALUES ({placeholders})",
                values,
            )

        conn.commit()
        print(f"Table '{T_NEW}' has been created, ordered by UUID with readable text.")


if __name__ == "__main__":
    main()
