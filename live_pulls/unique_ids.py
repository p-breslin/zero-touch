from __future__ import annotations
import os
import hashlib
import logging
from pathlib import Path
from dotenv import load_dotenv
from utils.helpers import db_manager
from utils.logging_setup import setup_logging
from scripts.paths import DATA_DIR

"""
Adds a UUID column to MATCHED_USERS, where UUID is a 20-character SHA1 hash of "JIRA_ID|GITHUB_ID" for each row.

Steps:
  1. Read MATCHED_USERS into pandas.
  2. Compute UUID = first 20 hex chars of sha1(f"{JIRA_ID}|{GITHUB_ID}").
  3. Overwrite MATCHED_USERS with the same data plus the new UUID column.
"""

# Config -----------------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")
T_USERS = "MATCHED_USERS"


def add_user_id_column():
    with db_manager(DB_PATH) as conn:
        # 1. Load the entire MATCHED_USERS table into a DataFrame
        df = conn.execute(f'SELECT * FROM "{T_USERS}"').fetchdf()

        # 2. Compute the new UUID column
        def make_user_id(row):
            jira_id = row["JIRA_ID"] or ""
            github_id = row["GITHUB_ID"] or ""
            digest = hashlib.sha1(f"{jira_id}|{github_id}".encode()).hexdigest()
            return digest[:20]

        df["UUID"] = df.apply(make_user_id, axis=1)

        # 3. Overwrite MATCHED_USERS with the updated DataFrame
        conn.execute(f'DROP TABLE IF EXISTS "{T_USERS}";')
        conn.register("TMP_MATCHED", df)
        conn.execute(f'CREATE TABLE "{T_USERS}" AS SELECT * FROM TMP_MATCHED;')
        log.info("Added UUID column to %s (%d rows updated).", T_USERS, len(df))


if __name__ == "__main__":
    add_user_id_column()
