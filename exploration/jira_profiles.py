from __future__ import annotations
import os
import logging
from pathlib import Path
from typing import List, Tuple
from dotenv import load_dotenv
from jira import JIRA, JIRAError
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

TABLE_NAME = "JIRA_USER_PROFILES_"
COMPANY_NAME = os.environ["COMPANY_NAME"]
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")

# JIRA API configuration
JIRA_DOMAIN = os.environ["JIRA_SERVER_URL"]
JIRA_EMAIL = os.environ["JIRA_USERNAME"]
JIRA_API_TOKEN = os.environ["JIRA_TOKEN"]

# DDL and upsert SQL
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ACCOUNT_ID   TEXT PRIMARY KEY,
    DISPLAY_NAME TEXT,
    EMAIL        TEXT
)
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    ACCOUNT_ID,
    DISPLAY_NAME,
    EMAIL
)
VALUES (?, ?, ?)
ON CONFLICT (ACCOUNT_ID) DO UPDATE SET
    DISPLAY_NAME = excluded.DISPLAY_NAME,
    EMAIL = excluded.EMAIL
"""


def fetch_jira_users() -> List[Tuple[str, str, str]]:
    jira = JIRA(server=JIRA_DOMAIN, basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN))

    users = []
    start = 0
    max_results = 50

    while True:
        try:
            response = jira._session.get(
                url=jira._options["server"] + "/rest/api/3/users/search",
                params={"startAt": start, "maxResults": max_results, "query": "."},
            )
            batch = response.json()
        except JIRAError as e:
            print(f"JIRA API error: {e}")
            break

        if not batch:
            break

        users.extend(batch)
        start += max_results

    return [
        (user.get("accountId"), user.get("displayName"), user.get("emailAddress"))
        for user in users
        if user.get("accountId")
    ]


def main():
    records = fetch_jira_users()
    if not records:
        print("No users found.")
        return

    with db_manager(DB_PATH) as conn:
        conn.execute(DDL)
        conn.executemany(UPSERT_SQL, records)
        conn.commit()


if __name__ == "__main__":
    main()
