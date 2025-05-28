from __future__ import annotations
import os
import duckdb
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)
DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
CSV_PATH = Path(DATA_DIR, "ACTIVE_USERS.csv")
TABLE_NAME = "JIRA_ACTIVE_USERS"


# SQL block --------------------------------------------------------------------
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    JIRA_ID             TEXT,
    JIRA_DISPLAY_NAME   TEXT,
    JIRA_EMAIL          TEXT
);
"""

# Main logic -------------------------------------------------------------------
df = pd.read_csv(CSV_PATH)
df = df[["User id", "User name", "email"]].rename(
    columns={
        "User id": "JIRA_ID",
        "User name": "JIRA_DISPLAY_NAME",
        "email": "JIRA_EMAIL",
    }
)

with duckdb.connect(DB_PATH) as conn:
    conn.execute(DDL)
    conn.register("temp_users", df)
    conn.execute(f"DELETE FROM {TABLE_NAME}")  # clear table before insert
    conn.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM temp_users")
    conn.unregister("temp_users")
    conn.commit()

log.info(f"Inserted {len(df)} rows into {TABLE_NAME}.")
