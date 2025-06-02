from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

"""
Creates the JIRA_EPICS table from PROJECTS in the main JIRA database.

Steps
-----
1. Selects ID, KEY, NAME from the PROJECTS table.
2. Writes results into the staging table JIRA_EPICS.
"""

# Configuration ---------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
SCHEMA_JIRA = f"{COMPANY}_JIRA_"

STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
MAIN_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_NAME')}.duckdb")

T_SRC = "PROJECTS"
T_TARGET = "JIRA_EPICS"

DDL = f"""
CREATE OR REPLACE TABLE {T_TARGET} AS
SELECT
    ID,
    KEY,
    NAME
FROM "{SCHEMA_JIRA}"."{T_SRC}"
"""


# Pipeline ---------------------------------------------------------------------
def _create_epics():
    with db_manager(STG_DB) as stg, db_manager(MAIN_DB, read_only=True) as main:
        log.info("Creating %s from %s.%s", T_TARGET, SCHEMA_JIRA, T_SRC)

        result = main.execute(f"""
            SELECT
                ID,
                KEY,
                NAME
            FROM "{SCHEMA_JIRA}"."{T_SRC}"
        """).fetchdf()

        stg.register("TMP_EPICS", result)
        stg.execute(f"CREATE OR REPLACE TABLE {T_TARGET} AS SELECT * FROM TMP_EPICS")
        log.info("Wrote %d rows to %s", len(result), T_TARGET)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Starting JIRA_EPICS staging -> %s", T_TARGET)
    _create_epics()
    log.info("Done.")


if __name__ == "__main__":
    main()
