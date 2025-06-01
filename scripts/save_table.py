from __future__ import annotations
import os
import duckdb
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
EXPORT_PATH = Path(DATA_DIR, "committer_diffs.csv")
TABLE_NAME = "COMMITTER_DIFFS"

N = 7  # how many rows to export


# Export function --------------------------------------------------------------
def export_table_to_csv():
    log.info("Exporting table %s to CSV at %s", TABLE_NAME, EXPORT_PATH)
    with duckdb.connect(DB_PATH) as conn:
        df = conn.execute(f"SELECT * FROM {TABLE_NAME}").fetchdf()
        df.head(N).to_csv(EXPORT_PATH, index=False)
    log.info("Export complete. %d rows written.", len(df.head(N)))


if __name__ == "__main__":
    export_table_to_csv()
