import os
import duckdb
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR

load_dotenv()


def duckdb_init(db_name=os.getenv("DUCKDB_STAGING_NAME")):
    savepath = Path(DATA_DIR / f"{db_name}.duckdb")

    # Delete existing file to ensure it's empty
    if os.path.exists(savepath):
        os.remove(savepath)
        print(f"Existing '{db_name}' removed.")

    # Connect to create the new database file
    con = duckdb.connect(savepath)
    print(f"Created new DuckDB database: {db_name}")

    con.close()
