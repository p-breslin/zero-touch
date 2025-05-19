import os
import duckdb
from dotenv import load_dotenv
from pathlib import Path
from src.paths import DATA_DIR

load_dotenv()
db_filename = f"{os.getenv('DUCKDB_SUBSET_NAME')}.duckdb"
savepath = Path(DATA_DIR / db_filename)

# Delete existing file to ensure it's empty
if os.path.exists(savepath):
    os.remove(savepath)
    print(f"Existing '{db_filename}' removed.")

# Connect to create the new database file
con = duckdb.connect(savepath)
print(f"Created new DuckDB database: {db_filename}")

con.close()
