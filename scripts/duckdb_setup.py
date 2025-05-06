import os
import duckdb
import logging
from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

# Paths
export_root = "data/snowflake_exports"
db_path = "data/MELTANO_DATABASE.duckdb"

# Connect to DuckDB (creates file if it doesn't exist)
con = duckdb.connect(db_path)
log.info(f"Connected to DuckDB at {db_path}")

# Traverse each schema folder
for schema_name in os.listdir(export_root):
    schema_path = os.path.join(export_root, schema_name)
    if not os.path.isdir(schema_path):
        continue

    log.info(f"Creating schema: {schema_name}")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    # Load each CSV into the schema
    for filename in os.listdir(schema_path):
        if filename.endswith(".csv"):
            table_name = os.path.splitext(filename)[0]
            csv_path = os.path.join(schema_path, filename)

            log.info(f"Loading {schema_name}.{table_name} from {csv_path}")
            con.execute(f"""
                CREATE OR REPLACE TABLE {schema_name}.{table_name} AS
                SELECT * FROM read_csv_auto('{csv_path}')
            """)

con.close()
log.info("DuckDB database built with full schema/table structure.")
