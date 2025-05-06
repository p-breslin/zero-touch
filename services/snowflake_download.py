import os
import logging
import argparse
import pandas as pd
from dotenv import load_dotenv
from utils.logging_setup import setup_logging
from connection_manager import ConnectionManager


load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


class Client:
    def __init__(self, schema_name: str, database_name: str = None):
        self.conn_mgr = ConnectionManager(
            schema_name=schema_name, database_name=database_name
        )

    def __del__(self):
        self.conn_mgr.close_connection()

    def list_tables(self):
        connection = self.conn_mgr.get_connection()
        cursor = connection.cursor()
        try:
            cursor.execute("SHOW TABLES")
            return [row[1] for row in cursor.fetchall()]  # Table name in col 2
        finally:
            cursor.close()

    def export_table(self, table_name, output_dir):
        connection = self.conn_mgr.get_connection()
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, connection)
        output_path = os.path.join(output_dir, f"{table_name}.csv")
        df.to_csv(output_path, index=False)
        log.info(f"Exported {table_name} to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Export all tables from a Snowflake database."
    )
    parser.add_argument("schema_name", help="Schema name (required)")
    args = parser.parse_args()

    if not args.schema_name:
        parser.error("You must specify schema_name")

    output_dir = f"data/snowflake_exports/{args.schema_name}"
    os.makedirs(output_dir, exist_ok=True)

    client = Client(schema_name=args.schema_name)
    tables = client.list_tables()

    for table in tables:
        client.export_table(table, output_dir)
    log.info("Export complete.")


if __name__ == "__main__":
    main()
