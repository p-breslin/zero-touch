import os
import csv
import sys
import duckdb
import logging
from collections import defaultdict
from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)
con = duckdb.connect("data/MELTANO_DATABASE.duckdb")


def table_names():
    """SQL query to get all tables by schema."""
    query = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name;
    """

    # Execute and fetch results
    results = con.execute(query).fetchall()

    # Group and print by schema
    schema_tables = defaultdict(list)
    for schema, table in results:
        schema_tables[schema].append(table)

    for schema, tables in schema_tables.items():
        print(f"Schema: {schema}")
        for table in tables:
            print(f"  - {table}")


def column_headers():
    """Saves column headers for each table in each schema as CSV."""
    output_dir = "data/column_headers"
    os.makedirs(output_dir, exist_ok=True)
    summary_rows = []

    # Fetch the schemas
    schemas = con.sql("SELECT schema_name FROM information_schema.schemata").fetchall()
    schemas = [s[0] for s in schemas if not s[0].startswith("information_schema")]

    for schema in schemas:
        log.info(f"Processing schema: {schema}")
        tables = con.sql(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
        """).fetchall()

        schema_output_dir = os.path.join(output_dir, schema)
        os.makedirs(schema_output_dir, exist_ok=True)

        for (table,) in tables:
            try:
                result = con.sql(f"DESCRIBE {schema}.{table}").fetchall()
                column_names = [row[0] for row in result]

                csv_path = os.path.join(schema_output_dir, f"{table}.csv")
                with open(csv_path, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["column_name"])
                    writer.writerows([[col] for col in column_names])

                # Add to combined summary
                for col in column_names:
                    summary_rows.append([schema, table, col])

                log.info(f"Saved column headers to {csv_path}")
            except Exception as e:
                log.error(f"Error processing {schema}.{table}: {e}")

    # Write combined CSV
    summary_csv = os.path.join(output_dir, "_all_columns.csv")
    with open(summary_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["schema", "table", "column_name"])
        writer.writerows(summary_rows)

    log.info(f"Saved combined column summary to {summary_csv}")


def is_empty(val):
    """Checks if value is None, blank, or looks empty ('[]', '{}', etc)."""
    if val is None:
        return True
    if isinstance(val, str) and val.strip() in ("", "[]", "{}", "null", "None"):
        return True
    if isinstance(val, (list, dict)) and len(val) == 0:
        return True
    return False


def column_examples(output_path="data/column_examples.csv", max_rows_to_scan=100):
    """
    Exports column names for each table within each schema.
    Includes an example value per column (skips empty values until one found).
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    rows = []

    schemas = con.sql("SELECT schema_name FROM information_schema.schemata").fetchall()
    schemas = [s[0] for s in schemas if not s[0].startswith("information_schema")]

    for schema in schemas:
        log.info(f"Processing schema: {schema}")
        tables = con.sql(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
        """).fetchall()

        for (table,) in tables:
            try:
                column_info = con.sql(
                    f"PRAGMA table_info('{schema}.{table}')"
                ).fetchall()
                column_names = [col[1] for col in column_info]

                result = con.sql(
                    f"SELECT * FROM {schema}.{table} LIMIT {max_rows_to_scan}"
                ).fetchall()
                if not result:
                    continue

                col_examples = {col: None for col in column_names}

                for row in result:
                    for col, val in zip(column_names, row):
                        if col_examples[col] is None and not is_empty(val):
                            col_examples[col] = val
                    if all(v is not None for v in col_examples.values()):
                        break

                for col in column_names:
                    if col_examples[col] is not None:
                        rows.append(
                            {
                                "schema": schema,
                                "table": table,
                                "column_name": col,
                                "example_value": col_examples[col],
                            }
                        )

            except Exception as e:
                log.error(f"Error processing {schema}.{table}: {e}")

    # Write to CSV
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["schema", "table", "column_name", "example_value"]
        )
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"Saved column examples to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sql_queries.py <function_name>")
        sys.exit(1)

    func_name = sys.argv[1]

    if func_name in globals() and callable(globals()[func_name]):
        globals()[func_name]()
    else:
        print(f"Function '{func_name}' not found.")

    con.close()
