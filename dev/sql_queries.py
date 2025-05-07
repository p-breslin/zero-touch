import duckdb
from collections import defaultdict

con = duckdb.connect('data/MELTANO_DATABASE.duckdb')

# Query to get all tables by schema
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