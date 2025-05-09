import os
import logging
from dotenv import load_dotenv
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, Engine
from utils.logging_setup import setup_logging

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


def create_db_engine(db_type: str = "duckdb") -> Engine:
    """Creates a sqlalchemy engine for Agno Agents to access via SQL calls."""

    if db_type.lower() == "snowflake":
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

        try:
            engine = create_engine(
                URL(
                    user=user,
                    password=password,
                    account=account,
                    database=database,
                    schema=schema,
                    warehouse=warehouse,
                )
            )
            log.info("Snowflake engine created.")
            return engine
        except Exception as e:
            log.error(f"Error creating Snowflake engine or connecting: {e}")
            raise

    elif db_type.lower() == "duckdb":
        db_name = os.getenv("SNOWFLAKE_DATABASE")
        abs_path = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(abs_path, f"../data/{db_name}.duckdb")
        duckdb_url = f"duckdb:///{db_path}"
        try:
            engine = create_engine(duckdb_url)
            log.info("DuckDB engine created.")
            return engine
        except Exception as e:
            log.error(f"Error creating DuckDB engine or connecting: {e}")
            raise
