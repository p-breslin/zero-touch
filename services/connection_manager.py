import os
import logging
import snowflake.connector
from dotenv import load_dotenv
from utils.logging_setup import setup_logging

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self, schema_name, database_name=None):
        self._conn = None
        self._schema_name = schema_name.upper()
        self._database_name = (
            database_name if database_name else os.getenv("SNOWFLAKE_DATABASE")
        )

    def get_connection(self):
        if self._conn is None:
            try:
                self._conn = snowflake.connector.connect(
                    user=os.getenv("SNOWFLAKE_USER"),
                    password=os.getenv("SNOWFLAKE_PASSWORD"),
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                    database=self._database_name,
                    schema=self._schema_name,
                )
                log.info("Snowflake connection established.")
            except Exception as e:
                log.error(f"Failed to connect to Snowflake: {e}")
                raise e
        return self._conn

    def close_connection(self):
        if self._conn:
            try:
                self._conn.close()
                log.info("Snowflake connection closed.")
            except Exception as e:
                log.error(f"Failed to close Snowflake connection: {e}")
                raise e
            finally:
                self._conn = None
