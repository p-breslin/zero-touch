import os
import logging
from dotenv import load_dotenv
from arango import ArangoClient
from utils.logging_setup import setup_logging


load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


# Fresh reset (deleting the database if exists)
try:
    # Connect to ArangoDB server
    client = ArangoClient(hosts=os.getenv("ARANGO_HOST"))

    try:
        # Authenticate with root user (required to manage databases)
        sys_db = client.db(
            "_system", username="root", password=os.getenv("ARANGO_PASSWORD")
        )

        db_name = os.getenv("ARANGO_DB")

        # Check if the database exists, then delete it
        if sys_db.has_database(db_name):
            sys_db.delete_database(db_name)
            log.info(f"Database '{db_name}' deleted successfully.")
        else:
            log.warning(f"Database '{db_name}' does not exist.")

        sys_db.create_database(db_name)
        log.info(f"New database '{db_name}' created successfully.")

    except Exception as e:
        log.error(f"Failed to authenticate with root: {e}")
        raise

except Exception as e:
    log.error(f"Failed to connect to ArangoDB server: {e}")
    raise
