import os
import logging
from dotenv import load_dotenv
from utils.logging_setup import setup_logging
from utils.helpers import get_system_db, get_arango_db

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# Collection names
VERTEX_COLLECTIONS = ["Departments", "Epics", "Teams", "Users", "Repos"]
EDGE_COLLECTIONS = ["dept_to_epic", "epic_to_team", "team_to_user", "user_to_repo"]


def initialize_database():
    """
    Deletes the target database if it exists, then creates it anew.
    Returns a handle to the freshly created database.
    """
    try:
        sys_db = get_system_db()
    except Exception as e:
        log.error(f"Failed to connect to ArangoDB _system database: {e}")
        raise

    db_name = os.getenv("ARANGO_DB")

    # If the target database exists, delete it
    if sys_db.has_database(db_name):
        try:
            sys_db.delete_database(db_name)
            log.info(f"Deleted existing database '{db_name}'.")
        except Exception as e:
            log.error(f"Failed to delete existing database '{db_name}': {e}")
            raise
    else:
        log.info(f"Database '{db_name}' does not exist; will create new one.")

    # Create a fresh database
    try:
        sys_db.create_database(db_name)
        log.info(f"Created new database '{db_name}'.")
    except Exception as e:
        log.error(f"Failed to create database '{db_name}': {e}")
        raise

    # Return a handle to the new database
    try:
        return get_arango_db()
    except Exception as e:
        log.error(f"Failed to connect to newly created database '{db_name}': {e}")
        raise


def ensure_collections(db):
    """
    Given a database handle, ensure that all vertex and edge collections exist.
    If a collection is missing, create it.
    """
    # Create vertex collections
    for col_name in VERTEX_COLLECTIONS:
        if db.has_collection(col_name):
            log.info(f"Vertex collection '{col_name}' already exists; skipping.")
        else:
            try:
                db.create_collection(col_name)
                log.info(f"Created vertex collection '{col_name}'.")
            except Exception as e:
                log.error(f"Failed to create vertex collection '{col_name}': {e}")
                raise

    # Create edge collections
    for edge_name in EDGE_COLLECTIONS:
        if db.has_collection(edge_name):
            log.info(f"Edge collection '{edge_name}' already exists; skipping.")
        else:
            try:
                db.create_collection(edge_name, edge=True)
                log.info(f"Created edge collection '{edge_name}'.")
            except Exception as e:
                log.error(f"Failed to create edge collection '{edge_name}': {e}")
                raise


def main():
    db = initialize_database()
    ensure_collections(db)
    log.info("Graph initialization complete.")


if __name__ == "__main__":
    main()
