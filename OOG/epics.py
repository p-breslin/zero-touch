import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging
from utils.helpers import get_arango_db, db_manager

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# Constants
DEPARTMENT_KEY = "engineering"
ARANGO_EPICS_COLL = "Epics"
ARANGO_EDGE_DEPT_TO_EPIC = "dept_to_epic"
DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")


def import_epics_and_edges():
    """
    1. Reads distinct PROJECT_NAME (epic_key) and PROJECT_KEY from JIRA_ISSUES.
    2. Insert each epic as a document in `Epics`, unless it already exists.
    3. For each epic, insert an edge in `dept_to_epic` linking
       "Departments/engineering" -> "Epics/<epic_key>". Use a deterministic _key
       for the edge so repeated runs don't duplicate edges.
    """
    db = get_arango_db()

    # Ensure required collections exist
    if not db.has_collection(ARANGO_EPICS_COLL):
        log.error(f"Collection '{ARANGO_EPICS_COLL}' does not exist.")
        return
    if not db.has_collection(ARANGO_EDGE_DEPT_TO_EPIC):
        log.error(f"Collection '{ARANGO_EDGE_DEPT_TO_EPIC}' does not exist.")
        return

    epic_col = db.collection(ARANGO_EPICS_COLL)
    edge_col = db.collection(ARANGO_EDGE_DEPT_TO_EPIC)

    # 1. Query all distinct epics (project_key + project_name) from JIRA_ISSUES
    sql = """
        SELECT DISTINCT
            PROJECT_KEY   AS epic_key,
            PROJECT_NAME  AS epic_title
        FROM JIRA_ISSUES
        WHERE PROJECT_KEY IS NOT NULL
    """

    with db_manager(DB_PATH) as conn:
        rows = conn.execute(sql).fetchall()

    for epic_key, epic_title in rows:
        # 2.a Insert epic document if it doesn't exist
        if epic_col.has(epic_key):
            log.info(f"Epic '{epic_key}' already exists; skipping insert.")
        else:
            epic_doc = {"_key": epic_key, "title": epic_title}
            try:
                epic_col.insert(epic_doc)
                log.info(f"Inserted Epic '{epic_key}'.")
            except Exception as e:
                log.error(f"Failed to insert Epic '{epic_key}': {e}")
                raise

        # 2.b Insert dept_to_epic edge if it doesn't exist
        edge_key = f"{DEPARTMENT_KEY}-{epic_key}"
        if edge_col.has(edge_key):
            log.info(f"Edge '{edge_key}' already exists; skipping.")
        else:
            edge_doc = {
                "_key": edge_key,
                "_from": f"Departments/{DEPARTMENT_KEY}",
                "_to": f"Epics/{epic_key}",
            }
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted edge dept_to_epic '{edge_key}'.")
            except Exception as e:
                log.error(f"Failed to insert edge '{edge_key}': {e}")
                raise


def main():
    import_epics_and_edges()
    log.info("Epics node inserted.")


if __name__ == "__main__":
    main()
