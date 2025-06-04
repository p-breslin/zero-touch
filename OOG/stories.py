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
ARANGO_TEAMS_COLL = "Teams"
ARANGO_EDGE_EPIC_TO_TEAM = "epic_to_team"
ARANGO_EPICS_COLL = "Epics"
DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")


def import_stories_and_edges():
    """
    1. Read each issue (story) from JIRA_ISSUES.
    2. Insert each as a document in `Teams` (using ISSUE_KEY as _key).
    3. For each story, insert an edge in `epic_to_team` linking
       "Epics/<PROJECT_KEY>" -> "Teams/<ISSUE_KEY>".
    """
    db = get_arango_db()

    # Ensure required collections exist
    if not db.has_collection(ARANGO_TEAMS_COLL):
        log.error(f"Collection '{ARANGO_TEAMS_COLL}' does not exist.")
        return
    if not db.has_collection(ARANGO_EDGE_EPIC_TO_TEAM):
        log.error(f"Collection '{ARANGO_EDGE_EPIC_TO_TEAM}' does not exist.")
        return
    if not db.has_collection(ARANGO_EPICS_COLL):
        log.error(f"Collection '{ARANGO_EPICS_COLL}' does not exist.")
        return

    team_col = db.collection(ARANGO_TEAMS_COLL)
    edge_col = db.collection(ARANGO_EDGE_EPIC_TO_TEAM)

    # 1. Query all stories from JIRA_ISSUES
    sql = """
        SELECT
            ISSUE_KEY,
            SUMMARY,
            ISSUE_TYPE_NAME,
            STATUS_NAME,
            CREATED_TIMESTAMP,
            UPDATED_TIMESTAMP,
            PROJECT_KEY
        FROM JIRA_ISSUES
        WHERE ISSUE_KEY IS NOT NULL
    """

    with db_manager(DB_PATH) as conn:
        rows = conn.execute(sql).fetchall()

    for (
        issue_key,
        summary,
        issue_type,
        status,
        created_ts,
        updated_ts,
        project_key,
    ) in rows:
        # Convert datetime to ISO strings (or leave as None)
        created_str = created_ts.isoformat() if created_ts is not None else None
        updated_str = updated_ts.isoformat() if updated_ts is not None else None

        # 2.a Insert story document if it doesn't exist
        if team_col.has(issue_key):
            log.info(f"Team (Story) '{issue_key}' already exists; skipping insert.")
        else:
            team_doc = {
                "_key": issue_key,
                "summary": summary,
                "issue_type": issue_type,
                "status": status,
                "created_ts": created_str,
                "updated_ts": updated_str,
            }
            try:
                team_col.insert(team_doc)
                log.info(f"Inserted Team (Story) '{issue_key}'.")
            except Exception as e:
                log.error(f"Failed to insert Team '{issue_key}': {e}")
                raise

        # 3. Insert epic_to_team edge if it doesn't exist
        if not project_key:
            log.warning(
                f"Issue '{issue_key}' has no PROJECT_KEY; skipping edge creation."
            )
            continue

        # Check that the referenced Epic exists
        if not db.collection(ARANGO_EPICS_COLL).has(project_key):
            log.warning(
                f"Referenced Epic '{project_key}' not found for Story '{issue_key}'; skipping edge."
            )
            continue

        edge_key = f"{project_key}-{issue_key}"
        if edge_col.has(edge_key):
            log.info(f"Edge '{edge_key}' already exists; skipping.")
        else:
            edge_doc = {
                "_key": edge_key,
                "_from": f"Epics/{project_key}",
                "_to": f"Teams/{issue_key}",
            }
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted edge epic_to_team '{edge_key}'.")
            except Exception as e:
                log.error(f"Failed to insert edge '{edge_key}': {e}")
                raise


def main():
    import_stories_and_edges()
    log.info("Stories node inserted.")


if __name__ == "__main__":
    main()
