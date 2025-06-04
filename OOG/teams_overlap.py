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

# Collection names
EPICS_COLL = "Epics"
EPIC_COLLAB_COLL = "epic_collaboration"
DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")


def ensure_and_populate_epic_collaboration():
    """
    1. Verify that 'Epics' vertex collection exists.
    2. Create 'epic_collaboration' edge collection if missing.
    3. Query all pairs of epics that share at least one user, counting how many users they share.
    4. For each (epic1, epic2, sharedCount), insert or update an edge in 'epic_collaboration'
       with:
         _key:         "<epic1>-<epic2>"
         _from:        "Epics/<epic1>"
         _to:          "Epics/<epic2>"
         sharedCount:  <number of shared users>
    """
    db = get_arango_db()

    # 1. Verify that Epics vertex collection exists
    if not db.has_collection(EPICS_COLL):
        log.error(f"Required vertex collection '{EPICS_COLL}' does not exist.")
        return

    # 2. Create (if necessary) the 'epic_collaboration' edge collection
    if db.has_collection(EPIC_COLLAB_COLL):
        log.info(
            f"Edge collection '{EPIC_COLLAB_COLL}' already exists; proceeding to populate."
        )
    else:
        try:
            db.create_collection(EPIC_COLLAB_COLL, edge=True)
            log.info(f"Created edge collection '{EPIC_COLLAB_COLL}'.")
        except Exception as e:
            log.error(f"Failed to create edge collection '{EPIC_COLLAB_COLL}': {e}")
            return

    edge_col = db.collection(EPIC_COLLAB_COLL)
    epic_col = db.collection(EPICS_COLL)

    # 3. Query pairs of epics sharing users
    sql = """
        WITH user_epic AS (
            SELECT DISTINCT
                m.UUID       AS user_uuid,
                j.PROJECT_KEY AS epic_key
            FROM JIRA_ISSUES AS j
            JOIN MATCHED_USERS AS m
              ON j.ASSIGNEE_ACCOUNT_ID = m.JIRA_ID
              OR  j.REPORTER_ACCOUNT_ID = m.JIRA_ID
            WHERE j.PROJECT_KEY IS NOT NULL
              AND m.UUID IS NOT NULL
        )
        SELECT
            a.epic_key  AS epic1,
            b.epic_key  AS epic2,
            COUNT(*)    AS sharedCount
        FROM user_epic a
        JOIN user_epic b
          ON a.user_uuid = b.user_uuid
         AND a.epic_key < b.epic_key
        GROUP BY a.epic_key, b.epic_key
    """

    with db_manager(DB_PATH) as conn:
        rows = conn.execute(sql).fetchall()

    # 4. Insert or update each collaboration edge
    for epic1, epic2, shared_count in rows:
        # Skip if either Epic does not exist
        if not epic_col.has(epic1):
            log.warning(f"Epic '{epic1}' not found in '{EPICS_COLL}'; skipping.")
            continue
        if not epic_col.has(epic2):
            log.warning(f"Epic '{epic2}' not found in '{EPICS_COLL}'; skipping.")
            continue

        edge_key = f"{epic1}-{epic2}"
        from_id = f"{EPICS_COLL}/{epic1}"
        to_id = f"{EPICS_COLL}/{epic2}"

        if edge_col.has(edge_key):
            # Update existing edge's sharedCount
            try:
                edge_col.update({"_key": edge_key, "sharedCount": shared_count})
                log.info(f"Updated edge '{edge_key}' with sharedCount={shared_count}.")
            except Exception as e:
                log.error(f"Failed to update edge '{edge_key}': {e}")
                return
        else:
            edge_doc = {
                "_key": edge_key,
                "_from": from_id,
                "_to": to_id,
                "sharedCount": shared_count,
            }
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted edge '{edge_key}' (sharedCount={shared_count}).")
            except Exception as e:
                log.error(f"Failed to insert edge '{edge_key}': {e}")
                return


def main():
    ensure_and_populate_epic_collaboration()
    log.info("Epic collaboration network created.")


if __name__ == "__main__":
    main()
