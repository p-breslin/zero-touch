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
USERS_COLL = "Users"
EPICS_COLL = "Epics"
USER_TO_EPIC_DIRECT_COLL = "user_to_epic_direct"

DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")


def ensure_and_populate_user_to_epic_direct():
    """
    1. Verify that 'Users' and 'Epics' vertex collections exist.
    2. Create 'user_to_epic_direct' edge collection if missing.
    3. Read each (user_uuid, epic_key) pair from JIRA_ISSUES joined with MATCHED_USERS, counting distinct stories that connect them.
    4. For each pair, insert or update an edge in 'user_to_epic_direct' with the computed storyCount.
    """
    db = get_arango_db()

    # 1. Verify that the Users and Epics vertex collections exist
    for coll_name in (USERS_COLL, EPICS_COLL):
        if not db.has_collection(coll_name):
            log.error(f"Required vertex collection '{coll_name}' does not exist.")
            return

    # 2. Create (if necessary) the 'user_to_epic_direct' edge collection
    if db.has_collection(USER_TO_EPIC_DIRECT_COLL):
        log.info(
            f"Edge collection '{USER_TO_EPIC_DIRECT_COLL}' already exists; proceeding to populate."
        )
    else:
        try:
            db.create_collection(USER_TO_EPIC_DIRECT_COLL, edge=True)
            log.info(f"Created edge collection '{USER_TO_EPIC_DIRECT_COLL}'.")
        except Exception as e:
            log.error(
                f"Failed to create edge collection '{USER_TO_EPIC_DIRECT_COLL}': {e}"
            )
            return

    edge_col = db.collection(USER_TO_EPIC_DIRECT_COLL)
    user_col = db.collection(USERS_COLL)
    epic_col = db.collection(EPICS_COLL)

    # 3. Query each user-epic pair with a count of distinct stories
    sql = """
        SELECT
            m.UUID            AS user_uuid,
            j.PROJECT_KEY     AS epic_key,
            COUNT(DISTINCT j.ISSUE_KEY) AS story_count
        FROM JIRA_ISSUES AS j
        JOIN MATCHED_USERS AS m
          ON j.ASSIGNEE_ACCOUNT_ID = m.JIRA_ID
          OR  j.REPORTER_ACCOUNT_ID = m.JIRA_ID
        WHERE j.PROJECT_KEY IS NOT NULL
          AND m.UUID IS NOT NULL
        GROUP BY m.UUID, j.PROJECT_KEY
    """

    with db_manager(DB_PATH) as conn:
        rows = conn.execute(sql).fetchall()

    # 4. Insert or update each edge
    for user_uuid, epic_key, story_count in rows:
        # Skip if either User or Epic does not exist in Arango
        if not user_col.has(user_uuid):
            log.warning(f"User '{user_uuid}' not found in '{USERS_COLL}'; skipping.")
            continue
        if not epic_col.has(epic_key):
            log.warning(f"Epic '{epic_key}' not found in '{EPICS_COLL}'; skipping.")
            continue

        edge_key = f"{user_uuid}-{epic_key}"
        if edge_col.has(edge_key):
            # Update existing edge's storyCount
            try:
                edge_col.update({"_key": edge_key, "storyCount": story_count})
                log.info(f"Updated '{edge_key}' with storyCount={story_count}.")
            except Exception as e:
                log.error(f"Failed to update edge '{edge_key}': {e}")
                return
        else:
            edge_doc = {
                "_key": edge_key,
                "_from": f"Users/{user_uuid}",
                "_to": f"Epics/{epic_key}",
                "storyCount": story_count,
            }
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted edge '{edge_key}' (storyCount={story_count}).")
            except Exception as e:
                log.error(f"Failed to insert edge '{edge_key}': {e}")
                return


def main():
    ensure_and_populate_user_to_epic_direct()
    log.info("'user_to_epic_direct' edge collection created.")


if __name__ == "__main__":
    main()
