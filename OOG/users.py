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
ARANGO_USERS_COLL = "Users"
ARANGO_EDGE_TEAM_TO_USER = "team_to_user"
ARANGO_TEAMS_COLL = "Teams"
DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")


def import_users_and_memberships():
    """
    1. Read user profiles from MATCHED_USERS joined with DEVELOPER_INFERENCE.
       Includes alias fields.
    2. Insert each user as a document in `Users`.
    3. Read story-user memberships from JIRA_ISSUES via ASSIGNEE_ACCOUNT_ID and REPORTER_ACCOUNT_ID.
    4. Insert an edge in `team_to_user` for each (story, user) with roleOnStory.
    """
    db = get_arango_db()

    # Ensure required collections exist
    for coll in (ARANGO_USERS_COLL, ARANGO_EDGE_TEAM_TO_USER, ARANGO_TEAMS_COLL):
        if not db.has_collection(coll):
            log.error(f"Collection '{coll}' does not exist")
            return

    user_col = db.collection(ARANGO_USERS_COLL)
    edge_col = db.collection(ARANGO_EDGE_TEAM_TO_USER)
    team_col = db.collection(ARANGO_TEAMS_COLL)

    # 1. Query user profiles including aliases
    user_sql = """
        SELECT
            m.UUID,
            m.JIRA_ID,
            m.JIRA_DISPLAY_NAME,
            m.JIRA_EMAIL,
            m.GITHUB_ID,
            m.GITHUB_ID_ALIAS,
            m.GITHUB_LOGIN,
            m.GITHUB_LOGIN_ALIAS,
            m.GITHUB_DISPLAY_NAME,
            m.GITHUB_DISPLAY_NAME_ALIAS,
            m.GITHUB_EMAIL,
            m.GITHUB_EMAIL_ALIAS,
            di.ROLE,
            di.EXPERIENCE_LEVEL,
            di.SKILLS
        FROM MATCHED_USERS AS m
        LEFT JOIN DEVELOPER_INFERENCE AS di
          ON m.UUID = di.UUID
    """
    with db_manager(DB_PATH) as conn:
        user_rows = conn.execute(user_sql).fetchall()

    # 2. Insert each user document
    for (
        uuid,
        jira_id,
        jira_display_name,
        jira_email,
        github_id,
        github_id_alias,
        github_login,
        github_login_alias,
        github_display_name,
        github_display_name_alias,
        github_email,
        github_email_alias,
        role,
        experience_level,
        skills_raw,
    ) in user_rows:
        # Parse skills_raw into a list (comma-separated)
        if skills_raw:
            skills_list = [s.strip() for s in skills_raw.split(",") if s.strip()]
        else:
            skills_list = []

        if user_col.has(uuid):
            log.info(f"User '{uuid}' already exists; skipping insert.")
        else:
            user_doc = {
                "_key": uuid,
                "jira_id": jira_id,
                "jira_display_name": jira_display_name,
                "jira_email": jira_email,
                "github_id": github_id,
                "github_id_alias": github_id_alias,
                "github_login": github_login,
                "github_login_alias": github_login_alias,
                "github_display_name": github_display_name,
                "github_display_name_alias": github_display_name_alias,
                "github_email": github_email,
                "github_email_alias": github_email_alias,
                "role": role,
                "experience_level": experience_level,
                "skills": skills_list,
            }
            try:
                user_col.insert(user_doc)
                log.info(f"Inserted User '{uuid}'.")
            except Exception as e:
                log.error(f"Failed to insert User '{uuid}': {e}")
                raise

    # 3. Query story-user memberships (assignees + reporters)
    membership_sql = """
        SELECT
          j.ISSUE_KEY,
          m.UUID AS user_uuid,
          'Assignee' AS roleOnStory
        FROM JIRA_ISSUES AS j
        JOIN MATCHED_USERS AS m
          ON j.ASSIGNEE_ACCOUNT_ID = m.JIRA_ID
        WHERE j.ASSIGNEE_ACCOUNT_ID IS NOT NULL
        UNION ALL
        SELECT
          j.ISSUE_KEY,
          m.UUID AS user_uuid,
          'Reporter' AS roleOnStory
        FROM JIRA_ISSUES AS j
        JOIN MATCHED_USERS AS m
          ON j.REPORTER_ACCOUNT_ID = m.JIRA_ID
        WHERE j.REPORTER_ACCOUNT_ID IS NOT NULL
    """
    with db_manager(DB_PATH) as conn:
        membership_rows = conn.execute(membership_sql).fetchall()

    # 4. Insert each team_to_user edge
    for issue_key, user_uuid, role_on_story in membership_rows:
        # Skip if the Team or User does not exist
        if not team_col.has(issue_key):
            log.warning(
                f"Team/Story '{issue_key}' not found; skipping edge creation for user '{user_uuid}'."
            )
            continue
        if not user_col.has(user_uuid):
            log.warning(
                f"User '{user_uuid}' not found; skipping edge creation for story '{issue_key}'."
            )
            continue

        edge_key = f"{issue_key}-{user_uuid}"
        if edge_col.has(edge_key):
            log.info(f"Edge '{edge_key}' already exists; skipping.")
        else:
            edge_doc = {
                "_key": edge_key,
                "_from": f"Teams/{issue_key}",
                "_to": f"Users/{user_uuid}",
                "roleOnStory": role_on_story,
            }
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted edge team_to_user '{edge_key}'.")
            except Exception as e:
                log.error(f"Failed to insert team_to_user edge '{edge_key}': {e}")
                raise


def main():
    import_users_and_memberships()
    log.info("Users imported.")


if __name__ == "__main__":
    main()
