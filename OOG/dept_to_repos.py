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
DEPARTMENTS_COLL = "Departments"
REPOS_COLL = "Repos"
DEPT_TO_REPO_DIRECT_COLL = "dept_to_repo_direct"

DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")

# Department key (root)
DEPARTMENT_KEY = "engineering"


def ensure_and_populate_dept_to_repo_direct():
    """
    1. Verify that 'Departments' and 'Repos' vertex collections exist.
    2. Create 'dept_to_repo_direct' edge collection if missing.
    3. Query all distinct repos (from GITHUB_COMMITS and GITHUB_PRS) that are touched by any user who is assigned or reported on any JIRA_ISSUE.
    4. Insert or update an edge 'Departments/engineering' -> 'Repos/<repo_key>' with no additional properties.
    """
    db = get_arango_db()

    # 1. Verify that Departments and Repos vertex collections exist
    for coll_name in (DEPARTMENTS_COLL, REPOS_COLL):
        if not db.has_collection(coll_name):
            log.error(f"Required vertex collection '{coll_name}' does not exist.")
            return

    # 2. Create (if necessary) the 'dept_to_repo_direct' edge collection
    if db.has_collection(DEPT_TO_REPO_DIRECT_COLL):
        log.info(
            f"Edge collection '{DEPT_TO_REPO_DIRECT_COLL}' already exists; proceeding to populate."
        )
    else:
        try:
            db.create_collection(DEPT_TO_REPO_DIRECT_COLL, edge=True)
            log.info(f"Created edge collection '{DEPT_TO_REPO_DIRECT_COLL}'.")
        except Exception as e:
            log.error(
                f"Failed to create edge collection '{DEPT_TO_REPO_DIRECT_COLL}': {e}"
            )
            return

    edge_col = db.collection(DEPT_TO_REPO_DIRECT_COLL)
    dept_col = db.collection(DEPARTMENTS_COLL)
    repo_col = db.collection(REPOS_COLL)

    # Verify that the "engineering" document is present:
    if not dept_col.has(DEPARTMENT_KEY):
        log.error(
            f"Department document 'Departments/{DEPARTMENT_KEY}' not found; cannot create edges."
        )
        return

    # 3. Query all distinct repos touched by any user on any Jira issue

    # Commit-based repos:
    commit_subquery = """
        SELECT DISTINCT
            g.ORG || '/' || g.REPO AS full_name
        FROM GITHUB_COMMITS AS g
        JOIN MATCHED_USERS AS m
          ON g.AUTHOR_ID = m.GITHUB_ID
        WHERE EXISTS (
          SELECT 1
          FROM JIRA_ISSUES AS j
          WHERE (j.ASSIGNEE_ACCOUNT_ID = m.JIRA_ID OR j.REPORTER_ACCOUNT_ID = m.JIRA_ID)
        )
    """
    # PR-based repos (for fallback or additional coverage)
    pr_subquery = """
        SELECT DISTINCT
            p.ORG || '/' || p.REPO AS full_name
        FROM GITHUB_PRS AS p
        JOIN MATCHED_USERS AS m
          ON p.USER_ID = m.GITHUB_ID
        WHERE EXISTS (
          SELECT 1
          FROM JIRA_ISSUES AS j
          WHERE (j.ASSIGNEE_ACCOUNT_ID = m.JIRA_ID OR j.REPORTER_ACCOUNT_ID = m.JIRA_ID)
        )
    """
    full_sql = f"""
        {commit_subquery}
        UNION
        {pr_subquery}
    """

    with db_manager(DB_PATH) as conn:
        rows = conn.execute(full_sql).fetchall()

    # 4. Insert or update each edge
    for (full_name,) in rows:
        # Convert full_name (e.g. "org/my-repo") -> key-safe (e.g. "org__my-repo")
        key_safe = full_name.replace("/", "__")

        # Ensure the Repo vertex exists
        if not repo_col.has(key_safe):
            log.warning(f"Repo '{key_safe}' not found in '{REPOS_COLL}'; skipping.")
            continue

        # Edge key: combine department and repo
        edge_key = f"{DEPARTMENT_KEY}-{key_safe}"
        from_id = f"Departments/{DEPARTMENT_KEY}"
        to_id = f"Repos/{key_safe}"

        if edge_col.has(edge_key):
            log.info(f"Edge '{edge_key}' already exists; skipping.")
        else:
            edge_doc = {"_key": edge_key, "_from": from_id, "_to": to_id}
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted edge '{edge_key}'.")
            except Exception as e:
                log.error(f"Failed to insert edge '{edge_key}': {e}")
                return


def main():
    ensure_and_populate_dept_to_repo_direct()
    log.info("Department to Repos collection created and inserted.")


if __name__ == "__main__":
    main()
