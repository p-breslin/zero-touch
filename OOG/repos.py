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
ARANGO_REPOS_COLL = "Repos"
ARANGO_EDGE_USER_TO_REPO = "user_to_repo"
ARANGO_USERS_COLL = "Users"
DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")


def import_repos_and_commits():
    """
    1. Read distinct repos from GITHUB_COMMITS, GITHUB_PRS; insert into `Repos`.
    2. Aggregate commit data, mapping AUTHOR_ID -> user_uuid via MATCHED_USERS.GITHUB_ID, insert `user_to_repo` edges with firstCommit & commitCount.
    3. Aggregate PR data for any (user, repo) pairs not covered by commits, insert fallback `user_to_repo` edges with firstPR & prCount.
    """
    db = get_arango_db()

    # Ensure required collections exist
    for coll in (ARANGO_REPOS_COLL, ARANGO_EDGE_USER_TO_REPO, ARANGO_USERS_COLL):
        if not db.has_collection(coll):
            log.error(f"Collection '{coll}' does not exist")
            return

    repo_col = db.collection(ARANGO_REPOS_COLL)
    edge_col = db.collection(ARANGO_EDGE_USER_TO_REPO)
    user_col = db.collection(ARANGO_USERS_COLL)

    # 1. Insert distinct repos from both GITHUB_COMMITS and GITHUB_PRS
    repo_sql = """
        SELECT DISTINCT
            ORG || '/' || REPO AS full_name,
            ORG,
            REPO
        FROM GITHUB_COMMITS
        UNION
        SELECT DISTINCT
            ORG || '/' || REPO AS full_name,
            ORG,
            REPO
        FROM GITHUB_PRS
    """
    with db_manager(DB_PATH) as conn:
        repo_rows = conn.execute(repo_sql).fetchall()

    for full_name, org, repo_name in repo_rows:
        # Use full_name as the _key (must replace any slashes with '__')
        key_safe = full_name.replace("/", "__")
        if repo_col.has(key_safe):
            log.info(f"Repo '{key_safe}' already exists; skipping insert.")
        else:
            repo_doc = {
                "_key": key_safe,
                "org": org,
                "repo": repo_name,
                "full_name": full_name,
            }
            try:
                repo_col.insert(repo_doc)
                log.info(f"Inserted Repo '{key_safe}'.")
            except Exception as e:
                log.error(f"Failed to insert Repo '{key_safe}': {e}")
                raise

    # 2. Aggregate commit data and insert edges
    commit_sql = """
        SELECT
            m.UUID AS user_uuid,
            g.ORG || '/' || g.REPO AS full_name,
            MIN(g.COMMIT_TIMESTAMP) AS first_commit_ts,
            COUNT(*) AS commit_count
        FROM GITHUB_COMMITS AS g
        JOIN MATCHED_USERS AS m
          ON g.AUTHOR_ID = m.GITHUB_ID
        GROUP BY m.UUID, g.ORG, g.REPO
    """
    with db_manager(DB_PATH) as conn:
        commit_rows = conn.execute(commit_sql).fetchall()

    for user_uuid, full_name, first_commit_ts, commit_count in commit_rows:
        # Convert timestamp to ISO string
        first_commit_str = first_commit_ts.isoformat() if first_commit_ts else None
        # Convert full_name => key_safe
        key_safe = full_name.replace("/", "__")

        # Skip if the User or Repo does not exist
        if not user_col.has(user_uuid):
            log.warning(
                f"User '{user_uuid}' not found; skipping commit edge for '{full_name}'."
            )
            continue
        if not repo_col.has(key_safe):
            log.warning(
                f"Repo '{key_safe}' not found; skipping commit edge for user '{user_uuid}'."
            )
            continue

        edge_key = f"{user_uuid}-{key_safe}"
        if edge_col.has(edge_key):
            log.info(f"Edge '{edge_key}' already exists; skipping.")
        else:
            edge_doc = {
                "_key": edge_key,
                "_from": f"Users/{user_uuid}",
                "_to": f"Repos/{key_safe}",
                "firstCommit": first_commit_str,
                "commitCount": commit_count,
            }
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted commit edge '{edge_key}'.")
            except Exception as e:
                log.error(f"Failed to insert commit edge '{edge_key}': {e}")
                raise

    # 3. Aggregate PR data for fallback (users/repos not covered above)
    pr_sql = """
        SELECT
            m.UUID AS user_uuid,
            p.ORG || '/' || p.REPO AS full_name,
            MIN(p.CREATED_AT) AS first_pr_ts,
            COUNT(*) AS pr_count
        FROM GITHUB_PRS AS p
        JOIN MATCHED_USERS AS m
          ON p.USER_ID = m.GITHUB_ID
        GROUP BY m.UUID, p.ORG, p.REPO
    """
    with db_manager(DB_PATH) as conn:
        pr_rows = conn.execute(pr_sql).fetchall()

    for user_uuid, full_name, first_pr_ts, pr_count in pr_rows:
        # Convert timestamp to ISO string
        first_pr_str = first_pr_ts.isoformat() if first_pr_ts else None
        # Convert full_name => key_safe
        key_safe = full_name.replace("/", "__")

        # Skip if the User or Repo does not exist
        if not user_col.has(user_uuid):
            log.warning(
                f"User '{user_uuid}' not found; skipping PR edge for '{full_name}'."
            )
            continue
        if not repo_col.has(key_safe):
            log.warning(
                f"Repo '{key_safe}' not found; skipping PR edge for user '{user_uuid}'."
            )
            continue

        edge_key = f"{user_uuid}-{key_safe}"
        if edge_col.has(edge_key):
            log.info(
                f"Edge '{edge_key}' already exists (commit already handled); skipping PR fallback."
            )
        else:
            edge_doc = {
                "_key": edge_key,
                "_from": f"Users/{user_uuid}",
                "_to": f"Repos/{key_safe}",
                "firstPR": first_pr_str,
                "prCount": pr_count,
            }
            try:
                edge_col.insert(edge_doc)
                log.info(f"Inserted PR fallback edge '{edge_key}'.")
            except Exception as e:
                log.error(f"Failed to insert PR edge '{edge_key}': {e}")
                raise


def main():
    import_repos_and_commits()
    log.info("Repos imported.")


if __name__ == "__main__":
    main()
