from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging

# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
T_TEMP = "TEMP_ROLE_INFERENCES_PER_LINK"
T_ATTR = "PERSON_ATTRIBUTES"

AGG_SQL = f"""
WITH counts AS (
  SELECT ASSIGNEE_ID, INFERRED_ROLE, COUNT(*) AS n
  FROM   {T_TEMP}
  WHERE  INFERRED_ROLE IS NOT NULL
  GROUP  BY ASSIGNEE_ID, INFERRED_ROLE
), ranked AS (
  SELECT *,
         row_number()
           OVER (PARTITION BY ASSIGNEE_ID ORDER BY n DESC, INFERRED_ROLE) AS rn
  FROM counts
)
SELECT ASSIGNEE_ID, INFERRED_ROLE
FROM   ranked
WHERE  rn = 1
"""


def update_roles():
    with db_manager(DB) as conn:
        rows = conn.execute(AGG_SQL).fetchall()
        if not rows:
            log.info("No role aggregates to apply.")
            return
        conn.executemany(
            f"UPDATE {T_ATTR} SET ROLE = ? WHERE ASSIGNEE_JIRA_ACCOUNT_ID = ?;",
            [(r, a) for a, r in rows],
        )
        conn.commit()
        log.info("Updated ROLE for %d people", len(rows))


if __name__ == "__main__":
    update_roles()
