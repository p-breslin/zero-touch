"""
Enriches CONSOLIDATED_GH_USERS. For every row where GITHUB_DISPLAY_NAME IS NULL
  1) Compare GITHUB_LOGIN (normalized) with all distinct COMMITTER_NAME
     values (normalized) from GITHUB_COMMITS using a composite fuzzy score.
    2) If similarity >= MATCH_THRESHOLD, update:
         GITHUB_DISPLAY_NAME <- matched COMMITTER_NAME
         GITHUB_EMAIL        <- existing | matched COMMITTER_EMAIL
"""

from __future__ import annotations
import os
import re
import logging
from pathlib import Path
from rapidfuzz import fuzz
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
MATCH_THRESHOLD = 75  # 0-100 token_set_ratio to accept a match

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

STG_DB = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
T_USERS_GH = "CONSOLIDATED_GH_USERS"
T_COMMITS = "GITHUB_COMMITS"

# Generic normalization
_camelsplit = re.compile(r"([a-z])([A-Z])")
_non_alpha = re.compile(r"[^a-z\s]+")


# Helpers ----------------------------------------------------------------------
def norm(raw: str) -> str:
    """
    1. split camelCase → camel Case
    2. replace non-letters (digits, hyphens, etc.) with space
    3. lowercase + collapse spaces
    """
    s = _camelsplit.sub(r"\1 \2", raw)  # camel -> space
    s = _non_alpha.sub(" ", s.lower())  # keep letters
    return " ".join(s.split())


def composite_score(a: str, b: str) -> int:
    """greatest of several RapidFuzz scorers"""
    return max(
        fuzz.token_set_ratio(a, b),
        fuzz.token_sort_ratio(a, b),
        fuzz.partial_ratio(a, b),
    )


# Main logic -------------------------------------------------------------------
def main() -> None:
    with db_manager(STG_DB) as conn:
        # Pull missing-name users
        users_missing = conn.execute(f"""
            SELECT GITHUB_LOGIN, GITHUB_EMAIL
            FROM   {T_USERS_GH}
            WHERE  GITHUB_DISPLAY_NAME IS NULL
        """).fetchall()

        if not users_missing:
            log.info("Nothing to enrich — all users have display names.")
            return

        # Pull distinct committer names + email
        commit_names = conn.execute(f"""
            SELECT DISTINCT COMMITTER_NAME, COMMITTER_EMAIL
            FROM   {T_COMMITS}
            WHERE  COMMITTER_NAME IS NOT NULL
        """).fetchall()

    # Prepare candidate list
    name_candidates = [
        (raw_name, email, norm(raw_name)) for raw_name, email in commit_names
    ]

    updates: list[tuple[str, str | None, str]] = []  # (name,email,login)

    # Attempt match for each missing user
    for login, cur_email in users_missing:
        login_norm = norm(login)
        best_score = 0
        best_name = None
        best_email = None

        for raw_name, cm_email, name_norm in name_candidates:
            score = composite_score(login_norm, name_norm)
            if score > best_score:
                best_score, best_name, best_email = score, raw_name, cm_email
                if best_score == 100:
                    break

        if best_score >= MATCH_THRESHOLD and best_name:
            updates.append(
                (
                    best_name,
                    best_email if not cur_email else cur_email,
                    login,
                )
            )
            log.debug("Matched %-22s -> %-25s (score=%d)", login, best_name, best_score)
        else:
            log.debug("No reliable match for %-22s (best=%d)", login, best_score)

    if not updates:
        log.info("No matches >= %d - nothing updated.", MATCH_THRESHOLD)
        return

    # Apply updates
    with db_manager(STG_DB) as conn:
        conn.executemany(
            f"""
            UPDATE {T_USERS_GH}
            SET    GITHUB_DISPLAY_NAME = ?,
                   GITHUB_EMAIL        = COALESCE(GITHUB_EMAIL, ?)
            WHERE  GITHUB_LOGIN        = ?;
            """,
            updates,
        )
        conn.commit()
        log.info("Enriched %d CONSOLIDATED_GH_USERS rows.", len(updates))


if __name__ == "__main__":
    main()
