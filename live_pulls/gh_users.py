"""
Populates GITHUB_ACTIVE_USERS (staging DB) with all distinct author and committer identities from GITHUB_COMMITS. This prepares a unique reference table of GitHub identities for downstream mapping and enrichment.

Steps
-----
1. Read all rows from GITHUB_COMMITS.
2. Extract distinct author and committer IDs and identity fields.
3. Insert into GITHUB_ACTIVE_USERS.

Note: Some IDs are associated with differing Names and Emails. This will be accounted for in Alias columns.
"""

from __future__ import annotations
import os
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
from typing import List, Tuple, Dict

from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# STG_DB = Path(DATA_DIR, f"{os.environ['DUCKDB_STAGING_NAME']}.duckdb")
STG_DB = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")

T_COMMITS = "GITHUB_COMMITS"
T_USERS = "GITHUB_ACTIVE_USERS"


# DDL --------------------------------------------------------------------------
DDL_USERS = f"""
CREATE TABLE IF NOT EXISTS {T_USERS} (
    ID TEXT,
    DISPLAY_NAME TEXT,
    EMAIL TEXT,
    LOGIN TEXT,
    ALIAS_DISPLAY_NAME TEXT,
    ALIAS_EMAIL TEXT,
    ALIAS_LOGIN TEXT
);
"""


# Helpers ----------------------------------------------------------------------
def _distinct_identities(conn) -> List[Tuple[str, str, str, str]]:
    """Return all identities from author and committer columns."""
    q = f"""
        SELECT DISTINCT
            AUTHOR_ID     AS ID,
            AUTHOR_NAME   AS DISPLAY_NAME,
            AUTHOR_EMAIL  AS EMAIL,
            AUTHOR_LOGIN  AS LOGIN
        FROM {T_COMMITS}

        UNION

        SELECT DISTINCT
            COMMITTER_ID     AS ID,
            COMMITTER_NAME   AS DISPLAY_NAME,
            COMMITTER_EMAIL  AS EMAIL,
            COMMITTER_LOGIN  AS LOGIN
        FROM {T_COMMITS};
    """
    return conn.execute(q).fetchall()


def _collapse_by_id(
    identities: List[Tuple[str, str, str, str]],
) -> List[Tuple[str, str, str, str, str, str, str]]:
    result = []

    grouped: Dict[str, Dict[str, set]] = defaultdict(
        lambda: {"DISPLAY_NAME": set(), "EMAIL": set(), "LOGIN": set()}
    )

    for id_, name, email, login in identities:
        if id_ is not None:
            grouped[id_]["DISPLAY_NAME"].add(name)
            grouped[id_]["EMAIL"].add(email)
            grouped[id_]["LOGIN"].add(login)
        else:
            # No ID: treat each unique (name, email, login) as its own group
            result.append(
                (
                    None,
                    name,
                    email,
                    login,
                    json.dumps([]),
                    json.dumps([]),
                    json.dumps([]),
                )
            )

    # Process ID-based groups (non-null IDs)
    for id_, fields in grouped.items():
        name_set = fields["DISPLAY_NAME"]
        email_set = fields["EMAIL"]
        login_set = fields["LOGIN"]

        canonical_name = sorted(name_set)[0]
        canonical_email = sorted(email_set)[0]
        canonical_login = sorted(login_set)[0]

        result.append(
            (
                id_,
                canonical_name,
                canonical_email,
                canonical_login,
                json.dumps(sorted(name_set - {canonical_name})),
                json.dumps(sorted(email_set - {canonical_email})),
                json.dumps(sorted(login_set - {canonical_login})),
            )
        )

    return result


# Insert -----------------------------------------------------------------------
def _insert(records: List[Tuple[str, str, str, str, str, str, str]]):
    if not records:
        log.info("No user records to insert.")
        return

    with db_manager(STG_DB) as conn:
        conn.execute(DDL_USERS)
        conn.executemany(
            f"""
            INSERT INTO {T_USERS} (
                ID, DISPLAY_NAME, EMAIL, LOGIN,
                ALIAS_DISPLAY_NAME, ALIAS_EMAIL, ALIAS_LOGIN
            )
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            records,
        )
        conn.commit()
        log.info("Inserted %d GitHub user records into %s.", len(records), T_USERS)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging distinct GitHub user identities from %s.", T_COMMITS)

    with db_manager(STG_DB, read_only=True) as conn:
        raw_identities = _distinct_identities(conn)
        collapsed = _collapse_by_id(raw_identities)

    _insert(collapsed)
    log.info("Done staging GitHub users.")


if __name__ == "__main__":
    main()
