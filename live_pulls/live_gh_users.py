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

"""
Populates GITHUB_ACTIVE_USERS with all distinct author and committer identities from GITHUB_COMMITS. Includes alias information.

Steps
-----
1. Extracts author/committer IDs and identity fields from GITHUB_COMMITS.
2. Collapses by ID, picking a canonical DISPLAY_NAME, EMAIL, LOGIN and gathering any differing values into ALIASES.
3. Computes ALIAS_ID for each ID (any other GitHub IDs that share LOGIN/EMAIL).
"""


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")
T_COMMITS = "GITHUB_COMMITS"
T_USERS = "GITHUB_ACTIVE_USERS"


# DDL --------------------------------------------------------------------------
DDL_USERS = f"""
CREATE TABLE IF NOT EXISTS {T_USERS} (
    ID TEXT,
    DISPLAY_NAME TEXT,
    EMAIL TEXT,
    LOGIN TEXT,
    ALIAS_ID TEXT,
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
) -> List[Tuple[str, str, str, str, str, str, str, str]]:
    """
    Collapse raw (ID, DISPLAY_NAME, EMAIL, LOGIN) tuples by ID:
      - Pick canonical values by sorting each set and taking the first one.
      - Build alias lists for DISPLAY_NAME, EMAIL, LOGIN.
      - Compute ALIAS_ID: any other IDs sharing the same LOGIN or EMAIL.
    Returns a list of tuples.
    """
    result = []

    # Step A: group by ID -> collect all names/emails/logins per ID
    grouped: Dict[str, Dict[str, set]] = defaultdict(
        lambda: {"DISPLAY_NAME": set(), "EMAIL": set(), "LOGIN": set()}
    )
    # Also collect mapping from login->IDs and email->IDs
    login_to_ids: Dict[str, set] = defaultdict(set)
    email_to_ids: Dict[str, set] = defaultdict(set)

    for id_, name, email, login in identities:
        if id_ is not None:
            # Normalize None fields to empty string to keep grouping consistent
            nm = name or ""
            em = email or ""
            ln = login or ""
            grouped[id_]["DISPLAY_NAME"].add(nm)
            grouped[id_]["EMAIL"].add(em)
            grouped[id_]["LOGIN"].add(ln)
            # track login->IDs, email->IDs for alias-ID logic
            login_to_ids[ln].add(id_)
            email_to_ids[em].add(id_)
        else:
            # ID is null: treat each unique (name,email,login) as its own record with no alias-ID
            result.append(
                (
                    None,
                    name or "",
                    email or "",
                    login or "",
                    json.dumps([]),  # no alias IDs
                    json.dumps([]),
                    json.dumps([]),
                    json.dumps([]),
                )
            )

    # Step B: for each non-null ID, pick canonical values and compute aliases
    for id_, fields in grouped.items():
        name_set = fields["DISPLAY_NAME"]
        email_set = fields["EMAIL"]
        login_set = fields["LOGIN"]

        canonical_name = sorted(name_set)[0]
        canonical_email = sorted(email_set)[0]
        canonical_login = sorted(login_set)[0]

        # Build alias lists (excluding canonical)
        alias_names = sorted(name_set - {canonical_name})
        alias_emails = sorted(email_set - {canonical_email})
        alias_logins = sorted(login_set - {canonical_login})

        # Compute alias IDs: IDs that share this canonical_login/canonical_email
        related_ids = set()
        related_ids.update(login_to_ids.get(canonical_login, set()))
        related_ids.update(email_to_ids.get(canonical_email, set()))
        # Exclude the current ID itself
        alias_ids = sorted(related_ids - {id_})

        result.append(
            (
                id_,
                canonical_name,
                canonical_email,
                canonical_login,
                json.dumps(alias_ids),
                json.dumps(alias_names),
                json.dumps(alias_emails),
                json.dumps(alias_logins),
            )
        )

    return result


# Insert -----------------------------------------------------------------------
def _insert(records: List[Tuple[str, str, str, str, str, str, str, str]]):
    if not records:
        log.info("No user records to insert.")
        return

    with db_manager(DB) as conn:
        conn.execute(DDL_USERS)
        conn.executemany(
            f"""
            INSERT INTO {T_USERS} (
                ID, DISPLAY_NAME, EMAIL, LOGIN,
                ALIAS_ID, ALIAS_DISPLAY_NAME, ALIAS_EMAIL, ALIAS_LOGIN
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            records,
        )
        conn.commit()
        log.info("Inserted %d GitHub user records into %s.", len(records), T_USERS)


# Entry point ------------------------------------------------------------------
def main():
    log.info("Staging distinct GitHub user identities from %s.", T_COMMITS)

    with db_manager(DB, read_only=True) as conn:
        raw_identities = _distinct_identities(conn)
        collapsed = _collapse_by_id(raw_identities)

    _insert(collapsed)
    log.info("Done staging GitHub users.")


if __name__ == "__main__":
    main()
