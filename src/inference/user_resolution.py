from __future__ import annotations
import os
import re
import json
import duckdb
import hashlib
import logging
import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
from dotenv import load_dotenv
from typing import Optional, Any
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging

"""
Resolves users between JIRA and GitHub sources using multi-pass matching.

Description
-----------
Links user identities between JIRA and GitHub datasets by comparing primary fields and known aliases, then writes resolved matches to a single output table. Applies increasingly flexible matching strategies to maximize match confidence:

    1. Email match: matches users based on exact normalized email addresses.
    2. Name token overlap: uses Jaccard similarity on tokenized display names and logins.
    3. Pattern match: checks login strings against generated name-based patterns.
    4. Fuzzy string match: applies token set ratio between cleaned display names and logins.

Each match is scored and tagged with the chosen method and confidence. Users are matched one-to-one based on the best available match and inserted into the MATCHED_USERS table. Unmatched users from each system are saved to separate tables: UNMATCHED_GITHUB_USERS and UNMATCHED_JIRA_USERS.

Steps
-----
1. Normalizes emails, cleans names, and expands alias fields.
2. Evaluates matches across all four tiers of logic.
3. Deduplicates and selects highest-confidence matches.
4. Writes matched results to MATCHED_USERS.
5. Writes unmatched JIRA and GitHub users to respective staging tables.
"""

# Configuration ---------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
T_JIRA = "JIRA_ACTIVE_USERS"
T_GH = "GITHUB_ACTIVE_USERS"
T_TARGET = "MATCHED_USERS"

DDL = f"""
CREATE OR REPLACE TABLE {T_TARGET} (
    DB_ID TEXT PRIMARY KEY,
    JIRA_ID TEXT,
    GITHUB_ID TEXT,
    JIRA_DISPLAY_NAME TEXT,
    JIRA_EMAIL TEXT,
    GITHUB_DISPLAY_NAME TEXT,
    GITHUB_EMAIL TEXT,
    GITHUB_LOGIN TEXT,
    GITHUB_DISPLAY_NAME_ALIAS TEXT[],
    GITHUB_EMAIL_ALIAS TEXT[],
    GITHUB_LOGIN_ALIAS TEXT[],
    MATCHING_METHOD TEXT,
    MATCH_CONFIDENCE DOUBLE
);
"""


# Helpers ---------------------------------------------------------------------
def normalize_email(e: Optional[str]) -> Optional[str]:
    return e.lower().strip() if isinstance(e, str) else None


def tokenize(t: Optional[str]) -> set[str]:
    if not t:
        return set()
    parts = re.split(r"[.\\s_\\-]+", t.lower())
    return {re.sub(r"[^a-z0-9]", "", p) for p in parts if len(p) > 1}


def jaccard(a: set[str], b: set[str]) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def patterns(name: str) -> set[str]:
    if not name:
        return set()
    parts = [re.sub(r"[^a-z]", "", p.lower()) for p in name.split()]
    out: set[str] = set()
    if parts:
        out.add(parts[0])
        if len(parts) > 1:
            out |= {
                parts[-1],
                parts[0] + parts[-1],
                parts[-1] + parts[0],
                parts[0][0] + parts[-1],
                parts[0] + parts[-1][0],
                parts[0][0] + parts[0],
            }
            if len(parts) > 2:
                out.add(parts[0][0] + parts[1])
    return {p for p in out if len(p) > 2}


def clean(s: Optional[str]) -> str:
    return re.sub(r"\\s+", " ", re.sub(r"[^a-z0-9]", " ", (s or "").lower())).strip()


def make_db_id(jira_id: Optional[str], gh_id: Optional[str]) -> str:
    digest = hashlib.sha1(f"{jira_id}|{gh_id}".encode()).hexdigest()
    return digest[:20]


def listify(x: Any) -> list[str]:
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            parsed = json.loads(x)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


# Core logic -------------------------------------------------------------------
def resolve_users():
    with duckdb.connect(DB_PATH) as cx:
        log.info("Loading users…")
        cx.execute(DDL)

        jira = (
            cx.execute("SELECT ID, DISPLAY_NAME, EMAIL FROM JIRA_ACTIVE_USERS")
            .fetchdf()
            .rename(
                columns={
                    "ID": "JIRA_ID",
                    "DISPLAY_NAME": "JIRA_DISPLAY_NAME",
                    "EMAIL": "JIRA_EMAIL",
                }
            )
        )

        gh = cx.execute("""
            SELECT
                ID,
                DISPLAY_NAME,
                EMAIL,
                LOGIN,
                ALIAS_DISPLAY_NAME,
                ALIAS_EMAIL,
                ALIAS_LOGIN
            FROM GITHUB_ACTIVE_USERS
        """).fetchdf()

    jira["EMAIL_NORM"] = jira["JIRA_EMAIL"].apply(normalize_email)
    gh["EMAIL_NORM"] = gh["EMAIL"].apply(normalize_email)
    gh["ALIAS_EMAIL"] = gh["ALIAS_EMAIL"].apply(listify)
    gh["ALIAS_DISPLAY_NAME"] = gh["ALIAS_DISPLAY_NAME"].apply(listify)
    gh["ALIAS_LOGIN"] = gh["ALIAS_LOGIN"].apply(listify)

    candidate_matches = []

    for _, j in jira.iterrows():
        j_tokens = tokenize(j["JIRA_DISPLAY_NAME"])
        j_email = j["EMAIL_NORM"]
        j_clean = clean(j["JIRA_DISPLAY_NAME"])
        j_id = j["JIRA_ID"]

        for _, g in gh.iterrows():
            gh_id = g["ID"]

            alias_email = listify(g["ALIAS_EMAIL"])
            alias_login = listify(g["ALIAS_LOGIN"])
            alias_display = listify(g["ALIAS_DISPLAY_NAME"])

            email_pool = [normalize_email(g["EMAIL"])] + [
                normalize_email(e) for e in alias_email
            ]
            login_pool = [g["LOGIN"]] + alias_login
            name_pool = [g["DISPLAY_NAME"]] + alias_display

            method, score = None, 0.0

            if j_email and j_email in email_pool:
                method = "TIER_1_EMAIL"
                score = 1.0
            elif any(
                jaccard(j_tokens, tokenize(n)) >= 0.5 for n in name_pool + login_pool
            ):
                method = "TIER_2_JACCARD"
                score = 0.5
            elif any(
                all(((lp or "").lower().startswith(p), len(p) > 2))
                or all(((lp or "").lower().endswith(p), len(p) > 2))
                for lp in login_pool
                for p in patterns(j["JIRA_DISPLAY_NAME"])
            ):
                method = "TIER_2.5_PATTERN"
                score = 0.75
            else:
                best = max(
                    fuzz.token_set_ratio(j_clean, clean(n))
                    for n in name_pool + login_pool
                )
                if best >= 90:
                    method = "TIER_3_FUZZY"
                    score = round(best / 100, 2)

            if method:
                candidate_matches.append(
                    dict(
                        DB_ID=make_db_id(j_id, gh_id),
                        JIRA_ID=j_id,
                        GITHUB_ID=gh_id,
                        JIRA_DISPLAY_NAME=j["JIRA_DISPLAY_NAME"],
                        JIRA_EMAIL=j["JIRA_EMAIL"],
                        GITHUB_DISPLAY_NAME=g["DISPLAY_NAME"],
                        GITHUB_EMAIL=g["EMAIL"],
                        GITHUB_LOGIN=g["LOGIN"],
                        GITHUB_DISPLAY_NAME_ALIAS=alias_display,
                        GITHUB_EMAIL_ALIAS=alias_email,
                        GITHUB_LOGIN_ALIAS=alias_login,
                        MATCHING_METHOD=method,
                        MATCH_CONFIDENCE=score,
                    )
                )

    df_all = pd.DataFrame(candidate_matches)
    df_all.sort_values(by="MATCH_CONFIDENCE", ascending=False, inplace=True)
    df_best = df_all.drop_duplicates(subset=["JIRA_ID"], keep="first").drop_duplicates(
        subset=["GITHUB_ID"], keep="first"
    )

    with duckdb.connect(DB_PATH) as cx:
        cx.execute(f"DELETE FROM {T_TARGET}")
        cx.register("TMP_RESOLVED", df_best)
        cx.execute(f"INSERT INTO {T_TARGET} SELECT * FROM TMP_RESOLVED")
        log.info("Wrote %d matched users to %s", len(df_best), T_TARGET)

    # Unmatched tables ---------------------------------------------------------
    T_UNMATCHED_GH = "UNMATCHED_GITHUB_USERS"
    T_UNMATCHED_JIRA = "UNMATCHED_JIRA_USERS"

    gh_unmatched = gh[~gh["ID"].isin(df_best["GITHUB_ID"])].copy()
    jira_unmatched = jira[~jira["JIRA_ID"].isin(df_best["JIRA_ID"])].copy()

    with duckdb.connect(DB_PATH) as cx:
        # Unmatched GitHub users
        cx.execute(f"""
            CREATE OR REPLACE TABLE {T_UNMATCHED_GH} AS
            SELECT * FROM (SELECT * FROM gh_unmatched)
        """)

        # Unmatched JIRA users
        cx.execute(f"""
            CREATE OR REPLACE TABLE {T_UNMATCHED_JIRA} AS
            SELECT * FROM (SELECT * FROM jira_unmatched)
        """)

        log.info(
            "Wrote %d unmatched GitHub users to %s", len(gh_unmatched), T_UNMATCHED_GH
        )
        log.info(
            "Wrote %d unmatched JIRA users to %s", len(jira_unmatched), T_UNMATCHED_JIRA
        )


if __name__ == "__main__":
    resolve_users()
