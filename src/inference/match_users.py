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
Resolves users between JIRA and GitHub sources using a multi-stage matching approach:

    1. Exact email match (including alias emails).
    2. Substring-based name match:
       - If the “cleaned” JIRA display_name (letters+digits only, lowercase)
         is a prefix of the “cleaned” GitHub display_name (or vice versa),
         accept with high confidence (0.95 if GitHub contains JIRA, 0.85 if JIRA contains GitHub).
    3. Blocked name-based match: For each remaining JIRA/GitHub pair within the same “block” (first three letters of the first token of display_name), compute a combined similarity (max of Jaccard token overlap and fuzzy token_set_ratio). If that >= 0.85, accept.

Each match is tagged with METHOD_1_EMAIL, METHOD_SUBSTRING, or METHOD_NAME_SIM. We then perform a greedy 1:1 selection by descending confidence and a small tier ranking. Final matched pairs are written to MATCHED_USERS; unmatched rows go to UNMATCHED_GITHUB_USERS and UNMATCHED_JIRA_USERS.
"""

# Configuration ---------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")
T_JIRA = "JIRA_ACTIVE_USERS"
T_GH = "GITHUB_ACTIVE_USERS"
T_TARGET = "MATCHED_USERS"
T_UNMATCHED_GH = "UNMATCHED_GITHUB_USERS"
T_UNMATCHED_JIRA = "UNMATCHED_JIRA_USERS"

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


def tokenize(t: Optional[str]) -> set[str]:
    if not t:
        return set()
    parts = re.findall(r"[a-z0-9]+", re.sub(r"[^a-zA-Z0-9]", "", t.lower()))
    camel = re.sub(r"([a-z])([A-Z])", r"\1 \2", t)
    camel_parts = re.split(r"[.\s_\-]+", camel.lower())
    return {re.sub(r"[^a-z0-9]", "", p) for p in parts + camel_parts if len(p) > 1}


def jaccard(a: set[str], b: set[str]) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]", " ", (s or "").lower())).strip()


def make_db_id(jira_id: Optional[str], gh_id: Optional[str]) -> str:
    digest = hashlib.sha1(f"{jira_id}|{gh_id}".encode()).hexdigest()
    return digest[:20]


def block_key(name: Optional[str]) -> str:
    """
    Blocking key: first three letters of the first token (lowercased), or "" if no tokens.
    """
    if not name:
        return ""
    tokens = re.findall(r"[a-zA-Z]+", name.lower())
    if not tokens:
        return ""
    return tokens[0][:3]


def name_similarity(a: Optional[str], b: Optional[str]) -> float:
    """
    Compute max(Jaccard(token sets), fuzzy token_set_ratio on cleaned strings) in [0..1].
    """
    if not a or not b:
        return 0.0
    toks_a = tokenize(a)
    toks_b = tokenize(b)
    sim_jac = jaccard(toks_a, toks_b)
    clean_a = clean_text(a)
    clean_b = clean_text(b)
    sim_fuz = fuzz.token_set_ratio(clean_a, clean_b) / 100.0
    return max(sim_jac, sim_fuz)


def strip_to_alnum(s: Optional[str]) -> str:
    """Strip out all non-alphanumeric characters and lowercase."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


# Core logic -------------------------------------------------------------------
def resolve_users():
    # Step 1: Load raw tables into pandas
    with duckdb.connect(DB_PATH) as cx:
        cx.execute(DDL)

        jira = (
            cx.execute(f"SELECT ID, DISPLAY_NAME, EMAIL FROM {T_JIRA}")
            .fetchdf()
            .rename(
                columns={
                    "ID": "JIRA_ID",
                    "DISPLAY_NAME": "JIRA_DISPLAY_NAME",
                    "EMAIL": "JIRA_EMAIL",
                }
            )
        )

        gh = cx.execute(f"""
            SELECT
                ID,
                DISPLAY_NAME,
                EMAIL,
                LOGIN,
                ALIAS_DISPLAY_NAME,
                ALIAS_EMAIL,
                ALIAS_LOGIN
            FROM {T_GH}
        """).fetchdf()

    # Normalize JIRA emails
    jira["EMAIL_NORM"] = jira["JIRA_EMAIL"].apply(normalize_email)

    # Normalize GitHub primary email + parse alias arrays
    gh["EMAIL_NORM"] = gh["EMAIL"].apply(normalize_email)
    gh["ALIAS_EMAIL"] = gh["ALIAS_EMAIL"].apply(listify)
    gh["ALIAS_DISPLAY_NAME"] = gh["ALIAS_DISPLAY_NAME"].apply(listify)
    gh["ALIAS_LOGIN"] = gh["ALIAS_LOGIN"].apply(listify)

    candidate_matches: list[dict[str, Any]] = []

    # Step 2: Exact email matching (including alias emails)
    gh_email_records: list[dict[str, Any]] = []
    for _, row in gh.iterrows():
        gh_id = row["ID"]
        primary_email = row["EMAIL_NORM"]
        if primary_email:
            gh_email_records.append({"GITHUB_ID": gh_id, "EMAIL_NORM": primary_email})
        for alias in row["ALIAS_EMAIL"]:
            norm_alias = normalize_email(alias)
            if norm_alias:
                gh_email_records.append({"GITHUB_ID": gh_id, "EMAIL_NORM": norm_alias})

    gh_email_df = pd.DataFrame.from_records(gh_email_records)

    exact_merge = pd.merge(
        jira,
        gh_email_df,
        on="EMAIL_NORM",
        how="inner",
        suffixes=("_jira", "_gh"),
    )

    if not exact_merge.empty:
        exact_full = pd.merge(
            exact_merge,
            gh,
            left_on="GITHUB_ID",
            right_on="ID",
            how="left",
            suffixes=("_jira", "_gh_full"),
        )

        for _, row in exact_full.iterrows():
            j_id = row["JIRA_ID"]
            g_id = row["GITHUB_ID"]
            candidate_matches.append(
                {
                    "DB_ID": make_db_id(j_id, g_id),
                    "JIRA_ID": j_id,
                    "GITHUB_ID": g_id,
                    "JIRA_DISPLAY_NAME": row["JIRA_DISPLAY_NAME"],
                    "JIRA_EMAIL": row["JIRA_EMAIL"],
                    "GITHUB_DISPLAY_NAME": row["DISPLAY_NAME"],
                    "GITHUB_EMAIL": row["EMAIL"],
                    "GITHUB_LOGIN": row["LOGIN"],
                    "GITHUB_DISPLAY_NAME_ALIAS": row["ALIAS_DISPLAY_NAME"],
                    "GITHUB_EMAIL_ALIAS": row["ALIAS_EMAIL"],
                    "GITHUB_LOGIN_ALIAS": row["ALIAS_LOGIN"],
                    "MATCHING_METHOD": "METHOD_1_EMAIL",
                    "MATCH_CONFIDENCE": 1.0,
                }
            )

    matched_jira_ids = set(exact_merge["JIRA_ID"]) if not exact_merge.empty else set()
    matched_gh_ids = set(exact_merge["GITHUB_ID"]) if not exact_merge.empty else set()

    # Step 3: Remove exact-matched users
    jira_rem = jira[~jira["JIRA_ID"].isin(matched_jira_ids)].copy()
    gh_rem = gh[~gh["ID"].isin(matched_gh_ids)].copy()

    # Step 4: Compute blocking key (first 3 letters of first token)
    jira_rem["BLOCK"] = jira_rem["JIRA_DISPLAY_NAME"].apply(block_key)
    gh_rem["BLOCK"] = gh_rem["DISPLAY_NAME"].apply(block_key)

    # Step 5: Substring-based + name-based matching within each block
    for block, jira_block in jira_rem.groupby("BLOCK"):
        gh_block = gh_rem[gh_rem["BLOCK"] == block]
        if gh_block.empty:
            continue

        for _, j in jira_block.iterrows():
            j_id = j["JIRA_ID"]
            j_name = j["JIRA_DISPLAY_NAME"]
            j_email = j["JIRA_EMAIL"]
            j_nospace = strip_to_alnum(j_name)

            for _, g in gh_block.iterrows():
                g_id = g["ID"]
                g_display = g["DISPLAY_NAME"] or ""
                g_login = g["LOGIN"] or ""
                g_nospace = strip_to_alnum(g_display)

                # 2a. Substring match on cleaned display_name
                if j_nospace and g_nospace:
                    if g_nospace.startswith(j_nospace):
                        candidate_matches.append(
                            {
                                "DB_ID": make_db_id(j_id, g_id),
                                "JIRA_ID": j_id,
                                "GITHUB_ID": g_id,
                                "JIRA_DISPLAY_NAME": j_name,
                                "JIRA_EMAIL": j_email,
                                "GITHUB_DISPLAY_NAME": g_display,
                                "GITHUB_EMAIL": g["EMAIL"],
                                "GITHUB_LOGIN": g_login,
                                "GITHUB_DISPLAY_NAME_ALIAS": g["ALIAS_DISPLAY_NAME"],
                                "GITHUB_EMAIL_ALIAS": g["ALIAS_EMAIL"],
                                "GITHUB_LOGIN_ALIAS": g["ALIAS_LOGIN"],
                                "MATCHING_METHOD": "METHOD_SUBSTRING",
                                "MATCH_CONFIDENCE": 0.95,
                            }
                        )
                        continue  # skip name-similarity if GitHub contains JIRA

                    elif j_nospace.startswith(g_nospace):
                        candidate_matches.append(
                            {
                                "DB_ID": make_db_id(j_id, g_id),
                                "JIRA_ID": j_id,
                                "GITHUB_ID": g_id,
                                "JIRA_DISPLAY_NAME": j_name,
                                "JIRA_EMAIL": j_email,
                                "GITHUB_DISPLAY_NAME": g_display,
                                "GITHUB_EMAIL": g["EMAIL"],
                                "GITHUB_LOGIN": g_login,
                                "GITHUB_DISPLAY_NAME_ALIAS": g["ALIAS_DISPLAY_NAME"],
                                "GITHUB_EMAIL_ALIAS": g["ALIAS_EMAIL"],
                                "GITHUB_LOGIN_ALIAS": g["ALIAS_LOGIN"],
                                "MATCHING_METHOD": "METHOD_SUBSTRING",
                                "MATCH_CONFIDENCE": 0.85,
                            }
                        )
                        continue  # skip name-similarity if JIRA contains GitHub

                # 2b. Combined Jaccard + fuzzy if no substring hit
                sim_name = name_similarity(j_name, g_display)
                sim_login = name_similarity(j_name, g_login)
                best_sim = max(sim_name, sim_login)

                if best_sim >= 0.85:
                    score = round(best_sim, 2)
                    candidate_matches.append(
                        {
                            "DB_ID": make_db_id(j_id, g_id),
                            "JIRA_ID": j_id,
                            "GITHUB_ID": g_id,
                            "JIRA_DISPLAY_NAME": j_name,
                            "JIRA_EMAIL": j_email,
                            "GITHUB_DISPLAY_NAME": g_display,
                            "GITHUB_EMAIL": g["EMAIL"],
                            "GITHUB_LOGIN": g_login,
                            "GITHUB_DISPLAY_NAME_ALIAS": g["ALIAS_DISPLAY_NAME"],
                            "GITHUB_EMAIL_ALIAS": g["ALIAS_EMAIL"],
                            "GITHUB_LOGIN_ALIAS": g["ALIAS_LOGIN"],
                            "MATCHING_METHOD": "METHOD_NAME_SIM",
                            "MATCH_CONFIDENCE": score,
                        }
                    )

    # Build DataFrame of all candidates
    df_all = pd.DataFrame(candidate_matches)

    # Filter low-confidence (< 0.50) and apply tier ranking
    if not df_all.empty:
        df_all = df_all[df_all["MATCH_CONFIDENCE"] >= 0.50]

        TIER_RANK = {
            "METHOD_1_EMAIL": 1,
            "METHOD_SUBSTRING": 2,
            "METHOD_NAME_SIM": 3,
        }
        df_all["RANK"] = df_all["MATCHING_METHOD"].map(TIER_RANK)

        df_all.sort_values(
            by=["MATCH_CONFIDENCE", "RANK"], ascending=[False, True], inplace=True
        )

        # Greedy 1:1 selection
        df_best_jira = df_all.drop_duplicates(subset=["JIRA_ID"], keep="first")
        used_gh_ids = set(df_best_jira["GITHUB_ID"])
        df_best_gh = df_all[~df_all["GITHUB_ID"].isin(used_gh_ids)].drop_duplicates(
            subset=["GITHUB_ID"], keep="first"
        )

        df_best = pd.concat([df_best_jira, df_best_gh]).drop_duplicates(
            subset=["JIRA_ID", "GITHUB_ID"]
        )
        df_best.sort_values(
            by=["MATCH_CONFIDENCE", "RANK"], ascending=[False, True], inplace=True
        )
    else:
        # No candidates: create empty DataFrame with correct columns
        df_best = pd.DataFrame(
            columns=[
                "DB_ID",
                "JIRA_ID",
                "GITHUB_ID",
                "JIRA_DISPLAY_NAME",
                "JIRA_EMAIL",
                "GITHUB_DISPLAY_NAME",
                "GITHUB_EMAIL",
                "GITHUB_LOGIN",
                "GITHUB_DISPLAY_NAME_ALIAS",
                "GITHUB_EMAIL_ALIAS",
                "GITHUB_LOGIN_ALIAS",
                "MATCHING_METHOD",
                "MATCH_CONFIDENCE",
                "RANK",
            ]
        )

    log.info("Selected %d best matches (greedy 1:1)", len(df_best))

    # Step 6: Insert matched users into DuckDB
    with duckdb.connect(DB_PATH) as cx:
        cx.execute(f"DELETE FROM {T_TARGET}")

        if not df_best.empty:
            to_insert = df_best.drop(columns=["RANK"], errors="ignore")
            cx.register("TMP_RESOLVED", to_insert)
            cx.execute(f"INSERT INTO {T_TARGET} SELECT * FROM TMP_RESOLVED")
            log.info("Wrote %d matched users to %s", len(to_insert), T_TARGET)
        else:
            log.info("No matches to write to %s", T_TARGET)

    # Step 7: Write unmatched tables
    unmatched_gh = gh[~gh["ID"].isin(df_best["GITHUB_ID"])].copy()
    unmatched_jira = jira[~jira["JIRA_ID"].isin(df_best["JIRA_ID"])].copy()

    for col in ("EMAIL_NORM", "BLOCK"):
        if col in unmatched_gh.columns:
            unmatched_gh = unmatched_gh.drop(columns=[col], errors="ignore")
        if col in unmatched_jira.columns:
            unmatched_jira = unmatched_jira.drop(columns=[col], errors="ignore")

    with duckdb.connect(DB_PATH) as cx:
        cx.register("gh_unmatched_tmp", unmatched_gh)
        cx.execute(
            f"""
            CREATE OR REPLACE TABLE {T_UNMATCHED_GH} AS
            SELECT * FROM gh_unmatched_tmp
        """
        )
        log.info(
            "Wrote %d unmatched GitHub users to %s", len(unmatched_gh), T_UNMATCHED_GH
        )

        cx.register("jira_unmatched_tmp", unmatched_jira)
        cx.execute(
            f"""
            CREATE OR REPLACE TABLE {T_UNMATCHED_JIRA} AS
            SELECT * FROM jira_unmatched_tmp
        """
        )
        log.info(
            "Wrote %d unmatched JIRA users to %s", len(unmatched_jira), T_UNMATCHED_JIRA
        )


if __name__ == "__main__":
    resolve_users()
