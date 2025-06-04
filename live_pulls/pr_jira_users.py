from __future__ import annotations
import os
import re
import duckdb
import logging
import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
from dotenv import load_dotenv
from typing import Optional, Any
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging

"""
Matches only the PR users that are not already in MATCHED_USERS against JIRA_ACTIVE_USERS. For each unmatched PR login, compare to every JIRA display name using:
    1. Exact-cleaned substring (confidence 0.95)
    2. Token-based substring (token length >= 4) (confidence 0.90)
    3. Fuzzy name similarity (Jaccard or token_set_ratio) if >= 0.85

Pick the single best JIRA match per PR (by descending confidence, then method priority). Write results to PR_USERS_JIRA with columns: JIRA_DISPLAY_NAME, JIRA_ID, GITHUB_LOGIN, GITHUB_ID.
"""

# Configuration ---------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.getenv('LIVE_DB_NAME')}.duckdb")
T_JIRA = "JIRA_ACTIVE_USERS"
T_PR = "GITHUB_PRS"
T_TARGET = "PR_USERS_JIRA"
T_MATCHED = "MATCHED_USERS"

# Threshold constants
TH_SUBSTRING = 0.95
TH_TOKEN = 0.90
TH_FUZZY = 0.85

# Priorities for tie‐breaking (lower = higher priority)
METHOD_PRIORITY = {
    "METHOD_SUBSTRING": 1,
    "METHOD_TOKEN_SUBSTRING": 2,
    "METHOD_NAME_SIM": 3,
}

DDL = f"""
CREATE OR REPLACE TABLE {T_TARGET} (
    JIRA_DISPLAY_NAME TEXT,
    JIRA_ID TEXT,
    GITHUB_LOGIN TEXT,
    GITHUB_ID TEXT
);
"""


# Helpers ---------------------------------------------------------------------
def tokenize(text: Optional[str]) -> set[str]:
    if not text:
        return set()
    alnum = re.sub(r"[^a-zA-Z0-9]", "", text.lower())
    parts = re.findall(r"[a-z0-9]+", alnum)
    camel = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    camel_parts = re.split(r"[.\s_\-]+", camel.lower())
    return {re.sub(r"[^a-z0-9]", "", p) for p in parts + camel_parts if len(p) > 1}


def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]", " ", (s or "").lower())).strip()


def strip_to_alnum(s: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def strip_trailing_digits(s: str) -> str:
    return re.sub(r"\d+$", "", s)


def name_similarity(a: Optional[str], b: Optional[str]) -> float:
    if not a or not b:
        return 0.0
    toks_a = tokenize(a)
    toks_b = tokenize(b)
    sim_jac = len(toks_a & toks_b) / len(toks_a | toks_b) if toks_a and toks_b else 0.0
    clean_a = clean_text(a)
    clean_b = clean_text(b)
    sim_fuz = fuzz.token_set_ratio(clean_a, clean_b) / 100.0
    return max(sim_jac, sim_fuz)


# Core logic -------------------------------------------------------------------
def resolve_pr_users():
    with duckdb.connect(DB_PATH) as cx:
        cx.execute(DDL)

        # 1) Fetch PR users not in MATCHED_USERS (including login aliases)
        unmatched_prs = cx.execute(
            f"""
            SELECT USER_ID AS GITHUB_ID, USER_LOGIN AS GITHUB_LOGIN
            FROM (
              SELECT
                USER_LOGIN,
                USER_ID,
                ROW_NUMBER() OVER (PARTITION BY USER_LOGIN ORDER BY UPDATED_AT DESC) AS rn
              FROM {T_PR}
            ) sub
            WHERE rn = 1
              AND USER_LOGIN NOT IN (
                SELECT GITHUB_LOGIN FROM {T_MATCHED}
                UNION
                SELECT UNNEST(GITHUB_LOGIN_ALIAS) FROM {T_MATCHED}
              )
            """
        ).fetchdf()

        log.info(f"PR users not in MATCHED_USERS table:\n{unmatched_prs}\n")

        # 2) Fetch all JIRA active users
        jira = cx.execute(
            f"SELECT ID AS JIRA_ID, DISPLAY_NAME AS JIRA_DISPLAY_NAME FROM {T_JIRA}"
        ).fetchdf()

    # Drop any nulls
    unmatched_prs = unmatched_prs[
        unmatched_prs["GITHUB_LOGIN"].notnull() & unmatched_prs["GITHUB_ID"].notnull()
    ].copy()
    jira = jira[jira["JIRA_DISPLAY_NAME"].notnull() & jira["JIRA_ID"].notnull()].copy()

    # Prepare cleaned fields
    unmatched_prs["CLEAN_LOGIN"] = unmatched_prs["GITHUB_LOGIN"].apply(strip_to_alnum)
    unmatched_prs["BASE_LOGIN"] = unmatched_prs["CLEAN_LOGIN"].apply(
        strip_trailing_digits
    )

    jira["CLEAN_NAME"] = jira["JIRA_DISPLAY_NAME"].apply(strip_to_alnum)
    jira["TOKENS"] = jira["JIRA_DISPLAY_NAME"].apply(lambda x: list(tokenize(x)))

    log.debug(f"Cleaned PR info:\n{unmatched_prs}\n")
    log.debug(f"Cleaned JIRA info:\n{jira}\n")

    # Collect candidate matches
    candidates: list[dict[str, Any]] = []

    for _, pr in unmatched_prs.iterrows():
        pr_id = pr["GITHUB_ID"]
        pr_login = pr["GITHUB_LOGIN"]
        pr_clean = pr["CLEAN_LOGIN"]
        pr_base = pr["BASE_LOGIN"]

        best_match: dict[str, Any] = {}
        best_score = 0.0
        best_priority = 999

        for _, j in jira.iterrows():
            jira_id = j["JIRA_ID"]
            jira_name = j["JIRA_DISPLAY_NAME"]
            j_clean = j["CLEAN_NAME"]
            j_tokens = j["TOKENS"]

            # 1) Exact‐cleaned substring
            if j_clean and pr_clean:
                if pr_clean.startswith(j_clean) or j_clean.startswith(pr_clean):
                    score = TH_SUBSTRING
                    method = "METHOD_SUBSTRING"
                    priority = METHOD_PRIORITY[method]
                    if (score > best_score) or (
                        score == best_score and priority < best_priority
                    ):
                        best_score = score
                        best_priority = priority
                        best_match = {
                            "JIRA_DISPLAY_NAME": jira_name,
                            "JIRA_ID": jira_id,
                            "GITHUB_LOGIN": pr_login,
                            "GITHUB_ID": pr_id,
                            "MATCH_METHOD": method,
                            "MATCH_CONFIDENCE": score,
                            "PR_KEY": pr_login,
                        }
                    continue

            # 2) Token‐substring (require token length >= 4)
            if pr_base and j_tokens:
                for tok in j_tokens:
                    if len(tok) >= 4 and (
                        pr_base.startswith(tok) or pr_base.endswith(tok)
                    ):
                        score = TH_TOKEN
                        method = "METHOD_TOKEN_SUBSTRING"
                        priority = METHOD_PRIORITY[method]
                        if (score > best_score) or (
                            score == best_score and priority < best_priority
                        ):
                            best_score = score
                            best_priority = priority
                            best_match = {
                                "JIRA_DISPLAY_NAME": jira_name,
                                "JIRA_ID": jira_id,
                                "GITHUB_LOGIN": pr_login,
                                "GITHUB_ID": pr_id,
                                "MATCH_METHOD": method,
                                "MATCH_CONFIDENCE": score,
                                "PR_KEY": pr_login,
                            }
                        break  # no need to check other tokens

            # 3) Fuzzy name similarity
            sim1 = name_similarity(jira_name, pr_login)
            sim2 = name_similarity(jira_name, pr_clean)
            sim = max(sim1, sim2)
            if sim >= TH_FUZZY:
                score = round(sim, 2)
                method = "METHOD_NAME_SIM"
                priority = METHOD_PRIORITY[method]
                if (score > best_score) or (
                    score == best_score and priority < best_priority
                ):
                    best_score = score
                    best_priority = priority
                    best_match = {
                        "JIRA_DISPLAY_NAME": jira_name,
                        "JIRA_ID": jira_id,
                        "GITHUB_LOGIN": pr_login,
                        "GITHUB_ID": pr_id,
                        "MATCH_METHOD": method,
                        "MATCH_CONFIDENCE": score,
                        "PR_KEY": pr_login,
                    }

        # If we found any match with confidence >= 0.50, record it
        if best_match and best_score >= 0.50:
            candidates.append(best_match)

    # Build DataFrame of candidates
    df_candidates = pd.DataFrame(candidates)

    if not df_candidates.empty:
        # Greedy 1:1 by PR_KEY (each PR gets only its top match already chosen)
        # Just drop duplicates on PR_KEY (keep first)
        df_candidates.sort_values(
            by=["MATCH_CONFIDENCE", "MATCH_METHOD"],
            ascending=[False, True],
            inplace=True,
        )
        df_best = df_candidates.drop_duplicates(subset=["PR_KEY"], keep="first")

        # Finally, drop helper columns
        df_to_insert = df_best[
            ["JIRA_DISPLAY_NAME", "JIRA_ID", "GITHUB_LOGIN", "GITHUB_ID"]
        ].copy()
    else:
        df_to_insert = pd.DataFrame(
            columns=["JIRA_DISPLAY_NAME", "JIRA_ID", "GITHUB_LOGIN", "GITHUB_ID"]
        )

    log.info("Selected %d PR -> JIRA matches", len(df_to_insert))
    log.debug(f"Results:\n{df_to_insert}\n")

    # Step 3: Insert into PR_USERS_JIRA
    with duckdb.connect(DB_PATH) as cx:
        cx.execute(f"DELETE FROM {T_TARGET}")
        if not df_to_insert.empty:
            cx.register("TMP_PR_JIRA", df_to_insert)
            cx.execute(f"INSERT INTO {T_TARGET} SELECT * FROM TMP_PR_JIRA")
            log.info("Wrote %d rows to %s", len(df_to_insert), T_TARGET)
        else:
            log.info("No PR -> JIRA matches to write to %s", T_TARGET)


if __name__ == "__main__":
    resolve_pr_users()
