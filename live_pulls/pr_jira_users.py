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
Matches users from GITHUB_PRS against JIRA_ACTIVE_USERS by comparing USER_LOGIN
to JIRA_DISPLAY_NAME, but first excludes any USER_LOGIN that already appears in
the MATCHED_USERS table's GITHUB_LOGIN column. Uses a two-phase approach:

Phase 1: Substring and token-substring matching across all pairs (no blocking).
    1a. Clean both JIRA_DISPLAY_NAME and USER_LOGIN to alphanumeric lowercase.
    1b. Strip trailing digits from the cleaned USER_LOGIN.
    1c. If the cleaned JIRA name is contained within the cleaned (and digit-stripped) USER_LOGIN, or vice versa, accept with high confidence.
    1d. If any token of the cleaned JIRA name (length >= 4) is a suffix or prefix of the cleaned (digit-stripped) USER_LOGIN, accept with confidence.

Phase 2: Blocked name-based fuzzy matching for any JIRA and PR users not matched in Phase 1.
    - Compute a simple “block key” = first 3 letters of the cleaned name/login.
    - Within each block, compute combined similarity = max(Jaccard(token overlap), fuzzy token_set_ratio) between JIRA_DISPLAY_NAME and USER_LOGIN. If >= TH_FUZZY, accept.

Each match is tagged with the matching method. Then perform a greedy 1:1 selection by descending confidence and tier ranking.
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
TH_SUBSTRING_HIGH = 0.95
TH_SUBSTRING_LOW = 0.90
TH_FUZZY = 0.85

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
    parts = re.findall(r"[a-z0-9]+", re.sub(r"[^a-zA-Z0-9]", "", text.lower()))
    camel = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    camel_parts = re.split(r"[.\s_\-]+", camel.lower())
    return {re.sub(r"[^a-z0-9]", "", p) for p in parts + camel_parts if len(p) > 1}


def jaccard(a: set[str], b: set[str]) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]", " ", (s or "").lower())).strip()


def name_similarity(a: Optional[str], b: Optional[str]) -> float:
    """
    Compute max(Jaccard(token sets), fuzzy token_set_ratio of cleaned strings) in [0..1].
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


def strip_trailing_digits(s: str) -> str:
    """Remove any trailing digits from the end of the string."""
    return re.sub(r"\d+$", "", s)


def block_key(name: Optional[str]) -> str:
    """
    Blocking key: first three letters of the first token (lowercased), or "" if no tokens.
    """
    if not name:
        return ""
    tokens = re.findall(r"[a-zA-Z]+", name.lower())
    return tokens[0][:3] if tokens else ""


# Core logic -------------------------------------------------------------------
def resolve_pr_users():
    # Step 1: Load JIRA_ACTIVE_USERS, GITHUB_PRS, + existing matched GH logins
    with duckdb.connect(DB_PATH) as cx:
        cx.execute(DDL)

        jira = cx.execute(
            f"SELECT ID AS JIRA_ID, DISPLAY_NAME AS JIRA_DISPLAY_NAME FROM {T_JIRA}"
        ).fetchdf()

        prs = cx.execute(
            f"SELECT USER_ID AS GITHUB_ID, USER_LOGIN AS GITHUB_LOGIN FROM {T_PR}"
        ).fetchdf()

        existing_matched = cx.execute(
            f"SELECT LOWER(GITHUB_LOGIN) AS LOGIN_LOWER FROM {T_MATCHED}"
        ).fetchdf()
        # Keep only non-empty, non-null logins
        matched_logins = (
            existing_matched["LOGIN_LOWER"].dropna().astype(str).str.strip()
        )
        matched_logins_set = set(x for x in matched_logins if x)

    # Drop any nulls
    prs = prs[prs["GITHUB_LOGIN"].notnull() & prs["GITHUB_ID"].notnull()].copy()
    jira = jira[jira["JIRA_DISPLAY_NAME"].notnull() & jira["JIRA_ID"].notnull()].copy()

    # Exclude PR users whose login appears (case-insensitive) in MATCHED_USERS
    prs["GITHUB_LOGIN_LOWER"] = prs["GITHUB_LOGIN"].str.lower().str.strip()
    prs = prs[~prs["GITHUB_LOGIN_LOWER"].isin(matched_logins_set)].copy()

    # Prepare cleaned columns (no blocking for Phase 1)
    jira["CLEAN_NAME"] = jira["JIRA_DISPLAY_NAME"].apply(strip_to_alnum)
    prs["CLEAN_LOGIN"] = prs["GITHUB_LOGIN"].apply(strip_to_alnum)
    prs["CLEAN_LOGIN_BASE"] = prs["CLEAN_LOGIN"].apply(strip_trailing_digits)

    # Phase 1: Substring + token-substring on all pairs (no blocking)
    candidate_phase1: list[dict[str, Any]] = []
    matched_jira_ids: set[str] = set()
    matched_pr_ids: set[str] = set()

    columns_phase1 = [
        "JIRA_DISPLAY_NAME",
        "JIRA_ID",
        "GITHUB_LOGIN",
        "GITHUB_ID",
        "MATCH_METHOD",
        "MATCH_CONFIDENCE",
        "RANK",
    ]

    for _, j in jira.iterrows():
        j_id = j["JIRA_ID"]
        j_name = j["JIRA_DISPLAY_NAME"]
        j_clean = j["CLEAN_NAME"]
        j_tokens = list(tokenize(j_name))

        for _, p in prs.iterrows():
            p_id = p["GITHUB_ID"]
            p_login = p["GITHUB_LOGIN"]
            p_clean = p["CLEAN_LOGIN"]
            p_base = p["CLEAN_LOGIN_BASE"]

            # Skip if already matched in Phase 1
            if j_id in matched_jira_ids or p_id in matched_pr_ids:
                continue

            # 1a. Substring match on cleaned or digit-stripped base
            if j_clean and p_base:
                if p_base.startswith(j_clean) or j_clean.startswith(p_base):
                    candidate_phase1.append(
                        {
                            "JIRA_DISPLAY_NAME": j_name,
                            "JIRA_ID": j_id,
                            "GITHUB_LOGIN": p_login,
                            "GITHUB_ID": p_id,
                            "MATCH_METHOD": "METHOD_SUBSTRING",
                            "MATCH_CONFIDENCE": TH_SUBSTRING_HIGH,
                            "RANK": 1,
                        }
                    )
                    matched_jira_ids.add(j_id)
                    matched_pr_ids.add(p_id)
                    continue

            # 1b. Token-substring match: any JIRA token (length >= 4) is suffix/prefix of p_base
            if p_base and j_tokens:
                found_token_match = False
                for tok in j_tokens:
                    if len(tok) >= 4 and (
                        p_base.endswith(tok) or p_base.startswith(tok)
                    ):
                        candidate_phase1.append(
                            {
                                "JIRA_DISPLAY_NAME": j_name,
                                "JIRA_ID": j_id,
                                "GITHUB_LOGIN": p_login,
                                "GITHUB_ID": p_id,
                                "MATCH_METHOD": "METHOD_TOKEN_SUBSTRING",
                                "MATCH_CONFIDENCE": TH_SUBSTRING_LOW,
                                "RANK": 2,
                            }
                        )
                        matched_jira_ids.add(j_id)
                        matched_pr_ids.add(p_id)
                        found_token_match = True
                        break
                if found_token_match:
                    continue

            # 1c. Cleaned login vs cleaned name (alternate substring)
            if j_clean and p_clean:
                if p_clean.startswith(j_clean) or j_clean.startswith(p_clean):
                    candidate_phase1.append(
                        {
                            "JIRA_DISPLAY_NAME": j_name,
                            "JIRA_ID": j_id,
                            "GITHUB_LOGIN": p_login,
                            "GITHUB_ID": p_id,
                            "MATCH_METHOD": "METHOD_SUBSTRING",
                            "MATCH_CONFIDENCE": TH_SUBSTRING_LOW,
                            "RANK": 1,
                        }
                    )
                    matched_jira_ids.add(j_id)
                    matched_pr_ids.add(p_id)
                    continue

    df_phase1 = (
        pd.DataFrame(candidate_phase1, columns=columns_phase1)
        if candidate_phase1
        else pd.DataFrame(columns=columns_phase1)
    )

    # Phase 2: Blocked fuzzy matching (only against what remains unmatched)
    jira_rem = jira[~jira["JIRA_ID"].isin(matched_jira_ids)].copy()
    prs_rem = prs[~prs["GITHUB_ID"].isin(matched_pr_ids)].copy()

    # Build block keys now
    jira_rem["BLOCK"] = jira_rem["CLEAN_NAME"].apply(block_key)
    prs_rem["BLOCK"] = prs_rem["CLEAN_LOGIN_BASE"].apply(block_key)

    candidate_phase2: list[dict[str, Any]] = []
    columns_phase2 = [
        "JIRA_DISPLAY_NAME",
        "JIRA_ID",
        "GITHUB_LOGIN",
        "GITHUB_ID",
        "MATCH_METHOD",
        "MATCH_CONFIDENCE",
        "RANK",
    ]

    for block, jira_block in jira_rem.groupby("BLOCK"):
        prs_block = prs_rem[prs_rem["BLOCK"] == block]
        if prs_block.empty:
            continue

        for _, j in jira_block.iterrows():
            j_id = j["JIRA_ID"]
            j_name = j["JIRA_DISPLAY_NAME"]

            for _, p in prs_block.iterrows():
                p_id = p["GITHUB_ID"]
                p_login = p["GITHUB_LOGIN"]

                sim = name_similarity(j_name, p_login)
                if sim >= TH_FUZZY:
                    candidate_phase2.append(
                        {
                            "JIRA_DISPLAY_NAME": j_name,
                            "JIRA_ID": j_id,
                            "GITHUB_LOGIN": p_login,
                            "GITHUB_ID": p_id,
                            "MATCH_METHOD": "METHOD_NAME_SIM",
                            "MATCH_CONFIDENCE": round(sim, 2),
                            "RANK": 3,
                        }
                    )

    df_phase2 = (
        pd.DataFrame(candidate_phase2, columns=columns_phase2)
        if candidate_phase2
        else pd.DataFrame(columns=columns_phase2)
    )

    # Combine Phase1 + Phase2 candidates
    if not df_phase1.empty and not df_phase2.empty:
        df_all = pd.concat([df_phase1, df_phase2], ignore_index=True)
    elif not df_phase1.empty:
        df_all = df_phase1.copy()
    else:
        df_all = df_phase2.copy()

    # Filter < 0.50 and sort
    if not df_all.empty:
        df_all = df_all[df_all["MATCH_CONFIDENCE"] >= 0.50]
        df_all.sort_values(
            by=["MATCH_CONFIDENCE", "RANK"], ascending=[False, True], inplace=True
        )

        # Greedy 1:1 selection
        df_best_jira = df_all.drop_duplicates(subset=["JIRA_ID"], keep="first")
        used_pr_ids = set(df_best_jira["GITHUB_ID"])
        df_best_pr = df_all[~df_all["GITHUB_ID"].isin(used_pr_ids)].drop_duplicates(
            subset=["GITHUB_ID"], keep="first"
        )

        df_best = pd.concat([df_best_jira, df_best_pr]).drop_duplicates(
            subset=["JIRA_ID", "GITHUB_ID"]
        )
        df_best.sort_values(
            by=["MATCH_CONFIDENCE", "RANK"], ascending=[False, True], inplace=True
        )
    else:
        df_best = pd.DataFrame(
            columns=[
                "JIRA_DISPLAY_NAME",
                "JIRA_ID",
                "GITHUB_LOGIN",
                "GITHUB_ID",
                "MATCH_METHOD",
                "MATCH_CONFIDENCE",
                "RANK",
            ]
        )

    log.info("Selected %d best PR->JIRA matches", len(df_best))

    # Step 4: Insert into PR_USERS_JIRA
    with duckdb.connect(DB_PATH) as cx:
        cx.execute(f"DELETE FROM {T_TARGET}")

        if not df_best.empty:
            to_insert = df_best[
                ["JIRA_DISPLAY_NAME", "JIRA_ID", "GITHUB_LOGIN", "GITHUB_ID"]
            ]
            cx.register("TMP_PR_JIRA", to_insert)
            cx.execute(f"INSERT INTO {T_TARGET} SELECT * FROM TMP_PR_JIRA")
            log.info("Wrote %d rows to %s", len(to_insert), T_TARGET)
        else:
            log.info("No PR->JIRA matches to write to %s", T_TARGET)


if __name__ == "__main__":
    resolve_pr_users()
