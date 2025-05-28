from __future__ import annotations
import os
import re
import duckdb
import logging
import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
from typing import Optional
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.logging_setup import setup_logging


# Configuration ----------------------------------------------------------------
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

DB_PATH = Path(DATA_DIR, f"{os.getenv('DUCKDB_STAGING_NAME')}.duckdb")
T_JIRA = "JIRA_ACTIVE_USERS"
T_GH = "RESOLVABLE_GITHUB_USERS"
T_TARGET = "RESOLVABLE_USERS"

DDL = f"""
CREATE OR REPLACE TABLE {T_TARGET} (
    JIRA_ID TEXT,
    JIRA_DISPLAY_NAME TEXT,
    JIRA_EMAIL TEXT,
    GITHUB_ID TEXT,
    GITHUB_DISPLAY_NAME TEXT,
    GITHUB_EMAIL TEXT,
    GITHUB_LOGIN TEXT,
    MATCHING_METHOD TEXT,
    MATCH_CONFIDENCE DOUBLE
);
"""


# Cleaning utilities -----------------------------------------------------------
def normalize_email(email: Optional[str]) -> Optional[str]:
    return email.lower().strip() if isinstance(email, str) else None


def tokenize(text: Optional[str]) -> set[str]:
    if not text:
        return set()
    parts = re.split(r"[.\s_\-]+", text.lower())
    return {re.sub(r"[^a-z0-9]", "", p) for p in parts if len(p) > 1}


def clean_for_fuzzy(s: Optional[str]) -> str:
    if not s or not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]", " ", s.lower())).strip()


def jaccard_similarity(set1: set[str], set2: set[str]) -> float:
    return len(set1 & set2) / len(set1 | set2) if set1 and set2 else 0.0


def generate_username_patterns(name: str) -> set[str]:
    if not name:
        return set()
    parts = [re.sub(r"[^a-z]", "", p.lower()) for p in name.split() if p]
    patterns = set()
    if parts:
        patterns.add(parts[0])  # First name
        if len(parts) > 1:
            patterns.add(parts[-1])  # Last name
            patterns.add(parts[0] + parts[-1])
            patterns.add(parts[-1] + parts[0])
            patterns.add(parts[0][0] + parts[-1])
            patterns.add(parts[0] + parts[-1][0])
            patterns.add(parts[0][0] + parts[0])
            if len(parts) > 2:
                patterns.add(parts[0][0] + parts[1])
    return {p for p in patterns if p}


# Core logic -------------------------------------------------------------------
def resolve_users():
    with duckdb.connect(DB_PATH) as conn:
        log.info("Loading source tables...")
        conn.execute(DDL)

        jira = conn.execute(f"""
            SELECT ACCOUNT_ID AS JIRA_ID,
                   DISPLAY_NAME AS JIRA_DISPLAY_NAME,
                   EMAIL AS JIRA_EMAIL
            FROM {T_JIRA}
        """).fetchdf()

        gh = conn.execute(f"SELECT * FROM {T_GH}").fetchdf()

    jira["JIRA_EMAIL_NORM"] = jira["JIRA_EMAIL"].apply(normalize_email)
    gh["GH_EMAIL_NORM"] = gh["GITHUB_EMAIL"].apply(normalize_email)

    matched_jira = set()
    matched_gh = set()
    results = []

    # Tier 1: Exact Email
    for j_idx, j in jira.iterrows():
        if not j["JIRA_EMAIL_NORM"]:
            continue
        matches = gh[
            (gh["GH_EMAIL_NORM"] == j["JIRA_EMAIL_NORM"]) & (~gh.index.isin(matched_gh))
        ]
        if not matches.empty:
            g = matches.iloc[0]
            results.append((*j[:3], *g[:4], "TIER_1_EMAIL", 1.0))
            matched_jira.add(j_idx)
            matched_gh.add(g.name)

    # Tier 2: Token Overlap via Jaccard
    for j_idx, j in jira.iterrows():
        if j_idx in matched_jira:
            continue
        j_tokens = tokenize(j["JIRA_DISPLAY_NAME"])
        for g_idx, g in gh.iterrows():
            if g_idx in matched_gh:
                continue
            g_tokens = tokenize(g["GITHUB_DISPLAY_NAME"]) | tokenize(g["GITHUB_LOGIN"])
            sim = jaccard_similarity(j_tokens, g_tokens)
            if sim >= 0.5:
                results.append((*j[:3], *g[:4], "TIER_2_JACCARD", round(sim, 2)))
                matched_jira.add(j_idx)
                matched_gh.add(g_idx)
                break

    # Tier 2.5: Name-Derived Pattern Match
    for j_idx, j in jira.iterrows():
        if j_idx in matched_jira:
            continue
        patterns = generate_username_patterns(j["JIRA_DISPLAY_NAME"])
        for g_idx, g in gh.iterrows():
            if g_idx in matched_gh:
                continue
            login = (g["GITHUB_LOGIN"] or "").lower()

            for p in patterns:
                if len(p) < 3:
                    continue  # Skip too-short patterns that cause false positives
                if login == p or login.startswith(p) or login.endswith(p):
                    results.append((*j[:3], *g[:4], "TIER_2.5_NAME_PATTERN", 0.75))
                    matched_jira.add(j_idx)
                    matched_gh.add(g_idx)
                    break
            else:
                continue
            break

    # Tier 3: Fuzzy Matching
    for j_idx, j in jira.iterrows():
        if j_idx in matched_jira:
            continue
        j_clean = clean_for_fuzzy(j["JIRA_DISPLAY_NAME"])
        best_score = 0
        best_match = None
        for g_idx, g in gh.iterrows():
            if g_idx in matched_gh:
                continue
            score = max(
                fuzz.token_set_ratio(
                    j_clean, clean_for_fuzzy(g["GITHUB_DISPLAY_NAME"] or "")
                ),
                fuzz.token_set_ratio(j_clean, clean_for_fuzzy(g["GITHUB_LOGIN"] or "")),
            )
            if score > best_score:
                best_score = score
                best_match = (g_idx, g)
        if best_match and best_score >= 85:
            g_idx, g = best_match
            results.append((*j[:3], *g[:4], "TIER_3_FUZZY", round(best_score / 100, 2)))
            matched_jira.add(j_idx)
            matched_gh.add(g_idx)

    # Unmatched Records
    for j_idx, j in jira.iterrows():
        if j_idx not in matched_jira:
            results.append((*j[:3], None, None, None, None, "UNMATCHED_JIRA", None))

    for g_idx, g in gh.iterrows():
        if g_idx not in matched_gh:
            results.append((None, None, None, *g[:4], "UNMATCHED_GITHUB", None))

    # Write to database
    columns = [
        "JIRA_ID",
        "JIRA_DISPLAY_NAME",
        "JIRA_EMAIL",
        "GITHUB_ID",
        "GITHUB_DISPLAY_NAME",
        "GITHUB_EMAIL",
        "GITHUB_LOGIN",
        "MATCHING_METHOD",
        "MATCH_CONFIDENCE",
    ]
    df = pd.DataFrame(results, columns=columns)
    df = df[df["GITHUB_ID"].notnull()]  # Keep only matched GitHub entries

    with duckdb.connect(DB_PATH) as conn:
        conn.execute(f"DELETE FROM {T_TARGET}")
        conn.register("temp_matches", df)
        conn.execute(f"INSERT INTO {T_TARGET} SELECT * FROM temp_matches")
        log.info("Wrote %d matched rows to %s", len(df), T_TARGET)


if __name__ == "__main__":
    resolve_users()
