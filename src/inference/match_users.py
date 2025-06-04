from __future__ import annotations
import os
import re
import json
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
Resolves users between JIRA and GitHub sources using multi-stage matching:

    1. Exact email match (including alias emails).
    2. Substring-based name match:
       - If the “cleaned” JIRA display_name (letters+digits only, lowercase)
         is a prefix of the “cleaned” GitHub display_name (or vice versa),
         accept with high confidence (0.95 if GitHub contains JIRA, 0.85 if JIRA contains GitHub).
    3. Blocked name-based match: For each remaining JIRA/GitHub pair that share
       either the same first-name block or the same last-name block, compute a combined similarity (max of Jaccard token overlap and fuzzy token_set_ratio). If >= 0.85, accept.

Each match is tagged with the matching method. We then perform a greedy 1:1 selection by descending confidence and a small tier ranking. Any single JIRA_ID that ends up matching multiple GITHUB_IDs will have the “extra” GITHUB_IDs stored in GITHUB_ID_ALIAS. Additionally, if a matched GitHub row itself has ALIAS_IDs (from GITHUB_ACTIVE_USERS), those will be included in GITHUB_ID_ALIAS, and any secondary GitHub IDs' display_name/email/login are folded into GITHUB_DISPLAY_NAME_ALIAS, GITHUB_EMAIL_ALIAS, and GITHUB_LOGIN_ALIAS.

Final matched pairs are written to MATCHED_USERS; unmatched rows go to
UNMATCHED_GITHUB_USERS and UNMATCHED_JIRA_USERS.
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
    JIRA_ID TEXT,
    GITHUB_ID TEXT,
    GITHUB_ID_ALIAS TEXT[],
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


def block_key_first(name: Optional[str]) -> str:
    """
    Blocking key #1: first three letters of the first token (lowercased),
    or "" if no tokens.
    """
    if not name:
        return ""
    tokens = re.findall(r"[a-zA-Z]+", name.lower())
    return tokens[0][:3] if tokens else ""


def block_key_last(name: Optional[str]) -> str:
    """
    Blocking key #2: first three letters of the last token (lowercased),
    or "" if no tokens.
    """
    if not name:
        return ""
    tokens = re.findall(r"[a-zA-Z]+", name.lower())
    return tokens[-1][:3] if tokens else ""


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

        gh = cx.execute(
            f"""
            SELECT
                ID,
                DISPLAY_NAME,
                EMAIL,
                LOGIN,
                ALIAS_DISPLAY_NAME,
                ALIAS_EMAIL,
                ALIAS_LOGIN,
                ALIAS_ID
            FROM {T_GH}
        """
        ).fetchdf()

    # Parse alias arrays. If ALIAS_ID was NULL/NaN, force [].
    gh["ALIAS_ID"] = gh["ALIAS_ID"].apply(listify)
    gh["EMAIL_NORM"] = gh["EMAIL"].apply(normalize_email)
    gh["ALIAS_EMAIL"] = gh["ALIAS_EMAIL"].apply(listify)
    gh["ALIAS_DISPLAY_NAME"] = gh["ALIAS_DISPLAY_NAME"].apply(listify)
    gh["ALIAS_LOGIN"] = gh["ALIAS_LOGIN"].apply(listify)

    # Normalize JIRA emails
    jira["EMAIL_NORM"] = jira["JIRA_EMAIL"].apply(normalize_email)

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

            # Start GITHUB_ID_ALIAS with any ALIAS_IDs in GITHUB_ACTIVE_USERS
            alias_ids = row["ALIAS_ID"] or []
            candidate_matches.append(
                {
                    "JIRA_ID": j_id,
                    "GITHUB_ID": g_id,
                    "GITHUB_ID_ALIAS": alias_ids.copy(),
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

    # Step 4: Compute both first-name and last-name blocks
    jira_rem["BLOCK_FIRST"] = jira_rem["JIRA_DISPLAY_NAME"].apply(block_key_first)
    jira_rem["BLOCK_LAST"] = jira_rem["JIRA_DISPLAY_NAME"].apply(block_key_last)

    gh_rem["BLOCK_FIRST"] = gh_rem["DISPLAY_NAME"].apply(block_key_first)
    gh_rem["BLOCK_LAST"] = gh_rem["DISPLAY_NAME"].apply(block_key_last)

    # Step 5: Substring-based + name-based matching within each block
    for _, j in jira_rem.iterrows():
        j_id = j["JIRA_ID"]
        j_name = j["JIRA_DISPLAY_NAME"]
        j_email = j["JIRA_EMAIL"]
        j_nospace = strip_to_alnum(j_name)

        block1 = j["BLOCK_FIRST"]
        block2 = j["BLOCK_LAST"]
        gh_candidates = gh_rem[
            (gh_rem["BLOCK_FIRST"] == block1)
            | (gh_rem["BLOCK_FIRST"] == block2)
            | (gh_rem["BLOCK_LAST"] == block1)
            | (gh_rem["BLOCK_LAST"] == block2)
        ]

        if gh_candidates.empty:
            continue

        for _, g in gh_candidates.iterrows():
            g_id = g["ID"]
            g_display = g["DISPLAY_NAME"] or ""
            g_login = g["LOGIN"] or ""
            g_nospace = strip_to_alnum(g_display)

            # Include the pre-existing ALIAS_IDs from GitHub ACTIVE USERS
            base_alias_ids: list[str] = g["ALIAS_ID"].copy() if g["ALIAS_ID"] else []

            # 2a. Substring match on cleaned display_name
            if j_nospace and g_nospace:
                if g_nospace.startswith(j_nospace):
                    candidate_matches.append(
                        {
                            "JIRA_ID": j_id,
                            "GITHUB_ID": g_id,
                            "GITHUB_ID_ALIAS": base_alias_ids.copy(),
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
                    continue  # skip fuzzy if GitHub contains JIRA

                elif j_nospace.startswith(g_nospace):
                    candidate_matches.append(
                        {
                            "JIRA_ID": j_id,
                            "GITHUB_ID": g_id,
                            "GITHUB_ID_ALIAS": base_alias_ids.copy(),
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
                    continue  # skip fuzzy if JIRA contains GitHub

            # 2b. Combined Jaccard + fuzzy if no substring hit
            sim_name = name_similarity(j_name, g_display)
            sim_login = name_similarity(j_name, g_login)
            best_sim = max(sim_name, sim_login)

            if best_sim >= 0.85:
                score = round(best_sim, 2)
                candidate_matches.append(
                    {
                        "JIRA_ID": j_id,
                        "GITHUB_ID": g_id,
                        "GITHUB_ID_ALIAS": base_alias_ids.copy(),
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

    # Filter out very weak matches
    df_all = df_all[df_all["MATCH_CONFIDENCE"] >= 0.50] if not df_all.empty else df_all

    # Attach tier priority rank
    TIER_RANK = {
        "METHOD_1_EMAIL": 1,
        "METHOD_SUBSTRING": 2,
        "METHOD_NAME_SIM": 3,
    }
    if not df_all.empty:
        df_all["RANK"] = df_all["MATCHING_METHOD"].map(TIER_RANK)

        # Sort by descending confidence, then by tier rank
        df_all.sort_values(
            by=["MATCH_CONFIDENCE", "RANK"], ascending=[False, True], inplace=True
        )

        # Greedy 1:1 selection for primary pairs
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
        df_best = pd.DataFrame(
            columns=[
                "JIRA_ID",
                "GITHUB_ID",
                "GITHUB_ID_ALIAS",
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

    # Now collapse multiple-GitHub-ID rows for same JIRA_ID into a single row:
    #   - Pick the “primary” GitHub_ID (highest confidence, lowest rank).
    #   - Put other GitHub_IDs (+ associated ALIAS_IDs) into GITHUB_ID_ALIAS.
    #   - Merge secondary GH rows’ DISPLAY_NAME, EMAIL, LOGIN (and aliases)

    if not df_best.empty:
        # Build a quick lookup of GitHub rows by ID
        gh_index = gh.set_index("ID", drop=False)

        # Build alias_map: JIRA_ID -> all candidate GitHub_IDs in df_all
        alias_map: dict[str, list[str]] = {
            jira_id: group["GITHUB_ID"].tolist()
            for jira_id, group in df_all.groupby("JIRA_ID")
        }

        collapsed_rows: list[dict[str, Any]] = []
        for jira_id, group in df_best.groupby("JIRA_ID"):
            # Sort by descending confidence, then ascending rank
            group_sorted = group.sort_values(
                by=["MATCH_CONFIDENCE", "RANK"], ascending=[False, True]
            ).reset_index(drop=True)

            primary_row = group_sorted.loc[0].copy()

            # Gather all other GitHub IDs in df_all for this JIRA_ID
            all_ids = alias_map.get(jira_id, [])
            other_ids = [gid for gid in all_ids if gid != primary_row["GITHUB_ID"]]

            # Base alias IDs from the primary
            base_alias_ids = primary_row.get("GITHUB_ID_ALIAS") or []
            merged_alias_ids = sorted(set(base_alias_ids + other_ids))

            # Base alias display_name/email/login from the primary
            base_alias_display_names = (
                primary_row.get("GITHUB_DISPLAY_NAME_ALIAS") or []
            )
            base_alias_emails = primary_row.get("GITHUB_EMAIL_ALIAS") or []
            base_alias_logins = primary_row.get("GITHUB_LOGIN_ALIAS") or []

            # Collect extra alias info from each other_id
            extra_display_names: list[str] = []
            extra_emails: list[str] = []
            extra_logins: list[str] = []

            for oid in other_ids:
                # Force this into a DataFrame to handle duplicates
                try:
                    subdf = gh_index.loc[[oid]]  # always returns a DataFrame
                except KeyError:
                    # If that ID is somehow not in gh_index, skip
                    continue

                for _, subrow in subdf.iterrows():
                    # Append the canonical values from that GitHub row
                    if subrow["DISPLAY_NAME"]:
                        extra_display_names.append(subrow["DISPLAY_NAME"])
                    if subrow["EMAIL"]:
                        extra_emails.append(subrow["EMAIL"])
                    if subrow["LOGIN"]:
                        extra_logins.append(subrow["LOGIN"])

                    # Also append that row’s own alias arrays
                    for dn in subrow.get("ALIAS_DISPLAY_NAME") or []:
                        extra_display_names.append(dn)
                    for em in subrow.get("ALIAS_EMAIL") or []:
                        extra_emails.append(em)
                    for lg in subrow.get("ALIAS_LOGIN") or []:
                        extra_logins.append(lg)

                    # And include that row’s ALIAS_ID as well
                    for aid in subrow.get("ALIAS_ID") or []:
                        if aid not in merged_alias_ids:
                            merged_alias_ids.append(aid)

            merged_alias_display_names = sorted(
                set(base_alias_display_names + extra_display_names)
            )
            merged_alias_emails = sorted(set(base_alias_emails + extra_emails))
            merged_alias_logins = sorted(set(base_alias_logins + extra_logins))

            # Overwrite fields in the primary row before appending
            primary_row["GITHUB_ID_ALIAS"] = merged_alias_ids
            primary_row["GITHUB_DISPLAY_NAME_ALIAS"] = merged_alias_display_names
            primary_row["GITHUB_EMAIL_ALIAS"] = merged_alias_emails
            primary_row["GITHUB_LOGIN_ALIAS"] = merged_alias_logins

            collapsed_rows.append(primary_row)

        df_collapsed = pd.DataFrame(collapsed_rows)
        df_to_insert = df_collapsed.drop(columns=["RANK"], errors="ignore")
    else:
        df_to_insert = df_best

    log.info("Selected %d best matches (after collapsing)", len(df_to_insert))

    # Step 6: Insert matched users (with alias columns)
    with duckdb.connect(DB_PATH) as cx:
        cx.execute(f"DELETE FROM {T_TARGET}")

        if not df_to_insert.empty:
            cx.register("TMP_RESOLVED", df_to_insert)
            cx.execute(f"INSERT INTO {T_TARGET} SELECT * FROM TMP_RESOLVED")
            log.info("Wrote %d matched users to %s", len(df_to_insert), T_TARGET)
        else:
            log.info("No matches to write to %s", T_TARGET)

    # Step 7: Write unmatched tables - build and remove a set of all GitHub IDs that have been used either as primary matches or aliases

    # 1) Collect all primary GH IDs:
    primary_gh_ids = set(df_to_insert["GITHUB_ID"].tolist())

    # 2) Collect all alias GH IDs (flatten every row’s GITHUB_ID_ALIAS list):
    alias_gh_ids: set[str] = set()
    if not df_to_insert.empty:
        for alias_list in df_to_insert["GITHUB_ID_ALIAS"].tolist():
            alias_gh_ids.update(alias_list)

    all_matched_gh_ids = primary_gh_ids.union(alias_gh_ids)

    # Exclude every matched ID (primary/alias) from the “unmatched” GitHub set
    unmatched_gh = gh[~gh["ID"].isin(all_matched_gh_ids)].copy()

    # For JIRA, filter out any JIRA_ID that appears in df_to_insert
    unmatched_jira = jira[~jira["JIRA_ID"].isin(df_to_insert["JIRA_ID"])].copy()

    # Drop helper columns before writing
    for col in ("EMAIL_NORM", "BLOCK_FIRST", "BLOCK_LAST"):
        if col in unmatched_gh.columns:
            unmatched_gh.drop(columns=[col], inplace=True)
        if col in unmatched_jira.columns:
            unmatched_jira.drop(columns=[col], inplace=True)

    with duckdb.connect(DB_PATH) as cx:
        # Unmatched GitHub users
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

        # Unmatched JIRA users
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
