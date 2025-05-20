from __future__ import annotations
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from scripts.paths import DATA_DIR
from utils.helpers import db_manager
from typing import Any, Dict, List, Tuple
from utils.logging_setup import setup_logging

"""
This script gathers and consolidates various GitHub user identity signals. Its purpose is to create a comprehensive, deduplicated collection of all observed GitHub identity touchpoints for later resolution against JIRA profiles.

    1. Extracts raw signals (GitHub user ID, login, Git name, Git email) from the GITHUB_COMMITS and GITHUB_PRS tables in the staging DuckDB. 
    2. Enriches these signals using profile information from the USERS_SUMMARY table in the main DB. 
    3. Creates unique "fingerprints" for each combination of signals and upserts these consolidated identity signals, along with their sources, into a GITHUB_IDENTITY_SIGNALS table in the staging DB. 
"""

# configuration
load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

COMPANY = os.environ["COMPANY_NAME"]
TABLE_SIGNALS = "GITHUB_IDENTITY_SIGNALS"
TABLE_COMMITS = "GITHUB_COMMITS"
TABLE_PRS = "GITHUB_PRS"
TABLE_USERS_SUM = "USERS_SUMMARY"
SCHEMA_GH_MAIN = f"{COMPANY}_GITHUB_"

DB_MAIN = Path(DATA_DIR, f"{os.environ['DUCKDB_NAME']}.duckdb")
DB_SUBSET = Path(DATA_DIR, f"{os.environ['DUCKDB_SUBSET_NAME']}.duckdb")


# helpers
def _t(v: Any) -> str | None:  # text-coerce
    return None if v is None else str(v)


def _e(v: Any) -> str | None:  # email-coerce
    return None if v is None else str(v).lower().strip() or None


# DuckDB management
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_SIGNALS} (
    signal_fingerprint   TEXT PRIMARY KEY,
    github_user_id       TEXT,
    github_login         TEXT,
    git_name             TEXT,
    git_email            TEXT,
    github_profile_name  TEXT,
    github_profile_email TEXT,
    sources              VARCHAR[]
);
"""

# data pulls
SQL_USERS = f"""
SELECT "ID", "LOGIN", "NAME", "EMAIL"
FROM "{SCHEMA_GH_MAIN}"."{TABLE_USERS_SUM}"
WHERE "ID" IS NOT NULL OR "LOGIN" IS NOT NULL;
"""

SQL_COMMITS = f"""
SELECT
    commit_sha,
    author_id,     author_login,     author_name,     author_email,
    committer_id,  committer_login,  committer_name,  committer_email
FROM "{TABLE_COMMITS}";
"""

SQL_PRS = f"""
SELECT pr_internal_id, github_user_id, github_user_login, role_in_pr
FROM "{TABLE_PRS}";
"""


# build lookup maps
def _user_maps(
    conn_main,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    rows = conn_main.execute(SQL_USERS).fetchall()
    by_id, by_login = {}, {}
    for uid, login, name, email in rows:
        entry = {
            "github_user_id": _t(uid),
            "github_login": _t(login),
            "github_profile_name": _t(name),
            "github_profile_email": _e(email),
        }
        if entry["github_user_id"]:
            by_id[entry["github_user_id"]] = entry
        if entry["github_login"]:
            by_login[entry["github_login"]] = entry
    log.info("Loaded %d user summaries", len(by_id) or len(by_login))
    return by_id, by_login


# raw-signal extraction
def _commit_signals(conn_sub) -> List[Dict[str, Any]]:
    rows = conn_sub.execute(SQL_COMMITS).fetchall()
    out: List[Dict[str, Any]] = []
    for (
        sha,
        aid,
        alog,
        aname,
        aemail,
        cid,
        clog,
        cname,
        cemail,
    ) in rows:
        # author
        if any((aid, alog, aname, aemail)):
            out.append(
                dict(
                    github_user_id=_t(aid),
                    github_login=_t(alog),
                    git_name=_t(aname),
                    git_email=_e(aemail),
                    source=f"COMMITS_AUTHOR:{sha}",
                )
            )
        # committer (add even if == author?  cheap to dedupe later)
        if any((cid, clog, cname, cemail)):
            out.append(
                dict(
                    github_user_id=_t(cid),
                    github_login=_t(clog),
                    git_name=_t(cname),
                    git_email=_e(cemail),
                    source=f"COMMITS_COMMITTER:{sha}",
                )
            )
    log.info("Commit signals: %d", len(out))
    return out


def _pr_signals(conn_sub) -> List[Dict[str, Any]]:
    rows = conn_sub.execute(SQL_PRS).fetchall()
    out = [
        dict(
            github_user_id=_t(uid),
            github_login=_t(login),
            git_name=None,
            git_email=None,
            source=f"PRS_{role}:{prid}",
        )
        for prid, uid, login, role in rows
        if uid or login
    ]
    log.info("PR signals: %d", len(out))
    return out


# consolidation
def _consolidate(
    signals: List[Dict[str, Any]],
    by_id: Dict[str, Dict[str, Any]],
    by_login: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    bucket: Dict[str, Dict[str, Any]] = {}

    for sig in signals:
        uid, login = sig["github_user_id"], sig["github_login"]
        git_name, git_email = sig["git_name"], sig["git_email"]

        # pull enrichments
        uinfo = by_id.get(uid) or by_login.get(login) or {}
        profile_name = uinfo.get("github_profile_name")
        profile_email = uinfo.get("github_profile_email")
        uid = uid or uinfo.get("github_user_id")
        login = login or uinfo.get("github_login")

        # fingerprint = everything we know, pipe-joined (unique key)
        fp = "|".join(
            _t(x) or "none"
            for x in (uid, login, git_email, git_name, profile_email, profile_name)
        )

        rec = bucket.setdefault(
            fp,
            dict(
                signal_fingerprint=fp,
                github_user_id=uid,
                github_login=login,
                git_name=git_name,
                git_email=git_email,
                github_profile_name=profile_name,
                github_profile_email=profile_email,
                sources=[],
            ),
        )
        rec["sources"].append(sig["source"])

        # fill blanks if later signal has more data
        rec["github_user_id"] = uid or rec["github_user_id"]
        rec["github_login"] = login or rec["github_login"]
        rec["git_name"] = git_name or rec["git_name"]
        rec["git_email"] = git_email or rec["git_email"]
        rec["github_profile_name"] = profile_name or rec["github_profile_name"]
        rec["github_profile_email"] = profile_email or rec["github_profile_email"]

    log.info("Consolidated -> %d unique fingerprints", len(bucket))
    return list(bucket.values())


# upsert
UPSERT = f"""
INSERT INTO "{TABLE_SIGNALS}" (
    signal_fingerprint,
    github_user_id,
    github_login,
    git_name,
    git_email,
    github_profile_name,
    github_profile_email,
    sources
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(signal_fingerprint) DO UPDATE SET
    github_user_id        = excluded.github_user_id,
    github_login          = excluded.github_login,
    git_name              = excluded.git_name,
    git_email             = excluded.git_email,
    github_profile_name   = excluded.github_profile_name,
    github_profile_email  = excluded.github_profile_email,
    sources               = list_distinct(list_concat({TABLE_SIGNALS}.sources, excluded.sources));
"""


def _upsert(conn_sub, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        log.info("No new identity signals.")
        return

    conn_sub.execute(DDL)

    data = [
        (
            r["signal_fingerprint"],
            r["github_user_id"],
            r["github_login"],
            r["git_name"],
            r["git_email"],
            r["github_profile_name"],
            r["github_profile_email"],
            r["sources"],
        )
        for r in rows
    ]
    conn_sub.executemany(UPSERT, data)
    conn_sub.commit()
    log.info("Upserted %d rows -> %s", len(rows), TABLE_SIGNALS)


# entry point
def main() -> None:
    with (
        db_manager(DB_MAIN, read_only=True) as main_conn,
        db_manager(DB_SUBSET) as sub_conn,
    ):
        by_id, by_login = _user_maps(main_conn)
        signals = _commit_signals(sub_conn) + _pr_signals(sub_conn)
        consolidated = _consolidate(signals, by_id, by_login)
        _upsert(sub_conn, consolidated)


if __name__ == "__main__":
    log.info("Staging GitHub identity signals -> %s", TABLE_SIGNALS)
    main()
    log.info("Done.")
