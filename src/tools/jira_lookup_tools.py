from __future__ import annotations
import duckdb
import logging
from typing import Any, Dict, List
from agno.tools import Toolkit, tool


# configuration
log = logging.getLogger(__name__)
TABLE = "JIRA_USER_PROFILES"


class JiraLookupTools(Toolkit):
    """
    Toolkit exposing two JIRA directory helpers:

    -> search_jira_users_by_email
    -> search_jira_users_by_name

    Return type for both tools: List[Dict[str, Any]] with keys {jira_account_id, jira_display_name, jira_email_address}.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        super().__init__(name="jira_lookup_tools")
        self.conn = conn

    # exact e-mail match
    @tool(
        name="search_jira_users_by_email",
        description=(
            "Look up JIRA profiles whose email exactly matches the given "
            "address (case-insensitive)."
        ),
        cache_results=True,
    )
    def search_jira_users_by_email(self, email: str) -> List[Dict[str, Any]]:
        if not email:
            log.debug("Email lookup called with empty value")
            return []

        rows = self.conn.execute(
            f"""
            SELECT jira_account_id,
                   jira_display_name,
                   jira_email_address
            FROM {TABLE}
            WHERE jira_email_address = ?;
            """,
            (email.lower(),),
        ).fetchall()
        cols = [c[0] for c in self.conn.description]
        return [dict(zip(cols, r)) for r in rows]

    # fuzzy name match
    @tool(
        name="search_jira_users_by_name",
        description=(
            "Return up to 5 JIRA profiles whose display name contains the "
            "given substring (case-insensitive)."
        ),
        cache_results=True,
    )
    def search_jira_users_by_name(
        self, name_query: str, limit: int = 5
    ) -> List[Dict[str, Any]]:
        if not name_query:
            log.debug("Name lookup called with empty value")
            return []

        rows = self.conn.execute(
            f"""
            SELECT jira_account_id,
                   jira_display_name,
                   jira_email_address
            FROM {TABLE}
            WHERE jira_display_name ILIKE ?
            LIMIT ?;
            """,
            (f"%{name_query}%", limit),
        ).fetchall()
        cols = [c[0] for c in self.conn.description]
        return [dict(zip(cols, r)) for r in rows]


# Convenience factory (avoids importing the class at call-site)
def build_jira_lookup_tools(conn: duckdb.DuckDBPyConnection) -> JiraLookupTools:
    """Return a ready-to-use JiraLookupTools instance bound to conn."""
    return JiraLookupTools(conn)
