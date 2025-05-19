from typing import List, Optional
from pydantic import BaseModel, Field


class ColumnSelection(BaseModel):
    source_field: str = Field(
        ...,
        description="The original column name from the source table, OR a dot-separated path for JSON fields (e.g., 'AUTHOR.email' for a JSON column named 'AUTHOR' with a key 'email').",
    )
    alias: str = Field(
        ...,
        description="The desired alias for this field in the query result (e.g., 'author_email'). This helps standardize field names for downstream processing.",
    )
    is_json_path: bool = Field(
        ...,
        description="MUST be true if 'source_field' represents a dot-separated path into a JSON column; otherwise, false.",
    )


class TargetSchema(BaseModel):
    table: str = Field(
        ...,
        description="Fully qualified name of the database table, including its schema prefix (e.g., 'XFLOW_DEV_GITHUB_.USERS', 'XFLOW_DEV_JIRA_.ISSUES').",
    )
    columns: List[ColumnSelection] = Field(
        ...,
        description="A list of ColumnSelection objects detailing which fields to retrieve from this table and how.",
    )
    table_query_hints: Optional[str] = Field(
        default=None,
        description="Optional SQL hints specific to querying this table, e.g., 'LIMIT 100', 'WHERE created_at > \\'2023-01-01\\'', or notes for SELECT DISTINCT strategies. These are applied directly to this table's query.",
    )


class PlanDetails(BaseModel):
    tables: List[TargetSchema] = Field(
        ...,
        description="A list of TargetSchema objects, each defining data retrieval for a specific table from this platform (GitHub or JIRA).",
    )
    platform_query_hints: Optional[str] = Field(
        default=None,
        description="Optional general SQL construction hints applicable across multiple tables for this platform, e.g., common JOIN conditions.",
    )


class SQLPlan(BaseModel):
    plan_summary: str = Field(
        ...,
        description="A concise, human-readable summary of the overall data retrieval approach and the purpose of this plan.",
    )
    strategy_notes: Optional[str] = Field(
        default=None,
        description="High-level notes or strategy suggestions for downstream agents on how to process or interpret the data that will be fetched according to this plan. Can also include notes on matching strategies between GitHub and JIRA data.",
    )
    github: PlanDetails = Field(
        ..., description="The detailed plan for retrieving data from GitHub tables."
    )
    jira: PlanDetails = Field(
        ..., description="The detailed plan for retrieving data from JIRA tables."
    )
