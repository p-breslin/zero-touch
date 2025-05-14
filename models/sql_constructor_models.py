from typing import List, Optional
from pydantic import BaseModel, Field


class SQLQuery(BaseModel):
    platform: str = Field(
        ..., description="The platform ('github' or 'jira') this query targets."
    )
    table_name: str = Field(
        ...,
        description="Original fully qualified table name from the plan, for context and logging.",
    )
    sql_string: str = Field(
        ...,
        description="The exact, ready-to-execute SQL SELECT query string for this table.",
    )


class SQLQueries(BaseModel):
    plan_summary: Optional[str] = Field(
        None,
        description="The plan_summary from the input SQLPlan, carried forward for context.",
    )
    strategy_notes: Optional[str] = Field(
        None,
        description="The strategy_notes from the input SQLPlan, carried forward for context.",
    )
    queries: List[SQLQuery] = Field(
        ...,
        description="A list of all SQL query strings to be executed, one for each table in the original SQLPlan.",
    )
