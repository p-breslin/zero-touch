from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class SingleTableResult(BaseModel):
    platform: str = Field(
        ..., description="The platform ('github' or 'jira') these results belong to."
    )
    table_name: str = Field(
        ..., description="The name of the table these results are from."
    )
    rows: List[Dict[str, Any]] = Field(
        ..., description="The list of rows (dictionaries) retrieved for this table."
    )


class SQLResults(BaseModel):
    results: Dict[str, List[Dict[str, Any]]] = Field(
        default_factory=lambda: {"github": [], "jira": []},
        description="A dictionary where keys are platform names ('github', 'jira') and values are lists of ALL rows retrieved for that platform. Each row is a dictionary of column_alias:value.",
    )


class AggregatedData(BaseModel):
    sql_results: SQLResults = Field(
        ...,
        description="The aggregated raw data from all queries, grouped by platform.",
    )
    plan_summary: Optional[str] = Field(
        None,
        description="The original plan summary from the Planner_Agent, carried forward.",
    )
    strategy_notes: Optional[str] = Field(
        None,
        description="The original strategy notes from the Planner_Agent, carried forward.",
    )
