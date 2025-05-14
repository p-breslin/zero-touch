from typing import Any, List, Dict
from pydantic import BaseModel, Field


class SQLResults(BaseModel):
    results: Dict[str, List[Dict[str, Any]]] = Field(
        ...,
        description="A dictionary where keys are platform names ('github', 'jira') and values are lists of rows. Each row is a dictionary of column_alias:value.",
    )
