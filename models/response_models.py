from pydantic import BaseModel
from typing import Any, List, Dict, Optional


class TargetSchema(BaseModel):
    table: str
    columns: List[str]


class PlanDetails(BaseModel):
    tables: List[TargetSchema]
    query_hints: Optional[str] = None


class SQLPlan(BaseModel):
    plan_summary: str  # High-level description of the plan
    strategy_notes: Optional[str] = None
    github: PlanDetails
    jira: PlanDetails


class SQLResults(BaseModel):
    # Key results by platform ({"github": [row1, row2], "jira": [rowA, rowB]})
    results: Dict[str, List[Dict[str, Any]]]


class AccountInfo(BaseModel):
    platform: str  # "github" or "jira"
    login: Optional[str] = None
    email: Optional[str] = None


class Identity(BaseModel):
    primary_identity: str
    all_emails: List[str]
    accounts: List[AccountInfo]
    # Add a confidence score later?


class IdentityList(BaseModel):
    identities: List[Identity]
