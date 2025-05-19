from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class AccountInfo(BaseModel):
    platform: str = Field(
        ..., description="The source platform, e.g., 'github' or 'jira'."
    )
    platform_specific_id: Optional[str] = Field(
        None,
        description="The user's unique ID on that specific platform (e.g., GitHub numeric ID, JIRA AccountID).",
    )
    login: Optional[str] = Field(
        default=None, description="User login/username on the platform."
    )
    email: Optional[str] = Field(
        default=None,
        description="User email associated with this account on the platform.",
    )


class Identity(BaseModel):
    canonical_identifier_value: str = Field(
        ...,
        description="A chosen primary/canonical identifier for this unique person.",
    )
    canonical_identifier_type: Literal[
        "email",
        "github_login",
        "github_id",
        "jira_accountid",
        "jira_key",
        "resolved_name",
        "other_id",
    ] = Field(..., description="The type of the canonical_identifier_value.")
    display_name: Optional[str] = Field(
        None,
        description="A primary human-readable display name chosen for this resolved person.",
    )
    all_emails: List[str] = Field(
        default_factory=list,
        description="A list of all unique, normalized email addresses associated with this resolved person across all linked accounts.",
    )
    accounts: List[AccountInfo] = Field(
        ...,
        description="A list of source accounts (GitHub, JIRA) that have been resolved to this single person.",
    )
    confidence_score: Optional[float] = Field(
        None,
        description="A score (e.g., 0.0-1.0) indicating the agent's confidence in this identity resolution bundle.",
    )
    resolution_method_notes: Optional[str] = Field(
        None,
        description="Brief notes on how this identity was primarily resolved or any ambiguities.",
    )


class IdentityList(BaseModel):
    identities: List[Identity] = Field(
        ...,
        description="A list of unique Identity objects, each representing a resolved person.",
    )
    unresolved_github_records: Optional[List[str]] = Field(
        default_factory=list,
        description="Raw GitHub records (JSON encoded strings) that could not be confidently matched to an Identity.",
    )
    unresolved_jira_records: Optional[List[str]] = Field(
        default_factory=list,
        description="Raw JIRA records (JSON encoded strings) that could not be confidently matched to an Identity.",
    )
    resolution_summary: Optional[str] = Field(
        None,
        description="The Identity_Agent's overall summary of the resolution process, findings, and any challenges.",
    )
