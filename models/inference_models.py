from pydantic import BaseModel, Field
from typing import Optional, List, Literal


class IssueKey(BaseModel):
    key: str


class MatchedJiraProfile(BaseModel):
    jira_account_id: str
    jira_display_name: Optional[str] = None
    jira_email_address: Optional[str] = None
    match_type: Literal["email", "name"]
    confidence: float = Field(..., ge=0.0, le=1.0)
    reasoning: Optional[str] = None


class IdentityInference(BaseModel):
    # Input GitHub signals for reference in the output
    signal_fingerprint: str | None = None
    github_user_id: Optional[str] = None
    github_login: Optional[str] = None
    git_name: Optional[str] = None
    git_email: Optional[str] = None
    github_profile_name: Optional[str] = None
    github_profile_email: Optional[str] = None

    # Matches found
    matched_jira_profiles: List[MatchedJiraProfile] = []
    notes: Optional[str] = None  # e.g., "No confident JIRA match found."
