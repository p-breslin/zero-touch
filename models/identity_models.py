from typing import List, Optional
from pydantic import BaseModel, Field
# import uuid # If you decide to add a unique_person_id


class AccountInfo(BaseModel):
    platform: str = Field(
        ..., description="The source platform, e.g., 'github' or 'jira'."
    )
    login: Optional[str] = Field(
        default=None, description="User login/username on the platform."
    )
    email: Optional[str] = Field(
        default=None,
        description="User email associated with this account on the platform.",
    )
    # account_id: Optional[str] = Field(default=None, description="Platform-specific account identifier.")


class Identity(BaseModel):
    primary_identity: str = Field(
        ...,
        description="A chosen primary identifier for this unique person (e.g., a resolved primary email or a canonical name).",
    )
    all_emails: List[str] = Field(
        default_factory=list,
        description="A list of all unique email addresses associated with this resolved person across all linked accounts.",
    )
    accounts: List[AccountInfo] = Field(
        ...,
        description="A list of source accounts (GitHub, JIRA) that have been resolved to this single person.",
    )
    # unique_person_id: str = Field(default_factory=lambda: f"person_{uuid.uuid4().hex[:8]}")
    # confidence_score: Optional[float] = Field(default=None, description="A score indicating the confidence in this identity resolution.")


class IdentityList(BaseModel):
    identities: List[Identity] = Field(
        ...,
        description="A list of unique Identity objects, each representing a resolved person.",
    )
