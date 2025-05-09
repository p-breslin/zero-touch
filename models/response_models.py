from pydantic import BaseModel
from typing import Optional, List


class AccountInfo(BaseModel):
    platform: str  # "github" or "jira"
    login: Optional[str] = None
    email: Optional[str] = None


class Person(BaseModel):
    id: str  # Could be a UUID generated on creation
    primary_name: str
    all_emails: List[str]
    accounts: List[AccountInfo]
    # Add a confidence score later?


class PersonList(BaseModel):
    persons: List[Person]
