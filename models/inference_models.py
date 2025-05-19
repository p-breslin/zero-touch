from pydantic import BaseModel


class IssueKeys(BaseModel):
    key: str
