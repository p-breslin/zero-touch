from pydantic import BaseModel


class IssueKeys(BaseModel):
    keys_string: str
