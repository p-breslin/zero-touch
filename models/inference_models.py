from pydantic import BaseModel
from typing import Optional, Literal


class IssueKey(BaseModel):
    key: Optional[str] = None


class InferredRole(BaseModel):
    role: Literal[
        "Full Stack Engineer", "UX", "Backend Engineer", "AI Engineer", "Unknown"
    ]
