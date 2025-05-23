from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class IssueKey(BaseModel):
    key: Optional[str] = None


class RepoLabel(BaseModel):
    label: Optional[str] = Field(
        description="A concise, general functional label for the repository (max two words)."
    )


class CommitterInfo(BaseModel):
    role: Literal[
        "Full Stack Engineer", "UX", "Backend Engineer", "AI Engineer", "DevOps"
    ] = Field(
        ...,
        description="The primary functional role inferred from the committer's aggregated code.",
    )
    skills: Optional[List[str]] = Field(
        default_factory=list,
        description="A list of specific technical skills, programming languages, or frameworks evident from the code (e.g., ['Python', 'React', 'SQL', 'AWS Lambda', 'Terraform', 'Data Analysis']). Provide 3-7 key skills.",
    )
