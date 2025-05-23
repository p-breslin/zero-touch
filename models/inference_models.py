from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class IssueKey(BaseModel):
    key: Optional[str] = None


class RepoLabel(BaseModel):
    label: Optional[str] = Field(
        description="A concise, general functional label for the repository (max two words)."
    )


# class CommitterInfo(BaseModel):
#     role: Literal[
#         "Full Stack Engineer", "UX", "Backend Engineer", "AI Engineer", "DevOps"
#     ] = Field(
#         ...,
#         description="The primary functional role inferred from the committer's aggregated code.",
#     )
#     skills: Optional[List[str]] = Field(
#         default_factory=list,
#         description="A list of specific technical skills, programming languages, or frameworks evident from the code (e.g., ['Python', 'React', 'SQL', 'AWS Lambda', 'Terraform', 'Data Analysis']). Provide 3-7 key skills.",
#     )


class CommitterInfo(BaseModel):
    analysis: str = Field(
        description="A brief summary of observations from the code changes, including types of files modified, nature of changes, and notable patterns."
    )
    role: Literal[
        "Front End Developer",
        "Back End Developer",
        "AI Engineer",
        "DevOps Engineer",
        "Inconclusive",
    ] = Field(
        description="The determined primary role of the developer based on code changes."
    )
    experience_level: Literal["Junior", "Mid-level", "Senior", "Inconclusive"] = Field(
        description="The determined experience level of the developer based on complexity and scope of changes."
    )
    skills: List[str] = Field(
        default_factory=list,
        description="A list of identified technologies, languages, frameworks, or tools demonstrated.",
    )
    justification: str = Field(
        description="Detailed explanation for the conclusions on role, experience level, and skills, referencing specific code change examples."
    )
    notes: Optional[str] = Field(
        default=None,
        description="Optional field for any additional notes, or to explain why a determination was inconclusive if not covered in justification.",
    )
