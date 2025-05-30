from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class IssueKey(BaseModel):
    key: Optional[str] = None


class RepoLabel(BaseModel):
    label: Optional[str] = Field(
        description="A concise, general functional label for the repository (max two words)."
    )


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


class FileChangeDetail(BaseModel):
    file_path: str = Field(description="The full path of the modified file.")
    file_type_inference: str = Field(
        description="Inferred type or purpose of the file (e.g., 'Frontend UI Component', 'Backend API Endpoint', 'DevOps Script', 'Documentation', 'Test File', 'Configuration')."
    )
    key_changes_summary: str = Field(
        description="A brief 1-2 sentence summary of what changed in this specific file diff."
    )
    technologies_identified: List[str] = Field(
        default_factory=list,
        description="A list of specific languages, frameworks, or tools evident from the changes in this file.",
    )


class StructuredContribution(BaseModel):
    commit_hash: str = Field(description="The commit SHA associated with this change.")
    timestamp: str = Field(description="The timestamp of the commit.")
    repo_name: str = Field(description="The name of the repository.")
    files_changed: List[FileChangeDetail] = Field(
        description="A list of details for each file changed within this commit."
    )
    overall_commit_summary: Optional[str] = Field(
        default=None,
        description="A high-level summary of what this entire commit achieved, if discernible from multiple file changes. Can be similar to a good commit message.",
    )
    contribution_complexity_indicators: Optional[List[str]] = Field(
        default_factory=list,
        description="Indicators of complexity for this commit (e.g., 'minor typo fix', 'refactored core algorithm', 'added new API endpoint', 'significant architectural change').",
    )


class PreprocessedDiffOutput(BaseModel):
    contributions: List[StructuredContribution]


class DeveloperInfo(BaseModel):
    analysis: str = Field(
        description="A brief summary of observations from the code changes, including types of files modified, nature of changes, and notable patterns based on the structured contribution input."
    )
    role: Literal[
        "Front End Developer",
        "Back End Developer",
        "AI Engineer",
        "DevOps Engineer",
        "Inconclusive",
    ] = Field(
        description="The determined primary role of the developer based on the structured analysis of their code contributions."
    )
    experience_level: Literal["Junior", "Mid-level", "Senior", "Inconclusive"] = Field(
        description="The determined experience level of the developer based on the complexity, scope, and nature of their structured code contributions."
    )
    skills: List[str] = Field(
        default_factory=list,
        description="A list of specific technologies, languages, frameworks, or tools demonstrated and identified from the structured contributions.",
    )
    justification: str = Field(
        description="Detailed explanation for the conclusions on role, experience level, and skills, referencing patterns and summaries from the structured input contributions."
    )
