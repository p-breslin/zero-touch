from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class IssueKey(BaseModel):
    key: Optional[str] = None


class GeneratedCommitSummary(BaseModel):
    """Output model after analyzing a single commit diff."""

    summary: str = Field(
        ...,
        description="One- to two-sentence summary describing the purpose and scope of the changes in the commit.",
    )

    key_changes: List[str] = Field(
        ...,
        description="List of important changes or actions taken in the commit, ideally 2-5 bullet points.",
    )

    langs: List[str] = Field(
        ...,
        description="Languages used in the commit, e.g., 'python', 'typescript', 'java'.",
    )

    frameworks: List[str] = Field(
        ...,
        description="Frameworks or major libraries used, e.g., 'php', 'react', 'django', 'fastapi', 'vue'.",
    )


class PreprocessedCommitSummary(BaseModel):
    """Final commit-level summary. Combines agent results with metadata."""

    commit_message: str = Field(
        ..., description="The commit message as written by the developer."
    )

    summary: str = Field(
        ...,
        description="Concise agent-generated description of what the commit accomplished.",
    )

    key_changes: List[str] = Field(
        ..., description="Bullet points summarizing major changes in the commit."
    )

    langs: List[str] = Field(
        ..., description="Languages detected in the files modified by this commit."
    )

    frameworks: List[str] = Field(
        ...,
        description="Relevant frameworks or libraries inferred from the commit diff.",
    )

    loc_added: int = Field(
        ..., description="Number of lines of code added in the commit."
    )

    loc_removed: int = Field(
        ..., description="Number of lines of code removed in the commit."
    )

    file_count: int = Field(..., description="Number of files modified in the commit.")

    path_roots: List[str] = Field(
        ...,
        description="Top-level directory paths touched in this commit (e.g., 'api/user', 'client/hooks').",
    )


class PreprocessedDiffOutput(BaseModel):
    """Top-level user-level summary of all commit contributions."""

    last_90d_commits: int = Field(
        ...,
        description="Total number of commits authored by this user in the last 90 days.",
    )

    pr_review_comments: int = Field(
        ...,
        description="Number of pull request review comments written by this user in the last 90 days.",
    )

    commits: List[PreprocessedCommitSummary] = Field(
        ...,
        description="Chronologically ordered list of summarized commits (newest first).",
    )


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
