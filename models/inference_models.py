from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict


class IssueKey(BaseModel):
    key: Optional[str] = None


class GeneratedCommitSummary(BaseModel):
    """Output model after analyzing a single commit diff."""

    summary: str = Field(
        ...,
        description="One- to three-sentence summary describing the purpose and scope of the changes in the commit.",
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

    file_path: List[str] = Field(
        ...,
        description="Full file path changed in this commit.",
    )


class IssueInfo(BaseModel):
    issue_type: Optional[str] = Field(
        None,
        description="The type of the JIRA issue (e.g., 'Bug', 'Task', 'Story', 'Epic').",
    )
    summary: Optional[str] = Field(
        None, description="Short text summary of the issue as authored in JIRA."
    )
    description: Optional[str] = Field(
        None, description="Detailed description text from the JIRA issue, if available."
    )
    project_key: Optional[str] = Field(
        None,
        description="Key of the JIRA project that the issue belongs to.",
    )
    project_name: Optional[str] = Field(
        None, description="Full name of the JIRA project the issue belongs to."
    )


class InferenceOutput(BaseModel):
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
    associated_issues: Dict[str, IssueInfo] = Field(
        default_factory=dict,
        description="Dictionary of JIRA issues this user has interacted with, keyed by ISSUE_KEY.",
    )


class DeveloperInfo(BaseModel):
    analysis: str = Field(
        description="A brief summary of observations from the code changes, including types of files modified, nature of changes, and notable patterns based on the structured contribution input and project interactions."
    )
    role: Literal[
        "Front End Developer",
        "Back End Developer",
        "Full Stack Developer",
        "AI Engineer",
        "DevOps Engineer",
        "Data Scientist",
        "Technical Architect",
        "Inconclusive",
    ] = Field(
        description="The determined primary role of the developer based on structured analysis of their code contributions and JIRA interactions."
    )
    experience_level: Literal["Junior", "Mid-level", "Senior"] = Field(
        description="Determined experience level based on the complexity, autonomy, and scope demonstrated by their structured code contributions and project interactions."
    )
    skills: List[str] = Field(
        default_factory=list,
        description="A list of specific technologies, languages, frameworks, or tools demonstrated and identified from the structured contributions and/or project interactions.",
    )
    justification: str = Field(
        description="Detailed explanation for the conclusions on role, experience level, and skills, referencing patterns and summaries from the structured input contributions."
    )
