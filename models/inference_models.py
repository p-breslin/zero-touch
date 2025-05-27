from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class IssueKey(BaseModel):
    key: Optional[str] = None


class ConsolidatedEntity(BaseModel):
    canonical_name: str = Field(
        description="The chosen primary name for the consolidated entity (e.g., full real name or recognized bot name)."
    )
    original_names_user_names: List[str] = Field(
        default_factory=list,
        description="All unique NAME entries from the source ALL_IDENTITIES table that were merged into this entity.",
    )
    all_jira_ids: List[str] = Field(
        default_factory=list,
        description="All unique JIRA IDs (from JIRA_CREATOR_IDS, JIRA_REPORTER_IDS, JIRA_ASSIGNEE_IDS) associated with this entity.",
    )
    all_github_ids: List[str] = Field(
        default_factory=list,
        description="All unique GitHub IDs (from GH_AUTHOR_IDS, GH_COMMITTER_IDS) associated with this entity.",
    )
    all_pr_user_ids: List[str] = Field(
        default_factory=list,
        description="All unique PR User IDs (from PR_USER_IDS) associated with this entity.",
    )
    all_emails: List[str] = Field(
        default_factory=list,
        description="All unique, valid email addresses associated with this entity, excluding highly generic ones unless strongly corroborated.",
    )
    is_bot_or_system: bool = Field(
        default=False,
        description="Flag indicating if this entity is primarily identified as a bot or system account.",
    )
    notes: Optional[str] = Field(
        None,
        description="Any relevant notes about this consolidated entity, e.g., reasoning for canonical name choice, confirmation of bot status, or specific source identifiers if crucial.",
    )


class AmbiguousLink(BaseModel):
    entity1_identifiers: List[str] = Field(
        description="A list of key identifiers (e.g., canonical name or primary ID from an *interim* consolidated entity) for the first entity in the ambiguous link."
    )
    entity2_identifiers: List[str] = Field(
        description="A list of key identifiers for the second entity in the ambiguous link."
    )
    reason_for_ambiguity: str = Field(
        description="Explanation of why the link between these (groups of) records is considered ambiguous and was not merged."
    )


class IdentityInference(BaseModel):
    consolidated_entities: List[ConsolidatedEntity] = Field(
        description="A list of all distinct individuals and system accounts identified and consolidated."
    )
    ambiguous_links: List[AmbiguousLink] = Field(
        default_factory=list,
        description="A list of links between potential entities (or groups of records) that were too ambiguous to merge confidently.",
    )


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
