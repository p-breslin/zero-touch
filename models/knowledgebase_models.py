from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class MetadataItem(BaseModel):
    type: Literal["table", "column"] = Field(..., description="Type of schema item.")
    source_system: str = Field(
        ..., description="Source system, e.g., 'github', 'jira'."
    )
    schema_name: str = Field(..., description="Name of the database schema.")
    table: str = Field(..., description="Name of the table.")
    column: Optional[str] = Field(
        None, description="Name of the column, if type is 'column'."
    )


class KBSnippet(BaseModel):
    document_id: str = Field(
        ..., description="The ID of the retrieved document from the knowledge base."
    )
    text_content: str = Field(
        ...,
        description="The text content of the schema snippet from the knowledge base.",
    )
    metadata: MetadataItem = Field(
        ..., description="Associated metadata from the knowledge base."
    )


class CriticalItemReport(BaseModel):
    item_identifier: str = Field(
        ...,
        description="A unique string identifying the critical schema item from the checklist (e.g., 'GITHUB.USERS.EMAIL', 'JIRA.ISSUES.FIELDS.assignee').",
    )
    status: Literal[
        "found_complete", "partially_found", "not_found", "not_explicitly_searched"
    ] = Field(
        ..., description="Status of information retrieval for this critical item."
    )
    notes: Optional[str] = Field(
        None,
        description="Brief notes if not 'found_complete', e.g., why it's partial or what was attempted.",
    )


class KBInfo(BaseModel):
    query_used: str = Field(
        ..., description="The query formulated and used to search the knowledge base."
    )
    retrieved_snippets: List[KBSnippet] = Field(
        ..., description="A list of schema snippets retrieved from the knowledge base."
    )
    critical_items_status: Optional[List[CriticalItemReport]] = Field(
        default=None,
        description="Structured report on the coverage of critical schema items based on the agent's internal checklist.",
    )
    summary_of_findings: Optional[str] = Field(
        None,
        description="A brief summary of what was found or if anything crucial seems missing.",
    )
