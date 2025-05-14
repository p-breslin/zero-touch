from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class SchemaSnippet(BaseModel):
    document_id: str = Field(
        ..., description="The ID of the retrieved document from the knowledge base."
    )
    text_content: str = Field(
        ..., description="The text content of the schema snippet."
    )
    metadata: Dict[str, Any] = Field(
        ..., description="Associated metadata from the knowledge base."
    )


class SchemaInfo(BaseModel):
    query_used: str = Field(
        ..., description="The query formulated and used to search the knowledge base."
    )
    retrieved_snippets: List[SchemaSnippet] = Field(
        ..., description="A list of schema snippets retrieved from the knowledge base."
    )
    summary_of_findings: Optional[str] = Field(
        None,
        description="A brief summary of what was found or if anything crucial seems missing.",
    )
