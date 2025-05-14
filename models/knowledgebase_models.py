from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class KBSnippet(BaseModel):
    document_id: str = Field(
        ..., description="The ID of the retrieved document from the knowledge base."
    )
    text_content: str = Field(
        ...,
        description="The text content of the schema snippet from the knowledge base.",
    )
    metadata: Dict[str, Any] = Field(
        ..., description="Associated metadata from the knowledge base."
    )


class KBInfo(BaseModel):
    query_used: str = Field(
        ..., description="The query formulated and used to search the knowledge base."
    )
    retrieved_snippets: List[KBSnippet] = Field(
        ..., description="A list of schema snippets retrieved from the knowledge base."
    )
    summary_of_findings: Optional[str] = Field(
        None,
        description="A brief summary of what was found or if anything crucial seems missing.",
    )
