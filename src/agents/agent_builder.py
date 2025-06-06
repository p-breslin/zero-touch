import logging
from sqlalchemy import Engine
from typing import Any, List, Dict, Optional

from src.agents.base_agent import build_base_agent
from utils.helpers import load_yaml, resolve_model

# from agno.tools.sql import SQLTools
from agno.tools.thinking import ThinkingTools
from agno.knowledge.agent import AgentKnowledge

from models import (
    IssueKey,
    GeneratedCommitSummary,
    PreprocessedCommitSummary,
    IssueInfo,
    InferenceOutput,
    DeveloperInfo,
)

log = logging.getLogger(__name__)
MAPPINGS = {
    "IssueKey": IssueKey,
    "GeneratedCommitSummary": GeneratedCommitSummary,
    "PreprocessedCommitSummary": PreprocessedCommitSummary,
    "IssueInfo": IssueInfo,
    "InferenceOutput": InferenceOutput,
    "DeveloperInfo": DeveloperInfo,
}


def build_agent(
    agent_key: str,
    tools: Optional[List] = None,
    session_state: Optional[Dict[str, Any]] = None,
    db_engine: Optional[Engine] = None,
    knowledge_base: Optional[AgentKnowledge] = None,
):
    """
    Builds an agent based on the predefined configuration type.

    Args:
        agent_key: Selects a specific agent config from the configuration file.
        session_state: Optional state memory for the agent to reference.
        db_engine: Optional SQL database engine for the agent to access.
        knowledge_base: Optional knowledge base for RAG capabilities.
    """
    cfg = load_yaml(file="agents", key=agent_key)
    if not cfg:
        log.error(f"No configuration found for agent type: {agent_key}")
        raise ValueError(f"Invalid agent type: {agent_key}")

    # Load configuration params
    model_id = cfg.get("model_id", None)
    provider = cfg.get("provider", "openai")
    description = cfg.get("description", None)
    prompt_key = cfg.get("prompt_key", None)
    response_model = cfg.get("response_model", None)
    use_json_mode = cfg.get("use_json_mode", False)
    reasoning = cfg.get("reasoning", False)
    reasoning_model_id = cfg.get("reasoning_model_id", None)
    thinking_tools = cfg.get("thinking_tools", None)
    markdown = cfg.get("markdown", None)
    temperature = cfg.get("temperature", 0)
    debug_mode = cfg.get("debug_mode", False)
    show_tool_calls = cfg.get("show_tool_calls", False)
    add_datetime_to_instructions = cfg.get("add_datetime_to_instructions", False)

    log.debug(f"Building Agent: {agent_key}")

    # Resolve models
    response_model = MAPPINGS.get(response_model, None)
    LLM_base_model = resolve_model(
        provider=provider, model_id=model_id, temperature=temperature
    )
    if reasoning:
        LLM_reasoning_model = resolve_model(
            provider=provider, model_id=reasoning_model_id, reasoning=True
        )
    else:
        LLM_reasoning_model = None

    # Resolve tools
    selected_tools = tools if tools else []
    if thinking_tools:
        selected_tools.append(ThinkingTools(add_instructions=True))

    # Extract prompt key
    instructions = load_yaml(file="instructions", key=prompt_key)

    return build_base_agent(
        name=agent_key,
        model=LLM_base_model,
        tools=selected_tools,
        description=description,
        instructions=instructions,
        response_model=response_model,
        use_json_mode=use_json_mode,
        session_state=session_state,
        knowledge=knowledge_base,
        reasoning=reasoning,
        reasoning_model=LLM_reasoning_model,
        markdown=markdown,
        debug_mode=debug_mode,
        show_tool_calls=show_tool_calls,
        add_datetime_to_instructions=add_datetime_to_instructions,
    )
