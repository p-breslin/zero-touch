import logging
from sqlalchemy import Engine
from typing import Any, Dict, Optional

from utils.helpers import load_yaml, resolve_model
from agents.base_agent import build_base_agent
from models.response_models import PersonList

from agno.tools.sql import SQLTools
from agno.knowledge.agent import AgentKnowledge
from agno.vectordb.chroma.chromadb import ChromaDb

log = logging.getLogger(__name__)

MAPPINGS = {"PersonList": PersonList}


def build_agent(
    agent_key: str,
    session_state: Optional[Dict[str, Any]] = None,
    db_engine: Optional[Engine] = None,
    vector_db: Optional[ChromaDb] = None,
):
    """
    Builds an agent based on the predefined configuration type.
    """
    cfg = load_yaml(file="agents", key=agent_key)
    if not cfg:
        log.error(f"No configuration found for agent type: {agent_key}")
        raise ValueError(f"Invalid agent type: {agent_key}")

    # Load configuration params
    model_id = cfg.get("model_id", None)
    provider = cfg.get("provider", "openai")
    tool = cfg.get("tool", None)
    description = cfg.get("description", None)
    prompt_key = cfg.get("prompt_key", None)
    response_model = cfg.get("response_model", None)
    knowledge = cfg.get("knowledge", False)
    reasoning = cfg.get("reasoning", False)
    reasoning_model = cfg.get("reasoning_model", None)
    markdown = cfg.get("markdown", None)
    debug_mode = cfg.get("debug_mode", False)
    show_tool_calls = cfg.get("show_tool_calls", False)
    add_datetime_to_instructions = cfg.get("add_datetime_to_instructions", False)

    log.info(f"Building Agent: {agent_key}")

    # Resolve models
    response_model = MAPPINGS.get(response_model, None)
    LLM_base_model = resolve_model(provider=provider, model_id=model_id)
    if reasoning:
        LLM_reasoning_model = resolve_model(provider=provider, model_id=reasoning_model)
    else:
        LLM_reasoning_model = None

    # Resolve tools
    tools = [SQLTools(db_engine=db_engine)] if tool else []

    # Extract prompt key
    instructions = load_yaml(file="prompts", key=prompt_key)

    # Knowledge base
    knowledge_base = AgentKnowledge(vector_db=vector_db) if knowledge else None

    return build_base_agent(
        name=agent_key,
        model=LLM_base_model,
        tools=tools,
        description=description,
        instructions=instructions,
        response_model=response_model,
        session_state=session_state,
        knowledge=knowledge_base,
        reasoning=reasoning,
        reasoning_model=LLM_reasoning_model,
        markdown=markdown,
        debug_mode=debug_mode,
        show_tool_calls=show_tool_calls,
        add_datetime_to_instructions=add_datetime_to_instructions,
    )
