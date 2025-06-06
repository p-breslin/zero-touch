import logging
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Type

from agno.agent import Agent
from agno.knowledge.agent import AgentKnowledge

log = logging.getLogger(__name__)


def build_base_agent(
    # Core identifiers & components
    name: str,
    model: Any,
    tools: List[Any],
    # Prompting components
    description: Optional[str] = None,
    instructions: Optional[List[str]] = None,
    # Output structure & initial state
    response_model: Optional[Type[BaseModel]] = None,
    use_json_mode: Optional[bool] = False,
    session_state: Optional[Dict[str, Any]] = None,
    # Advanced capabilities
    knowledge: Optional[AgentKnowledge] = None,
    reasoning: bool = False,
    reasoning_model: Optional[Any] = None,
    # Configuration flags for Agno Agent behavior
    markdown: bool = False,
    debug_mode: bool = False,
    show_tool_calls: bool = True,
    add_datetime_to_instructions: bool = False,
) -> Agent:
    """
    Centralized base structure for building configured Agno Agent instances.

    Args:
        name: The unique name for this agent instance.
        model: Pre-configured instance of an Agno-compatible model.
        tools: Optional list of tools or toolkits for the agent to use.

        description: String describing the agent's overall role or persona.
        instructions: List of specific instructions for the agent to follow.

        response_model: Optional Pydantic model to structure and validate the agent's output.
        use_json_mode: When the model doesn't support structured outputs.
        session_state: Optional dict for the agent's initial session state.

        knowledge: An optional AgentKnowledge instance for RAG capabilities.
        reasoning: Enables Agno's "Reasoning Agent" pattern. (Default: False)
        reasoning_model: Optional pre-configured Agno-compatible model instance to be used specifically for reasoning, if `reasoning=True`.

        markdown: Agent will format responses in Markdown. (Default: False)
        debug_mode: Enables debug logging and richer output. (Default: False)
        show_tool_calls: Makes tool calls and their results visible in the agent's output. (Default: True)
        add_datetime_to_instructions: Adds the current datetime to the agent's instructions, giving it a sense of current time. (Default: False)

    Returns:
        A configured Agno Agent instance.
    """
    log.debug(f"Building agent '{name}'...")

    # Directly pass parameters to the Agno Agent constructor
    agent_instance = Agent(
        name=name,
        model=model,
        tools=tools,
        description=description,
        instructions=instructions,
        response_model=response_model,
        use_json_mode=use_json_mode,
        session_state=session_state,
        knowledge=knowledge,
        reasoning=reasoning,
        reasoning_model=reasoning_model,
        markdown=markdown,
        debug_mode=debug_mode,
        show_tool_calls=show_tool_calls,
        add_datetime_to_instructions=add_datetime_to_instructions,
    )

    log.debug(f"Agent '{name}' built successfully.")
    return agent_instance
