import os
import yaml
import logging
from pathlib import Path

from agno.models.google import Gemini
from agno.models.openai import OpenAIChat
from agno.models.openrouter import OpenRouter

from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def load_cfg(file, section=None):
    """Loads YAML configuration file."""
    try:
        path = Path(__file__).parent / f"../configs/{file}.yaml"
        with open(path, "r") as f:
            if section:
                return yaml.safe_load(f)[section]
            else:
                return yaml.safe_load(f)
    except Exception as e:
        log.error(f"Error loading the {file} configuration file: {e}")


def resolve_model(provider: str, model_id: str):
    """Selects LLM provider and model."""
    try:
        if provider == "openai":
            return OpenAIChat(id=model_id, temperature=0)
        elif provider == "google":
            return Gemini(id=model_id, temperature=0)
        elif provider == "openrouter":
            return OpenRouter(
                id=model_id, api_key=os.getenv("OPENROUTER_API_KEY"), temperature=0
            )
    except Exception as e:
        log.error(f"Error loading LLM provider/model: {e}")
