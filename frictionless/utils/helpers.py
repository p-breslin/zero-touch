from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict

import click
import duckdb
import yaml
from agno.models.google import Gemini
from agno.models.openai import OpenAIChat
from agno.models.openrouter import OpenRouter
from arango import ArangoClient
from pydantic import BaseModel

from scripts.paths import CONFIG_DIR, DATA_DIR

log = logging.getLogger(__name__)


def load_yaml(file, key=None):
    """Loads YAML configuration file."""
    try:
        path = CONFIG_DIR / f"{file}.yaml"
        with open(path, "r") as f:
            if key:
                return yaml.safe_load(f)[key]
            else:
                return yaml.safe_load(f)
    except Exception as e:
        log.error(f"Error loading {file}: {e}")


def resolve_model(
    provider: str, model_id: str, temperature: float = 0, reasoning: bool = False
):
    """Selects LLM provider and model."""
    try:
        if provider == "openai":
            if reasoning:
                return OpenAIChat(id=model_id)
            else:
                return OpenAIChat(id=model_id, temperature=temperature)

        elif provider == "google":
            if reasoning:
                return Gemini(id=model_id)
            else:
                return Gemini(id=model_id, temperature=temperature)

        elif provider == "openrouter":
            if reasoning:
                return OpenRouter(id=model_id, api_key=os.getenv("OPENROUTER_API_KEY"))
            else:
                return OpenRouter(
                    id=model_id,
                    api_key=os.getenv("OPENROUTER_API_KEY"),
                    temperature=temperature,
                )
    except Exception as e:
        log.error(f"Error loading LLM provider/model: {e}")


def validate_response(output_content, response_model, savefile=None):
    """
    Validates an agent's structured response against the predefined schema. Response then saved to a JSON file (in test_outputs/ by default).
    """
    try:
        # Convert to JSON if response not structured (like Google)
        if isinstance(output_content, str):
            output_content = parse_json(output_content)

        # Ensure JSON object is a Pydantic model instance
        if not isinstance(output_content, response_model):
            output_content = response_model(**output_content)

        if savefile:
            output_path = DATA_DIR / f"{savefile}.json"
            with open(output_path, "w") as f:
                json.dump(output_content.model_dump(), f, indent=4)
                log.info(f"Saved structured output to {output_path}")

        return output_content
    except IOError as e:
        log.error(f"Failed to write output file {output_path}: {e}")

    # Handle case if content isn't a Pydantic model
    except AttributeError:
        log.warning("Output content does not have model_dump method.")

        # Fallback: try saving raw content
        try:
            with open(output_path.with_suffix(".raw.json"), "w") as f:
                json.dump(output_content, f, indent=4)
        except Exception:
            log.error("Could not save raw output content.")
            return None


def validate_output(output_content, schema):
    """Validates an agent's structured response to the predefined schema."""
    try:
        # Convert to JSON if response not structured (like Google)
        if isinstance(output_content, str):
            print(output_content)
            output_content = parse_json(output_content)
            print(output_content)

        # Ensure JSON object is a Pydantic model instance
        if not isinstance(output_content, schema):
            output_content = schema(**output_content)

        return output_content

    # Handle case if content isn't a Pydantic model
    except AttributeError:
        log.warning("Output content does not have model_dump method.")


def parse_json(json_string: str):
    """Tries to parse a string as JSON."""
    try:
        # Strip whitespace
        text = json_string.strip()

        # Remove ticks if necessary
        text = text.strip().strip("`")
        if text.startswith("json"):
            text = text[4:].strip()

        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def safe_json(blob: Any) -> Dict[str, Any]:
    """Return a dict; never None."""
    if not blob:
        return {}
    if isinstance(blob, dict):
        return blob
    if isinstance(blob, (str, bytes, bytearray)):
        try:
            return json.loads(blob)
        except json.JSONDecodeError:
            log.debug("Bad JSON blob ignored: %s", blob)
    return {}


@contextmanager
def db_manager(path: Path, *, read_only: bool = False):
    conn = duckdb.connect(path, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def pydantic_to_gemini(output_model: BaseModel) -> str:
    return json.dumps(output_model.model_dump(), ensure_ascii=False, indent=None)


def get_arango_client():
    """
    Return an ArangoClient connected to the host specified in ARANGO_HOST.
    """
    host = os.getenv("ARANGO_HOST")
    return ArangoClient(hosts=host)


def get_system_db():
    """
    Return a handle to the _system database (using ARANGO_USERNAME/ARANGO_PASSWORD). Useful for creating or deleting databases.
    """
    client = get_arango_client()
    username = os.getenv("ARANGO_USERNAME", "root")
    password = os.getenv("ARANGO_PASSWORD")
    return client.db("_system", username=username, password=password)


def get_arango_db():
    """
    Return a handle to the target ArangoDB database (ARANGO_DB).
    Does not create or delete the database—just connects.
    """
    client = get_arango_client()
    username = os.getenv("ARANGO_USERNAME", "root")
    password = os.getenv("ARANGO_PASSWORD")
    db_name = os.getenv("ARANGO_DB")
    return client.db(db_name, username=username, password=password)


def confirm_with_timeout(prompt: str, timeout: int = 15, default: bool = True) -> bool:
    """Prompt user y/N, timeout after `timeout` seconds. Works on Unix/macOS only."""
    import signal

    class TimeoutExpired(Exception):
        pass

    def _timeout_handler(signum, frame):
        raise TimeoutExpired()

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout)
    try:
        return click.confirm(prompt, default=default)
    except TimeoutExpired:
        click.echo(
            f"\nNo response in {timeout}s — defaulting to {'Yes' if default else 'No'}."
        )
        return default
    finally:
        signal.alarm(0)
