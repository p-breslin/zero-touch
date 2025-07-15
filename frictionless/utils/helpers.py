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
from arango.database import StandardDatabase
from pydantic import BaseModel

from scripts.paths import CONFIG_DIR, DATA_DIR

log = logging.getLogger(__name__)


def load_yaml(file, key=None):
    """Loads and parses a YAML file from the CONFIG_DIR.

    Args:
        file (str): The base filename (without extension) of the YAML file to load.
        key (str, optional): Returns only this top-level key from the YAML data.

    Returns:
        dict | Any: Parsed YAML contents, or the sub-dictionary at `key` if specified.
    """
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
    """Returns an LLM client for the given provider, model ID, and config.

    Args:
        provider (str): One of 'openai', 'google', or 'openrouter'.
        model_id (str): Model name or version string for the provider.
        temperature (float, optional): Sampling temperature. Ignored if reasoning=True.
        reasoning (bool, optional): If True, uses deterministic behavior (temp ignored).

    Returns:
        An instance of OpenAIChat, Gemini, or OpenRouter configured accordingly.
    """
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
    """Validates structured output against a Pydantic schema and optionally saves it.

    Args:
        output_content (str | dict | BaseModel): The agent response to validate. If a string, it is parsed as JSON.
        response_model (BaseModel): The expected Pydantic schema to validate against.
        savefile (str, optional): If provided, saves validated output to `test_outputs/{savefile}.json`.

    Returns:
        BaseModel | dict | None: A validated instance of `response_model` or raw fallback dict.
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
    """Validates an agent's structured output against a Pydantic schema.

    Args:
        output_content (str | dict | BaseModel): The structured response from the agent. If a string, it is parsed as JSON.
        schema (type[BaseModel]): The expected Pydantic model class.

    Returns:
        BaseModel: A validated instance of the provided schema.
    """
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
    """Attempts to parse a string as JSON, optionally cleaning formatting artifacts.

    Args:
        json_string (str): A raw string potentially containing JSON content.

    Returns:
        dict | list | None: Parsed JSON object (dict or list), or None on failure.
    """
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
    """Attempts to return a JSON-compatible dictionary from any input.

    Args:
        blob (Any): A JSON string, dict-like object, or any arbitrary input.

    Returns:
        dict[str, Any]: Parsed dictionary if possible; otherwise an empty dict.
    """
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
    """Context manager for managing a DuckDB connection.

    Args:
        path (Path): Filesystem path to the DuckDB database.
        read_only (bool, optional): If True, opens the connection in read-only mode.

    Yields:
        duckdb.DuckDBPyConnection: An active DuckDB connection instance.
    """
    conn = duckdb.connect(path, read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def pydantic_to_gemini(output_model: BaseModel) -> str:
    """Serializes a Pydantic model to a compact JSON string for Gemini input.

    Args:
        output_model (BaseModel): A Pydantic model instance.

    Returns:
        str: JSON string representation of the model.
    """
    return json.dumps(output_model.model_dump(), ensure_ascii=False, indent=None)


def get_arango_client() -> ArangoClient:
    """Initializes and returns an ArangoClient using the ARANGO_HOST env variable.

    Returns:
        ArangoClient: An instance configured to connect to the target host.
    """
    host = os.getenv("ARANGO_HOST")
    return ArangoClient(hosts=host)


def get_system_db() -> StandardDatabase:
    """Returns a handle to the _system database.

    Uses ARANGO_USERNAME and ARANGO_PASSWORD environment variables.
    Useful for administrative tasks like creating or deleting databases.

    Returns:
        StandardDatabase: Authenticated connection to the _system database.
    """
    client = get_arango_client()
    username = os.getenv("ARANGO_USERNAME", "root")
    password = os.getenv("ARANGO_PASSWORD")
    return client.db("_system", username=username, password=password)


def get_arango_db() -> StandardDatabase:
    """Returns a handle to the target ArangoDB database specified in ARANGO_DB.

    This function assumes the database already exists. It does not create or delete databases.

    Returns:
        StandardDatabase: Authenticated connection to the target database.
    """
    client = get_arango_client()
    username = os.getenv("ARANGO_USERNAME", "root")
    password = os.getenv("ARANGO_PASSWORD")
    db_name = os.getenv("ARANGO_DB")
    return client.db(db_name, username=username, password=password)


def confirm_with_timeout(prompt: str, timeout: int = 15, default: bool = True) -> bool:
    """Prompt the user for yes/no input, timing out after `timeout` seconds.

    If the user provides no input within the allotted time, the default value is returned.

    Note:
        Only works on Unix-like systems (e.g. Linux/macOS). Will not work on Windows.

    Args:
        prompt (str): The confirmation message to display.
        timeout (int): Seconds to wait before falling back to the default.
        default (bool): Value to return if the prompt times out.

    Returns:
        bool: True for 'yes', False for 'no', or `default` if time expires.
    """
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
