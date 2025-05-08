import os
import sys
import json
import logging
import chromadb
import argparse
from dotenv import load_dotenv
from typing import List, Dict, Tuple
import chromadb.utils.embedding_functions as embedding_functions


from utils.helpers import load_cfg
from utils.logging_setup import setup_logging


load_dotenv()
setup_logging()
cfg = load_cfg("database_cfg", section="ChromaDB")
log = logging.getLogger(__name__)

# Paths
abs_path = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(abs_path, "../", cfg.get("DATA_PATH"))
CHROMA_PATH = os.path.join(DATA_PATH, "ChromaDB")

EMBEDDING_MODEL = cfg.get("EMBEDDING_MODEL")
GITHUB_COLLECTION = cfg.get("DB_NAME_GITHUB")
JIRA_COLLECTION = cfg.get("DB_NAME_JIRA")
GITHUB_JSON = cfg.get("GITHUB_JSON")
JIRA_JSON = cfg.get("JIRA_JSON")


def _load_and_chunk_schema(
    json_filepath: str,
) -> Tuple[List[str], List[str], List[Dict]]:
    """
    Loads schema descriptions from JSON, chunks them, and prepares for ChromaDB.

    The chunking approach creates separate chunks created for tables and their constituent columns.

        Table Chunk:
            Provides a high-level overview of the table. Gives the LLM context about the table's general purpose before diving into specific columns. Example RAG queries:
                - "Tell me about the COMMITS table in GitHub"
                - "Which tables are related to code changes?"

        Column Chunk:
            Provides granular detail about each specific column. Ex queries:
                - "What does the SHA column represent in GitHub COMMITS table?"
                - "Find columns related to user email addresses in JIRA"

    Chunking only by table would make it hard to retrieve specific info about a single column without getting the (potentially very long) description of the entire table, diluting relevance for column-specific queries.

    Chunking only by column would lose the valuable context of the table's overall purpose. Retrieving multiple columns for a table would require fetching many small, disconnected chunks.
    """
    ids = []
    documents = []  # Text chunks to be embedded
    metadatas = []  # Corresponding metadata

    path = os.path.join(DATA_PATH, json_filepath)
    log.info(f"Loading data from: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.critical(f"Error loading {json_filepath}: {e}")
        raise

    schema_name = list(data.keys())[0]
    schema_content = data[schema_name]

    log.info(f"Processing schema: {schema_name}")
    for table in schema_content.get("tables"):
        table_name = table.get("name")
        table_desc = table.get("description")

        # Create chunk for TABLE
        table_id = f"{schema_name}_{table_name}_table"  # Construct unique ID
        table_text = (
            f"Schema: {schema_name}, Table: {table_name}, Description: {table_desc}"
        )
        table_meta = {"type": "table", "schema": schema_name, "table": table_name}

        ids.append(table_id)
        documents.append(table_text)
        metadatas.append(table_meta)

        # Create chumks for COLUMNS
        for column in table.get("columns"):
            col_name = column.get("name")
            col_desc = column.get("description")

            # Construct unique ID
            col_id = f"{schema_name}_{table_name}_{col_name}_column"
            col_text = f"Schema: {schema_name}, Table: {table_name}, Column: {col_name}, Description: {col_desc}"
            col_meta = {
                "type": "column",
                "schema": schema_name,
                "table": table_name,
                "column": col_name,
            }

            ids.append(col_id)
            documents.append(col_text)
            metadatas.append(col_meta)

    log.info(f"Prepared {len(ids)} chunks for {schema_name}.")
    return ids, documents, metadatas


def setup_chroma_db():
    """
    Initializes a persistent ChromaDB client, creates collections for GitHub and JIRA schema descriptions, and populates them with embeddings.
    """
    if not os.path.exists(CHROMA_PATH):
        try:
            os.makedirs(CHROMA_PATH)
            log.info(f"Created ChromaDB directory: {CHROMA_PATH}")
        except Exception as e:
            log.critical(f"Failed to create directory at {CHROMA_PATH}: {e}")
            raise

    try:
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        log.info("ChromaDB client initialized.")
    except Exception as e:
        raise ConnectionError(f"Failed to initialize ChromaDB client: {e}")

    # Initialize the embedding function
    try:
        embedding_func = embedding_functions.OpenAIEmbeddingFunction(
            api_key=os.getenv("OPENAI_API_KEY"), model_name=EMBEDDING_MODEL
        )
        log.info(f"Initialized OpenAI embedding function with model: {EMBEDDING_MODEL}")
    except Exception as e:
        log.critical(f"Failed to initialize OpenAI embedding function: {e}")
        raise

    # Process Collections
    collections_to_process = [
        {"name": GITHUB_COLLECTION, "json_path": GITHUB_JSON},
        {"name": JIRA_COLLECTION, "json_path": JIRA_JSON},
    ]

    for collection_info in collections_to_process:
        collection_name = collection_info["name"]
        json_path = collection_info["json_path"]
        log.info(f"\n--- Processing Collection: {collection_name} ---")

        # Create the collection
        try:
            log.info(f"Creating collection: {collection_name}")
            collection = client.get_or_create_collection(
                name=collection_name,
                embedding_function=embedding_func,
                metadata={"hnsw:space": "cosine"},  # Default for semantic search
            )
            log.info(f"Collection '{collection_name}' ready.")
        except Exception as e:
            log.error(f"Failed to create collection '{collection_name}': {e}")
            raise

        # Load, Chunk, and Add the data
        ids, documents, metadatas = _load_and_chunk_schema(json_path)

        log.info(f"Adding {len(ids)} docs to collection '{collection_name}'")
        try:
            collection.upsert(
                ids=ids,
                documents=documents,
                metadatas=metadatas,
                # Embeddings are generated automatically by ChromaDB
                # when using an embedding_function with the collection
            )
            log.info(f"Successfully added documents in '{collection_name}'.")
        except Exception as e:
            log.error(f"ERROR adding documents to '{collection_name}': {e}")
            raise

    log.info("\nChromaDB setup process finished.")


def list_collections(show_columns: bool = False):
    """
    Lists all collection and tables in ChromaDB. Trying to connect to a client creates a new database; first check it's existence.
    """
    try:
        client = chromadb.PersistentClient(path=CHROMA_PATH)
        collections = client.list_collections()
    except Exception as e:
        log.critical(f"Critical error loading database: {e}")

    collection_names = [c.name for c in collections]
    target_collections = {"GitHub": GITHUB_COLLECTION, "Jira": JIRA_COLLECTION}

    for schema_label, collection_name in target_collections.items():
        print(f"\nSchema: {schema_label} (Collection: {collection_name})")

        if collection_name in collection_names:
            collection = client.get_collection(name=collection_name)

            # Query for docs (metadata indicates a table description)
            results = collection.get(
                where={"type": "table"},
                include=["metadatas"],
            )

            table_names = sorted(
                [
                    meta["table"]
                    for meta in results["metadatas"]
                    if meta and "table" in meta
                ]
            )

            if show_columns:
                for table_name in table_names:
                    print(f"\n  Table: {table_name}")

                    # $and for combining conditions
                    column_results = collection.get(
                        where={"$and": [{"type": "column"}, {"table": table_name}]},
                        include=["metadatas"],
                    )
                    column_names = sorted(
                        [
                            meta["column"]
                            for meta in column_results["metadatas"]
                            if meta and "column" in meta
                        ]
                    )

                    for col_name in column_names:
                        print(f"    - {col_name}")
            else:
                print("Tables:")
                for table_name in table_names:
                    print(f"- {table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manage ChromaDB Knowledge Base for Schemas."
    )
    subparsers = parser.add_subparsers(
        dest="command", help="Available commands", required=True
    )

    # Runs function for ChromaDB setup
    parser_setup = subparsers.add_parser(
        "setup", help="Initialize and populate the ChromaDB collections."
    )
    parser_setup.set_defaults(func=setup_chroma_db)

    # Runs function for listing the collections
    parser_list = subparsers.add_parser(
        "list", help="List collections and optionally their tables/columns."
    )
    parser_list.add_argument(
        "--show_columns",
        action="store_true",
        help="Include column names under each table in the listing.",
    )
    parser_list.set_defaults(func=list_collections)

    # Parse all args
    args = parser.parse_args()
    log.info(f"Executing command: {args.command}")

    func_args = vars(args)  # Get arguments as a dictionary
    command = func_args.pop("command")  # Remove command itself
    target_func = func_args.pop("func")  # Get the target function

    # Call the function with its specific arguments
    try:
        target_func(**func_args)
    except Exception as e:
        log.critical(f"Error with command '{command}': {e}", exc_info=True)
        sys.exit(1)
