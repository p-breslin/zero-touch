import os
import sys
import json
import logging
import chromadb
import argparse
from dotenv import load_dotenv
from typing import List, Dict, Tuple
import chromadb.utils.embedding_functions as embedding_functions

from src.paths import DATA_DIR
from utils.logging_setup import setup_logging


load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# Directory path
CHROMA_PATH = str(DATA_DIR / "ChromaDB")

# Setup
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL")
CHROMADB_COLLECTION = os.getenv("CHROMADB_COLLECTION")
SOURCE_FILES = {
    "github": "XFLOW_DEV_GITHUB_.json",
    "jira": "XFLOW_DEV_JIRA_.json",
}


def _load_and_chunk_schema(
    json_filename: str, source: str
) -> Tuple[List[str], List[str], List[Dict]]:
    """
    Loads schema descriptions from JSON, chunks them, and prepares for ChromaDB.

    The chunking approach creates separate chunks created for tables and their constituent columns, all for a given Source System (GitHub or Jira).

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

    path = DATA_DIR / json_filename
    log.info(f"Loading data from: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.critical(f"Error loading {json_filename}: {e}")
        raise

    schema_name = list(data.keys())[0]
    schema_content = data[schema_name]

    log.info(f"Processing schema: {schema_name}")
    for table in schema_content.get("tables"):
        table_name = table.get("name")
        table_desc = table.get("description")

        # Create chunk for TABLE
        table_id = f"{source}_{schema_name}_{table_name}_table"
        table_text = f"Source System: {source}, Schema: {schema_name}, Table: {table_name}, Description: {table_desc}"
        table_meta = {
            "type": "table",
            "source_system": source,
            "schema_name": schema_name,
            "table": table_name,
        }

        ids.append(table_id)
        documents.append(table_text)
        metadatas.append(table_meta)

        # Create chumks for COLUMNS
        for column in table.get("columns"):
            col_name = column.get("name")
            col_desc = column.get("description")

            # Construct unique ID
            col_id = f"{source}_{schema_name}_{table_name}_{col_name}_column"
            col_text = f"Source System: {source}, Schema: {schema_name}, Table: {table_name}, Column: {col_name}, Description: {col_desc}"
            col_meta = {
                "type": "column",
                "source_system": source,
                "schema_name": schema_name,
                "table": table_name,
                "column": col_name,
            }

            ids.append(col_id)
            documents.append(col_text)
            metadatas.append(col_meta)

    log.info(f"{len(ids)} chunks for {schema_name} (Source: {source}).")
    return ids, documents, metadatas


def setup_chroma_db():
    """
    Initializes a persistent ChromaDB client, creates a single unified collection of schema descriptions for both System Sources (GitHub and JIRA), and populates them with OpenAI embeddings.
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

    # Create the single unified collection
    try:
        log.info(f"Creating collection: {CHROMADB_COLLECTION}")
        collection = client.get_or_create_collection(
            name=CHROMADB_COLLECTION,
            embedding_function=embedding_func,
            metadata={"hnsw:space": "cosine"},  # Default for semantic search
        )
        log.info(f"Unified collection '{CHROMADB_COLLECTION}' ready.")
    except Exception as e:
        log.error(f"Failed to create collection '{CHROMADB_COLLECTION}': {e}")
        raise

    # Process each source file and add to the single collection
    for source, json_filename in SOURCE_FILES.items():
        log.info(f"Loading and adding {source} data ({json_filename})")
        ids, documents, metadatas = _load_and_chunk_schema(json_filename, source)

        log.info(
            f"Adding {len(ids)} docs from {source} into collection '{CHROMADB_COLLECTION}'"
        )
        try:
            collection.upsert(
                ids=ids,
                documents=documents,
                metadatas=metadatas,
                # Embeddings are generated automatically by ChromaDB
                # when using an embedding_function with the collection
            )
            log.info(
                f"Successfully added documents from {source} into '{CHROMADB_COLLECTION}'."
            )
        except Exception as e:
            log.error(
                f"ERROR adding documents from {source} into '{CHROMADB_COLLECTION}': {e}"
            )
            raise

    log.info("\nChromaDB setup process finished.")


def list_collections(show_columns: bool = False):
    """
    Lists all collection and tables in ChromaDB. Trying to connect to a client creates a new database; first check it's existence.
    """
    try:
        client = chromadb.PersistentClient(path=CHROMA_PATH)
    except Exception as e:
        log.critical(f"Critical error loading database: {e}")
        return

    collection = client.get_collection(name=CHROMADB_COLLECTION)
    log.info(f"Total docs in '{CHROMADB_COLLECTION}': {collection.count()}")

    # Group by source system for display
    for source in SOURCE_FILES.keys():
        print(f"\nSource System: {source.upper()}")
        results = collection.get(
            where={"$and": [{"type": "table"}, {"source_system": source}]},
            include=["metadatas"],
        )
        table_metas = sorted(results["metadatas"], key=lambda x: x.get("table", ""))

        current_schema_name = None
        for meta in table_metas:
            schema_name = meta["schema_name"]
            table_name = meta["table"]

            if schema_name != current_schema_name:
                print(f"Schema: {schema_name}")
                current_schema_name = schema_name

            print(f"Table: {table_name}")

            if show_columns:
                column_results = collection.get(
                    where={
                        "$and": [
                            {"type": "column"},
                            {"source_system": source},
                            {"schema_name": schema_name},
                            {"table": table_name},
                        ]
                    },
                    include=["metadatas"],
                )
                if column_results and column_results.get("metadatas"):
                    column_metas = sorted(
                        column_results["metadatas"], key=lambda x: x.get("column", "")
                    )
                    for col_meta in column_metas:
                        if col_meta and "column" in col_meta:
                            print(f"      - {col_meta['column']}")
                else:
                    print(f"No columns found for table '{table_name}'.")


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
