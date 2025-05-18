import os
import logging
import chromadb
from src.paths import DATA_DIR
from dotenv import load_dotenv
from utils.logging_setup import setup_logging
import chromadb.utils.embedding_functions as embedding_functions

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

CHROMA_PATH = str(DATA_DIR / "ChromaDB")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL")
CHROMADB_COLLECTION_NAME = os.getenv("CHROMADB_COLLECTION")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def query_chroma(query_text: str, n_results: int = 5):
    """
    Embeds a query, searches ChromaDB, and prints the results.
    """
    log.info(f"Initializing ChromaDB client from path: {CHROMA_PATH}")
    client = chromadb.PersistentClient(path=CHROMA_PATH)

    embedding_func = embedding_functions.OpenAIEmbeddingFunction(
        api_key=OPENAI_API_KEY, model_name=EMBEDDING_MODEL_NAME
    )

    log.info(f"Getting collection: {CHROMADB_COLLECTION_NAME}")
    try:
        collection = client.get_collection(
            name=CHROMADB_COLLECTION_NAME,
            embedding_function=embedding_func,
        )
    except Exception as e:
        log.error(f"Error getting collection '{CHROMADB_COLLECTION_NAME}': {e}")
        return

    log.info(f"Querying collection with: '{query_text}' for {n_results} results...")
    results = collection.query(
        query_texts=[query_text],
        n_results=n_results,
        include=[
            "documents",
            "metadatas",
            "distances",
        ],
    )

    if not results or not results.get("documents") or not results["documents"][0]:
        log.info("No results found.")
        return

    log.info("\n--- Query Results ---")
    for i in range(len(results["documents"][0])):
        doc_id = results["ids"][0][i]
        document = results["documents"][0][i]
        metadata = results["metadatas"][0][i]
        distance = (
            results["distances"][0][i]
            if results.get("distances") and results["distances"][0]
            else "N/A"
        )

        print(f"\nResult {i + 1}:")
        print(f"  ID: {doc_id}")
        print(
            f"  Distance: {distance:.4f}"
            if isinstance(distance, float)
            else f"  Distance: {distance}"
        )
        print(f"  Metadata: {metadata}")
        print(f"  Document: \n{'-' * 20}\n{document}\n{'-' * 20}")


if __name__ == "__main__":
    # --- Example Queries ---
    # General table understanding
    # test_query = "Tell me about the PULL_REQUESTS table in GitHub."
    # test_query = "What is the ISSUES table in JIRA for?"

    # Specific column
    # test_query = "What is the SHA column in the GitHub COMMITS table?"
    # test_query = "Describe the ID column in JIRA's PROJECTS table."

    # Column with nested structure (type_details)
    # test_query = "What are the properties of the USER object in the GitHub PULL_REQUESTS table?"
    # test_query = "Describe the structure of the ITEMS array in JIRA CHANGELOGS table."
    # test_query = "What fields are in the COMMIT object within GitHub PULL_REQUEST_COMMITS table?"
    # test_query = "How is the AUTHOR field structured in JIRA's ISSUE_COMMENTS table?"
    test_query = (
        "Tell me about the fields in the AUTHOR column in the JIRA CHANGELOGS table"
    )

    # Relationship / Purpose
    # test_query = "How are pull requests linked to commits in GitHub?"
    # test_query = "Which JIRA columns can I use to identify a user's email?"

    if len(os.sys.argv) > 1:
        user_query = " ".join(os.sys.argv[1:])
        log.info(f"Using query from command line: {user_query}")
        query_chroma(user_query)
    elif test_query:
        log.info(f"Using hardcoded test query: {test_query}")
        query_chroma(test_query)
    else:
        log.info("No query provided. Exiting.")
        log.info('Usage: python chromadb_queries.py "Your natural language query here"')
        log.info("Or uncomment one of the test_query variables in the script.")
