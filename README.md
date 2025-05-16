# Zero-Touch Organizational Insights Engine

This project aims to automatically infer and model an organization's structure by leveraging the Agno Agent Framework to analyze engineering and project management data sourced from GitHub and JIRA. The system employs an Extreme Task Decomposition strategy, where a central Orchestrator manages a team of highly specialized AI agents.

## Core Problem & Primary Goal

**Problem:** Understanding an organization's operational structure, especially in dynamic technology sectors, is complex. Traditional methods are often manual, static, and quickly outdated.

**Goal:** To develop an automated system capable of:
*   Analyzing raw data (commits, PRs, issues, comments) from GitHub and JIRA.
*   Deriving key organizational elements:
    *   Unique Persons (via identity resolution).
    *   Inferred Teams (from collaboration patterns).
    *   Products/Initiatives.
    *   Skills.
    *   Functional Roles.
*   Inferring relationships between these elements (MEMBER_OF, CONTRIBUTES_TO, etc.).
*   Constructing and populating a structured Object-Oriented Graph (OOG) in ArangoDB to serve as a dynamic semantic layer for organizational insights.

*(The current codebase primarily focuses on the data processing pipeline up to Identity Resolution.)*

## Architecture Overview

The system is built upon the **Agno Agent Framework** and uses a principle of **Extreme Task Decomposition**. A central `Orchestrator` workflow coordinates a series of specialized agents, each performing an atomic part of the overall task.

**Key Components:**
*   **Data Sources:** GitHub and JIRA data, initially stored in Snowflake and mirrored to a local DuckDB instance for development and testing.
*   **Knowledge Management Layer:** A ChromaDB vector database stores schema descriptions for GitHub and JIRA, enabling agents to plan data retrieval effectively using RAG.
*   **Agno Agent Framework Layer:** Multiple specialized and configurable Agno Agents managed by an `Orchestrator` workflow.
*   **Orchestration Logic:** Dictates the sequence, parallelism, data flow, and validation checks between agents.
*   **Structured Outputs:** Pydantic models define the data structures exchanged between agents, ensuring type safety and clear data contracts.
*   **Output Graph Database:** (Future) An ArangoDB instance will store the inferred OOG.

**High-Level Data Flow (Current Implementation):**
1.  **Orchestrator (`src/orchestrator.py`):** Initiates and manages the process.
2.  **KnowledgeBase Agent:** Retrieves relevant database schema information from ChromaDB based on a high-level query.
3.  **Planner Agent:** Analyzes the schema information and the data goal to create a detailed `SQLPlan` for data extraction.
4.  **SQL Constructor Agent:** Translates the `SQLPlan` into specific SQL query strings.
5.  **SQL Executor Agent(s):** The Orchestrator distributes SQL queries to instances of this agent, which execute them against the database (DuckDB in the current setup). Results are run in parallel.
6.  **Orchestrator (Aggregation):** Aggregates results from all SQL Executor Agents into a consolidated `AggregatedData` object.
7.  **Identity Agent:** Processes the aggregated GitHub and JIRA user data to resolve unique individuals, producing an `IdentityList`.

*(Further inference agents for teams, products, skills, and graph population are part of the project's broader scope as outlined in the planning documents.)*

## Key Technologies
*   **Python:** >=3.12
*   **Agno Agent Framework:** For building and orchestrating AI agents.
*   **LLMs:** OpenAI (GPT series), Google Gemini (planned).
*   **Vector Database:** ChromaDB for RAG with schema information.
*   **Data Storage/Querying:**
    *   Snowflake (primary enterprise source).
    *   DuckDB (local mirror for development/testing).
*   **Graph Database:** ArangoDB (for the final OOG - setup scripts provided).
*   **Pydantic:** For data validation and structured data exchange.
*   **SQLAlchemy:** For database interaction.
*   **Environment Management:** `python-dotenv`.
*   **Build/Dependency Management:** `uv` (mentioned in `pyproject.toml`).

## Directory Structure

```
p-breslin-zero-touch/
├── pyproject.toml            # Project metadata and dependencies
├── configs/                  # YAML configurations for agents, instructions, queries
├── dev/                      # Development utility scripts (e.g., SQL query tools)
├── models/                   # Pydantic models for data structures
├── scripts/                  # Setup scripts (ChromaDB, DuckDB, ArangoDB/KG)
├── services/                 # Database connection management, data download
├── src/                      # Main application source code
│   ├── orchestrator.py       # Core pipeline workflow
│   ├── paths.py              # Common path definitions
│   ├── test.py               # Test script for pipeline/agent execution
│   └── agents/               # Agent building logic
└── utils/                    # Helper utilities, logging setup
```

## Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone <repository-url>
    cd p-breslin-zero-touch
    ```

2.  **Python Version:**
    Ensure you have Python 3.12 or newer installed.

3.  **Install Dependencies:**
    It's recommended to use a virtual environment.
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```
    Using `uv` (as suggested by `pyproject.toml`):
    ```bash
    pip install uv
    uv pip install -e .
    ```
    Or using `pip`:
    ```bash
    pip install -e .
    ```

4.  **Environment Variables:**
    Create a `.env` file in the project root (`p-breslin-zero-touch/`) by copying `.env.example` (if one exists, otherwise create it from scratch). Fill in the necessary credentials and configurations:
    ```env
    # OpenAI
    OPENAI_API_KEY="sk-..."
    EMBEDDING_MODEL="text-embedding-3-large" # Or your preferred model

    # OpenRouter (Optional, if using OpenRouter provider)
    # OPENROUTER_API_KEY="sk-or-..."

    # ChromaDB
    CHROMADB_COLLECTION="zero_touch_schema_kb"

    # Snowflake (for data download and direct connection if not using DuckDB mirror)
    SNOWFLAKE_USER="your_user"
    SNOWFLAKE_PASSWORD="your_password"
    SNOWFLAKE_ACCOUNT="your_account_identifier" # e.g., xy12345.us-east-1
    SNOWFLAKE_DATABASE="MELTANO_DATABASE" # Or your specific database
    SNOWFLAKE_WAREHOUSE="your_warehouse"
    SNOWFLAKE_SCHEMA="XFLOW_DEV_GITHUB_" # Example schema for downloading, will be iterated

    # ArangoDB (for KG_setup.py)
    ARANGO_HOST="http://localhost:8529"
    ARANGO_PASSWORD="your_root_password"
    # ARANGO_DB is handled as a list in KG_setup.py (e.g., XFLOW_DEV_GITHUB_, XFLOW_DEV_JIRA_)
    ```

5.  **Database Setup:**

    *   **Snowflake Data Download (to CSVs for DuckDB):**
        The `services/snowflake_download.py` script can be used to export tables from Snowflake schemas into CSV files. These CSVs are then used by `scripts/duckdb_setup.py`.
        ```bash
        python services/snowflake_download.py XFLOW_DEV_GITHUB_
        python services/snowflake_download.py XFLOW_DEV_JIRA_
        # This will create data/snowflake_exports/XFLOW_DEV_GITHUB_/<table>.csv etc.
        ```

    *   **DuckDB Setup (Local Mirror):**
        This script ingests the CSV files (downloaded from Snowflake) into a local DuckDB database.
        ```bash
        python scripts/duckdb_setup.py
        # This creates/populates data/MELTANO_DATABASE.duckdb
        ```
        You can inspect the DuckDB database using `dev/sql_queries.py`:
        ```bash
        python dev/sql_queries.py table_names
        python dev/sql_queries.py column_headers
        python dev/sql_queries.py column_examples
        ```

    *   **ChromaDB Setup (Knowledge Base):**
        This script loads schema descriptions (expected as JSON files in `data/`) into ChromaDB.
        Ensure `data/XFLOW_DEV_GITHUB_.json` and `data/XFLOW_DEV_JIRA_.json` schema definition files exist. Their format is implied by `scripts/chromadb_setup.py`.
        ```bash
        python scripts/chromadb_setup.py setup
        ```
        To list collections and verify:
        ```bash
        python scripts/chromadb_setup.py list --show_columns
        ```

    *   **ArangoDB Setup (Graph Database Shell):**
        This script ensures the necessary databases exist in ArangoDB. It will delete and recreate them if they already exist.
        Ensure your ArangoDB instance is running and accessible.
        ```bash
        python scripts/KG_setup.py
        ```

## Running the Pipeline

The main orchestration logic is in `src/orchestrator.py`.

**Full Pipeline Run:**
    ```bash
    python src/orchestrator.py
    ```
    This will execute the entire implemented workflow: Knowledge Base -> Planner -> SQL Constructor -> SQL Executor -> Aggregation -> Identity Inference. Intermediate and final outputs are logged and may be saved to the `data/` directory (e.g., `KnowledgeBase_Agent.json`, `Planner_Agent.json`, `Data_Aggregation.json`, `Identity_Agent.json`).

## Configuration

The behavior of the agents and the overall pipeline is configured through YAML files in the `configs/` directory:
*   `configs/agents.yaml`: Defines the properties of each agent (model, provider, tools, response model, description, etc.).
*   `configs/instructions.yaml`: Contains detailed, role-specific instructions for each agent.
*   `configs/queries.yaml`: Stores templates for the initial queries or prompts fed to each agent.

## Models

Pydantic models defined in the `models/` directory are crucial for:
*   Structuring the data exchanged between agents.
*   Validating agent outputs.
*   Providing clear schemas for LLMs to populate.

Key models include: `KBInfo`, `SQLPlan`, `SQLQueries`, `AggregatedData`, `IdentityList`, etc.

## Future Work
Based on the project plan, future development will likely include:
*   Implementation of further Inference Agents (Team Inference, Product/Initiative Inference, Skills Inference, Role Inference).
*   Implementation of the Graph Agent to populate ArangoDB with the inferred entities and relationships.
*   Refinement of existing agent instructions and capabilities.
*   Enhanced error handling and retry mechanisms in the orchestrator.
*   More sophisticated caching and state management.

## Contributing
(Placeholder for contributing guidelines)

## License
(Placeholder for license information)