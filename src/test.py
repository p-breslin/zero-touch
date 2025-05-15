import os
import json
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Coroutine, AsyncIterator, Sequence

from dotenv import load_dotenv

from agno.workflow import Workflow
from agno.agent import Agent, RunResponse
from agno.knowledge.agent import AgentKnowledge
from agno.embedder.openai import OpenAIEmbedder
from agno.vectordb.chroma.chromadb import ChromaDb

from src.paths import DATA_DIR
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging
from services.create_engine import create_db_engine
from utils.helpers import validate_response, load_yaml
from models import (
    KBInfo,
    SQLPlan,
    SQLQueries,
    SQLQuery,
    SingleTableResult,
    SQLResults,
    AggregatedData,
)

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Helper utilities
# ------------------------------------------------------------------------------


def _rows_from(content: Any) -> List[Dict[str, Any]]:
    """
    Normalises the SQL-tool output to a list[dict].
    Extracts a list of dictionaries (representing database rows) from the content field of an SQL_Executor_Agent's RunResponse.
    """
    if content is None:
        return []
    if isinstance(content, list):
        return content if all(isinstance(row, dict) for row in content) else []
    if isinstance(content, str):
        try:
            parsed = json.loads(content)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _parse_response(response: RunResponse | None, model: Any, agent_name: str):
    if not response or not response.content:
        raise ValueError(f"{agent_name} returned no content")
    return validate_response(response.content, model, savefile=agent_name)


# ------------------------------------------------------------------------------
# Main workflow
# ------------------------------------------------------------------------------


class Pipeline(Workflow):
    """Identity-inference orchestrator with leaner plumbing."""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._init_kb()
        self.db_engine = create_db_engine()
        log.info("Pipeline ready - session %s", self.session_id)

    # ------------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------------

    def _init_kb(self) -> None:
        db_path = Path(DATA_DIR, "ChromaDB")
        embedder = OpenAIEmbedder(
            id=os.getenv("EMBEDDING_MODEL", "text-embedding-3-large"),
            api_key=os.getenv("OPENAI_API_KEY"),
            dimensions=3072,
        )
        self.knowledge_base = AgentKnowledge(
            vector_db=ChromaDb(
                collection=os.getenv("CHROMADB_COLLECTION"),
                path=str(db_path),
                persistent_client=True,
                embedder=embedder,
            ),
            num_documents=10,
        )

    def _agent(self, key: str, *, with_kb: bool = False) -> Agent:
        return build_agent(
            agent_key=key,
            db_engine=self.db_engine,
            knowledge_base=self.knowledge_base if with_kb else None,
        )

    async def _exec_sql(
        self, agent: Agent, q: SQLQuery, template: str
    ) -> SingleTableResult:
        sql = template.format(
            sql_query_json=json.dumps({"sql_to_execute": q.get("sql_string")})
        )
        resp = await agent.arun(sql)

        # _rows_from will normalize the output to List[Dict[str, Any]]
        rows = _rows_from(resp.content)
        log.info(
            f"Fetched {len(rows)} rows for table '{q.get('table_name')}' (Platform: {q.get('platform')})."
        )
        return SingleTableResult(
            platform=q.get("platform"), table_name=q.get("table_name"), rows=rows
        )

    # --------------------------------------------------------------------------
    # Workflow entry‑point
    # --------------------------------------------------------------------------

    async def arun(self) -> AsyncIterator[RunResponse]:
        # Load queries and saved outputs
        queries = load_yaml("queries")
        output_path = str(DATA_DIR / "output")

        # 1. Knowledge Base Agent ----------------------------------------------
        log.info("Loading output from KnowledgeBase_Agent...")
        with open(f"{output_path}/KnowledgeBase_Agent.json", "r") as f:
            schema_info = json.load(f)
        self.session_state["knowledgebase_info"] = json.dumps(schema_info, indent=2)

        # 2. Planner Agent -----------------------------------------------------
        log.info("Loading output from Planner_Agent...")
        with open(f"{output_path}/Planner_Agent.json", "r") as f:
            sql_plan = json.load(f)
        self.session_state["sql_plan"] = json.dumps(sql_plan, indent=2)

        # 3. SQL Constructor Agent ---------------------------------------------
        log.info("Loading output from SQL_Constructor_Agent...")
        with open(f"{output_path}/SQL_Constructor_Agent.json", "r") as f:
            sql_queries = json.load(f)
        self.session_state["sql_queries"] = json.dumps(sql_queries, indent=2)

        # 4. SQL Execution Agent (parallel) ------------------------------------
        executor_template = queries["sql_executor_query"]
        executor = self._agent("SQL_Executor_Agent")
        tasks: Sequence[Coroutine[Any, Any, SingleTableResult]] = [
            self._exec_sql(executor, sql_q, executor_template)
            for sql_q in sql_queries.get("queries")
        ]

        # All results including those with empty rows but where task didn't fail
        raw_execution_results = await asyncio.gather(*tasks)
        table_results: List[SingleTableResult] = [
            res for res in raw_execution_results if res is not None
        ]

        self.session_state["individual_sql_execution_results"] = [
            t.model_dump() for t in table_results
        ]
        yield RunResponse(
            event="SQL_Executor_Agent_complete",
            content=f"{len(table_results)} tables fetched",
            run_id=self.run_id,
            session_id=self.session_id,
        )

        # 5. Aggregation (Orchestrator logic) ----------------------------------
        github_master_list: List[Dict[str, Any]] = []
        jira_master_list: List[Dict[str, Any]] = []

        # Iterate over the successfully processed objects
        for single_table_result in table_results:
            if single_table_result.platform == "github":
                github_master_list.extend(single_table_result.rows)
            elif single_table_result.platform == "jira":
                jira_master_list.extend(single_table_result.rows)

        sql_results = SQLResults(
            results={"github": github_master_list, "jira": jira_master_list}
        )

        aggregated_data = AggregatedData(
            sql_results=sql_results,
            plan_summary=sql_plan.get("plan_summary"),
            strategy_notes=sql_plan.get("strategy_notes"),
        )

        self.session_state["aggregated_data"] = aggregated_data.model_dump()
        log.info("Data aggregation complete.")

        yield RunResponse(
            event="Data_Aggregation_complete",
            content=f"Data aggregated. {len(github_master_list)} GitHub rows and {len(jira_master_list)} JIRA rows accumulated.",
            run_id=self.run_id,
            session_id=self.session_id,
        )


# ------------------------------------------------------------------------------
# Runner
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    from agno.storage.sqlite import SqliteStorage
    from agno.utils.pprint import pprint_run_response

    storage = SqliteStorage(
        table_name="orchestrator_sessions",
        db_file=str(DATA_DIR / "orchestrator_main_session_storage.db"),
        auto_upgrade_schema=True,
    )

    pipeline = Pipeline(
        name="MainOrchestrator",
        session_id="fixed_test_session_id",
        storage=storage,
        debug_mode=True,
    )

    async def _main():
        chunks = []
        final_event_reached = "workflow_started"
        async for chunk in pipeline.arun():
            chunks.append(chunk)
            pprint_run_response(chunk, markdown=False)
            final_event_reached = chunk.event

            if "failed" in (chunk.event or "").lower():
                log.error(f"Workflow failed after: {final_event_reached}")
                break

            elif final_event_reached == "Data_Aggregation_complete":
                data = pipeline.session_state.get("aggregated_data")
                output_path = Path(DATA_DIR / "Data_Aggregation.json")

                with open(output_path, "w") as f:
                    json.dump(data, f, indent=4)
                    log.info(f"Saved structured output to {output_path}")

            else:
                log.warning(
                    f"Workflow completed but last event was: {final_event_reached}"
                )

    asyncio.run(_main())
