import os
import json
import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Coroutine, AsyncIterator, Sequence

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
    AggregatorInput,
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
            sql_query_json=json.dumps({"sql_to_execute": q.sql_string})
        )
        resp = await agent.arun(sql)

        # _rows_from will normalize the output to List[Dict[str, Any]]
        rows = _rows_from(resp.content)
        log.info(
            f"Fetched {len(rows)} rows for table '{q.table_name}' (Platform: {q.platform})."
        )
        return SingleTableResult(
            platform=q.platform, table_name=q.table_name, rows=rows
        )

    # --------------------------------------------------------------------------
    # Workflow entry‑point
    # --------------------------------------------------------------------------

    async def arun(self) -> AsyncIterator[RunResponse]:
        queries = load_yaml("queries")

        # 1. Knowledge Base Agent ----------------------------------------------
        kb_info = (
            KBInfo.model_validate(self.session_state["knowledgebase_info"])
            if "knowledgebase_info" in self.session_state
            else None
        )
        if not kb_info:
            kb_resp = await self._agent("KnowledgeBase_Agent", with_kb=True).arun(
                queries["knowledgebase_query"]
            )
            kb_info = _parse_response(kb_resp, KBInfo, "KnowledgeBase_Agent")
            self.session_state["knowledgebase_info"] = kb_info.model_dump()
        yield RunResponse(
            event="KnowledgeBase_Agent_complete",
            content="KB ready",
            run_id=self.run_id,
            session_id=self.session_id,
        )

        # 2. Planner Agent -----------------------------------------------------
        sql_plan = (
            SQLPlan.model_validate(self.session_state["sql_plan"])
            if "sql_plan" in self.session_state
            else None
        )
        if not sql_plan:
            plan_query = queries["planner_query"].format(
                knowledgebase_info=kb_info.model_dump_json(indent=2)
            )
            plan_resp = await self._agent("Planner_Agent").arun(plan_query)
            sql_plan = _parse_response(plan_resp, SQLPlan, "Planner_Agent")
            self.session_state["sql_plan"] = sql_plan.model_dump()
        yield RunResponse(
            event="Planner_Agent_complete",
            content="Plan ready",
            run_id=self.run_id,
            session_id=self.session_id,
        )

        # 3. SQL Constructor Agent ---------------------------------------------
        sql_queries = (
            SQLQueries.model_validate(self.session_state["sql_queries"])
            if "sql_queries" in self.session_state
            else None
        )
        if not sql_queries:
            constructor_q = queries["sql_constructor_query"].format(
                sql_plan_json=sql_plan.model_dump_json(indent=2)
            )
            cons_resp = await self._agent("SQL_Constructor_Agent").arun(constructor_q)
            sql_queries = _parse_response(
                cons_resp, SQLQueries, "SQL_Constructor_Agent"
            )
            self.session_state["sql_queries"] = sql_queries.model_dump()
        yield RunResponse(
            event="SQL_Constructor_Agent_complete",
            content="SQL built",
            run_id=self.run_id,
            session_id=self.session_id,
        )

        # 4. SQL Execution Agent (parallel) ------------------------------------
        executor_template = queries["sql_executor_query"]
        executor = self._agent("SQL_Executor_Agent")
        tasks: Sequence[Coroutine[Any, Any, SingleTableResult]] = [
            self._exec_sql(executor, sql_q, executor_template)
            for sql_q in sql_queries.queries
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
            event="SQL_Execution_Agent_complete",
            content=f"{len(table_results)} tables fetched",
            run_id=self.run_id,
            session_id=self.session_id,
        )

        # 5. Aggregation Agent -------------------------------------------------
        agg_query = queries["aggregator_query"].format(
            aggregator_input_json=AggregatorInput(
                table_results=table_results
            ).model_dump_json(indent=2)
        )
        agg_agent = self._agent("Aggregator_Agent")
        agg_resp = await agg_agent.arun(agg_query)
        agg_results: Optional[AggregatedData] = _parse_response(
            agg_resp, AggregatedData, agg_agent.name
        )
        self.session_state["aggregated_data"] = agg_results.model_dump()
        log.info("Data aggregation complete.")

        yield RunResponse(
            event="Aggregation_Agent_complete",
            content="Aggregation done",
            run_id=self.run_id,
            session_id=self.session_id,
        )


# ------------------------------------------------------------------------------
# Ad‑hoc test runner (optional)
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
        async for chunk in pipeline.arun():
            pprint_run_response(chunk, markdown=False)

    asyncio.run(_main())
