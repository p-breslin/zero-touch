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
    try:
        if not response or not response.content:
            raise ValueError(f"{agent_name} returned no content")
        return validate_response(response.content, model, savefile=agent_name)
    except Exception as e:
        log.error(
            "%s validation failed: %s",
            agent_name,
            e,
            extra={"stage": agent_name},
        )
        raise


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
    # Helpers
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
            kb_agent = self._agent("KnowledgeBase_Agent", with_kb=True)
            kb_resp = await kb_agent.arun(queries["knowledgebase_query"])
            try:
                kb_info = _parse_response(kb_resp, KBInfo, kb_agent.name)
            except Exception:
                log.error("Skipping run: KB validation failed")
                return
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
            planner_query = queries["planner_query"].format(
                knowledgebase_info=kb_info.model_dump_json(indent=2)
            )

            plan_agent = self._agent("Planner_Agent")
            plan_resp = await plan_agent.arun(planner_query)
            try:
                sql_plan = _parse_response(plan_resp, SQLPlan, plan_agent.name)
            except Exception:
                log.error("Skipping run: Planner validation failed")
                return
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
            cons_query = queries["sql_constructor_query"].format(
                sql_plan_json=sql_plan.model_dump_json(indent=2)
            )

            cons_agent = self._agent("SQL_Constructor_Agent")
            cons_resp = await cons_agent.arun(cons_query)
            try:
                sql_queries = _parse_response(cons_resp, SQLQueries, cons_agent.name)
            except Exception:
                log.error("Skipping run: SQL Constructor validation failed")
                return
            self.session_state["sql_queries"] = sql_queries.model_dump()

        yield RunResponse(
            event="SQL_Constructor_Agent_complete",
            content="SQL built",
            run_id=self.run_id,
            session_id=self.session_id,
        )

        # 4. SQL Executor Agent (parallel) -------------------------------------
        executor_template = queries["sql_executor_query"]
        executor = self._agent("SQL_Executor_Agent")
        tasks: Sequence[Coroutine[Any, Any, SingleTableResult]] = [
            self._exec_sql(executor, sql_q, executor_template)
            for sql_q in sql_queries.queries
        ]

        # All results including those with empty rows but where task didn't fail
        raw_execution_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any failures
        failures = [e for e in raw_execution_results if isinstance(e, Exception)]
        if failures:
            log.error(
                "SQL_Executor_Agent encountered %d failures (continuing).",
                len(failures),
                extra={"stage": "SQL_Executor_Agent", "run_id": self.run_id},
            )

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
            plan_summary=sql_plan.plan_summary,
            strategy_notes=sql_plan.strategy_notes,
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
