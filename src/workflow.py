import os
import logging
from typing import Any
from dotenv import load_dotenv

from agno.workflow import Workflow
from agno.agent import RunResponse
from agno.knowledge.agent import AgentKnowledge
from agno.embedder.openai import OpenAIEmbedder
from agno.vectordb.chroma.chromadb import ChromaDb

from src.paths import DATA_DIR
from utils.helpers import validate_response, load_yaml
from agents.agent_builder import build_agent
from utils.logging_setup import setup_logging
from services.create_engine import create_db_engine
from models.response_models import SQLPlan, SQLResults, IdentityList


load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


class Pipeline(Workflow):
    """
    A minimal workflow to orchestrate the identification of unique individuals from GitHub and JIRA user data.
    """

    knowledge_base: AgentKnowledge
    db_engine: Any

    def __init__(self, **workflow_kwargs: Any):
        super().__init__(**workflow_kwargs)

        # Set up ChromaDB source for the Knowledge Base
        db_path = str(DATA_DIR / "ChromaDB")
        embedder = OpenAIEmbedder(id=os.getenv("EMBEDDING_MODEL"), dimensions=3072)
        chromadb_collection = ChromaDb(
            collection=os.getenv("CHROMADB_COLLECTION"),
            path=db_path,
            persistent_client=True,
            embedder=embedder,
        )
        self.knowledge_base = AgentKnowledge(vector_db=chromadb_collection)

        # Set up the SQL database engine
        self.db_engine = create_db_engine()
        log.info("Workflow initialized")

    def create_agent(self, key, knowledge=False):
        kb = self.knowledge_base if knowledge else None
        agent = build_agent(
            agent_key=key,
            db_engine=self.db_engine,
            knowledge_base=kb,
        )
        log.info(f"Agent '{agent.name}' built.")
        return agent

    def process_response(self, response, model, agent_name):
        try:
            structured_content = validate_response(
                response.content, model, savefile=agent_name
            )
            log.info(f"{agent_name} response validated.")
            return structured_content
        except Exception as e:
            log.exception(f"Validation error: {response.content}: {e}")
            return None

    def run(self):
        """
        Executes the inference process.
        """

        queries = load_yaml("queries")

        # 1) Planner Agent
        planner_agent = self.create_agent("KnowledgeBase_Agent", knowledge=True)
        planner_resp: RunResponse = planner_agent.run(queries["planner_query"])

        if planner_resp and planner_resp.content:
            log.info(f"{planner_agent.name} finished. Validating response...")
            sql_plan = self.process_response(planner_resp, SQLPlan, planner_agent.name)
            if not sql_plan:
                log.error("Halting workflow: Response validation failed.")
                return
        else:
            log.critical(f"Halt: No response from {planner_agent.name}.")

        # 2) SQL Agent
        sql_agent = self.create_agent(key="SQL_Agent")
        sql_agent_query = queries["sql_query"].format(
            sql_plan=sql_plan.model_dump_json(indent=2)
        )
        sql_resp: RunResponse = sql_agent.run(sql_agent_query)

        if sql_resp and sql_resp.content:
            log.info(f"{sql_agent.name} finished. Validating response...")
            sql_results = self.process_response(sql_resp, SQLResults, sql_agent.name)
            if not sql_results:
                log.error("Halting workflow: Response validation failed.")
                return
        else:
            log.critical(f"Halt: No response from {sql_agent.name}.")

        # 3) Identity Agent
        identity_agent = self.create_agent(key="Identity_Agent")
        identity_agent_query = queries["identity_query"].format(
            sql_results=sql_results.model_dump_json(indent=2),
        )
        identity_resp: RunResponse = identity_agent.run(identity_agent_query)

        if identity_resp and identity_resp.content:
            log.info(f"{identity_agent.name} finished. Validating response...")
            identity_results = self.process_response(
                identity_resp, IdentityList, identity_agent.name
            )
            if not identity_results:
                log.error("Halting workflow: Response validation failed.")
                return
        else:
            log.critical(f"Halt: No response from {identity_agent.name}.")


if __name__ == "__main__":
    workflow = Pipeline()
    workflow.run()
