import json
import logging
import os
from datetime import datetime, timedelta, timezone

from clients.onboarding_client import OnboardingApiClient
from configs import cfg
from utils.data_source_definition import active_repos, jira_projects
from utils.logger import setup_logging

setup_logging(level=2)
log = logging.getLogger(__name__)

DAYS_BACK = 90
start_time_obj = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
start_time_str = start_time_obj.strftime("%Y-%m-%dT%H:%M:%SZ")


def connect_sources():
    """Runs the full sequence of connecting both GitHub and Jira data sources."""
    client = OnboardingApiClient(
        base_url=cfg.ONBOARDING_API_URL,
        email=cfg.ADMIN_EMAIL,
        password=cfg.ADMIN_PASSWORD,
    )
    client.authenticate()

    try:
        # Generate the Customer-scoped token
        CUSTOMER_EMAIL = cfg.NEW_CUSTOMER_PAYLOAD["email"]
        client.generate_customer_token(CUSTOMER_EMAIL)

        # Connect GitHub (2-step: Auth then cfg)
        log.info("\n[CONNECTING GITHUB - STEP 1/2: STORING PAT]")
        repos = active_repos(DAYS_BACK)
        GITHUB_CONNECT_PAYLOAD = {
            "source_name": "GitHub",
            "source_cfg": {
                "repository": " ".join(repos),
                "start_date": start_time_str,
            },
        }

        gh_pat_resp = client.store_github_pat(os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN"))
        log.info("GitHub connect response:\n" + json.dumps(gh_pat_resp, indent=2))

        log.info("\n[CONNECTING GITHUB - STEP 2/2: SENDING cfg]")
        gh_connect_resp = client.connect_data_source(GITHUB_CONNECT_PAYLOAD)
        log.info("GitHub Connect response:\n" + json.dumps(gh_connect_resp, indent=2))

        # Connect Jira (1-step: Combined Auth/cfg)
        log.info("\n[CONNECTING]: Jira")
        project_keys = jira_projects()

        JIRA_CONNECT_PAYLOAD = {
            "source_name": "Jira",
            "source_cfg": {
                "email": os.getenv("JIRA_USERNAME"),
                "domain": os.getenv("JIRA_SERVER_URL"),
                "projects": project_keys,
                "api_token": os.getenv("JIRA_TOKEN"),
                "start_date": start_time_str,
            },
        }
        jira_resp = client.connect_data_source(JIRA_CONNECT_PAYLOAD)
        log.info("Jira connect response:\n" + json.dumps(jira_resp, indent=2))

    except Exception as e:
        log.error(f"\n[CRITICAL ERROR]: {e}")


if __name__ == "__main__":
    connect_sources()
