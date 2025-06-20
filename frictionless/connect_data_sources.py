import os
import json
import config
import logging
from utils.logger import setup_logging
from datetime import datetime, timedelta, timezone
from clients.onboarding_client import OnboardingApiClient
from data_source_definition import jira_projects, active_repos

setup_logging()
log = logging.getLogger(__name__)

DAYS_BACK = 90
start_time_obj = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
start_time_str = start_time_obj.strftime("%Y-%m-%dT%H:%M:%SZ")


def connect_sources():
    """
    Runs the full sequence of connecting both GitHub and Jira data sources.
    """
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )

    try:
        # Authenticate as the Partner
        partner_token = client.authenticate()

        # Generate the Customer-scoped token
        CUSTOMER_EMAIL = config.NEW_CUSTOMER_PAYLOAD["email"]
        customer_token = client.generate_customer_token(partner_token, CUSTOMER_EMAIL)
        if not customer_token:
            raise Exception("Failed to get customer-scoped token.")

        # Connect GitHub (2-step: Auth then Config)
        log.info("\n[CONNECTING GITHUB - STEP 1/2: STORING PAT]")
        repos = active_repos(DAYS_BACK)
        GITHUB_CONNECT_PAYLOAD = {
            "source_name": "GitHub",
            "source_config": {
                "repository": " ".join(repos),
                "start_date": start_time_str,
            },
        }

        gh_pat_resp = client.store_github_pat(
            customer_token, os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
        )
        log.info("GitHub connect response:\n" + json.dumps(gh_pat_resp, indent=2))

        log.info("\n[CONNECTING GITHUB - STEP 2/2: SENDING CONFIG]")
        gh_connect_resp = client.connect_data_source(
            customer_token, GITHUB_CONNECT_PAYLOAD
        )
        log.info("GitHub Connect response:\n" + json.dumps(gh_connect_resp, indent=2))

        # Connect Jira (1-step: Combined Auth/Config)
        log.info("\n[CONNECTING]: Jira")
        project_keys = jira_projects()

        JIRA_CONNECT_PAYLOAD = {
            "source_name": "Jira",
            "source_config": {
                "email": os.getenv("JIRA_USERNAME"),
                "domain": os.getenv("JIRA_SERVER_URL"),
                "projects": project_keys,
                "api_token": os.getenv("JIRA_TOKEN"),
                "start_date": start_time_str,
            },
        }
        jira_resp = client.connect_data_source(customer_token, JIRA_CONNECT_PAYLOAD)
        log.info("Jira connect response:\n" + json.dumps(jira_resp, indent=2))

    except Exception as e:
        log.error(f"\n[CRITICAL ERROR]: {e}")


if __name__ == "__main__":
    connect_sources()
