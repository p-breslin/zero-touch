import os
import sys
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

from jira import JIRA
from github import Github, Auth
from jira.exceptions import JIRAError
from github.GithubException import GithubException

load_dotenv()
log = logging.getLogger(__name__)


def get_jira_client():
    """
    Construct a JIRA client using environment variables:
      JIRA_SERVER: e.g. "https://your-domain.atlassian.net"
      JIRA_USERNAME:   your Atlassian account email
      JIRA_TOKEN:  a personal API token
    """
    server = os.getenv("JIRA_SERVER_URL")
    user = os.getenv("JIRA_USERNAME")
    token = os.getenv("JIRA_TOKEN")

    if not all([server, user, token]):
        sys.exit(
            "Error: Please set JIRA_SERVER_URL, JIRA_USERNAME, and JIRA_TOKEN environment variables."
        )

    try:
        return JIRA(server=server, basic_auth=(user, token))
    except JIRAError as e:
        sys.exit(f"Failed to connect to JIRA: {e}")


def jira_projects():
    """Retrieve all projects and print key and name."""
    try:
        jira_client = get_jira_client()
        projects = jira_client.projects()
    except JIRAError as e:
        sys.exit(f"Error fetching projects: {e}")

    if not projects:
        log.info("No projects found.")
        return

    log.info(f"Found {len(projects)} project(s):")
    keys = []
    for proj in projects:
        keys.append(proj.key)
        log.debug(f"  > {proj.key} - {proj.name}")
    return keys


def get_github_client():
    token = os.getenv("GITHUB_PERONAL_ACCESS_TOKEN")
    if not token:
        sys.exit(
            "Error: Please set GITHUB_TOKEN environment variable to a Personal Access Token with repo scope."
        )
    return Github(token)


def active_repos(DAYS_BACK):
    """
    Fetch and list all repositories for a user or organization that were active(i.e. received a push) within the last 90 days.
    """
    GH_TOKEN = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
    if not GH_TOKEN:
        sys.exit("Error: Please set GITHUB_PERSONAL_ACCESS_TOKEN environment variable.")
    G = Github(auth=Auth.Token(GH_TOKEN), per_page=100, retry=3)

    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    org_name = os.getenv("GITHUB_ORG_NAME")
    if not org_name:
        log.error("GITHUB_ORG_NAME environment variable is not set.")
        return []

    try:
        org = G.get_organization(org_name)
        repos = org.get_repos(type="sources")  # only “source” repos (no forks)
    except GithubException as exc:
        log.error("Error fetching repos from org %s: %s", org_name, exc)
        return []

    active = []
    for repo in repos:
        # repo.pushed_at is a datetime in UTC or None
        if repo.pushed_at and repo.pushed_at > cutoff:
            log.debug(f"> {repo.full_name} Last push: {repo.pushed_at.isoformat()}")
            active.append(repo.full_name)
    log.info(f"Found {len(active)} active repos in the last {DAYS_BACK} days.")
    return active
