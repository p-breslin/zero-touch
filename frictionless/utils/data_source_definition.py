import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from github import Auth, Github
from github.GithubException import GithubException
from jira import JIRA
from jira.exceptions import JIRAError

load_dotenv()
log = logging.getLogger(__name__)


def get_jira_client():
    """Constructs and returns a JIRA client using credentials from env variables.

    Required Environment Variables:
        JIRA_SERVER_URL (str): Base URL of your JIRA instance.
        JIRA_USERNAME (str): Atlassian account email.
        JIRA_TOKEN (str): Personal API token for authentication.

    Returns:
        JIRA: An authenticated JIRA client instance.
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
    """Fetches and logs all JIRA projects accessible to the user.

    Returns:
        list[str]: A list of JIRA project keys.
    """
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
    """Returns an authenticated GitHub client using a personal access token.

    Required Environment Variable:
        GITHUB_PERSONAL_ACCESS_TOKEN (str): A GitHub token with at least `repo` scope.

    Returns:
        Github: An authenticated GitHub client.
    """
    token = os.getenv("GITHUB_PERONAL_ACCESS_TOKEN")
    if not token:
        sys.exit(
            "Error: Please set GITHUB_TOKEN environment variable to a Personal Access Token with repo scope."
        )
    return Github(token)


def active_repos(DAYS_BACK):
    """Lists all GitHub repos in the org that had commits pushed within the last N days.

    Args:
        DAYS_BACK (int): Number of days to look back from today.

    Environment Variables:
        GITHUB_PERSONAL_ACCESS_TOKEN (str): GitHub token with org access.
        GITHUB_ORG_NAME (str): The organization name to scan.

    Returns:
        list[str]: List of active repo full names (e.g., "org/repo").
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
