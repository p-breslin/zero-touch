import json
import logging

from clients.mysql_client import mysql_cursor

from .poller import PollResult

log = logging.getLogger(__name__)


def create_customer(client, payload: dict) -> dict:
    """Creates a new customer via the Onboarding API.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.
        payload (dict): Dict matching the NEW_CUSTOMER_PAYLOAD schema.
    """
    log.debug("Customer payload:\n%s", json.dumps(payload, indent=2))
    log.info("Creating customer...")
    resp = client.create_customer(payload)
    log.debug("Customer creation response:\n%s", json.dumps(resp, indent=2))

    summary = {
        "Company": resp.get("company_name"),
        "Industry": resp.get("industry_name"),
        "Email": resp.get("email"),
    }
    log.info("Customer created:\n%s", json.dumps(summary, indent=2))


def poll_customer_db(client) -> PollResult:
    """
    PollResult that returns done=True once the customer's DB exists.

    Args:
        client (OnboardingApiClient): Authenticated API client.

    Returns:
        PollResult: done=True when payload['db_exists'] is truthy, else done=False.
    """
    status = client.check_db_status()
    payload = status.get("payload", {}) or {}
    if payload.get("db_exists"):
        return PollResult(done=True, value=payload)
    log.debug("DB not ready yet, will retry.")
    return PollResult(done=False)


def generate_customer_token(client, email: str) -> None:
    """Generates and caches a customer token for subsequent calls.

    Args:
        client: Authenticated OnboardingApiClient instance.
        email: The email address of the customer.
    """
    log.info("Generating token for customer %s...", email)
    client.generate_customer_token(email)
    log.debug("Customer token obtained for %s", email)


def delete_customer(client, email: str) -> bool:
    """Deletes a customer via a direct MySQL call (no API available for this).

    Args:
        client: Authenticated OnboardingApiClient instance.
        email: The email address of the customer to delete.

    Returns:
        True if deletion succeeded; False otherwise.
    """
    log.info("Looking up customer '%s' for deletion...", email)
    customers = client.list_customers()
    record = next((c for c in customers if c.get("email") == email), None)

    if not record:
        log.error("Cannot find customer with email %s", email)
        return False

    customer_id = record.get("user_id")
    log.info("Deleting customer record with ID %s from MySQL...", customer_id)
    with mysql_cursor() as cursor:
        cursor.execute("DELETE FROM `core_user` WHERE `id` = %s", (customer_id,))
        affected = cursor.rowcount

    if affected:
        log.info("Successfully deleted %d row(s) for customer %s", affected, email)
        return True
    else:
        log.warning("No rows deleted for customer %s (ID %s)", email, customer_id)
        return False
