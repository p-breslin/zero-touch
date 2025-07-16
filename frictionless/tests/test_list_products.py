import json
import logging
import sys

from clients.onboarding_client import OnboardingApiClient
from configs import cfg
from utils.logger import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def fetch_products():
    """Lists the available Products."""
    customer_email = cfg.NEW_CUSTOMER_PAYLOAD["email"]
    client = OnboardingApiClient(
        base_url=cfg.ONBOARDING_API_URL,
        email=cfg.ADMIN_EMAIL,
        password=cfg.ADMIN_PASSWORD,
    )

    try:
        # Authenticate as Partner
        log.info("Authenticating partner to generate a customer token..")
        client.authenticate()

        # Generate a token scoped to the newly created customer
        log.info(f"Generating token for customer: {customer_email}")
        customer_token = client.generate_customer_token(customer_email)
        log.info("Successfully generated customer-scoped token.")

        # List available products
        available_products = client.list_products(customer_token)
        log.info("Available Products:\n" + json.dumps(available_products, indent=2))

    except Exception as e:
        log.error(f"Error during database initialization flow: {e}")
        sys.exit(1)


if __name__ == "__main__":
    fetch_products()
