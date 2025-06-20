import json
import config
import logging
from utils.logger import setup_logging
from clients.onboarding_client import OnboardingApiClient

setup_logging()
log = logging.getLogger(__name__)


def customer_creation():
    """Runs the process of authenticating and creating a new customer."""
    log.info("Creating customer and model")

    # Initialize the API client with credentials from the config file
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )

    try:
        # Authenticate and cache the access token
        client.authenticate()

        # Create a new customer
        log.debug(
            f"Customer payload: {json.dumps(config.NEW_CUSTOMER_PAYLOAD, indent=2)}"
        )
        customer_creation_response = client.create_customer(
            customer_payload=config.NEW_CUSTOMER_PAYLOAD
        )
        log.info(f"Customer created for {config.NEW_CUSTOMER_PAYLOAD['email']}")
        log.debug(f"Response from server\n: {customer_creation_response}")

    except Exception as e:
        log.error(f"\nAn error occurred during the customer creation: {e}")


if __name__ == "__main__":
    customer_creation()
