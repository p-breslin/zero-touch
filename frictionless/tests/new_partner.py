import json
import config
import logging
from utils.logger import setup_logging
from clients.onboarding_client import OnboardingApiClient

setup_logging()
log = logging.getLogger(__name__)


def partner_creation():
    """Runs the process of authenticating and creating a new partner."""
    log.info("Process to create a new partner initated.")

    # Initialize the API client with credentials from the config file
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )

    try:
        # Authenticate and retrieve the access token
        access_token = client.authenticate()
        log.info("Partner authenticated.")

        if not access_token:
            log.error("Could not retrieve partner access token.")
            return

        # Use the access token to create a new partner
        log.debug(
            f"Partner payload: {json.dumps(config.NEW_PARTNER_PAYLOAD, indent=2)}"
        )
        partner_creation_response = client.create_partner(
            access_token=access_token, partner_payload=config.NEW_PARTNER_PAYLOAD
        )
        log.info(f"Response from server\n: {partner_creation_response}")

    except Exception as e:
        log.error(f"\nAn error occurred during the partner creation: {e}")


if __name__ == "__main__":
    partner_creation()
