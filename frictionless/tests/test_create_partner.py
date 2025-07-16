import json
import logging

from clients.onboarding_client import OnboardingApiClient
from configs import cfg
from utils.logger import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def partner_creation():
    """Runs the process of authenticating and creating a new partner."""
    log.info("Process to create a new partner initated.")

    # Initialize the API client with credentials from the cfg file
    client = OnboardingApiClient(
        base_url=cfg.ONBOARDING_API_URL,
        email=cfg.ADMIN_EMAIL,
        password=cfg.ADMIN_PASSWORD,
    )

    try:
        # Authenticate and retrieve the access token
        client.authenticate()
        log.info("Partner authenticated.")

        # Use the access token to create a new partner
        log.debug(f"Partner payload: {json.dumps(cfg.NEW_PARTNER_PAYLOAD, indent=2)}")
        resp = client.create_partner(partner_payload=cfg.NEW_PARTNER_PAYLOAD)
        log.info(f"Response from server\n: {resp}")

    except Exception as e:
        log.error(f"\nAn error occurred during the partner creation: {e}")


if __name__ == "__main__":
    partner_creation()
