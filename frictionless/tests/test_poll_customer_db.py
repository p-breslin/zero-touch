import json
import logging
import sys
import time

from clients.onboarding_client import OnboardingApiClient
from configs import cfg
from utils.logger import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def poll_db():
    """Polls the API for the database creation status for a new customer."""
    customer_email = cfg.NEW_CUSTOMER_PAYLOAD["email"]
    client = OnboardingApiClient(
        base_url=cfg.ONBOARDING_API_URL,
        email=cfg.ADMIN_EMAIL,
        password=cfg.ADMIN_PASSWORD,
    )

    try:
        # 1) Authenticate and generate the customer-scoped token
        log.info("Authenticating partner to generate a customer token..")
        client.authenticate()
        customer_token = client.generate_customer_token(customer_email)
        log.info(f"Successfully generated token for customer: {customer_email}")

        # 2) Start the polling loop
        start_time = time.time()
        timeout_seconds = cfg.TIMEOUT_MINUTES * 60

        while True:
            # Check for timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout_seconds:
                log.error(
                    f"Timeout of {cfg.TIMEOUT_MINUTES} minutes exceeded. Halting."
                )
                raise TimeoutError("Database creation did not complete in time.")

            # Call the check-db API
            status_response = client.check_db_status(customer_token)
            log.debug("Poll response:\n" + json.dumps(status_response, indent=2))

            # A successful response contains a 'db_exists' key
            if status_response["payload"].get("db_exists"):
                log.info("Success! Database has been created.")
                log.debug(
                    "DB Details: " + json.dumps(status_response["payload"], indent=2)
                )
                break

            log.info(
                f"DB not ready yet (Message: '{status_response.get('payload')}'). "
                f"Waiting {cfg.POLLING_INTERVAL_SECONDS} seconds..."
            )
            time.sleep(cfg.POLLING_INTERVAL_SECONDS)

        log.info("--- Database is ready for data upload. ---")

    except Exception as e:
        log.error(f"An error occurred during database polling: {e}")
        sys.exit(1)


if __name__ == "__main__":
    poll_db()
