import json
import config
import logging
from utils.logger import setup_logging
from clients.onboarding_client import OnboardingApiClient

setup_logging()
log = logging.getLogger(__name__)


def fetch_models():
    """Fetches available models for customer onboarding."""
    log.info("Initiating model discovery")

    try:
        # Initialize the client
        client = OnboardingApiClient(
            base_url=config.ONBOARDING_API_URL,
            email=config.ADMIN_EMAIL,
            password=config.ADMIN_PASSWORD,
        )

        # Authenticate
        client.authenticate()

        # Get the list of all available model templates
        models = client.list_industries()

        log.debug("\n--- Available Industry Categories (Model Templates) ---")
        log.debug(json.dumps(models, indent=2))

        model_tuples = {}
        for model in models:
            model_tuples[model["id"]] = model["categoryName"]
        print(json.dumps(model_tuples, indent=2))

    except Exception as e:
        log.error(f"An error occurred during the model discovery process: {e}")


if __name__ == "__main__":
    fetch_models()
