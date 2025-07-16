import logging
from typing import Any, Optional

from clients.onboarding_client import OnboardingApiClient
from configs import cfg

log = logging.getLogger(__name__)


def authenticate(config: Optional[Any] = None):
    """Authenticates against the Onboarding API.

    Args:
        config (Optional[Any]): Optional configuration module or object. If not provided, the top-level `cfg` module is used.

    Returns:
        An instance of OnboardingApiClient with a valid session.
    """
    config = config or cfg

    client = OnboardingApiClient(
        base_url=cfg.ONBOARDING_API_URL,
        email=cfg.ADMIN_EMAIL,
        password=cfg.ADMIN_PASSWORD,
    )

    log.info("Authenticating partner...")
    client.authenticate()
    log.debug("Authentication successful for %s", cfg.ADMIN_EMAIL)

    return client
