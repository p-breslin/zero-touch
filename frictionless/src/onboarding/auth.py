import logging
from typing import Any, Optional

import config
from clients.onboarding_client import OnboardingApiClient

log = logging.getLogger(__name__)


def authenticate(cfg: Optional[Any] = None):
    """Authenticates against the Onboarding API.

    Args:
        cfg (Optional[Any]): Optional configuration module or object. If not provided, the top-level `config` module is used.

    Returns:
        An instance of OnboardingApiClient with a valid session.
    """
    cfg = cfg or config

    client = OnboardingApiClient(
        base_url=cfg.ONBOARDING_API_URL,
        email=cfg.ADMIN_EMAIL,
        password=cfg.ADMIN_PASSWORD,
    )

    log.info("Authenticating partner...")
    client.authenticate()
    log.debug("Authentication successful for %s", cfg.ADMIN_EMAIL)

    return client
