import json
import logging

log = logging.getLogger(__name__)


def set_product(client, payload: dict) -> dict:
    """Sets a product for the customer via the Onboarding API.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.
        payload (dict): Dict matching the SET_PRODUCT_PAYLOAD schema.

    Returns:
        The JSON response from client.set_product().
    """
    log.debug("Set-product payload:\n%s", json.dumps(payload, indent=2))
    log.info("Setting product '%s'...", payload.get("product_name"))
    resp = client.set_product(payload)
    log.debug("Set-product response:\n%s", json.dumps(resp, indent=2))
    log.info("Product set: %s", payload.get("product_name"))
    return resp


def set_package(client, payload: dict) -> dict:
    """Sets a package for the customer via the Onboarding API.

    Args:
        client: Authenticated OnboardingApiClient instance.
        payload: Dict matching the SET_PACKAGE_PAYLOAD schema.

    Returns:
        The JSON response from client.set_package().
    """
    log.debug("Set-package payload:\n%s", json.dumps(payload, indent=2))
    log.info("Setting package '%s'...", payload.get("packageId"))
    resp = client.set_package(payload)
    log.debug("Set-package response:\n%s", json.dumps(resp, indent=2))
    log.info("Package set: %s", payload.get("packageId"))
    return resp
