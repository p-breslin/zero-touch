import json
import logging
import sys

log = logging.getLogger(__name__)


def publish_metrics(client, ext_batch_id: str) -> dict:
    """Publishes the computed metrics.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.
        ext_batch_id (str): The external job ID for the metric computation.
    """
    log.info("Publishing the metric computation...")
    try:
        resp = client.publish(ext_batch_id)
    except Exception as e:
        log.error("Unexpected error calling compute API: %s", e)
        sys.exit(1)

    payload = resp.get("payload", {})
    params = payload.get("params", {})

    vendor_id = params.get("vendorId", "unknown")
    tz = params.get("tz", {})
    db_id = params.get("dbId", "unknown")
    db_name = params.get("dbName", "unknown")

    log.info("Response message: %s", payload.get("message", "unknown"))
    log.info(
        "Publish summary: vendorId=%s, tz=%s, dbId=%s, dbName=%s",
        vendor_id,
        json.dumps(tz),
        db_id,
        db_name,
    )
    log.debug("Publish payload:\n%s", json.dumps(payload, indent=2))
    return payload
