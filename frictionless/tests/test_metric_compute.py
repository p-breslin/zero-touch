import json
import logging
import sys
import time

from dotenv import load_dotenv

import config
from clients.onboarding_client import OnboardingApiClient
from utils.logger import setup_logging

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


def main():
    # Initialize and authenticate
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )
    client.authenticate()
    client.generate_customer_token(customer_email=config.NEW_PARTNER_PAYLOAD["email"])

    try:
        resp = client.metric_compute()
    except Exception as e:
        log.error(f"Unexpected error calling compute-values API: {e}")
        sys.exit(1)
    log.debug(json.dumps(resp, indent=2))
    job_id = resp["payload"]["extJobId"]

    # Poll compute status
    start = time.time()
    timeout = config.TIMEOUT_SECONDS * 2
    interval = config.POLLING_INTERVAL_SECONDS
    log.info(f"Polling for metric compute completion (jobId = '{job_id}')...")

    while True:
        elapsed = time.time() - start
        if elapsed > timeout:
            log.warning(f"Timed out after {timeout / 60} minutes.")
            sys.exit(1)

        try:
            summary = client.compute_summary(jobId=job_id)
        except Exception as e:
            log.error(f"Unexpected error calling compute-summary API: {e}")
            sys.exit(1)

        payload = summary.get("payload", {})
        data_list = payload.get("data") or []
        if not data_list:
            log.warning("compute-summary returned no data; retrying...")
            time.sleep(interval)
            continue

        # Aggregation status is the final element in the data list
        aggregation = data_list[-1]
        status = aggregation.get("result_status")
        if status == "Completed":
            total = aggregation.get("total_jobs", "unknown")
            log.info(
                f"Metric compute completed in {elapsed:.1f}s. Total jobs: {total}."
            )
            break

        # Compute still in progress
        log.info(f"Current status = '{status}'; retrying in {interval}s...")
        log.debug("Poll response payload:\n" + json.dumps(payload, indent=2))
        time.sleep(interval)

    # Metric compute details
    try:
        job_status = client.compute_job_status()
    except Exception as e:
        log.error(f"Unexpected error calling listComputeJobStatus API: {e}")
        sys.exit(1)

    payload = job_status.get("payload", {})
    print(json.dumps(payload, indent=2))
    log.info("Metric computation complete.")


if __name__ == "__main__":
    main()
