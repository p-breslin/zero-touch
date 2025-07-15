import json
import logging
import sys

from .poller import PollResult, wait_for

log = logging.getLogger(__name__)


def compute_metrics(client) -> str:
    """Kicks off metric computation and polls until complete.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.

    Returns:
        The external job ID for the metric compute.
    """
    log.info("Triggering metric compute...")
    try:
        resp = client.metric_compute()
    except Exception as e:
        log.error("Unexpected error calling compute API: %s", e)
        sys.exit(1)

    log.debug("Compute response:\n%s", json.dumps(resp, indent=2))
    job_id = resp.get("payload", {}).get("extJobId")
    if not job_id:
        log.error("No extJobId found in compute response")
        sys.exit(1)

    log.info("Metric compute job started (jobId=%s)", job_id)
    return job_id


def wait_for_compute_completion(
    client, job_id: str, interval: float, timeout: float
) -> dict:
    """
    Poll compute-summary until the compute job finishes.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.
        job_id: The external job ID returned by compute_metrics().
        interval (float): Seconds between status checks.
        timeout (float): Maximum seconds to wait before aborting.

    Returns:
        The final aggregation entry dict when compute is complete.
    """

    def poll_status() -> PollResult:
        try:
            summary = client.compute_summary(jobId=job_id)
        except Exception as e:
            log.error("Error calling compute-summary API: %s", e)
            sys.exit(1)

        payload = summary.get("payload", {})
        data_list = payload.get("data") or []
        if not data_list:
            log.info("No summary data yet; retrying...")
            return PollResult(done=False)

        aggregation = data_list[-1]
        status = aggregation.get("result_status")
        if status == "Completed":
            return PollResult(done=True, value=aggregation)

        log.info("Compute status = '%s'; retrying...", status)
        log.debug("Summary payload:\n%s", json.dumps(payload, indent=2))
        return PollResult(done=False)

    final = wait_for(
        poll_status,
        interval=interval,
        timeout=timeout,
        on_retry=lambda _: None,
        on_timeout=lambda elapsed: log.error(
            "Metric compute polling timed out after %.1f seconds", elapsed
        ),
    )

    total = final.get("total_jobs", "unknown")
    log.info("Metric compute completed. Total jobs: %s", total)
    return final


def fetch_compute_job_status(client) -> dict:
    """
    Fetch detailed compute job status list.

    :param client: Authenticated OnboardingApiClient instance.
    :returns: The payload dict from client.compute_job_status().
    """
    log.info("Fetching compute job status list...")
    try:
        resp = client.compute_job_status()
    except Exception as e:
        log.error("Unexpected error calling job-status API: %s", e)
        sys.exit(1)

    payload = resp.get("payload", {})
    log.debug("Compute job status payload:\n%s", json.dumps(payload, indent=2))
    return payload
