import json
import logging
import sys

import requests

from .poller import PollResult, wait_for

log = logging.getLogger(__name__)


def upload_and_wait(
    client, file_info: dict, base_path: str, interval: float, timeout: float
) -> dict:
    """Uploads a single file via the Onboarding API and polls until it's processed.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.
        file_info (dict): Dict with keys 'file', 'filetype', and 'description'.
        base_path (str): Filesystem path prefix where files live.
        interval (float): Seconds between poll attempts.
        timeout (float): Maximum seconds to wait before aborting.

    Returns:
        The final status entry dict when processing is complete.
    """
    filename = file_info["file"]
    log.info("Starting upload for %s", filename)

    # 1) Kick off the upload
    try:
        with open(f"{base_path}{filename}", "rb") as fp:
            files = {"file1": (filename, fp, file_info["filetype"])}
            metadata = {
                "description": file_info["description"],
                "fileCount": "1",
            }
            client.file_upload(files=files, metadata=metadata)
            log.info("Upload in process: %s", filename)
    except requests.HTTPError as e:
        log.error("Upload failed: HTTP %s\n%s", e.response.status_code, e.response.text)
        sys.exit(1)

    # 2) Poll for completion
    def predicate_fn() -> PollResult:
        try:
            status = client.file_upload_status()
        except requests.HTTPError as e:
            code = e.response.status_code
            body = e.response.text
            try:
                err_msg = e.response.json().get("error", "")
            except ValueError:
                err_msg = body

            # If metrics aren't ready yet, retry
            if code == 500 and "Please wait" in err_msg:
                log.info("Metrics not ready yet; retrying...")
                return PollResult(done=False)

            # Any other error is fatal
            log.error("Status check failed: HTTP %s\n%s", code, err_msg)
            sys.exit(1)

        entries = status.get("data") or []
        if not entries:
            log.info("File upload incomplete; retrying...")
            return PollResult(done=False)

        entry = entries[0]
        fs = entry.get("file_status")
        if fs == "stats-processed":
            return PollResult(done=True, value=entry)

        log.info("Current status = '%s'; retrying...", fs)
        log.debug("Poll response entry:\n%s", json.dumps(entry, indent=2))
        return PollResult(done=False)

    final_entry = wait_for(
        predicate_fn,
        interval=interval,
        timeout=timeout,
        on_retry=lambda _: None,
        on_timeout=lambda elapsed: log.error(
            "Upload polling timed out after %.1f seconds", elapsed
        ),
    )

    log.info("%s processed successfully.", filename)
    log.debug("Final details:\n%s", json.dumps(final_entry, indent=2))
    return final_entry
