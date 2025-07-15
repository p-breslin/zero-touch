import json
import logging
import sys

import httpx

from src.onboarding.errors import FatalApiError, RetryableError

from .poller import PollResult, wait_for

log = logging.getLogger(__name__)


def poll_file_upload(client) -> PollResult:
    """Polls the status of a file upload.

    Args:
        client (OnboardingApiClient): Authenticated OnboardingApiClient instance.

    Returns:
        PollResult: The outcome of a single polling iteration.
    """
    try:
        status = client.file_upload_status()
    except RetryableError:
        return PollResult(done=False)
    except FatalApiError as e:
        log.error("Fatal upload-status error: %s", e)
        sys.exit(1)

    entries = status.get("data") or []
    if not entries:
        log.info("Upload incomplete;")
        return PollResult(done=False)

    entry = entries[0]
    if entry.get("file_status") == "stats-processed":
        return PollResult(done=True, value=entry)

    log.debug("Details:\n%s", json.dumps(entry, indent=2))
    return PollResult(done=False, info=entry)


def upload_and_wait(
    client, file_info: dict, base_path: str, interval: float, timeout: float
) -> dict:
    """Uploads customer data files synchronously and polls them for completion.

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

    # Initiate the upload
    try:
        with open(f"{base_path}{filename}", "rb") as fp:
            client.file_upload(
                files={"file1": (filename, fp, file_info["filetype"])},
                metadata={"description": file_info["description"], "fileCount": "1"},
            )
    except httpx.HTTPStatusError as e:
        log.error("Upload failed: HTTP %s\n%s", e.response.status_code, e.response.text)
        sys.exit(1)

    # Poll for completion
    final_entry = wait_for(
        poll_file_upload,
        client,  # poll arg
        interval=interval,
        timeout=timeout,
        on_retry=lambda result: log.info(
            "Status = '%s' (%s)",
            result.info.get("file_status") if result.info else "unknown",
            filename,
        ),
        on_timeout=lambda secs: log.error("Timed out after %.1f s", secs),
    )

    log.info("%s processed successfully", filename)
    log.debug("Final details:\n%s", final_entry)
    return final_entry
