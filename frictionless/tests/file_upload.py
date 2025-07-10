import sys
import json
import time
import config
import logging
import requests
from dotenv import load_dotenv
from utils.logger import setup_logging
from clients.onboarding_client import OnboardingApiClient

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


FILE_PATH = "data/xflow_engineering/{file}"

demo_data = {
    "file": "xflow_engineering_org_data_demo.xlsx",
    "description": "Engineering Demo Data",
    "filetype": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

kpi_data = {
    "file": "KPI_Engineering_CustomerSupport_Report_3.csv",
    "description": "KPI Data",
    "filetype": "text/csv",
}


def main():
    # Initialize and authenticate
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )
    client.authenticate()
    client.generate_customer_token(customer_email=config.NEW_PARTNER_PAYLOAD["email"])

    for meta in (demo_data, kpi_data):
        try:
            # Open the file in binary mode
            with open(FILE_PATH.format(file=meta["file"]), "rb") as fp:
                files = {
                    "file1": (  # name must match what the server expects
                        meta["file"],  # name sent to server
                        fp,  # file‐object
                        meta["filetype"],
                    )
                }
                metadata = {
                    "description": meta["description"],
                    "fileCount": "1",
                }
                resp = client.file_upload(
                    files=files,
                    metadata=metadata,
                )
                log.info(
                    f"Upload kicked off: {meta['file']} (message: {resp.get('message')})"
                )

        except requests.HTTPError as e:
            log.error(
                f"Upload failed: HTTP {e.response.status_code}\n{e.response.text}"
            )
            sys.exit(1)

        # Poll upload status
        start = time.time()
        timeout = config.TIMEOUT_MINUTES * 60 * 30
        interval = config.POLLING_INTERVAL_SECONDS
        log.info("Polling for file upload completion...")

        while True:
            elapsed = time.time() - start
            if elapsed > timeout:
                log.warning(f"Timed out after {timeout / 60} minutes.")
                sys.exit(1)

            try:
                status = client.file_upload_status()
            except requests.HTTPError as e:
                code = e.response.status_code
                body = e.response.text
                try:
                    err_msg = e.response.json().get("error", "")
                except ValueError:
                    err_msg = body

                # Special-case: wait for metrics creation 500 error
                if code == 500 and "Please wait" in err_msg:
                    log.info("Metrics not ready yet; retrying...")
                    time.sleep(interval)
                    continue

                # Anything else is fatal
                log.error(f"Status check failed: HTTP {code}\n{body}")
                sys.exit(1)

            # Now status is a 200 JSON dict
            entries = status.get("data") or None
            if not entries:
                log.info(f"File upload incomplete. Retrying in {interval}s...")
                time.sleep(interval)
                continue

            # Each new upload takes first index in list
            entry = entries[0]
            fs = entry.get("file_status")

            if fs == "stats-processed":
                log.info(
                    f"{meta['file']} processed successfully. Upload time: {elapsed:.1f}s"
                )
                log.debug("Details:\n" + json.dumps(entry, indent=2))
                break

            # Still uploading
            log.info(f"Current status = '{fs}'; retrying in {interval}s...")
            log.debug("Poll response:\n" + json.dumps(entry, indent=2))
            time.sleep(interval)

    log.info("All files uploaded.")


if __name__ == "__main__":
    main()
