import argparse
import json
import logging
import sys
import time

import httpx

from clients.mysql_client import mysql_cursor
from clients.onboarding_client import OnboardingApiClient
from configs import cfg
from utils.logger import setup_logging
from utils.model_validation import validate_model

log = logging.getLogger(__name__)


def pipeline():
    try:
        # 1) Initialize & authenticate ======
        client = OnboardingApiClient(
            base_url=cfg.ONBOARDING_API_URL,
            email=cfg.ADMIN_EMAIL,
            password=cfg.ADMIN_PASSWORD,
        )
        log.info("Authenticating partner...")
        client.authenticate()

        # 2) Create a customer ======
        customer_payload = dict(cfg.NEW_CUSTOMER_PAYLOAD)
        log.debug("Customer payload:\n" + json.dumps(customer_payload, indent=2))
        log.info("Creating customer...")
        customer_resp = client.create_customer(customer_payload)
        log.debug("Customer creation response:\n" + json.dumps(customer_resp, indent=2))
        log.info(
            f"Customer created:\n{
                json.dumps(
                    {
                        'Company': customer_resp['company_name'],
                        'Industry': customer_resp['industry_name'],
                        'Email': customer_resp['email'],
                    },
                    indent=2,
                )
            }"
        )

        # 3) Generate customer token ======
        customer_email = customer_payload["email"]
        log.info(f"Generating token for customer {customer_email}...")
        client.generate_customer_token(customer_email)
        log.debug("Customer token obtained.")

        # 4) Set product ======
        product_payload = dict(cfg.SET_PRODUCT_PAYLOAD)
        log.debug("Set-product payload:\n" + json.dumps(product_payload, indent=2))
        log.info("Setting product...")
        product_resp = client.set_product(product_payload)
        log.info(f"Product set: {product_payload['product_name']}")
        log.debug("Set-product response:\n" + json.dumps(product_resp, indent=2))

        # 5) Set package ======
        package_payload = dict(cfg.SET_PACKAGE_PAYLOAD)
        log.debug("Set-package payload:\n" + json.dumps(package_payload, indent=2))
        log.info("Setting package...")
        package_resp = client.set_package(package_payload)
        log.info(f"Package set: {package_payload['packageId']}")
        log.debug("Set-package response:\n" + json.dumps(package_resp, indent=2))

        # 6) Poll for database creation ======
        start_time = time.time()
        timeout = cfg.TIMEOUT_SECONDS / 2
        interval = cfg.POLLING_INTERVAL_SECONDS
        log.info("Polling for database creation...")

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                log.error(f"Timeout of {timeout / 60} minutes exceeded; aborting.")
                sys.exit(1)

            status = client.check_db_status()
            log.debug("Poll response:\n" + json.dumps(status, indent=2))

            if status.get("payload", {}).get("db_exists"):
                log.info("Database is ready!")
                log.debug("DB Details:\n" + json.dumps(status["payload"], indent=2))
                break

            log.info(
                f"Database not ready (message: {status.get('payload')}). "
                f"Retrying in {interval}s..."
            )
            time.sleep(interval)

        # 7) Validate model ======
        validate_model(client, cfg.NEW_CUSTOMER_PAYLOAD["industryId"])

        log.info(
            "\nOnboarding complete: model, product, and package have been set. Customer database successfully created."
        )

        # 8) File upload (skipping datasource connection for now) ======
        for info in (cfg.DEMO_DATA_INFO, cfg.KPI_DATA_INFO):
            try:
                # File must be opened in binary mode
                with open(f"{cfg.FILE_UPLOAD_PATH}{info['file']}", "rb") as fp:
                    files = {
                        "file1": (
                            info["file"],
                            fp,
                            info["filetype"],
                        )
                    }
                    metadata = {
                        "description": info["description"],
                        "fileCount": "1",
                    }
                    resp = client.file_upload(
                        files=files,
                        metadata=metadata,
                    )
                    log.info(f"Upload in process: {info['file']})")

            except httpx.HTTPError as e:
                log.error(
                    f"Upload failed: HTTP {e.response.status_code}\n{e.response.text}"
                )
                sys.exit(1)

            # Poll upload status
            start = time.time()
            timeout = cfg.TIMEOUT_SECONDS
            log.info("Polling for file upload completion...")

            while True:
                elapsed = time.time() - start
                if elapsed > timeout:
                    log.warning(f"Timed out after {timeout / 60} minutes.")
                    sys.exit(1)

                try:
                    status = client.file_upload_status()
                except httpx.HTTPError as e:
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
                        f"{info['file']} processed successfully. Upload time: {elapsed:.1f}s"
                    )
                    log.debug("Details:\n" + json.dumps(entry, indent=2))
                    break

                # Still uploading
                log.info(f"Current status = '{fs}'; retrying in {interval}s...")
                log.debug("Poll response:\n" + json.dumps(entry, indent=2))
                time.sleep(interval)

        log.info("All files uploaded.")

        # 9) Metric compute =====
        try:
            resp = client.metric_compute()
        except Exception as e:
            log.error(f"Unexpected error calling compute-values API: {e}")
            sys.exit(1)

        log.debug(json.dumps(resp, indent=2))
        job_id = resp["payload"]["extJobId"]

        # Poll compute status
        start = time.time()
        timeout = cfg.TIMEOUT_SECONDS * 8
        interval = cfg.POLLING_INTERVAL_SECONDS
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

        # 10) Prompt to delete the customer =====
        answer = (
            input("Would you like to DELETE this customer? (Y/N): ").strip().lower()
        )
        if answer in ("Y", "y", "yes"):
            log.info("Deleting customer...")

            # Fetch the customer details
            customers = client.list_customers()
            customer_email = cfg.NEW_CUSTOMER_PAYLOAD["email"]
            record = next((c for c in customers if c["email"] == customer_email), None)

            if record:
                log.debug(json.dumps(record, indent=2))
                customer_id = record["user_id"]
                log.info(f"Deleting customer with ID = {customer_id}...")

                with mysql_cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM `core_user` WHERE `id` = %s", (customer_id,)
                    )
                    log.debug(f"Deleted {cursor.rowcount} row(s)")
                    log.info("Customer deleted.")
            else:
                log.error(f"Cannot find customer with email {customer_email}")

        else:
            log.info("Keeping customer. Exiting without deletion.")

    except Exception as e:
        log.error(f"Error during onboarding: {e}")
        sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Configures the logging level.")
    parser.add_argument(
        "-l",
        "--log-level",
        type=int,
        choices=[10, 20, 30, 40, 50],
        default=20,
        help="Logging level (10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(level=args.log_level)
    pipeline()


if __name__ == "__main__":
    main()
