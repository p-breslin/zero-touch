import sys
import json
import time
import config
import logging
import argparse
from utils.logger import setup_logging
from clients.mysql_client import mysql_cursor
from utils.model_validation import validate_model
from clients.onboarding_client import OnboardingApiClient

log = logging.getLogger(__name__)


def pipeline():
    try:
        # 1) Initialize & authenticate
        client = OnboardingApiClient(
            base_url=config.ONBOARDING_API_URL,
            email=config.ADMIN_EMAIL,
            password=config.ADMIN_PASSWORD,
        )
        log.info("Authenticating partner...")
        client.authenticate()

        # 2) Create a customer
        customer_payload = dict(config.NEW_CUSTOMER_PAYLOAD)
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

        # 3) Generate customer token
        customer_email = customer_payload["email"]
        log.info(f"Generating token for customer {customer_email}...")
        client.generate_customer_token(customer_email)
        log.debug("Customer token obtained.")

        # 4) Set product
        product_payload = dict(config.SET_PRODUCT_PAYLOAD)
        log.debug("Set-product payload:\n" + json.dumps(product_payload, indent=2))
        log.info("Setting product...")
        product_resp = client.set_product(product_payload)
        log.info(f"Product set: {product_payload['product_name']}")
        log.debug("Set-product response:\n" + json.dumps(product_resp, indent=2))

        # 5) Set package
        package_payload = dict(config.SET_PACKAGE_PAYLOAD)
        log.debug("Set-package payload:\n" + json.dumps(package_payload, indent=2))
        log.info("Setting package...")
        package_resp = client.set_package(package_payload)
        log.info(f"Package set: {package_payload['packageId']}")
        log.debug("Set-package response:\n" + json.dumps(package_resp, indent=2))

        # 6) Poll for database creation
        start_time = time.time()
        timeout_seconds = config.TIMEOUT_MINUTES * 60

        log.info("Polling for database creation...")
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                log.error(
                    f"Timeout of {config.TIMEOUT_MINUTES} minutes exceeded; aborting."
                )
                sys.exit(1)

            status = client.check_db_status()
            log.debug("Poll response:\n" + json.dumps(status, indent=2))

            if status.get("payload", {}).get("db_exists"):
                log.info("Database is ready!")
                log.debug("DB Details:\n" + json.dumps(status["payload"], indent=2))
                break

            log.info(
                f"Database not ready (message: {status.get('payload')}). "
                f"Retrying in {config.POLLING_INTERVAL_SECONDS}s..."
            )
            time.sleep(config.POLLING_INTERVAL_SECONDS)

        # 7) Validate model
        validate_model(client, config.NEW_CUSTOMER_PAYLOAD["industryId"])

        log.info(
            "\nOnboarding complete: model, product, and package have been set. Customer database successfully created."
        )

        # Prompt to delete the customer
        answer = (
            input("Would you like to DELETE this customer? (Y/N): ").strip().lower()
        )
        if answer in ("Y", "y", "yes"):
            log.info("Deleting customer...")

            # Fetch the customer details
            customers = client.list_customers()
            customer_email = config.NEW_CUSTOMER_PAYLOAD["email"]
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
