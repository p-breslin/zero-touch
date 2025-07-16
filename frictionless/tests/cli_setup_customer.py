import argparse
import json
import logging
import sys
import time

import config
from clients.onboarding_client import OnboardingApiClient
from utils.logger import setup_logging

setup_logging(level=2)
log = logging.getLogger(__name__)


def fetch_models(client: OnboardingApiClient) -> dict[int, str]:
    """Returns a mapping of industry IDs to names."""
    models = client.list_industries()
    return {m["id"]: m["categoryName"] for m in models}


def fetch_products(client: OnboardingApiClient, token: str) -> dict[int, str]:
    """Returns a mapping of product IDs to names for a given customer token."""
    products = client.list_products(token)
    return {p["id"]: p["name"] for p in products}


def select_id(mapping: dict[int, str], cli_choice: int | None, entity_name: str) -> int:
    """Validates or prompts for selection of an ID from a mapping."""
    if cli_choice is not None:
        if cli_choice in mapping:
            return cli_choice
        print(
            f"Warning: invalid --{entity_name.lower().replace(' ', '-')}-id {cli_choice}. "
            f"Must be one of: {', '.join(str(i) for i in mapping)}."
        )

    # Interactive CLI prompt
    print(f"\nAvailable {entity_name}s:")
    for i, name in mapping.items():
        print(f"  {i}: {name}")

    selected = None
    while True:
        choice_str = input(f"Enter a {entity_name} ID: ").strip()
        if not choice_str.isdigit():
            print("Please enter a numeric ID.")
            continue
        choice = int(choice_str)
        if choice in mapping:
            selected = choice
            break
        print(f"ID {choice} not in the list. Try again.")

    log.info(f"Selected {entity_name}: {choice} -> {mapping[choice]}")
    return selected


def main():
    parser = argparse.ArgumentParser(
        description="Onboard a customer: select model, product, and package"
    )
    parser.add_argument(
        "--model-id",
        type=int,
        help="Select an Industry Category (model) by ID; prompts if omitted or invalid",
    )
    parser.add_argument(
        "--product-id",
        type=int,
        help="Select a Product by ID; prompts if omitted or invalid",
    )
    args = parser.parse_args()

    log.debug("Initializing OnboardingApiClient..")
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )

    try:
        # 1) Authentication
        log.info("Authenticating partner token..")
        client.authenticate()

        # 2) Fetch and display models
        log.info("Fetching available verticals..")
        model_map = fetch_models(client)
        chosen_model_id = select_id(model_map, args.model_id, "Model")

        # 3) Prepare the payload and create the customer with chosen model
        customer_payload = dict(config.NEW_CUSTOMER_PAYLOAD)
        customer_payload["industryId"] = chosen_model_id
        log.debug("Customer payload:\n" + json.dumps(customer_payload, indent=2))
        log.info("Creating customer..")
        customer_resp = client.create_customer(customer_payload)
        log.info(f"Customer created for email: {customer_resp['email']}")

        # 4) Generate a customer-scoped token
        customer_email = customer_payload["email"]
        log.info(f"Generating token for customer {customer_email}..")
        customer_token = client.generate_customer_token(customer_email)
        log.debug("Customer-scoped token obtained.")

        # 5) Fetch and select a product
        log.info("Listing available products..")
        product_map = fetch_products(client, customer_token)
        chosen_product_id = select_id(product_map, args.product_id, "Product")

        # 6) Set the product for the customer
        product_payload = dict(config.SET_PRODUCT_PAYLOAD)
        product_payload["product_name"] = product_map[chosen_product_id]
        log.debug("Set-product payload:\n" + json.dumps(product_payload, indent=2))
        log.info("Setting product..")
        product_resp = client.set_product(customer_token, product_payload)
        log.debug(f"Set-product response:\n{json.dumps(product_resp, indent=2)}")

        # 7) Set the package for the customer
        package_payload = dict(config.SET_PACKAGE_PAYLOAD)
        log.debug("Set-package payload:\n" + json.dumps(package_payload, indent=2))
        log.info("Setting package..")
        package_resp = client.set_package(customer_token, package_payload)
        log.debug(f"Set-package response:\n{json.dumps(package_resp, indent=2)}")

        # 8) Poll the status of the customer database creation
        start_time = time.time()
        timeout_seconds = config.TIMEOUT_MINUTES * 60

        log.info("Polling for database creation..")
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                log.error(
                    f"Timeout of {config.TIMEOUT_MINUTES} minutes exceeded; aborting."
                )
                sys.exit(1)

            status = client.check_db_status(customer_token)
            log.debug("Poll response:\n" + json.dumps(status, indent=2))

            if status.get("payload", {}).get("db_exists"):
                log.info("Database is ready!")
                log.debug("DB Details:\n" + json.dumps(status["payload"], indent=2))
                break

            log.info(
                f"Database not ready (message: {status.get('payload')}). "
                f"Retrying in {config.POLLING_INTERVAL_SECONDS}s.."
            )
            time.sleep(config.POLLING_INTERVAL_SECONDS)

        log.info(
            "Onboarding complete: model, product, and package have been set. Customer database successfully created."
        )

    except Exception as e:
        log.error(f"Error during onboarding: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
