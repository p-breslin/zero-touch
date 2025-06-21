import sys
import json
import config
import logging
from utils.logger import setup_logging
from clients.onboarding_client import OnboardingApiClient

setup_logging()
log = logging.getLogger(__name__)


def main():
    """
    Validates that a newly created customer's environment has been correctly instantiated from the selected model template.
    """
    log.info("=== Starting Model Validation Script")

    customer_email = config.NEW_CUSTOMER_PAYLOAD["email"]
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )

    try:
        # 1) Authenticate and get the necessary tokens
        log.info("Authenticating partner and customer tokens...")
        client.authenticate()
        partner_token = client._auth_token
        customer_token = client.generate_customer_token(customer_email)

        # 2) Validate KPIs
        log.info("Validating KPIs...")
        kpi_dict = client.list_kpis(
            customer_token, config.NEW_CUSTOMER_PAYLOAD["industryId"]
        )
        log.debug(json.dumps(kpi_dict, indent=2))
        kpis = kpi_dict.get("data", {})
        if not kpis:
            log.warning("Validation Warning: No KPIs found in payload.")
        else:
            log.info(f"Found {len(kpis)} KPIs.")
            log.debug(json.dumps(kpis, indent=2))
            print("--- Available KPIs ---\n")
            kpi_map = {
                kpi["id"]: {
                    "functionName": kpi["functionName"],
                    "name": kpi["name"],
                    "metric_attributes": len(kpi.get("metric_attributes", [])),
                }
                for kpi in kpis
            }
            print(json.dumps(kpi_map, indent=2))

        # 3) Validate Contexts
        log.info("Validating Contexts...")
        contexts = client.list_contexts(customer_token)
        context_data = contexts.get("data")
        if not context_data:
            log.warning("Validation Warning: No Contexts found in data.")
        else:
            log.info(f"Validation Success: Found {len(context_data)} Contexts.")
            print("--- Available Contexts ---\n" + json.dumps(context_data, indent=2))

        # 4) Validate Functions and their Dictionaries
        log.info("Validating Functions and Dictionaries...")
        # Use the partner token for domain-level checks
        functions = client.list_functions(partner_token)
        if not functions:
            log.warning("Validation Warning: No Functions found.")
        else:
            log.info(f"Validation Success: Found {len(functions[:2])} Functions.")
            for func in functions[:2]:
                f_code = func.get("functionCode")
                f_name = func.get("functionName")
                print(f"\n--- Dictionary for Function: '{f_name}' ({f_code}) ---")
                dictionary_tables = client.get_dictionary_list(partner_token, f_code)
                print(json.dumps(dictionary_tables, indent=2))

    except Exception as e:
        log.error(f"An error occurred during model validation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
