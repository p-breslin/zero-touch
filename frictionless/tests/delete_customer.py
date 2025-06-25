import os
import sys
import json
import config
import logging
import mysql.connector
from dotenv import load_dotenv
from utils.logger import setup_logging
from clients.onboarding_client import OnboardingApiClient

load_dotenv()
setup_logging()
log = logging.getLogger(__name__)


def main():
    """Deletes a customer and partner account."""

    customer_email = config.NEW_CUSTOMER_PAYLOAD["email"]
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )

    try:
        # 1) Authenticate
        log.info("Authenticating partner...")
        client.authenticate()

        # 2) Obtain customer details
        customers = client.list_customers()
        log.debug(json.dumps(customers, indent=2))
        record = next((c for c in customers if c["email"] == customer_email), None)

        if record:
            customer_id = record["user_id"]
            partner_id = record["partner_id"]
        else:
            customer_id = None
            partner_id = None

        log.info(f"IDs for {customer_email}:\n")
        log.info(f"Customer ID: {customer_id}\n")
        log.info(f"Partner ID: {partner_id}")

        # 3) Delete the customer
        delete_core_user(customer_id)
        log.info("Customer deleted.")

        # 4) Delete the partner
        client.delete_partner(partner_id)
        log.info("Partner deleted.")

    except Exception as e:
        log.error(f"An error occurred during customer deletion: {e}")
        sys.exit(1)


def delete_core_user(customer_id):
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USR"),
            password=os.getenv("MYSQL_PWD"),
            database=os.getenv("MYSQL_DB"),
            autocommit=False,
        )
        cursor = conn.cursor()

        sql = "DELETE FROM `core_user` WHERE `id` = %s"
        cursor.execute(sql, (customer_id,))

        conn.commit()
        print(f"Deleted {cursor.rowcount} row(s) where id = {customer_id}")

    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}", file=sys.stderr)
        sys.exit(1)

    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals() and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    main()
