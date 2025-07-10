import os
import logging
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)


# Settings for the Model Definition API (dats-api)
ONBOARDING_API_URL = "https://onboarding-dev-1.xflow-in.dev"
DATS_API_URL = "https://domain-dev-1.xflow-in.dev"

# Credentials for an admin with permissions to create new partners/customers
ADMIN_EMAIL = "peter.breslin@experienceflow.ai"
ADMIN_PASSWORD = os.getenv("XFLOW_PWD")

# Payload to be used to create a new partner via the API
NEW_PARTNER_PAYLOAD = {
    "industryId": 1915,  # 1906
    "email": "peter.breslin+1@experienceflow.ai",
    "password": os.getenv("XFLOW_PWD"),
    "name": "Peter Breslin",
    "first_name": "Peter",
    "last_name": "Breslin",
    "company": "Frictionless Test",
    "role": 3402,
    "partner_id": 257,
    "contact_number": 1234567890,
    "industry_name": "",
    "zip_code": 94105,
    "country": "USA",
    "state": "California",
    "created_at": 0,
    "created_by": 2212,
    "created_by_name": "",
    "tags": [],
    "industry_category": None,
    "is_template": False,
    "role_display_name": "",
}

# Payloads for customer set-up
NEW_CUSTOMER_PAYLOAD = {
    "role": 5503,
    "industryId": 1915,  # 1873  Sales=1937
    "company_name": "Frictionless Test",
    "email": "peter.breslin+1@experienceflow.ai",
    "password": os.getenv("XFLOW_PWD"),
    "country": "United States",
    "state": "California",
    "zip_code": "94105",
    "first_name": "Peter",
    "last_name": "Breslin",
    "countryCode": "+1",
    "contact_number": "11234567890",
    "created_by": 2212,
}
SET_PRODUCT_PAYLOAD = {"product_name": "EDNS STRATEGY"}
SET_PACKAGE_PAYLOAD = {"packageId": "STANDARD"}

# Configuration for the polling loop
POLLING_INTERVAL_SECONDS = 15
TIMEOUT_SECONDS = 60 * 15

# Data files for upload
FILE_UPLOAD_PATH = "data/xflow_engineering/"
DEMO_DATA_INFO = {
    "file": "xflow_engineering_org_data_demo.xlsx",
    "description": "Engineering Demo Data",
    "filetype": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}
KPI_DATA_INFO = {
    "file": "KPI_Engineering_CustomerSupport_Report_3.csv",
    "description": "KPI Data",
    "filetype": "text/csv",
}

# Connection details for the ArangoDB instance where graphs are stored
ARANGO_HOST = "http://arangodb.in.dev.xflow/"
ARANGO_USER = "root"
ARANGO_PASSWORD = "Dm6UjGZMMwmje"

# Graph engine API settings
EDNS_GRAPH_API_BASE_URL = "http://graph.in.dev.xflow"
GRAPH_API_EMAIL = "graph-test@xflow.ai"
GRAPH_API_PASSWORD = os.getenv("XFLOW_PWD")
