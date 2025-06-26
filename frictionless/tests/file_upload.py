import sys
import json
import config
import requests
from dotenv import load_dotenv
from utils.logger import setup_logging
from clients.onboarding_client import OnboardingApiClient

load_dotenv()
setup_logging()


FILE_PATH = "data/xflow_org_master_dictionary.xlsx"
DESCRIPTION = "Org master dictionary"
FILE_COUNT = 1


def main():
    # Initialize and authenticate
    client = OnboardingApiClient(
        base_url=config.ONBOARDING_API_URL,
        email=config.ADMIN_EMAIL,
        password=config.ADMIN_PASSWORD,
    )
    client.authenticate()

    # Upload the file
    try:
        result = client.upload_file(
            file_path=FILE_PATH,
            description=DESCRIPTION,
            file_count=FILE_COUNT,
        )
        print("Upload succeeded:", result)

    except requests.HTTPError as e:
        # this will show the 500 and any error payload/body
        print(f"Upload failed: HTTP {e.response.status_code}")
        print("Response body:")
        print(e.response.text)
        sys.exit(1)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
