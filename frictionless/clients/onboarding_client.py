import logging
import warnings
from typing import Any, Dict, List, Optional

import httpx
from urllib3.exceptions import InsecureRequestWarning

from src.onboarding.errors import FatalApiError, RetryableError

log = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=InsecureRequestWarning)  # annoying


class OnboardingApiClient:
    """A client for interacting with the onboarding API. Handles auth and API calls."""

    def __init__(self, base_url: str, email: str, password: str):
        """
        Initializes the client with the API base URL and admin credentials.

        Args:
            base_url (str): The base URL of the onboarding API.
            email (str): The email of the administrative user.
            password (str): The password of the administrative user.
        """
        self.base_url = base_url
        self.email = email
        self.password = password
        self.session = httpx.Client(verify=False)
        self._auth_token: Optional[str] = None
        self._customer_auth_token: Optional[str] = None
        log.debug(f"Onboarding API client initialized for URL: {self.base_url}")

    def _request(
        self,
        method: str,
        path: str,
        *,
        token: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        expected_key: Optional[str] = None,
    ) -> Any:
        """
        Internal helper to make HTTP requests.

        Args:
            method (str): HTTP method ('get', 'post', etc.).
            path (str): API path (appended to base_url).
            token (str, optional): JWT for Authorization header.
            json_data (dict, optional): JSON body to send.
            files (dict, optional): Payload for file upload data.
            metadata (dict, optional): File upload information.
            expected_key (str, optional): If provided, return response_json[expected_key].

        Returns:
            Parsed JSON response, or the sub-key if expected_key is given.
        """
        url = f"{self.base_url}{path}"
        headers: Dict[str, str] = {}

        if json_data is not None:
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = f"Token {token}"
        log.debug(f"Request headers: {headers}")

        log.debug(f"{method.upper()} {url}")
        resp = self.session.request(
            method,
            url,
            params=params,
            headers=headers,
            json=json_data,
            files=files,
            data=metadata,
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            log.error(f"HTTP {resp.status_code} on {url}: {resp.text}")
            raise

        # Safely handle no-content / empty-body responses
        if resp.status_code == 204 or not resp.text.strip():
            # For DELETE (or any no-content), return None or empty dict
            return {} if expected_key else None

        # Otherwise, attempt JSON decode but catch failures
        try:
            data = resp.json()
        except ValueError:
            # Body wasn’t valid JSON; swallow and return None/{}
            return {} if expected_key else None

        # If caller asked for a sub-key, pull it out (default to {})
        if expected_key:
            return data.get(expected_key, {})

        return data

    def authenticate(self) -> str:
        """
        Authenticates with the onboarding API and caches the JWT access token.

        Returns:
            str: The JSON Web Token (JWT).
        """
        payload = {"email": self.email, "password": self.password}
        data = self._request("post", "/api/user/signin", json_data=payload)
        token = data.get("token")
        if not token:
            raise RuntimeError("Authentication succeeded but no token in response")
        self._auth_token = token
        log.info("Authentication successful. Token cached.")
        log.debug(f"Authenticated token: {token}")

    def create_partner(self, partner_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a new partner."""
        return self._request(
            "post",
            "/api/partner/",
            token=self._auth_token,
            json_data=partner_payload,
        )

    # === Industry Details =====================================================

    def list_industries(self) -> List[Dict[str, Any]]:
        """Retrieves all available industries (model templates)."""
        return self._request(
            "get",
            "/api/industry",
            token=self._auth_token,
            expected_key="data",
        )

    def list_industry_categories(self) -> List[Dict[str, Any]]:
        """Retrieves all available industry categories (model templates)."""
        return self._request(
            "get",
            "/api/industry/category",
            token=self._auth_token,
            expected_key="data",
        )

    def get_industry_details(self, industry_id: int) -> Dict[str, Any]:
        """Retrieves detailed configuration for a specific industry/model."""
        return self._request(
            "get",
            f"/api/industry/{industry_id}",
            token=self._auth_token,
            expected_key="data",
        )

    # ==== Customer Creation ===================================================

    def create_customer(self, customer_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a new customer under the partner account."""
        return self._request(
            "post",
            "/api/onboarding/partner/register-client",
            token=self._auth_token,
            json_data=customer_payload,
        )

    def generate_customer_token(self, customer_email: str) -> str:
        """Generates a session token for a specific customer."""
        data = self._request(
            "post",
            "/api/onboarding/partner/generate-client-token",
            token=self._auth_token,
            json_data={"email": customer_email},
        )
        token = data.get("token")
        if not token:
            raise RuntimeError("No customer token found in response")
        log.debug(f"Customer token generated: {token}")
        self._customer_auth_token = token

    # === Product and Package ==================================================

    def list_products(self) -> List[Dict[str, Any]]:
        """Lists the available products that can be set for a customer."""
        return self._request(
            "get",
            "/api/product",
            token=self._customer_auth_token,
            expected_key="data",
        )

    def set_product(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Initializes the customer's product."""
        return self._request(
            "post",
            "/api/set-product",
            token=self._customer_auth_token,
            json_data=payload,
        )

    def set_package(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Triggers the customer's database creation."""
        return self._request(
            "post",
            "/api/set-package",
            token=self._customer_auth_token,
            json_data=payload,
        )

    def check_db_status(self) -> Dict[str, Any]:
        """Polls the creation status of the customer's database."""
        return self._request(
            "get", "/api/vendor/check-db", token=self._customer_auth_token
        )

    # === Model Validation =====================================================

    def list_kpis(self, industry_id: int) -> List[Dict[str, Any]]:
        """Lists all KPIs available for the customer."""
        return self._request(
            "get",
            f"/api/industry-all-kpi/{industry_id}",
            token=self._auth_token,
            params={"type": 1},
        )

    def list_functions(self) -> List[Dict[str, Any]]:
        """Lists all functions."""
        return self._request(
            "get", "/api/function", token=self._auth_token, expected_key="data"
        )

    def list_contexts(self) -> List[Dict[str, Any]]:
        """Lists all context types available for the customer."""
        return self._request(
            "get", "/api/contextTypes", token=self._auth_token, expected_key="data"
        )

    def industry_metric_functions(self, industry_id: int) -> List[Dict[str, Any]]:
        """Lists all context types available for the customer."""
        return self._request(
            "get",
            f"/api/industry-metric/function/{industry_id}",
            token=self._auth_token,
            expected_key="data",
        )

    def get_dictionary_list(self, function_code: str) -> List[Dict[str, Any]]:
        """Gets the list of dictionaries for a given function code."""
        return self._request(
            "get",
            f"/api/domains/dictionaryList/{function_code}",
            token=self._auth_token,
            expected_key="data",
        )

    def get_dictionary(
        self,
        function_code: str,
    ) -> List[Dict[str, Any]]:
        """Gets the list of dictionaries for a given function code."""
        return self._request(
            "post",
            "/api/domains/getDictionary",
            token=self._auth_token,
            json_data={"functionCode": function_code},
        )

    # === Connect Data Sources =================================================

    def store_github_pat(self, pat: str) -> Dict[str, Any]:
        """Stores a GitHub Personal Access Token for the customer."""
        return self._request(
            "post",
            "/api/datasource/storePAT",
            token=self._customer_auth_token,
            json_data={
                "source_name": "GitHub",
                "personal_access_token": pat,
            },
        )

    def file_upload(self, files, metadata) -> Dict[str, Any]:
        """Uploads a new data file to the customer account."""
        return self._request(
            "post",
            "/api/datasource/upload",
            token=self._customer_auth_token,
            files=files,
            metadata=metadata,
        )

    def file_upload_status(self) -> dict:
        """
        Polls the status of a file upload to the customer account.

        Note:
            When the upload is complete, the file-upload-status endpoint returns a 500 (“Please wait...”) error as it waits for the metrics service to be ready. We will treat this as a `RetryableError` to ensure the upload process is not halted. We will invoke a `FatalApiError` for any other HTTP failures.
        """
        try:
            status = self._request(
                "get",
                "/api/datasource/file-upload-status",
                token=self._customer_auth_token,
            )
            return status or {}
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            # Extract the text or JSON “error” field
            try:
                msg = e.response.json().get("error", "")
            except ValueError:
                msg = e.response.text or str(e)

            if code == 500 and "Please wait" in msg:
                raise RetryableError(msg)

            # Anything else is fatal
            raise FatalApiError(f"Status check failed [{code}]: {msg}")

    def connect_data_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Connects a new data source to the customer account."""
        return self._request(
            "post",
            "/api/datasource/connect",
            token=self._customer_auth_token,
            json_data=payload,
        )

    # === Metric Compute =======================================================

    def metric_compute(self):
        return self._request(
            "post",
            "/api/datasource/compute-values",
            token=self._customer_auth_token,
        )

    def compute_summary(self, jobId: str):
        return self._request(
            "post",
            "/api/vendor/compute-summary",
            token=self._customer_auth_token,
            json_data={"jobIds": jobId},
        )

    def compute_time_range(self, extBatchId: str, isPublished: int = 0):
        return self._request(
            "post",
            "/api/vendor/compute/time-range",
            token=self._customer_auth_token,
            json_data={"extBatchId": extBatchId, "isPublished": isPublished},
        )

    def compute_job_status(
        self,
        jobId: str,
        parentId: int = None,
        startDate: str = None,
        timeRange: int = None,
        isPublished: int = 0,
    ):
        return self._request(
            "post",
            "/api/vendor/listComputeJobStatus",
            token=self._customer_auth_token,
            json_data={
                "jobIds": jobId,
                "parentId": parentId,
                "startDate": startDate,
                "timeRange": timeRange,
                "isPublished": isPublished,
            },
        )

    # === Misc =================================================================

    def list_partners(self) -> List[Dict[str, Any]]:
        """Retrieves a list of all partner accounts accessible by the user."""
        return self._request(
            "get",
            "/api/partner/",
            token=self._auth_token,
        )

    def delete_partner(self, partner_id: int) -> Dict[str, Any]:
        """Deletes a specific partner account by its ID."""
        return self._request(
            "delete", f"/api/partner/{partner_id}", token=self._auth_token
        )

    def list_customers(self) -> List[Dict[str, Any]]:
        """Retrieves list of all customer accounts accessible by the partner."""
        return self._request(
            "get",
            "/api/onboarding/partner/list-clients",
            token=self._auth_token,
        )
