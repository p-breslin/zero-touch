import json
import logging

import config
from src.onboarding.auth import authenticate
from src.onboarding.customer import generate_customer_token
from src.onboarding.metrics import fetch_compute_job_status

log = logging.getLogger(__name__)


job_id1 = "672ee90a-3873-4f9c-ab2f-f522bda949d0"
job_id2 = "0a126d5f-9cc6-4fa1-ad92-e9dd224a6fbc"
job_id3 = "27a6250c-8414-4b5d-ab5d-4b4743bf7fb4"

cfg = config
client = authenticate(cfg)
generate_customer_token(client, cfg.NEW_CUSTOMER_PAYLOAD["email"])
status = fetch_compute_job_status(client, job_id3, parentId=1)
print(json.dumps(status, indent=2))

timerange = client.compute_time_range(extBatchId=job_id3)
print(json.dumps(timerange, indent=2))
