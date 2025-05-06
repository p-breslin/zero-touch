import os
import httpx
from dotenv import load_dotenv

load_dotenv()
API_BASE_URL = "https://graph-a4.xflow-in.dev/api/"
AUTH_TOKEN = os.getenv("EDNS_API_TOKEN")

async def get_metrics(db_name: str) -> dict:
    url = f"{API_BASE_URL}/api/v1/graph/{db_name}/metrics/"
    headers = {"Authorization": f"Token {AUTH_TOKEN}"}
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)

    if response.status_code != 200:
        return {"error": f"Status {response.status_code}", "detail": response.text}
    
    return response.json()