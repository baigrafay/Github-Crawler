
import aiohttp
import asyncio
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import datetime
import time
import json

GQL_URL = "https://api.github.com/graphql"

class GraphQLClient:
    def __init__(self, token, session: aiohttp.ClientSession, min_remaining=100):
        self.token = token
        self.session = session
        self.headers = {
            "Authorization": f"bearer {token}",
            "Accept": "application/vnd.github.v4+json"
        }
        self.min_remaining = min_remaining

    async def _maybe_wait_for_reset(self, rate):
        """
        If remaining is low, wait until resetAt.
        rate: dict with keys 'remaining' and 'resetAt' (ISO timestamp)
        """
        remaining = rate.get("remaining")
        reset_at = rate.get("resetAt")
        if remaining is None:
            return
        if remaining < self.min_remaining and reset_at:
            reset_dt = datetime.datetime.fromisoformat(reset_at.replace("Z","+00:00"))
            now = datetime.datetime.now(datetime.timezone.utc)
            wait_seconds = (reset_dt - now).total_seconds()
            if wait_seconds > 0:
                print(f"[graphql_client] Rate remaining {remaining} < {self.min_remaining}. Sleeping {int(wait_seconds)}s until reset.")
                await asyncio.sleep(wait_seconds + 1)

    @retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, Exception)))
    async def execute(self, query: str, variables: dict = None):
        payload = {"query": query, "variables": variables or {}}
        async with self.session.post(GQL_URL, json=payload, headers=self.headers, timeout=60) as resp:
            text = await resp.text()
            if resp.status >= 500:
            
                resp.raise_for_status()
            data = await resp.json()
            if "errors" in data:
                
                raise Exception(f"GraphQL errors: {data['errors']}")
            rate = data.get("data", {}).get("rateLimit") if isinstance(data.get("data"), dict) else None
            if rate:
                await self._maybe_wait_for_reset(rate)
            return data["data"]
