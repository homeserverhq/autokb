"""AutoKB API client with authentication passthrough.

This client forwards the user's Bearer token to AutoKB,
ensuring all operations respect the user's permissions.
"""

import os
from typing import Any, Optional

import httpx


class AutoKBClient:
    """Client for AutoKB API with auth passthrough."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.base_url = (base_url or os.getenv("AUTOKB_BASE_URL", "")).rstrip("/")
        self.api_key = api_key or os.getenv("AUTOKB_API_KEY", "")

        if not self.base_url:
            raise ValueError(
                "AutoKB URL required. Set AUTOKB_BASE_URL env var or pass base_url."
            )

    def _get_headers(self, api_key: Optional[str] = None) -> dict[str, str]:
        token = api_key or self.api_key
        headers = {
            "Content-Type": "application/json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def request(
        self,
        method: str,
        path: str,
        api_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers = self._get_headers(api_key)

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.request(
                method,
                url,
                headers=headers,
                **kwargs,
            )
            response.raise_for_status()

            if response.headers.get("content-type", "").startswith("application/json"):
                return response.json()
            return {"text": response.text}

    async def get(self, path: str, api_key: Optional[str] = None, **kwargs: Any) -> Any:
        return await self.request("GET", path, api_key, **kwargs)

    async def post(self, path: str, api_key: Optional[str] = None, **kwargs: Any) -> Any:
        return await self.request("POST", path, api_key, **kwargs)

    async def put(self, path: str, api_key: Optional[str] = None, **kwargs: Any) -> Any:
        return await self.request("PUT", path, api_key, **kwargs)

    async def delete(self, path: str, api_key: Optional[str] = None, **kwargs: Any) -> Any:
        return await self.request("DELETE", path, api_key, **kwargs)

    # ==========================================================================
    # Subscription Management
    # ==========================================================================

    async def create_subscription(
        self,
        plugin_id: str,
        name: str,
        config: dict = {},
        cron: Optional[str] = None,
        description: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> Any:
        body: dict[str, Any] = {"name": name, "config": config}
        if cron is not None:
            body["cron"] = cron
        if description is not None:
            body["description"] = description
        return await self.post(f"/api/subscriptions/{plugin_id}", api_key, json=body)

    async def edit_subscription(
        self,
        sub_id: str,
        config: dict = {},
        cron: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> Any:
        body: dict[str, Any] = {"config": config}
        if cron is not None:
            body["cron"] = cron
        return await self.put(f"/api/subscriptions/{sub_id}", api_key, json=body)

    async def delete_subscription(self, sub_id: str, api_key: Optional[str] = None) -> Any:
        return await self.delete(f"/api/subscriptions/{sub_id}", api_key)

    async def trigger_manual_update(self, sub_id: str, api_key: Optional[str] = None) -> Any:
        return await self.post(f"/api/subscriptions/{sub_id}/trigger", api_key)

    async def set_subscription_status(
        self, sub_id: str, status: str, api_key: Optional[str] = None
    ) -> Any:
        return await self.put(
            f"/api/subscriptions/{sub_id}/status", api_key, json={"status": status}
        )

    async def list_subscriptions(
        self, plugin_id: Optional[str] = None, api_key: Optional[str] = None
    ) -> Any:
        path = "/api/subscriptions"
        if plugin_id is not None:
            path = f"{path}?plugin_id={plugin_id}"
        return await self.get(path, api_key)

    async def get_subscription_status(self, sub_id: str, api_key: Optional[str] = None) -> Any:
        return await self.get(f"/api/subscriptions/{sub_id}", api_key)

    # ==========================================================================
    # Plugin Discovery
    # ==========================================================================

    async def list_plugins(self, api_key: Optional[str] = None) -> Any:
        return await self.get("/api/plugins", api_key)

    async def get_plugin_details(self, plugin_id: str, api_key: Optional[str] = None) -> Any:
        return await self.get(f"/api/plugins/{plugin_id}", api_key)

    async def get_plugin_schema(self, plugin_id: str, api_key: Optional[str] = None) -> Any:
        return await self.get(f"/api/plugins/{plugin_id}/schema", api_key)

    # ==========================================================================
    # System Health
    # ==========================================================================

    async def get_system_health(self, api_key: Optional[str] = None) -> Any:
        return await self.get("/api/health", api_key)

    # ==========================================================================
    # Custom Routes (Plugin-Specific)
    # ==========================================================================

    async def get_bible_versions(self, api_key: Optional[str] = None) -> Any:
        return await self.get("/api/plugins/eBiblePlugin/versions", api_key)
