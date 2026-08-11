import os
import sys
from contextvars import ContextVar
from typing import Annotated, Any, Optional, Literal

from croniter import croniter
from fastmcp import FastMCP, Context
from mcp.types import ToolAnnotations
from pydantic import BaseModel
from pydantic.functional_validators import AfterValidator
from toon_mcp import json_to_toon


def _validate_cron_expr(v: Optional[str]) -> Optional[str]:
    if v is not None and not croniter.is_valid(v):
        raise ValueError(f"Invalid cron expression: {v}")
    return v


CronExpr = Annotated[Optional[str], AfterValidator(_validate_cron_expr)]

from .client import AutoKBClient

# Context variable to store the current user's token
_current_user_token: ContextVar[Optional[str]] = ContextVar(
    "current_user_token", default=None
)


class AuthMiddleware:
    """ASGI middleware to extract Authorization header and set context variable."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))
            auth_header = headers.get(b"authorization", b"").decode()
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
                _current_user_token.set(token)
        await self.app(scope, receive, send)


# Initialize MCP server
mcp = FastMCP("autokb-mcp-server")

# Initialize client (URL from env)
_client: Optional[AutoKBClient] = None


def get_client() -> AutoKBClient:
    global _client
    if _client is None:
        _client = AutoKBClient()
    return _client


def get_user_token() -> Optional[str]:
    token = _current_user_token.get()
    if token:
        return token
    return os.getenv("AUTOKB_API_KEY")


# =============================================================================
# Parameter Models
# =============================================================================


class CreateSubscriptionParam(BaseModel):
    plugin_id: str
    name: str
    config: dict = {}
    cron: CronExpr = None
    description: Optional[str] = None


class EditSubscriptionParam(BaseModel):
    sub_id: str
    config: dict = {}
    cron: CronExpr = None


class DeleteSubscriptionParam(BaseModel):
    sub_id: str


class TriggerManualUpdateParam(BaseModel):
    sub_id: str


class SetSubscriptionStatusParam(BaseModel):
    sub_id: str
    status: Literal["ENABLED", "DISABLED"]


class ListSubscriptionsParam(BaseModel):
    plugin_id: Optional[str] = None


class GetSubscriptionStatusParam(BaseModel):
    sub_id: str


class GetPluginDetailsParam(BaseModel):
    plugin_id: str


class GetPluginSchemaParam(BaseModel):
    plugin_id: str


class CreateBibleSubscriptionParam(BaseModel):
    name: str
    version: str = "eng_kjv"
    cron: CronExpr = "0 0 1 1 *"


class CreateYouTubeSubscriptionParam(BaseModel):
    name: str
    channel_id: str
    language: str = "en"
    api_key: Optional[str] = None
    max_videos: int = 0
    cron: CronExpr = "0 0 * * 0"


class CreateIMAPSubscriptionParam(BaseModel):
    name: str
    host: str
    port: int = 993
    use_ssl: bool = True
    folder: str = "INBOX"
    user: Optional[str] = None
    password: Optional[str] = None
    cron: CronExpr = None


class CreateCrawl4AISubscriptionParam(BaseModel):
    name: str
    url: str
    max_depth: int = 10
    max_pages: int = 0
    cron: CronExpr = "0 0 * * 0"


class GetBibleVersionsParam(BaseModel):
    language: Optional[str] = None


# =============================================================================
# Subscription Management Tools
# =============================================================================


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Create Subscription', readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
)
async def create_subscription(
    plugin_id: str,
    name: str,
    config: dict = {},
    cron: Optional[str] = None,
    description: Optional[str] = None,
    ctx: Context = None,
) -> dict[str, Any]:
    """Create a subscription for any plugin (last resort fallback).

    Args:
        plugin_id: Plugin ID to create a subscription for.
        name: Subscription name.
        config: Plugin configuration as a JSON object.
        cron: Cron expression for scheduled execution (e.g. "0 0 * * *").
        description: Subscription description.
    """
    params = CreateSubscriptionParam(
        plugin_id=plugin_id, name=name, config=config,
        cron=cron, description=description,
    )
    return await get_client().create_subscription(
        plugin_id=params.plugin_id,
        name=params.name,
        config=params.config,
        cron=params.cron,
        description=params.description,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Edit Subscription', readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def edit_subscription(
    sub_id: str,
    config: dict = {},
    cron: Optional[str] = None,
    ctx: Context = None,
) -> dict[str, Any]:
    """Update configuration for an existing subscription. Name cannot be changed.

    Args:
        sub_id: Subscription ID to edit.
        config: Plugin configuration as a JSON object.
        cron: Cron expression for scheduled execution (e.g. "0 0 * * *").
    """
    params = EditSubscriptionParam(
        sub_id=sub_id, config=config, cron=cron,
    )
    return await get_client().edit_subscription(
        sub_id=params.sub_id,
        config=params.config,
        cron=params.cron,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Delete Subscription', readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False)
)
async def delete_subscription(
    sub_id: str,
    ctx: Context = None,
) -> dict[str, Any]:
    """Remove a subscription. This is a destructive action that cannot be undone.

    Args:
        sub_id: Subscription ID to delete.
    """
    params = DeleteSubscriptionParam(sub_id=sub_id)
    return await get_client().delete_subscription(
        sub_id=params.sub_id,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Trigger Manual Update', readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
)
async def trigger_manual_update(
    sub_id: str,
    ctx: Context = None,
) -> dict[str, Any]:
    """Manually trigger a single execution of a subscription.

    Args:
        sub_id: Subscription ID to trigger.
    """
    params = TriggerManualUpdateParam(sub_id=sub_id)
    return await get_client().trigger_manual_update(
        sub_id=params.sub_id,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Set Subscription Status', readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def set_subscription_status(
    sub_id: str,
    status: Literal["enabled", "disabled"],
    ctx: Context = None,
) -> dict[str, Any]:
    """Enable or disable a subscription. Disabling will prevent execution.

    Args:
        sub_id: Subscription ID.
        status: enabled or disabled.
    """
    status = status.upper()
    params = SetSubscriptionStatusParam(sub_id=sub_id, status=status)
    return await get_client().set_subscription_status(
        sub_id=params.sub_id,
        status=params.status,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='List Subscriptions', readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def list_subscriptions(
    plugin_id: Optional[str] = None,
    ctx: Context = None,
) -> dict[str, Any]:
    """List all subscriptions, optionally filtered by plugin.

    Args:
        plugin_id: Optional plugin ID to filter by.
    """
    params = ListSubscriptionsParam(plugin_id=plugin_id)
    raw = await get_client().list_subscriptions(
        plugin_id=params.plugin_id,
        api_key=get_user_token(),
    )
    return {"items": json_to_toon(raw)}


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Get Subscription Status', readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def get_subscription_status(
    sub_id: str,
    ctx: Context = None,
) -> dict[str, Any]:
    """Returns current status, last error, and progress for a subscription.

    Args:
        sub_id: Subscription ID.
    """
    params = GetSubscriptionStatusParam(sub_id=sub_id)
    return await get_client().get_subscription_status(
        sub_id=params.sub_id,
        api_key=get_user_token(),
    )


# =============================================================================
# Plugin Discovery Tools
# =============================================================================


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='List Plugins', readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def list_plugins(
    ctx: Context = None,
) -> dict[str, Any]:
    """List all available data source plugins and their metadata."""
    raw = await get_client().list_plugins(api_key=get_user_token())
    return {"items": json_to_toon(raw)}


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Get Plugin Details', readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def get_plugin_details(
    plugin_id: str,
    ctx: Context = None,
) -> dict[str, Any]:
    """Get full metadata (description, icon) for a specific plugin.

    Args:
        plugin_id: Plugin ID.
    """
    params = GetPluginDetailsParam(plugin_id=plugin_id)
    return await get_client().get_plugin_details(
        plugin_id=params.plugin_id,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Get Plugin Schema', readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def get_plugin_schema(
    plugin_id: str,
    ctx: Context = None,
) -> dict[str, Any]:
    """Returns the JSON Schema used to generate dynamic UI forms for a plugin.

    Args:
        plugin_id: Plugin ID.
    """
    params = GetPluginSchemaParam(plugin_id=plugin_id)
    return await get_client().get_plugin_schema(
        plugin_id=params.plugin_id,
        api_key=get_user_token(),
    )


# =============================================================================
# System Health Tool
# =============================================================================


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Get System Health', readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def get_system_health(
    ctx: Context = None,
) -> dict[str, Any]:
    """Checks connectivity to Redis, PostgreSQL, and the Manager API."""
    return await get_client().get_system_health(api_key=get_user_token())


# =============================================================================
# Custom Routes (Plugin-Specific)
# =============================================================================


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Create Bible Subscription', readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
)
async def create_bible_subscription(
    name: str,
    version: str = "eng_kjv",
    cron: Optional[str] = "0 0 1 1 *",
    ctx: Context = None,
) -> dict[str, Any]:
    """Create a Bible download subscription.

    Args:
        name: Subscription name (e.g. "KJV Bible Download").
        version: Bible version code (e.g. "eng_kjv", "BSB", "AAB"). Use get_bible_versions tool to see all available versions.
        cron: Cron expression for scheduled execution (e.g. "0 0 1 1 *").
    """
    params = CreateBibleSubscriptionParam(
        name=name, version=version, cron=cron,
    )
    return await get_client().create_subscription(
        plugin_id="eBiblePlugin",
        name=params.name,
        config={"version": params.version},
        cron=params.cron,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Create YouTube Subscription', readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
)
async def create_youtube_subscription(
    name: str,
    channel_id: str,
    language: str = "en",
    api_key: Optional[str] = None,
    max_videos: int = 0,
    cron: Optional[str] = "0 0 * * 0",
    ctx: Context = None,
) -> dict[str, Any]:
    """Create a YouTube channel transcript subscription.

    Args:
        name: Subscription name (e.g. "My Channel Transcripts").
        channel_id: YouTube channel ID (UC...), handle (@name), or channel URL.
        language: Transcript language code (e.g. "en", "es", "fr").
        api_key: YouTube Data API v3 key (optional, enables metadata and faster enumeration).
        max_videos: Max recent videos to process (0 = all videos).
        cron: Cron expression for scheduled execution (e.g. "0 0 * * 0").
    """
    params = CreateYouTubeSubscriptionParam(
        name=name, channel_id=channel_id, language=language,
        api_key=api_key, max_videos=max_videos, cron=cron,
    )
    config = {
        "channel_id": params.channel_id,
        "language": params.language,
        "api_key": params.api_key,
        "max_videos": params.max_videos,
    }
    return await get_client().create_subscription(
        plugin_id="youTubeTranscriptionPlugin",
        name=params.name,
        config=config,
        cron=params.cron,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Create IMAP Subscription', readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
)
async def create_imap_subscription(
    name: str,
    host: str,
    port: int = 993,
    use_ssl: bool = True,
    folder: str = "INBOX",
    user: Optional[str] = None,
    password: Optional[str] = None,
    cron: Optional[str] = None,
    ctx: Context = None,
) -> dict[str, Any]:
    """Create an IMAP email folder watch subscription.

    Args:
        name: Subscription name (e.g. "My Email Inbox").
        host: IMAP server hostname.
        port: IMAP server port (default 993).
        use_ssl: Use SSL for IMAP connection.
        folder: IMAP folder to watch (default "INBOX").
        user: IMAP username or email address.
        password: IMAP password.
        cron: Cron expression for scheduled execution (e.g. "0 0 * * *").
    """
    params = CreateIMAPSubscriptionParam(
        name=name, host=host, port=port, use_ssl=use_ssl,
        folder=folder, user=user, password=password,
        cron=cron,
    )
    config = {
        "host": params.host,
        "port": params.port,
        "use_ssl": params.use_ssl,
        "user": params.user,
        "password": params.password,
        "folder": params.folder,
    }
    return await get_client().create_subscription(
        plugin_id="imapFolderWatchPlugin",
        name=params.name,
        config=config,
        cron=params.cron,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Create Crawl4AI Subscription', readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False)
)
async def create_crawl4ai_subscription(
    name: str,
    url: str,
    max_depth: int = 10,
    max_pages: int = 0,
    cron: Optional[str] = "0 0 * * 0",
    ctx: Context = None,
) -> dict[str, Any]:
    """Create a website crawl subscription using Crawl4AI.

    Args:
        name: Subscription name (e.g. "My Site Crawl").
        url: Base URL of the website to crawl (e.g. https://example.com).
        max_depth: Crawl depth beyond the start page (0 = no limit, default 10).
        max_pages: Maximum pages to crawl (0 = unlimited).
        cron: Cron expression for scheduled execution (e.g. "0 0 * * 0").
    """
    params = CreateCrawl4AISubscriptionParam(
        name=name, url=url, max_depth=max_depth,
        max_pages=max_pages, cron=cron,
    )
    config = {
        "url": params.url,
        "max_depth": params.max_depth,
        "max_pages": params.max_pages,
    }
    return await get_client().create_subscription(
        plugin_id="crawl4AIWebScraperPlugin",
        name=params.name,
        config=config,
        cron=params.cron,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool(
    tags={'primary', 'autokb'}, annotations=ToolAnnotations(title='Get Bible Versions', readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
)
async def get_bible_versions(
    language: Optional[str] = None,
    ctx: Context = None,
) -> dict[str, Any]:
    """Returns available Bible versions with labels and language groupings.

    Args:
        language: Language name to filter by (e.g. "English", "Spanish", "French").
    """
    params = GetBibleVersionsParam(language=language)
    raw = await get_client().get_bible_versions(api_key=get_user_token())
    if params.language and raw.get("groups"):
        lang_versions = set(raw["groups"].get(params.language, []))
        raw["versions"] = [v for v in raw["versions"] if v in lang_versions]
        raw["labels"] = {k: v for k, v in raw["labels"].items() if k in lang_versions}
    return {"items": json_to_toon(raw)}


# =============================================================================
# Entry Point
# =============================================================================


def main():
    """Run the MCP server."""
    if not os.getenv("AUTOKB_BASE_URL"):
        print("ERROR: AUTOKB_BASE_URL environment variable is required", file=sys.stderr)
        print("Example: export AUTOKB_BASE_URL=http://autokb-web:80", file=sys.stderr)
        sys.exit(1)

    host = "0.0.0.0"
    port = int(os.getenv("MCP_PORT", "80"))
    path = "/mcp"
    app = mcp.http_app(path=path)
    app = AuthMiddleware(app)
    print(f"Starting AutoKB MCP server on http://{host}:{port}{path}")
    import uvicorn

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
