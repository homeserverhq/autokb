"""AutoKB MCP Server - Main entry point.

This MCP server exposes AutoKB's API as MCP tools, allowing AI assistants
to manage subscriptions, discover plugins, and check system health.

IMPORTANT: All operations use the current user's Bearer token automatically.
The token is extracted from the incoming HTTP request and forwarded to
AutoKB, ensuring all operations respect user permissions.
"""

import os
import sys
from contextvars import ContextVar
from typing import Any, Optional, Literal

from fastmcp import FastMCP, Context
from pydantic import BaseModel, Field
from toon_mcp import json_to_toon

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
    plugin_id: str = Field(description="Plugin ID to create a subscription for")
    name: str = Field(description="Subscription name")
    config: dict = Field(default={}, description="Plugin configuration as a JSON object")
    cron: Optional[str] = Field(default=None, description="Cron expression for scheduled execution")
    access_level: Optional[Literal["PRIVATE", "PUBLIC"]] = Field(
        default=None, description="Access level: PRIVATE or PUBLIC"
    )
    description: Optional[str] = Field(default=None, description="Subscription description")


class EditSubscriptionParam(BaseModel):
    sub_id: str = Field(description="Subscription ID to edit")
    config: dict = Field(default={}, description="Plugin configuration as a JSON object")
    cron: Optional[str] = Field(default=None, description="Cron expression for scheduled execution")
    access_level: Optional[Literal["PRIVATE", "PUBLIC"]] = Field(
        default=None, description="Access level: PRIVATE or PUBLIC"
    )


class DeleteSubscriptionParam(BaseModel):
    sub_id: str = Field(description="Subscription ID to delete")


class TriggerManualUpdateParam(BaseModel):
    sub_id: str = Field(description="Subscription ID to trigger")


class SetSubscriptionStatusParam(BaseModel):
    sub_id: str = Field(description="Subscription ID")
    status: Literal["ENABLED", "DISABLED"] = Field(
        description="New status: ENABLED or DISABLED"
    )


class ListSubscriptionsParam(BaseModel):
    plugin_id: Optional[str] = Field(
        default=None, description="Optional plugin ID to filter by"
    )


class GetSubscriptionStatusParam(BaseModel):
    sub_id: str = Field(description="Subscription ID")


class GetPluginDetailsParam(BaseModel):
    plugin_id: str = Field(description="Plugin ID")


class GetPluginSchemaParam(BaseModel):
    plugin_id: str = Field(description="Plugin ID")


class CreateBibleSubscriptionParam(BaseModel):
    name: str = Field(description="Subscription name (e.g. 'KJV Bible Download')")
    version: str = Field(
        default="eng_kjv",
        description="Bible version code. Common options: 'eng_kjv' (King James Version), "
                    "'BSB' (Berean Standard Bible), 'AAB' (Accessible Ancients Bible). "
                    "Use get_bible_versions tool to see all available versions."
    )
    cron: Optional[str] = Field(
        default="0 0 1 1 *",
        description="Cron expression. Bible data doesn't change — yearly recommended."
    )
    access_level: Optional[Literal["PRIVATE", "PUBLIC"]] = Field(default=None)


class CreateYouTubeSubscriptionParam(BaseModel):
    name: str = Field(description="Subscription name (e.g. 'My Channel Transcripts')")
    channel_id: str = Field(
        description="YouTube channel ID (UC...), handle (@name), or channel URL"
    )
    language: str = Field(
        default="en",
        description="Transcript language code (e.g. 'en', 'es', 'fr')"
    )
    api_key: Optional[str] = Field(
        default=None,
        description="YouTube Data API v3 key (optional, enables metadata and faster enumeration)"
    )
    max_videos: int = Field(
        default=0,
        description="Max recent videos to process (0 = all videos)"
    )
    cron: Optional[str] = Field(
        default="0 0 * * 0",
        description="Cron expression. Weekly recommended to pick up new uploads."
    )
    access_level: Optional[Literal["PRIVATE", "PUBLIC"]] = Field(default=None)


class CreateIMAPSubscriptionParam(BaseModel):
    name: str = Field(description="Subscription name (e.g. 'My Email Inbox')")
    host: str = Field(description="IMAP server hostname")
    port: int = Field(default=993, description="IMAP server port")
    use_ssl: bool = Field(default=True, description="Use SSL for IMAP connection")
    user: str = Field(description="IMAP username / email address")
    password: str = Field(description="IMAP password")
    folder: str = Field(default="INBOX", description="IMAP folder to watch")
    cron: Optional[str] = Field(
        default=None,
        description="Cron expression. Optional for event-based subscriptions."
    )
    access_level: Optional[Literal["PRIVATE", "PUBLIC"]] = Field(default=None)


class CreateCrawl4AISubscriptionParam(BaseModel):
    name: str = Field(description="Subscription name (e.g. 'My Site Crawl')")
    url: str = Field(description="Base URL of the website to crawl (e.g. https://example.com)")
    max_depth: int = Field(default=10, description="Crawl depth beyond the start page (0 = no limit)")
    max_pages: int = Field(default=0, description="Maximum pages to crawl (0 = unlimited)")
    cron: Optional[str] = Field(
        default="0 0 * * 0",
        description="Cron expression. Weekly recommended to pick up content changes."
    )
    access_level: Optional[Literal["PRIVATE", "PUBLIC"]] = Field(default=None)


class GetBibleVersionsParam(BaseModel):
    language: Optional[str] = Field(
        default=None,
        description="Language name to filter Bible versions by. "
                    "Use the full English language name, e.g. 'English', 'Spanish', 'French', "
                    "'German', 'Chinese', 'Arabic', 'Russian', 'Korean', 'Portuguese'."
    )


# =============================================================================
# Subscription Management Tools
# =============================================================================


@mcp.tool()
async def create_subscription(params: CreateSubscriptionParam, ctx: Context) -> Any:
    """Use this as a last resort if you cannot find a specific create function for a plugin."""
    return await get_client().create_subscription(
        plugin_id=params.plugin_id,
        name=params.name,
        config=params.config,
        cron=params.cron,
        access_level=params.access_level,
        description=params.description,
        api_key=get_user_token(),
    )


@mcp.tool()
async def edit_subscription(params: EditSubscriptionParam, ctx: Context) -> Any:
    """Update configuration for an existing subscription. Note: name cannot be changed."""
    return await get_client().edit_subscription(
        sub_id=params.sub_id,
        config=params.config,
        cron=params.cron,
        access_level=params.access_level,
        api_key=get_user_token(),
    )


@mcp.tool()
async def delete_subscription(params: DeleteSubscriptionParam, ctx: Context) -> Any:
    """Remove a subscription. WARNING: This is a destructive action that cannot be undone."""
    return await get_client().delete_subscription(
        sub_id=params.sub_id,
        api_key=get_user_token(),
    )


@mcp.tool()
async def trigger_manual_update(params: TriggerManualUpdateParam, ctx: Context) -> Any:
    """Manually trigger a single execution of a subscription."""
    return await get_client().trigger_manual_update(
        sub_id=params.sub_id,
        api_key=get_user_token(),
    )


@mcp.tool()
async def set_subscription_status(params: SetSubscriptionStatusParam, ctx: Context) -> Any:
    """Set subscription status to ENABLED or DISABLED. WARNING: Disabling will prevent execution."""
    return await get_client().set_subscription_status(
        sub_id=params.sub_id,
        status=params.status,
        api_key=get_user_token(),
    )


@mcp.tool()
async def list_subscriptions(params: ListSubscriptionsParam, ctx: Context) -> Any:
    """List all subscriptions (optionally filtered by plugin)."""
    raw = await get_client().list_subscriptions(
        plugin_id=params.plugin_id,
        api_key=get_user_token(),
    )
    return json_to_toon(raw)


@mcp.tool()
async def get_subscription_status(params: GetSubscriptionStatusParam, ctx: Context) -> Any:
    """Returns current status, last error, and progress."""
    return await get_client().get_subscription_status(
        sub_id=params.sub_id,
        api_key=get_user_token(),
    )


# =============================================================================
# Plugin Discovery Tools
# =============================================================================


@mcp.tool()
async def list_plugins(ctx: Context) -> Any:
    """List all available data source plugins and their metadata."""
    raw = await get_client().list_plugins(api_key=get_user_token())
    return json_to_toon(raw)


@mcp.tool()
async def get_plugin_details(params: GetPluginDetailsParam, ctx: Context) -> Any:
    """Get full metadata (description, icon) for a specific plugin."""
    return await get_client().get_plugin_details(
        plugin_id=params.plugin_id,
        api_key=get_user_token(),
    )


@mcp.tool()
async def get_plugin_schema(params: GetPluginSchemaParam, ctx: Context) -> Any:
    """Returns the JSON Schema used to generate dynamic UI forms."""
    return await get_client().get_plugin_schema(
        plugin_id=params.plugin_id,
        api_key=get_user_token(),
    )


# =============================================================================
# System Health Tool
# =============================================================================


@mcp.tool()
async def get_system_health(ctx: Context) -> Any:
    """Checks connectivity to Redis, PostgreSQL, and the Manager API."""
    return await get_client().get_system_health(api_key=get_user_token())


# =============================================================================
# Custom Routes (Plugin-Specific)
# =============================================================================


@mcp.tool()
async def create_bible_subscription(params: CreateBibleSubscriptionParam, ctx: Context) -> Any:
    """Create a Bible download subscription."""
    return await get_client().create_subscription(
        plugin_id="eBiblePlugin",
        name=params.name,
        config={"version": params.version},
        cron=params.cron,
        access_level=params.access_level,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool()
async def create_youtube_subscription(params: CreateYouTubeSubscriptionParam, ctx: Context) -> Any:
    """Create a YouTube channel transcript subscription."""
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
        access_level=params.access_level,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool()
async def create_imap_subscription(params: CreateIMAPSubscriptionParam, ctx: Context) -> Any:
    """Create an IMAP email folder watch subscription."""
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
        access_level=params.access_level,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool()
async def create_crawl4ai_subscription(params: CreateCrawl4AISubscriptionParam, ctx: Context) -> Any:
    """Create a website crawl subscription using Crawl4AI."""
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
        access_level=params.access_level,
        description=None,
        api_key=get_user_token(),
    )


@mcp.tool()
async def get_bible_versions(params: GetBibleVersionsParam, ctx: Context) -> Any:
    """Returns available Bible versions with labels and language groupings from the eBible plugin."""
    raw = await get_client().get_bible_versions(api_key=get_user_token())
    if params.language and raw.get("groups"):
        lang_versions = set(raw["groups"].get(params.language, []))
        raw["versions"] = [v for v in raw["versions"] if v in lang_versions]
        raw["labels"] = {k: v for k, v in raw["labels"].items() if k in lang_versions}
    return json_to_toon(raw)


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
