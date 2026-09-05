# AutoKB MCP Server

A Model Context Protocol (MCP) server that acts as a proxy between AI assistants and the AutoKB backend API. It exposes AutoKB's subscription management, plugin discovery, and system health functionality as MCP tools.

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `AUTOKB_BASE_URL` | Yes | — | AutoKB web service URL (e.g. `http://autokb-web:80`) |
| `MCP_PORT` | No | `80` | Port for the MCP server to listen on |

The MCP server has **no awareness of any API key**. It must never receive one
via environment, config, or fallback.

## Authentication

The MCP server is a **pure transparent relay**. It extracts the
`Authorization: Bearer <token>` header from incoming MCP HTTP requests and
forwards it verbatim to `AUTOKB_BASE_URL`. If the client sends no token,
nothing is forwarded — the server never invents, reads, or falls back to any
server-side key.

Authentication and authorization are the **backend's responsibility**: the
AutoKB web layer validates the token and rejects unauthenticated requests
(HTTP 401). Operators configure the accepted key on the backend only, not on
this MCP server.

## Connection Instructions

The MCP server runs as an ASGI application on `http://<host>:<port>/mcp`. Connect your MCP client to this endpoint with streamable HTTP transport.

## Tools

### Subscription Management
- `create_subscription` — Create a new subscription for a plugin (starts a background process)
- `edit_subscription` — Update a subscription's config, cron, or access level
- `delete_subscription` — Permanently remove a subscription
- `trigger_manual_update` — Trigger immediate execution of a subscription
- `set_subscription_status` — Enable or disable a subscription
- `list_subscriptions` — List all subscriptions (optionally filtered by plugin)
- `get_subscription_status` — Get current status, last error, and progress

### Plugin Discovery
- `list_plugins` — List all available data source plugins
- `get_plugin_details` — Get full metadata for a specific plugin
- `get_plugin_schema` — Get the JSON Schema for a plugin's config form

### System Health
- `get_system_health` — Check connectivity to Redis, PostgreSQL, and the Manager

## TOON Optimization

For list operations that return bulk data (`list_subscriptions`, `list_plugins`), the server applies TOON (Token-Optimized Object Notation) compression to reduce token usage by 30-60%, improving context window efficiency and reducing costs.
