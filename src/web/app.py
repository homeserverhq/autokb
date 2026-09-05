"""The AutoKB Web UI — aiohttp application with reverse-proxy to the Manager.

The web UI container is the primary front-facing access point. It:

* Serves the SPA (HTML/JS/CSS) from ``/web/templates`` and ``/web/static``.
* Proxies ``/api/*`` requests to the Manager at ``AUTOKB_MANAGER_URL``,
  injecting the ``AUTOKB_BACKEND_API_KEY`` header.
* Handles ``/auth/*`` (login/logout) locally.
* Authenticates all inbound requests (browser session cookie or a Bearer
  token). This is also the auth boundary for the MCP server, which relays
  the client's ``Authorization`` header verbatim.

Auth is session-based for the browser; API clients (including the MCP
server, which is a pure transparent relay with no key awareness) present
``Authorization: Bearer <AUTOKB_API_KEY>``.
"""

import asyncio
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
import traceback
from typing import Any, Awaitable, Callable, Dict, List, Optional

from datetime import datetime, timezone

import aiohttp
from aiohttp import web

ADMIN_USERNAME = os.environ.get("AUTOKB_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("AUTOKB_ADMIN_PASSWORD", "")
API_KEY = os.environ.get("AUTOKB_API_KEY", "")
BACKEND_API_KEY = os.environ.get("AUTOKB_BACKEND_API_KEY", "")
WEBHOOK_API_KEY = os.environ.get("AUTOKB_WEBHOOK_API_KEY", "")
MANAGER_URL = os.environ.get("AUTOKB_MANAGER_URL", "http://autokb-manager:80")
WEBUI_PORT = int(os.environ.get("WEBUI_PORT", "80"))
COOKIE_SECURE = os.environ.get("AUTOKB_USE_HTTPS", "0") == "1"

LOG_FILE = "/logs/web.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


def _assert_secrets_configured() -> None:
    """Refuse to boot with empty secrets. A default-empty environment
    otherwise turns 'Bearer ' (empty token) into valid authentication and
    leaves a passwordless admin sitting on the network."""
    missing = []
    if not ADMIN_PASSWORD:
        missing.append("AUTOKB_ADMIN_PASSWORD")
    if not API_KEY:
        missing.append("AUTOKB_API_KEY")
    if not BACKEND_API_KEY:
        missing.append("AUTOKB_BACKEND_API_KEY")
    if missing:
        print(
            "[web] FATAL: refusing to start: the following required secrets are not configured: "
            + ", ".join(missing),
            file=sys.stderr,
        )
        print(
            "[web] Set them (e.g. in stack.env / docker-compose environment) and restart.",
            file=sys.stderr,
        )
        sys.exit(1)


_assert_secrets_configured()


def _log(msg: str, **fields: Any) -> None:
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%d %H:%M:%S") + f".{now.microsecond // 1000:03d}"
    rest = " ".join(f"{k}={v}" for k, v in fields.items())
    line = f"{ts} [INFO] - [web] {msg} {rest}".rstrip()
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass
    print(line, file=sys.stdout, flush=True)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------
def _make_token(username: str) -> str:
    if not API_KEY:
        raise RuntimeError("AUTOKB_API_KEY not configured; cannot mint session tokens")
    payload = f"{username}:{int(time.time())}"
    sig = hmac.new(API_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{payload}:{sig}"


def _verify_token(token: str) -> Optional[str]:
    try:
        username, ts, sig = token.rsplit(":", 2)
    except ValueError:
        return None
    payload = f"{username}:{ts}"
    expected = hmac.new(API_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(expected, sig):
        return None
    try:
        ts_int = int(ts)
    except ValueError:
        return None
    if time.time() - ts_int > 24 * 3600:
        return None
    return username


def _bearer_token(request: web.Request) -> Optional[str]:
    """Return the non-empty Bearer token from the request, or None.

    A bare ``Authorization: Bearer `` (empty token) is treated as absent
    (returned as None) so an unset API key can never be 'matched' by an
    empty value.
    """
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        if token:
            return token
    return None


def _token_matches(token: Optional[str], expected: str) -> bool:
    """Constant-time comparison that rejects empty tokens and empty expected
    values, closing the 'empty == empty' authentication bypass."""
    return token is not None and bool(token) and bool(expected) and hmac.compare_digest(token, expected)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(HERE, "templates")
STATIC_DIR = os.path.join(HERE, "static")


async def _is_authed(request: web.Request) -> bool:
    """Check whether the request has a valid session cookie or MCP Bearer token."""
    session_token = request.cookies.get("autokb_session")
    if session_token and _verify_token(session_token):
        return True
    return _token_matches(_bearer_token(request), API_KEY)


async def handle_index(request: web.Request) -> web.Response:
    """Serve the SPA index for the main app.

    Auth-gated: unauthenticated requests receive a 302 to /login. Authenticated
    requests get the SPA shell, which then loads the dashboard view.
    """
    if not await _is_authed(request):
        raise web.HTTPFound("/login")
    with open(os.path.join(TEMPLATES_DIR, "index.html"), "rb") as f:
        return web.Response(body=f.read(), content_type="text/html")


async def handle_login(request: web.Request) -> web.Response:
    """Serve the SPA shell at /login. No auth required (the page IS the login)."""
    with open(os.path.join(TEMPLATES_DIR, "index.html"), "rb") as f:
        return web.Response(body=f.read(), content_type="text/html")


async def handle_static(request: web.Request) -> web.Response:
    # Derive the relative path from request.path (works whether routed via
    # the explicit /static/{path:.*} route or the catch-all /{tail:.*} route).
    rel = request.path[len("/static/"):]
    if ".." in rel.split("/"):
        return web.Response(status=404, text="Not found")
    full = os.path.normpath(os.path.join(STATIC_DIR, rel))
    if not full.startswith(STATIC_DIR) or not os.path.isfile(full):
        return web.Response(status=404, text="Not found")
    return web.FileResponse(full)


async def handle_auth_login(request: web.Request) -> web.Response:
    data = await request.json()
    username = data.get("username", "")
    password = data.get("password", "")
    if username != ADMIN_USERNAME or password != ADMIN_PASSWORD:
        return web.json_response({"error": "Invalid credentials"}, status=401)
    token = _make_token(username)
    resp = web.json_response({"ok": True, "username": username})
    # Cookie
    resp.set_cookie(
        "autokb_session", token, httponly=True, samesite="Lax", path="/",
        max_age=24 * 3600, secure=COOKIE_SECURE,
    )
    return resp


async def handle_auth_logout(request: web.Request) -> web.Response:
    resp = web.json_response({"ok": True})
    resp.del_cookie("autokb_session", path="/")
    return resp


async def handle_auth_check(request: web.Request) -> web.Response:
    # Check cookie or Bearer token
    session_token = request.cookies.get("autokb_session")
    if session_token and _verify_token(session_token):
        username = _verify_token(session_token)
        return web.json_response({"authenticated": True, "username": username, "auth_type": "session"})
    if _token_matches(_bearer_token(request), API_KEY):
        return web.json_response({"authenticated": True, "username": "mcp", "auth_type": "bearer"})
    return web.json_response({"authenticated": False}, status=401)


# ---------------------------------------------------------------------------
# Reverse proxy
# ---------------------------------------------------------------------------
async def handle_api_proxy(request: web.Request) -> web.Response:
    """Forward /api/* to the Manager.

    Auth is enforced at the top of this function: either a valid session
    cookie or a valid MCP Bearer token is required.
    """
    # Authentication
    session_token = request.cookies.get("autokb_session")
    token = _bearer_token(request)
    authed = False
    if session_token and _verify_token(session_token):
        authed = True
    if not authed and _token_matches(token, API_KEY):
        authed = True
    if not authed and _token_matches(token, WEBHOOK_API_KEY):
        if request.rel_url.path.startswith("/api/subscriptions/") and request.rel_url.path.endswith("/trigger"):
            authed = True
    if not authed:
        return web.json_response({"error": "Authentication required"}, status=401)

    # Build the upstream URL
    path = request.rel_url.path_qs  # includes the leading /api
    upstream = f"{MANAGER_URL.rstrip('/')}/{path}"  # path already starts with /api
    if not upstream.startswith(MANAGER_URL.rstrip("/") + "/api"):
        # Avoid double-prefixing
        upstream = f"{MANAGER_URL.rstrip('/')}{request.rel_url.path_qs}"

    # Build headers — inject backend API key, forward original auth if present
    headers: Dict[str, str] = dict(request.headers)
    headers["X-Api-Key"] = BACKEND_API_KEY
    # Don't forward the inbound Host header
    headers.pop("Host", None)
    # The aiohttp ClientSession can have issues with content-length; remove
    headers.pop("Content-Length", None)

    body: Optional[bytes] = None
    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        body = await request.read()
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
            async with session.request(
                method=request.method,
                url=upstream,
                headers=headers,
                data=body,
                allow_redirects=False,
            ) as resp:
                content = await resp.read()
                # Build the response, preserving status
                out_headers = dict(resp.headers)
                # Strip hop-by-hop / problematic headers
                for k in ("Content-Encoding", "Transfer-Encoding", "Connection", "Keep-Alive"):
                    out_headers.pop(k, None)
                # aiohttp forbids passing both Content-Type in headers and
                # content_type kwarg; use the header if present.
                content_type = out_headers.pop("Content-Type", None) or "application/json"
                return web.Response(
                    status=resp.status,
                    body=content,
                    headers=out_headers,
                    content_type=content_type,
                )
    except aiohttp.ClientError as exc:
        _log("api_proxy_error", error=str(exc), path=path)
        return web.json_response({"error": f"Upstream error: {exc}"}, status=502)


async def handle_sse_proxy(request: web.Request) -> web.StreamResponse:
    """Proxy SSE events from the Manager.

    The Manager's SSE endpoint yields ``data: ...\\n\\n`` frames as the
    FastAPI StreamingResponse is consumed. The aiohttp client must read
    those frames in small chunks and the aiohttp server must flush after
    every write, otherwise browsers (and ``requests``) see an empty body
    until the buffer fills or the connection closes.
    """
    # Auth
    session_token = request.cookies.get("autokb_session")
    authed = False
    if session_token and _verify_token(session_token):
        authed = True
    if not authed and _token_matches(_bearer_token(request), API_KEY):
        authed = True
    if not authed:
        return web.json_response({"error": "Authentication required"}, status=401)

    upstream = f"{MANAGER_URL.rstrip('/')}/api/events"
    headers = {
        "X-Api-Key": BACKEND_API_KEY,
        "Accept": "text/event-stream",
    }

    out_resp = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    # Disable Nagle's algorithm so each write goes out immediately.
    transport = request.transport
    if transport is not None:
        try:
            sock = transport.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(__import__("socket").IPPROTO_TCP, __import__("socket").TCP_NODELAY, 1)
        except Exception:
            pass
    await out_resp.prepare(request)

    try:
        # 60s total=None so the client-side keepalive never times out the proxy.
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(upstream, headers=headers) as resp:
                # Read in small chunks (1 byte) and flush after each write.
                # SSE frames are tiny (a few hundred bytes); a small read
                # window means the client gets events with sub-100ms latency.
                async for chunk in resp.content.iter_chunked(256):
                    if not chunk:
                        continue
                    await out_resp.write(chunk)
                    # Force the write buffer to the socket so the browser
                    # sees the event immediately rather than waiting for
                    # the kernel to flush.
                    await out_resp.drain()
    except (ConnectionResetError, asyncio.CancelledError, aiohttp.ClientError):
        pass
    except Exception as exc:  # noqa: BLE001
        _log("sse_proxy_error", error=str(exc))
    return out_resp


# ---------------------------------------------------------------------------
# Asset files (plugin icons, default icon)
# ---------------------------------------------------------------------------
ASSETS_DIR = os.environ.get("AUTOKB_ASSETS_DIR", "/assets")


async def handle_assets(request: web.Request) -> web.Response:
    # Derive the relative path from request.path (works whether routed via
    # the explicit /assets/{path:.*} route or the catch-all /{tail:.*} route).
    rel = request.path[len("/assets/"):]
    # Defense in depth: reject any traversal attempts.
    if ".." in rel.split("/"):
        return web.Response(status=404, text="Not found")
    full = os.path.normpath(os.path.join(ASSETS_DIR, rel))
    if not full.startswith(ASSETS_DIR) or not os.path.isfile(full):
        return web.Response(status=404, text="Not found")
    return web.FileResponse(full)


# ---------------------------------------------------------------------------
# Catch-all
# ---------------------------------------------------------------------------
async def handle_catchall(request: web.Request) -> web.Response:
    if request.path.startswith("/api/"):
        return await handle_api_proxy(request)
    if request.path.startswith("/auth/"):
        if request.path == "/auth/login":
            return await handle_auth_login(request)
        if request.path == "/auth/logout":
            return await handle_auth_logout(request)
        if request.path == "/auth/check":
            return await handle_auth_check(request)
        return web.json_response({"error": "Unknown auth route"}, status=404)
    if request.path.startswith("/static/"):
        return await handle_static(request)
    if request.path.startswith("/assets/"):
        return await handle_assets(request)
    return await handle_index(request)


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_route("*", "/api/events", handle_sse_proxy)
    app.router.add_route("*", "/auth/login", handle_auth_login)
    app.router.add_route("*", "/auth/logout", handle_auth_logout)
    app.router.add_route("*", "/auth/check", handle_auth_check)
    app.router.add_route("*", "/api/{tail:.*}", handle_api_proxy)
    app.router.add_route("GET", "/static/{path:.*}", handle_static)
    app.router.add_route("GET", "/assets/{path:.*}", handle_assets)
    # Auth-gated SPA shell at the root, and the login page at /login.
    app.router.add_route("*", "/", handle_index)
    app.router.add_route("*", "/login", handle_login)
    # Catch-all for any other path: serve static, assets, or fall through to
    # the SPA shell. The SPA's client-side auth check decides whether to
    # show the login form or the app view.
    app.router.add_route("*", "/{tail:.*}", handle_catchall)
    return app


app = make_app()
