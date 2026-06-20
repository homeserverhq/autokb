"""Dynamic mounting of plugin-defined custom API routes.

Each plugin can declare ``PluginRoute`` instances via ``get_custom_routes()``.
This module iterates over the in-memory registry and creates FastAPI route
handlers that forward into the plugin's handler functions.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, List

from fastapi import APIRouter, Request

if TYPE_CHECKING:
    from manager.registry import ManagerPluginRegistry  # noqa: F401


def mount_plugin_routes(router: APIRouter, registry: "ManagerPluginRegistry") -> None:
    """Register all plugin-defined custom routes on the given FastAPI router.

    Plugin authors return ``PluginRoute(path="/status", method="GET", handler=...)``
    from ``get_custom_routes()``. Each custom route is mounted at::

        /api/plugins/{plugin_id}{path}
    """
    for rec in registry.list_records():
        for route in rec.cls().get_custom_routes() or []:
            method = (route.method or "GET").upper()
            path = f"/api/plugins/{rec.plugin_id}{route.path}"
            handler = route.handler
            if method == "GET":
                @router.get(path)
                async def _get_handler(_request: Request, _h=handler):  # noqa: ANN001
                    return _await_if_coro(_h())
            elif method == "POST":
                @router.post(path)
                async def _post_handler(_request: Request, _h=handler):  # noqa: ANN001
                    return _await_if_coro(_h())
            elif method == "PUT":
                @router.put(path)
                async def _put_handler(_request: Request, _h=handler):  # noqa: ANN001
                    return _await_if_coro(_h())
            elif method == "DELETE":
                @router.delete(path)
                async def _delete_handler(_request: Request, _h=handler):  # noqa: ANN001
                    return _await_if_coro(_h())
            elif method == "PATCH":
                @router.patch(path)
                async def _patch_handler(_request: Request, _h=handler):  # noqa: ANN001
                    return _await_if_coro(_h())


def _await_if_coro(value: Any) -> Any:
    import asyncio
    if asyncio.iscoroutine(value):
        return asyncio.get_event_loop().run_until_complete(value)  # type: ignore[return-value]
    return value
