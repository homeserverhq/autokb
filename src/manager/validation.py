"""Target / subscription validation helpers for the Manager API.

Extracted from the Manager monolith so validation semantics live in one
place and are exercised identically by every endpoint.
"""

from fastapi import HTTPException

from utils.constants import ACCESS_PRIVATE, ACCESS_PUBLIC
from utils.misc_utils import DecryptionError, sanitize_name


_TARGET_NAME_MAX_LEN = 255
_SUBSCRIPTION_NAME_MAX_LEN = 255


def _validate_target_name(name: str) -> str:
    """Validate a Data Target name using the canonical-form check.

    The name is accepted only if it is already in canonical form
    (``sanitize_name(name) == name``) — i.e. sanitization changes nothing.
    The provided name is never converted or coalesced; invalid names are
    rejected outright with a clear error.
    """
    if not isinstance(name, str) or not name.strip():
        raise HTTPException(status_code=400, detail="Target name is required")
    name = name.strip()
    if len(name) > _TARGET_NAME_MAX_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Target name is too long ({len(name)} chars; max {_TARGET_NAME_MAX_LEN})",
        )
    try:
        if sanitize_name(name) != name:
            raise ValueError(name)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Target name {name!r} is invalid. Use only letters, numbers, "
                "periods, and hyphens — no spaces or symbols, no '..', and no "
                "leading or trailing period."
            ),
        )
    return name


def _validate_schedule_times(start, end) -> None:
    """Validate an optional daily upload window (``"HH:MM"``, 24-hour).

    Both empty → no scheduling (OK). Exactly one set, unparseable, out-of-range,
    or equal bounds → 400. Times are interpreted in the host's local timezone.
    """
    def _clean(v) -> str:
        if v is None:
            return ""
        if not isinstance(v, str):
            raise HTTPException(status_code=400, detail="schedule_start/schedule_end must be strings")
        return v.strip()

    s, e = _clean(start), _clean(end)
    if not s and not e:
        return
    if not s or not e:
        raise HTTPException(
            status_code=400,
            detail="schedule_start and schedule_end must both be set, or both left blank",
        )
    try:
        sh, sm_ = s.split(":", 1)
        eh, em_ = e.split(":", 1)
        shh, smm = int(sh), int(sm_)
        ehh, emm = int(eh), int(em_)
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(status_code=400, detail="Schedule times must be HH:MM (24-hour)")
    if not (0 <= shh <= 23 and 0 <= smm <= 59 and 0 <= ehh <= 23 and 0 <= emm <= 59):
        raise HTTPException(status_code=400, detail="Schedule times must be HH:MM (24-hour)")
    if shh * 60 + smm == ehh * 60 + emm:
        raise HTTPException(status_code=400, detail="Schedule start and end must differ")


def _validate_pages_per_batch(value) -> int:
    """Validate an optional ``pages_per_batch`` (int in [1, 100], default 10).

    None → 10. Non-integer, boolean, or out-of-range → 400.
    """
    if value is None:
        return 10
    if isinstance(value, bool) or not isinstance(value, int):
        raise HTTPException(
            status_code=400,
            detail="pages_per_batch must be an integer between 1 and 100",
        )
    if not (1 <= value <= 100):
        raise HTTPException(
            status_code=400,
            detail="pages_per_batch must be between 1 and 100",
        )
    return value


def _validate_access_level(value) -> str:
    """Validate an optional ``access_level`` (PRIVATE or PUBLIC, default PRIVATE).

    None → PRIVATE. Anything else outside the two allowed values → 400.
    """
    if value is None:
        return ACCESS_PRIVATE
    if value not in (ACCESS_PRIVATE, ACCESS_PUBLIC):
        raise HTTPException(
            status_code=400,
            detail="access_level must be either PRIVATE or PUBLIC",
        )
    return value


def _validate_subscription_name(name: str) -> str:
    """Validate a subscription name using the canonical-form check.

    Mirrors ``_validate_target_name`` but periods are not allowed —
    subscription names accept only letters, numbers, and hyphens.
    The provided name is never converted or coalesced; invalid names are
    rejected outright with a clear error.
    """
    if not isinstance(name, str) or not name.strip():
        raise HTTPException(status_code=400, detail="Subscription name is required")
    name = name.strip()
    if len(name) > _SUBSCRIPTION_NAME_MAX_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"Subscription name is too long ({len(name)} chars; max {_SUBSCRIPTION_NAME_MAX_LEN})",
        )
    if "." in name:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Subscription name {name!r} is invalid. Use only letters, "
                "numbers, and hyphens — no periods, spaces, or symbols."
            ),
        )
    try:
        if sanitize_name(name) != name:
            raise ValueError(name)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Subscription name {name!r} is invalid. Use only letters, "
                "numbers, and hyphens — no periods, spaces, or symbols."
            ),
        )
    return name


def _ensure_target_remote(ds_row, db, sink_registry, log) -> None:
    """Synchronously ensure the remote target resource exists.

    Called at target create/update time, BEFORE any queue items are pushed,
    so the remote resource (e.g. the OpenWebUI Knowledge Base) is guaranteed
    to exist before the worker reconciles. The recon engine must never create
    the remote target — it only reads ``remote_target_id`` from the DB.
    """
    if sink_registry is None:
        raise HTTPException(status_code=502, detail="Sink registry is not loaded")
    svc_row = db.get_sink(ds_row.service_id)
    if svc_row is None:
        raise HTTPException(status_code=404, detail="Sink not found")
    try:
        api_key = db.decrypt_target_api_key(ds_row)
    except DecryptionError as exc:
        raise HTTPException(status_code=500, detail=f"Target API key decryption failed: {exc}") from exc
    # Patch the decrypted api_key onto a copy of the row so the sink instance
    # (which reads ``target_row.api_key``) gets the real key.
    import copy
    patched = copy.copy(ds_row)
    patched.api_key = api_key
    svc = sink_registry.load_service_for_recon(svc_row.name, patched, db)
    if svc is None:
        raise HTTPException(status_code=502, detail=f"Sink service {svc_row.name!r} is not available")
    if svc.remote_target_id:
        return
    try:
        svc.base_add_target()
    except Exception as exc:  # noqa: BLE001
        log.error("target_remote_create_failed", target_id=ds_row.id,
                  service=svc_row.name, error=str(exc))