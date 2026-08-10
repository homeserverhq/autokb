"""Misc utilities: file I/O, sanitization, logging, SMTP, etc."""

import base64
import json
import logging
import os
import re
import smtplib
import ssl
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Any, Dict, List, Optional, Tuple

import structlog

from .constants import EXTRA_PARAM_KEYS, LOG_LEVEL


# ---------------------------------------------------------------------------
# Logging — structured JSON-ish lines with the mandatory fields
# ---------------------------------------------------------------------------
def _configure_logging(component: str, log_file: Optional[str] = None) -> structlog.stdlib.BoundLogger:
    """Configure structlog for the given component and optional log file.

    If ``log_file`` is given, logs are written to that file in addition to
    stdout. The format follows the spec's mandatory fields section.
    """
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        _inject_component(component),
        _format_event,
    ]

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(LOG_LEVEL)

    handlers: List[logging.Handler] = []

    # Stream handler (stdout) — skip when stdout is piped (child process)
    # to prevent pipe buffer deadlock when parent never reads the pipe.
    if not os.environ.get("KB_NO_STDOUT_LOG"):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(LOG_LEVEL)
        stream_handler.setFormatter(logging.Formatter("%(message)s"))
        root.addHandler(stream_handler)
        handlers.append(stream_handler)

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        handlers.append(file_handler)
        root.addHandler(file_handler)

    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, LOG_LEVEL, logging.INFO)),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return structlog.get_logger(component)


def _inject_component(component: str):
    def proc(logger, method_name, event_dict):
        event_dict.setdefault("component", component)
        return event_dict
    return proc


def _format_event(logger, method_name, event_dict):
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%d %H:%M:%S") + f".{now.microsecond // 1000:03d}"
    level = event_dict.pop("level", method_name.upper()).upper()
    event_dict.setdefault("action", "-")
    event_dict.setdefault("result", "-")
    event_dict.setdefault("subscription_id", "-")
    event_dict.setdefault("subscription_name", "-")
    rest = " ".join(
        f"{k}={_stringify(v)}" for k, v in event_dict.items() if k != "event"
    ) + f" event={event_dict.get('event', '')}"
    return f"{ts} [{level}] - {rest}"


def _stringify(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, str):
        s = v.replace(" ", "_").replace("\n", "\\n")
        if '"' in s:
            s = s.replace('"', '\\"')
        return f'"{s}"'
    return str(v)


def get_logger(component: str, log_file: Optional[str] = None) -> structlog.stdlib.BoundLogger:
    """Convenience wrapper for ``_configure_logging``."""
    return _configure_logging(component, log_file)


# ---------------------------------------------------------------------------
# sanitize_name
# ---------------------------------------------------------------------------
_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9.\-]")
_PERIOD_RE = re.compile(r"\.+")


def sanitize_name(name: str) -> str:
    """Strip everything except ``[a-zA-Z0-9.\\-]``; enforce period rules.

    Rules:
      * After stripping, no two periods may be adjacent.
      * The first character must be alphanumeric (no leading period).
      * The last character must be alphanumeric (no trailing period).
      * Must have at least one valid character.

    Raises ``ValueError`` for empty / period-only / rule-violating input.
    Idempotent: ``sanitize_name(sanitize_name(x)) == sanitize_name(x)``.
    """
    if not isinstance(name, str):
        raise ValueError(f"sanitize_name: name must be a string, got {type(name).__name__}")
    sanitized = _SANITIZE_RE.sub("", name)
    if not sanitized:
        raise ValueError(f"sanitize_name: no valid content in name {name!r}")
    if ".." in sanitized:
        # collapse any consecutive periods
        sanitized = _PERIOD_RE.sub(".", sanitized)
    if sanitized[0] == "." or sanitized[-1] == ".":
        raise ValueError(f"sanitize_name: period cannot be first or last char in {name!r}")
    return sanitized


def plugin_id_from_metadata(metadata: Dict[str, Any]) -> str:
    """Return the canonical plugin_id for a metadata dict."""
    return sanitize_name(metadata["name"])


def resolve_service_icon(cls: Any, metadata: Dict[str, Any],
                         assets_dir: str = "") -> str:
    """Derive the icon asset filename for a service class.

    Uses ``cls.icon()`` when defined (BaseSink / BaseSubscription derive it
    from ``metadata["name"]``); otherwise falls back to the ``icon`` key in
    ``metadata``. If the referenced file is missing from the assets
    directory, returns ``default_icon.png``. When the assets directory does
    not exist (e.g. the Worker, which does not mount ``/assets``), the
    derived name is returned unchanged.
    """
    if hasattr(cls, "icon"):
        try:
            icon = cls.icon()
        except Exception:  # noqa: BLE001
            icon = metadata.get("icon", "default_icon.png")
    else:
        icon = metadata.get("icon", "default_icon.png")
    if not icon:
        return "default_icon.png"
    assets_dir = assets_dir or os.environ.get("AUTOKB_ASSETS_DIR", "/assets")
    if os.path.isdir(assets_dir) and not os.path.isfile(os.path.join(assets_dir, icon)):
        return "default_icon.png"
    return icon


# ---------------------------------------------------------------------------
# Fernet-style symmetric encryption (uses base64 + AES via cryptography)
# ---------------------------------------------------------------------------
class PasswordCipher:
    """Lightweight Fernet-compatible wrapper.

    We use the standard `cryptography.fernet.Fernet` class. The key is
    base64url-encoded 32-byte value provided via the ``ENCRYPTION_KEY``
    env var. The key string is expected to be a base64url 32-byte value
    (44 chars). If it isn't, we attempt to derive a valid Fernet key from
    it deterministically.
    """

    def __init__(self, key: Optional[str] = None):
        from cryptography.fernet import Fernet
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

        key = key or os.environ.get("ENCRYPTION_KEY", "")
        if not key:
            raise ValueError("ENCRYPTION_KEY env var must be set")

        # If key is already valid Fernet format (44-char base64url -> 32 bytes), use it.
        try:
            self._fernet = Fernet(key.encode() if isinstance(key, str) else key)
            return
        except Exception:
            pass

        # Derive a 32-byte key from the supplied string using PBKDF2.
        salt = b"autokb-fernet-salt-v1"
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100_000)
        derived = base64.urlsafe_b64encode(kdf.derive(key.encode()))
        self._fernet = Fernet(derived)

    def encrypt(self, plaintext: str) -> str:
        if plaintext is None:
            return None
        return self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")

    def decrypt(self, token: str) -> str:
        if token is None:
            return None
        return self._fernet.decrypt(token.encode("ascii")).decode("utf-8")


# ---------------------------------------------------------------------------
# JSON Schema helpers
# ---------------------------------------------------------------------------
def augment_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Append _extra_param_1/2/3 string fields to the schema properties.

    The 3 extra params are reserved for future use (see DesignSpecification
    §3.3, "Extra Parameters invariant"). They are exposed in the schema so the
    UI can render inputs for them, but they are not declared ``required`` —
    the system auto-injects them into every subscription's ``config`` via
    :func:`ensure_extra_params` at the persistence layer.
    """
    schema = json.loads(json.dumps(schema))  # deep copy
    props = schema.setdefault("properties", {})
    schema.setdefault("required", [])
    for key in EXTRA_PARAM_KEYS:
        if key not in props:
            props[key] = {"type": "string"}
    return schema


def ensure_extra_params(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Return a new dict with all ``EXTRA_PARAM_KEYS`` present (defaulting to ``""``).

    Invariant: every subscription's ``config`` JSONB must contain
    ``_extra_param_1``, ``_extra_param_2``, and ``_extra_param_3``. They are
    placeholders for future data-source schema changes (see
    DesignSpecification §3.3) so that, if a data source adds a required
    credential later, the value can be assigned to one of these fields
    without rebuilding all subscriptions. Existing values are preserved
    (so a plugin that later adopts one of the fields keeps its value);
    missing keys are filled in with the empty string.

    The function never mutates the input dict; it returns a shallow copy.
    """
    out: Dict[str, Any] = dict(config) if config else {}
    for key in EXTRA_PARAM_KEYS:
        out.setdefault(key, "")
    return out


def collect_password_field_names(schema: Dict[str, Any]) -> List[str]:
    """Return the property names whose schema has ``format == "password"``."""
    out: List[str] = []
    for key, spec in (schema.get("properties") or {}).items():
        if isinstance(spec, dict) and spec.get("format") == "password":
            out.append(key)
    return out


def strip_password_fields(config: Dict[str, Any], password_fields: List[str]) -> Dict[str, Any]:
    """Return a shallow copy of ``config`` with password fields removed."""
    if not config:
        return config
    return {k: v for k, v in config.items() if k not in password_fields}


def encrypt_password_fields(config: Dict[str, Any], password_fields: List[str], cipher: PasswordCipher) -> Dict[str, Any]:
    """Encrypt any password-format fields present in ``config``."""
    if not config:
        return config
    out: Dict[str, Any] = {}
    for k, v in config.items():
        if k in password_fields and isinstance(v, str):
            out[k] = cipher.encrypt(v)
        else:
            out[k] = v
    return out


def decrypt_password_fields(config: Dict[str, Any], password_fields: List[str], cipher: PasswordCipher) -> Dict[str, Any]:
    """Decrypt password-format fields, leaving other fields untouched."""
    if not config:
        return config
    out: Dict[str, Any] = {}
    for k, v in config.items():
        if k in password_fields and isinstance(v, str) and v:
            try:
                out[k] = cipher.decrypt(v)
            except Exception:
                # If decryption fails (e.g. plain text during dev), pass through
                out[k] = v
        else:
            out[k] = v
    return out


def validate_config_against_schema(config: Dict[str, Any], schema: Dict[str, Any], password_fields: List[str], *, enforce_required_password: bool = True) -> None:
    """Validate ``config`` against ``schema``.

    Performs structural checks compatible with JSON Schema's ``type``,
    ``format`` (only ``password`` recognised here), ``enum``, and the
    ``required`` list. Raises ``ValueError`` on the first violation.
    """
    props = (schema or {}).get("properties", {}) or {}
    required = (schema or {}).get("required", []) or []

    if not isinstance(config, dict):
        raise ValueError("config must be a JSON object")

    # Required check (password fields optionally relaxed on edit)
    for key in required:
        if key not in config:
            if key in password_fields and not enforce_required_password:
                continue
            raise ValueError(f"Missing required field: {key}")

    for key, value in config.items():
        if key not in props:
            raise ValueError(f"Unknown field: {key}")
        spec = props[key] or {}
        expected_type = spec.get("type")
        if expected_type:
            ok = _type_matches(value, expected_type)
            if not ok:
                raise ValueError(f"Field {key!r} must be of type {expected_type}")
        if "enum" in spec and value not in spec["enum"]:
            raise ValueError(f"Field {key!r} must be one of {spec['enum']}, got {value!r}")
        if "minLength" in spec and isinstance(value, str) and len(value) < spec["minLength"]:
            raise ValueError(f"Field {key!r} must be at least {spec['minLength']} characters")
        if "maxLength" in spec and isinstance(value, str) and len(value) > spec["maxLength"]:
            raise ValueError(f"Field {key!r} must be at most {spec['maxLength']} characters")
        if "pattern" in spec and isinstance(value, str):
            if not re.search(spec["pattern"], value):
                raise ValueError(f"Field {key!r} does not match required pattern")
        if "minimum" in spec and isinstance(value, (int, float)) and value < spec["minimum"]:
            raise ValueError(f"Field {key!r} must be >= {spec['minimum']}")
        if "maximum" in spec and isinstance(value, (int, float)) and value > spec["maximum"]:
            raise ValueError(f"Field {key!r} must be <= {spec['maximum']}")


def _type_matches(value: Any, expected: str) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict)
    if expected == "null":
        return value is None
    return True


def schema_hash(schema: Dict[str, Any]) -> str:
    """sha256 of the *augmented* schema (deterministic JSON)."""
    import hashlib
    augmented = augment_schema(schema)
    payload = json.dumps(augmented, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


# ---------------------------------------------------------------------------
# Cron helpers (very simple, sufficient for our test cron expressions)
# ---------------------------------------------------------------------------
def is_valid_cron(expr: str) -> bool:
    """Validate a 5-field cron expression (minute hour dom month dow)."""
    if not expr or not isinstance(expr, str):
        return False
    parts = expr.split()
    if len(parts) != 5:
        return False
    for part in parts:
        if not _cron_field_valid(part):
            return False
    return True


def _cron_field_valid(field: str) -> bool:
    if field in ("*",):
        return True
    for piece in field.split(","):
        step = 1
        if "/" in piece:
            base, step_s = piece.split("/", 1)
            try:
                step = int(step_s)
            except ValueError:
                return False
        else:
            base = piece
        if base == "*":
            lo, hi = 0, 59
        elif "-" in base:
            lo_s, hi_s = base.split("-", 1)
            try:
                lo = int(lo_s); hi = int(hi_s)
            except ValueError:
                return False
        else:
            try:
                lo = hi = int(base)
            except ValueError:
                return False
        if step < 1:
            return False
        # Field ranges are permissive — minute/hour/dom/month/dow all
        # share [0, 59] union [1, 31] etc. We accept anything in [0, 365].
        if not (0 <= lo <= 365 and 0 <= hi <= 365):
            return False
    return True


def cron_due(expr: str, now: Optional[datetime] = None) -> bool:
    """Return True if the cron expression is "due" at ``now`` (i.e., matches
    the minute bucket). We use minute-resolution triggers for test speed."""
    if not is_valid_cron(expr):
        return False
    now = now or datetime.now()
    parts = expr.split()
    minute, hour, dom, month, dow = parts
    return (
        _cron_field_match(minute, now.minute, 0, 59)
        and _cron_field_match(hour, now.hour, 0, 23)
        and _cron_field_match(dom, now.day, 1, 31)
        and _cron_field_match(month, now.month, 1, 12)
        and _cron_field_match(dow, (now.weekday() + 1) % 7, 0, 6)  # cron: 0=Sun
    )


def _cron_field_match(field: str, value: int, lo: int, hi: int) -> bool:
    if field == "*":
        return True
    for piece in field.split(","):
        step = 1
        if "/" in piece:
            base, step_s = piece.split("/", 1)
            step = int(step_s)
        else:
            base = piece
        if base == "*":
            a, b = lo, hi
        elif "-" in base:
            a_s, b_s = base.split("-", 1)
            a, b = int(a_s), int(b_s)
        else:
            a = b = int(base)
        if a <= value <= b and ((value - a) % step == 0):
            return True
    return False


# ---------------------------------------------------------------------------
# SMTP
# ---------------------------------------------------------------------------
def send_smtp_notification(
    subject: str,
    body: str,
    smtp_host: str = "",
    smtp_port: int = 25,
    smtp_user: str = "",
    smtp_pass: str = "",
    from_addr: str = "",
    to_addr: str = "",
    use_tls: bool = True,
    use_ssl: bool = False,
    timeout: int = 5,
) -> bool:
    """Send an email via SMTP. Returns True on success.

    Failures are logged but never raised, so SMTP problems never break
    critical control flow.
    """
    smtp_host = smtp_host or os.environ.get("SMTP_HOST", "")
    smtp_port = int(smtp_port or os.environ.get("SMTP_PORT", "25"))
    smtp_user = smtp_user or os.environ.get("SMTP_USER", "")
    smtp_pass = smtp_pass or os.environ.get("SMTP_PASS", "")
    from_addr = from_addr or os.environ.get("SMTP_FROM", "autokb@localhost")
    to_addr = to_addr or os.environ.get("SMTP_NOTIFY_EMAIL", "")
    use_tls = use_tls if use_tls is not None else os.environ.get("SMTP_USE_TLS", "True").lower() == "true"
    use_ssl = use_ssl if use_ssl is not None else os.environ.get("SMTP_USE_SSL", "False").lower() == "true"

    if not smtp_host or not to_addr:
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr(("AutoKB", from_addr))
    msg["To"] = to_addr

    try:
        if use_ssl:
            ctx = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=timeout, context=ctx) as s:
                if smtp_user:
                    s.login(smtp_user, smtp_pass)
                s.sendmail(from_addr, [to_addr], msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=timeout) as s:
                s.ehlo()
                if use_tls:
                    s.starttls(context=ssl.create_default_context())
                    s.ehlo()
                if smtp_user:
                    s.login(smtp_user, smtp_pass)
                s.sendmail(from_addr, [to_addr], msg.as_string())
        return True
    except Exception as exc:  # pragma: no cover
        print(f"[smtp] send failed: {exc}", file=sys.stderr)
        return False


# ---------------------------------------------------------------------------
# UUIDv7 (we only need a monotonic UUID-like PK generator)
# ---------------------------------------------------------------------------
def uuid7() -> str:
    """Generate a UUIDv7-like id using the ``uuid7`` package if available,
    else fall back to a time-prefixed UUIDv4 for tests."""
    try:
        from uuid6 import uuid7 as _u7
        return str(_u7())
    except Exception:
        pass
    try:
        import uuid7 as _u7
        return str(_u7.uuid7())
    except Exception:
        # Fallback: time-ordered UUIDv4
        import uuid as _u
        ms = time.time_ns() // 1_000_000
        return str(_u.UUID(int=(ms << 80) | (_u.uuid4().int & ((1 << 80) - 1))))


def uuid4() -> str:
    """Generate a fully random (non-monotonic) UUIDv4 id."""
    import uuid as _u
    return str(_u.uuid4())


# ---------------------------------------------------------------------------
# SubscriptionCancelledError
# ---------------------------------------------------------------------------
class SubscriptionCancelledError(Exception):
    """Raised by progress_callback when the subscription has been disabled or
    deleted mid-execution. The Managed Execution Wrapper handles this
    silently (exit code 0, no EventLog/SMTP)."""


# ---------------------------------------------------------------------------
# SinkCancelledError
# ---------------------------------------------------------------------------
class SinkCancelledError(Exception):
    """Raised by ``BaseSink._check_cancel`` during sink recon when the target
    subscription link (or the whole subscription) is removed/disabled while a
    long-running upload loop is in flight.

    ``kind`` tells the recon engine how to react:
      * ``"link_removed"``  — the target_subscription row is gone / DELETED;
        halt this target and run the deferred-delete cleanup inline.
      * ``"link_disabled"`` — the target_subscription row is DISABLED; halt
        this target's uploads, keep rows already written.
      * ``"sub_gone"``      — the subscription row is gone / DELETED; abort
        the whole recon (the worker handles full cleanup next loop).
      * ``"sub_disabled"``  — the subscription is DISABLED; abort the whole
        recon (no cleanup needed).
    """

    def __init__(self, kind: str = "link_removed", *args):
        super().__init__(kind, *args)
        self.kind = kind


__all__ = [
    "get_logger",
    "sanitize_name",
    "plugin_id_from_metadata",
    "resolve_service_icon",
    "PasswordCipher",
    "augment_schema",
    "ensure_extra_params",
    "collect_password_field_names",
    "strip_password_fields",
    "encrypt_password_fields",
    "decrypt_password_fields",
    "validate_config_against_schema",
    "schema_hash",
    "is_valid_cron",
    "cron_due",
    "send_smtp_notification",
    "uuid7",
    "uuid4",
    "SubscriptionCancelledError",
    "SinkCancelledError",
]
