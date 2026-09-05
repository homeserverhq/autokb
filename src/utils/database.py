"""SQLAlchemy models, the DatabaseManager facade, and alembic helpers."""

import json
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, relationship, scoped_session, sessionmaker

from .constants import (
    ACCESS_PRIVATE,
    ALL_STATES,
    DEBOUNCE_SECONDS,
    ENQUEUEABLE_STATES,
    NOTIFY_CHANNEL,
    STATE_DELETED,
    STATE_DISABLED,
    STATE_ENABLED,
    STATE_ENQUEUED,
    STATE_ERROR,
    STATE_IN_PROGRESS,
    TRIGGERABLE_STATES,
    WATCHDOG_TIMEOUT_S,
    EXTRA_PARAM_KEYS,
    EXIT_RUNTIME_ERROR,
    EXIT_SCHEMA_VALIDATION,
    EXIT_SUCCESS,
    EXIT_TIMEOUT,
)
from .misc_utils import (
    DecryptionError,
    PasswordCipher,
    collect_password_field_names,
    encrypt_password_fields,
    decrypt_password_fields,
    ensure_extra_params,
    get_logger,
    schema_hash,
    uuid7,
    uuid4,
)


Base = declarative_base()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class Subscription(Base):
    __tablename__ = "subscriptions"
    id = Column(String(36), primary_key=True)
    plugin_id = Column(String(255), ForeignKey("plugin_registry_state.plugin_id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    config = Column(JSON, nullable=False, default=dict)
    status = Column(String(32), nullable=False, default=STATE_ENABLED, index=True)
    last_updated = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    last_heartbeat = Column(DateTime(timezone=True), nullable=True)
    last_error = Column(Text, nullable=True)
    last_message = Column(Text, nullable=True)
    progress = Column(Integer, nullable=False, default=0)
    sub_type = Column(String(32), nullable=False, default="SCHEDULED")
    cron = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    password_schema_hash = Column(String(128), nullable=True)

    __table_args__ = (UniqueConstraint("plugin_id", "name", name="uq_subscriptions_plugin_id_name"),)

    events = relationship("EventLog", back_populates="subscription", cascade="all, delete-orphan")


class EventLog(Base):
    __tablename__ = "event_log"
    id = Column(String(36), primary_key=True)
    subscription_id = Column(String(36), ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False, index=True)
    executed_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    exit_code = Column(Integer, nullable=False)
    exit_string = Column(String(255), nullable=False, default="")

    subscription = relationship("Subscription", back_populates="events")


class PluginRegistryState(Base):
    __tablename__ = "plugin_registry_state"
    plugin_id = Column(String(255), primary_key=True)
    schema_hash = Column(String(128), nullable=False)
    last_loaded = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Sink / Target Models
# ---------------------------------------------------------------------------
class Sink(Base):
    __tablename__ = "sink"
    id = Column(String(36), primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    targets = relationship("Target", back_populates="service", cascade="all, delete-orphan")


class Target(Base):
    __tablename__ = "target"
    id = Column(String(36), primary_key=True)
    service_id = Column(String(36), ForeignKey("sink.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    api_url = Column(Text, nullable=False)
    api_key = Column(Text, nullable=False)
    remote_target_id = Column(Text, nullable=True)
    target_extra_params = Column(JSON, default=dict)
    include_path_in_filename = Column(Boolean, nullable=False, default=False, server_default="false")
    schedule_start = Column(String(5), nullable=True)
    schedule_end = Column(String(5), nullable=True)
    pages_per_batch = Column(Integer, nullable=False, default=10, server_default="10")
    access_level = Column(String(7), nullable=False, default=ACCESS_PRIVATE, server_default="PRIVATE")

    service = relationship("Sink", back_populates="targets")


class TargetSubscription(Base):
    __tablename__ = "target_subscriptions"
    target_id = Column(String(36), ForeignKey("target.id", ondelete="CASCADE"), nullable=False, primary_key=True)
    subscription_id = Column(String(36), ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False, primary_key=True)
    status = Column(String(32), nullable=False, default="ENQUEUED")
    last_updated = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    last_message = Column(Text, nullable=True)


class AKBDatafile(Base):
    __tablename__ = "akb_datafile"
    id = Column(String(36), primary_key=True)
    subscription_id = Column(String(36), ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False, index=True)
    path = Column(Text, nullable=False, unique=True)
    size = Column(BigInteger, nullable=False)
    mtime = Column(DateTime(timezone=True), nullable=False)
    hash = Column(Text, nullable=False)
    last_checked = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class TargetDatafile(Base):
    __tablename__ = "target_datafile"
    target_id = Column(String(36), ForeignKey("target.id", ondelete="CASCADE"), nullable=False, primary_key=True)
    datafile_id = Column(String(36), ForeignKey("akb_datafile.id", ondelete="CASCADE"), nullable=False, primary_key=True)
    remote_datafile_id = Column(Text, nullable=False)
    hash = Column(Text, nullable=False)


# ---------------------------------------------------------------------------
# DatabaseManager
# ---------------------------------------------------------------------------
class DatabaseManager:
    """Thread-safe facade over the SQLAlchemy engine + scoped sessions."""

    def __init__(self, database_url: str, log_file: Optional[str] = None, component: str = "db"):
        self._url = database_url
        self._engine = create_engine(
            database_url,
            pool_size=20,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=1800,
            future=True,
        )
        self._session_factory = sessionmaker(bind=self._engine, expire_on_commit=False, future=True)
        self._scoped = scoped_session(self._session_factory)
        self._log = get_logger(component, log_file)
        self._cipher = PasswordCipher()
        self._lock = threading.RLock()

    @contextmanager
    def get_session(self) -> Iterable[Session]:
        """Context manager providing a scoped session."""
        session = self._scoped()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            self._scoped.remove()

    @property
    def engine(self):
        return self._engine

    def dispose(self, close: bool = True) -> None:
        self._engine.dispose(close=close)

    def update_last_message(self, sub_id: str, message: str) -> int:
        with self.get_session() as s:
            res = s.execute(
                text(
                    "UPDATE subscriptions SET last_message = :msg, last_updated = NOW() "
                    "WHERE id = :sid AND status != 'DELETED'"
                ),
                {"msg": message, "sid": sub_id},
            )
            if res.rowcount:
                s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            return res.rowcount

    # ----- subscription CRUD -----
    def create_subscription(
        self,
        plugin_id: str,
        name: str,
        config: Dict[str, Any],
        sub_type: str,
        cron: Optional[str],
        description: Optional[str] = None,
        password_field_names: Optional[List[str]] = None,
    ) -> Subscription:
        sid = str(uuid4())
        config = ensure_extra_params(config)
        encrypted = encrypt_password_fields(config, password_field_names or [], self._cipher)
        sub = Subscription(
            id=sid,
            plugin_id=plugin_id,
            name=name,
            config=encrypted,
            status=STATE_ENABLED,
            last_updated=datetime.now(timezone.utc),
            progress=0,
            sub_type=sub_type,
            cron=cron,
            description=description,
        )
        with self.get_session() as s:
            s.add(sub)
            try:
                s.flush()
            except IntegrityError as exc:
                raise ValueError(f"Subscription name already exists: {name}") from exc
            s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sid})
        self._log.info("subscription_created", sub_id=sid, plugin_id=plugin_id, name=name)
        return sub

    def get_subscription(self, sub_id: str) -> Optional[Subscription]:
        with self.get_session() as s:
            return s.query(Subscription).filter(Subscription.id == sub_id).first()

    def get_subscription_by_name(self, plugin_id: str, name: str) -> Optional[Subscription]:
        with self.get_session() as s:
            return s.query(Subscription).filter(Subscription.plugin_id == plugin_id, Subscription.name == name).first()

    def list_subscriptions(self, plugin_id: Optional[str] = None, include_deleted: bool = False) -> List[Subscription]:
        with self.get_session() as s:
            q = s.query(Subscription)
            if plugin_id:
                q = q.filter(Subscription.plugin_id == plugin_id)
            if not include_deleted:
                q = q.filter(Subscription.status != STATE_DELETED)
            return q.order_by(Subscription.last_updated.desc()).all()

    def list_event_based_active(self) -> List[Subscription]:
        with self.get_session() as s:
            return (
                s.query(Subscription)
                .filter(
                    Subscription.sub_type == "EVENT_BASED",
                    Subscription.status.in_([STATE_ENABLED, STATE_ENQUEUED, STATE_IN_PROGRESS]),
                )
                .all()
            )

    def list_stuck_in_flight(self) -> List[Subscription]:
        with self.get_session() as s:
            return (
                s.query(Subscription)
                .filter(Subscription.status.in_([STATE_IN_PROGRESS, STATE_ENQUEUED, STATE_DELETED]))
                .all()
            )

    def list_stale_in_progress(self, timeout_s: int) -> List[Subscription]:
        with self.get_session() as s:
            res = s.execute(
                text(
                    "SELECT id FROM subscriptions "
                    "WHERE status IN ('ENQUEUED', 'IN_PROGRESS') "
                    "AND (last_heartbeat IS NULL "
                    "  OR (NOW() - last_heartbeat) > make_interval(secs => :t))"
                ),
                {"t": timeout_s},
            ).fetchall()
            return res

    def backfill_extra_params(self) -> int:
        """Ensure every non-deleted subscription has all 3 ``_extra_param_*`` keys.

        The system invariant (see DesignSpecification §3.3) is that every
        subscription's ``config`` JSONB must contain ``_extra_param_1``,
        ``_extra_param_2``, and ``_extra_param_3`` so plugins can adopt
        them later without forcing a destructive rebuild. ``create_subscription``
        and ``update_subscription`` already inject defaults at write time;
        this method backfills legacy rows that predate that enforcement.

        Returns the number of subscriptions that were modified.
        """
        modified = 0
        with self.get_session() as s:
            rows = (
                s.query(Subscription)
                .filter(Subscription.status != STATE_DELETED)
                .all()
            )
            for sub in rows:
                cfg = sub.config or {}
                if all(k in cfg for k in EXTRA_PARAM_KEYS):
                    continue
                merged = ensure_extra_params(cfg)
                sub.config = merged
                modified += 1
            if modified:
                self._log.info(
                    "extra_params_backfilled",
                    action="startup",
                    result="ok",
                    count=modified,
                )
        return modified

    def update_subscription(
        self,
        sub_id: str,
        *,
        config: Optional[Dict[str, Any]] = None,
        cron: Optional[str] = None,
        password_field_names: Optional[List[str]] = None,
        merge_passwords: bool = False,
    ) -> Optional[Subscription]:
        with self.get_session() as s:
            sub = s.query(Subscription).filter(Subscription.id == sub_id).first()
            if not sub or sub.status == STATE_DELETED:
                return None
            if config is not None:
                # Preserve any _extra_param_* values the user previously set but
                # omitted from this update (mirrors the password-field merge
                # pattern below). Missing keys still default to "" via
                # ensure_extra_params so the invariant is maintained.
                existing_cfg = sub.config or {}
                for key in EXTRA_PARAM_KEYS:
                    if key not in config and key in existing_cfg:
                        config = dict(config)
                        config[key] = existing_cfg[key]
                config = ensure_extra_params(config)
                preserved = {}
                if merge_passwords and password_field_names:
                    existing = sub.config or {}
                    for pwd in password_field_names:
                        if pwd not in config or config.get(pwd) in (None, ""):
                            if pwd in existing:
                                preserved[pwd] = existing[pwd]
                                config = {k: v for k, v in config.items() if k != pwd}
                sub.config = encrypt_password_fields(config, password_field_names or [], self._cipher)
                if preserved:
                    merged = dict(sub.config)
                    merged.update(preserved)
                    sub.config = merged
            if cron is not None:
                sub.cron = cron
            sub.last_updated = datetime.now(timezone.utc)
            s.flush()
            s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            s.refresh(sub)
            return sub

    def delete_subscription(self, sub_id: str) -> bool:
        """Set status=DELETED (terminal state). Returns False if already DELETED."""
        with self.get_session() as s:
            res = s.execute(
                text("UPDATE subscriptions SET status = :st, last_updated = NOW() "
                     "WHERE id = :sid AND status != :st"),
                {"st": STATE_DELETED, "sid": sub_id},
            )
            if res.rowcount == 0:
                return False
            s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            self._log.info("subscription_deleted", sub_id=sub_id)
            return True

    def delete_subscription_row(self, sub_id: str) -> None:
        with self.get_session() as s:
            sub = s.query(Subscription).filter(Subscription.id == sub_id).first()
            if sub is not None:
                s.delete(sub)

    def count_subscriptions_for_plugin(self, plugin_id: str) -> int:
        with self.get_session() as s:
            return s.query(Subscription).filter(Subscription.plugin_id == plugin_id).count()

    def set_subscription_status(self, sub_id: str, new_status: str) -> Tuple[bool, Optional[str]]:
        """Set subscription status, enforcing the DELETED guard.

        Returns ``(success, error_message)`` where ``error_message`` is non-None
        only when the status transition is invalid.
        """
        if new_status not in (STATE_ENABLED, STATE_DISABLED):
            return False, f"Invalid status {new_status!r}"
        with self.get_session() as s:
            sub = s.query(Subscription).filter(Subscription.id == sub_id).first()
            if not sub:
                return False, "Subscription not found"
            if sub.status == STATE_DELETED:
                return False, "Cannot modify a DELETED subscription"
            old = sub.status
            sub.status = new_status
            sub.last_updated = datetime.now(timezone.utc)
            if new_status == STATE_ENABLED and old == STATE_ERROR:
                sub.last_error = None
            s.flush()
            s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            self._log.info(
                "state_transition",
                sub_id=sub_id,
                name=sub.name,
                old_status=old,
                new_status=new_status,
                by="user",
            )
            return True, None

    def update_status(self, sub_id: str, new_status: str, *, last_error: Optional[str] = None,
                      guard: str = "error_safe") -> int:
        """Generic status update with the DELETED + DISABLED/ERROR invariant guard.

        ``guard`` values:
          * ``"error_safe"`` (default) — ``WHERE status NOT IN ('DELETED', 'DISABLED')``
          * ``"success_to_enabled"`` — ``WHERE status IN ('ENQUEUED', 'IN_PROGRESS')``
          * ``"claim"`` — ``WHERE status IN ('ENQUEUED', 'IN_PROGRESS')``
        Returns the rowcount (0 means blocked).
        """
        with self.get_session() as s:
            if guard == "success_to_enabled":
                clause = "status IN ('ENQUEUED', 'IN_PROGRESS')"
            elif guard == "claim":
                clause = "status IN ('ENQUEUED', 'IN_PROGRESS')"
            else:
                clause = "status NOT IN ('DELETED', 'DISABLED')"
            sql = (
                f"UPDATE subscriptions SET status = :st, last_updated = NOW(), "
                f"last_error = COALESCE(:err, last_error) "
                f"WHERE id = :sid AND {clause}"
            )
            res = s.execute(
                text(sql),
                {"st": new_status, "sid": sub_id, "err": last_error},
            )
            if res.rowcount:
                s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            return res.rowcount

    def update_status_and_heartbeat(self, sub_id: str, new_status: str) -> int:
        with self.get_session() as s:
            res = s.execute(
                text(
                    "UPDATE subscriptions SET status = :st, last_heartbeat = NOW(), "
                    "last_updated = NOW() WHERE id = :sid "
                    "AND status NOT IN ('DELETED', 'DISABLED')"
                ),
                {"st": new_status, "sid": sub_id},
            )
            if res.rowcount:
                s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            return res.rowcount

    def update_heartbeat_and_progress(self, sub_id: str, progress: int) -> int:
        with self.get_session() as s:
            res = s.execute(
                text(
                    "UPDATE subscriptions SET last_heartbeat = NOW(), progress = :p, "
                    "last_updated = NOW() WHERE id = :sid AND status != 'DELETED'"
                ),
                {"p": max(0, min(100, int(progress))), "sid": sub_id},
            )
            if res.rowcount:
                s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            return res.rowcount

    def ensure_enqueued(self, sub_id: str) -> bool:
        """Idempotent ENQUEUED transition used by the worker's re-eval step.

        Flips ENABLED → ENQUEUED so the next ``mark_execution_start``
        claim can succeed. If the sub is already ENQUEUED / IN_PROGRESS
        / DISABLED / DELETED, it is left alone. Returns True if the
        transition was performed.
        """
        with self.get_session() as s:
            res = s.execute(
                text(
                    "UPDATE subscriptions SET status = 'ENQUEUED', last_updated = NOW() "
                    "WHERE id = :sid AND status = 'ENABLED'"
                ),
                {"sid": sub_id},
            )
            return bool(res.rowcount)

    def try_enqueue(self, sub_id: str) -> bool:
        """Atomic enqueue. Returns True if enqueued (or already DELETED for cleanup).

        Only transitions ``ENABLED`` -> ``ENQUEUED``. Subs that are already
        ``ENQUEUED`` or ``IN_PROGRESS`` are NOT clobbered — the caller can
        still push the id to Redis to schedule a follow-up execution, but
        the in-flight state of the subscription is preserved.
        """
        with self.get_session() as s:
            sub = s.query(Subscription).filter(Subscription.id == sub_id).first()
            if not sub:
                return False
            if sub.status == STATE_DELETED:
                # No status change; caller will push to Redis
                return True
            res = s.execute(
                text(
                    "UPDATE subscriptions SET status = 'ENQUEUED', "
                    "last_heartbeat = NOW(), last_updated = NOW() "
                    "WHERE id = :sid AND status = 'ENABLED'"
                ),
                {"sid": sub_id},
            )
            if res.rowcount:
                s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            return res.rowcount > 0

    def mark_execution_start(self, sub_id: str) -> int:
        """Worker claims a sub: ``ENQUEUED``/``IN_PROGRESS`` -> ``IN_PROGRESS``."""
        with self.get_session() as s:
            res = s.execute(
                text(
                    "UPDATE subscriptions SET status = 'IN_PROGRESS', last_heartbeat = NOW(), "
                    "last_updated = NOW() WHERE id = :sid "
                    "AND status IN ('ENQUEUED', 'IN_PROGRESS')"
                ),
                {"sid": sub_id},
            )
            if res.rowcount:
                s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})
            self._log.info("mark_execution_start_ran", sub_id=sub_id, rowcount=res.rowcount, action="claim", result="ok")
            return res.rowcount

    def record_execution(self, sub_id: str, exit_code: int, exit_string: str) -> None:
        with self.get_session() as s:
            sub = s.query(Subscription).filter(Subscription.id == sub_id).first()
            if sub is None or sub.status == STATE_DELETED:
                return
            entry = EventLog(
                id=str(uuid7()),
                subscription_id=sub_id,
                exit_code=exit_code,
                exit_string=exit_string[:255],
            )
            s.add(entry)
            s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :sid)"), {"sid": sub_id})

    def list_event_log(self, limit: Optional[int] = None) -> List[Tuple[EventLog, str, str]]:
        """Return (EventLog, subscription_name, plugin_id) tuples, newest first.

        If ``limit`` is ``None``, every row is returned. The API endpoint
        applies a 100k-row safety cap (see ``api_logging``); pass an
        explicit value from internal callers that need a different cap.
        """
        with self.get_session() as s:
            q = (
                s.query(EventLog, Subscription.name, Subscription.plugin_id)
                .join(Subscription, Subscription.id == EventLog.subscription_id)
                .order_by(EventLog.executed_at.desc())
            )
            if limit is not None:
                q = q.limit(limit)
            rows = q.all()
            return [(r[0], r[1], r[2]) for r in rows]

    def count_recent_events(self, sub_id: str, hours: int = 24) -> int:
        with self.get_session() as s:
            res = s.execute(
                text(
                    "SELECT COUNT(*) FROM event_log WHERE subscription_id = :sid "
                    "AND executed_at > NOW() - make_interval(hours => :h)"
                ),
                {"sid": sub_id, "h": hours},
            )
            return int(res.scalar() or 0)

    def count_recent_events_batch(self, hours: int = 24) -> Dict[str, int]:
        with self.get_session() as s:
            res = s.execute(
                text(
                    "SELECT subscription_id, COUNT(*) FROM event_log "
                    "WHERE executed_at > NOW() - make_interval(hours => :h) "
                    "GROUP BY subscription_id"
                ),
                {"h": hours},
            )
            return {str(row[0]): int(row[1]) for row in res.fetchall()}

    def clear_event_log(self) -> int:
        with self.get_session() as s:
            res = s.execute(text("DELETE FROM event_log"))
            return res.rowcount

    # ----- plugin registry state -----
    def get_plugin_state(self, plugin_id: str) -> Optional[PluginRegistryState]:
        with self.get_session() as s:
            return s.query(PluginRegistryState).filter(PluginRegistryState.plugin_id == plugin_id).first()

    def upsert_plugin_state(self, plugin_id: str, schema_hash_value: str) -> None:
        with self.get_session() as s:
            existing = s.query(PluginRegistryState).filter(PluginRegistryState.plugin_id == plugin_id).first()
            if existing:
                existing.schema_hash = schema_hash_value
                existing.last_loaded = datetime.now(timezone.utc)
            else:
                s.add(PluginRegistryState(
                    plugin_id=plugin_id,
                    schema_hash=schema_hash_value,
                    last_loaded=datetime.now(timezone.utc),
                ))

    def delete_plugin_state(self, plugin_id: str) -> None:
        with self.get_session() as s:
            existing = s.query(PluginRegistryState).filter(PluginRegistryState.plugin_id == plugin_id).first()
            if existing is not None:
                s.delete(existing)

    # ----- Sink service CRUD -----
    def upsert_sink(self, name: str, description: str = "") -> Any:
        with self.get_session() as s:
            existing = s.query(Sink).filter(Sink.name == name).first()
            if existing:
                if description:
                    existing.description = description
                return existing
            svc = Sink(id=str(uuid4()), name=name, description=description)
            s.add(svc)
            try:
                s.flush()
            except IntegrityError:
                s.rollback()
                existing = s.query(Sink).filter(Sink.name == name).first()
                return existing
            return svc

    def list_sinks(self) -> List[Sink]:
        with self.get_session() as s:
            return s.query(Sink).order_by(Sink.name).all()

    def get_sink(self, service_id: str) -> Optional[Sink]:
        with self.get_session() as s:
            return s.query(Sink).filter(Sink.id == service_id).first()

    def delete_sink(self, service_id: str) -> None:
        with self.get_session() as s:
            existing = s.query(Sink).filter(Sink.id == service_id).first()
            if existing is not None:
                s.delete(existing)

    # ----- Target CRUD -----
    def create_target(self, service_id: str, name: str, api_url: str, api_key: str,
                      target_extra_params: Dict[str, Any] = None,
                      include_path_in_filename: bool = False,
                      schedule_start: str = None, schedule_end: str = None,
                      pages_per_batch: int = 10,
                      access_level: str = ACCESS_PRIVATE) -> Target:
        encrypted = self._cipher.encrypt(api_key) if api_key else ""
        t = Target(
            id=str(uuid4()),
            service_id=service_id, name=name, api_url=api_url,
            api_key=encrypted,
            target_extra_params=target_extra_params or {},
            include_path_in_filename=bool(include_path_in_filename),
            schedule_start=schedule_start or None,
            schedule_end=schedule_end or None,
            pages_per_batch=pages_per_batch,
            access_level=access_level,
        )
        with self.get_session() as s:
            s.add(t)
            s.flush()
            self._log.info("target_created", target_id=t.id, name=name)
            return t

    def get_target(self, target_id: str) -> Optional[Target]:
        with self.get_session() as s:
            return s.query(Target).filter(Target.id == target_id).first()

    def list_targets(self, service_id: Optional[str] = None) -> List[Target]:
        with self.get_session() as s:
            q = s.query(Target)
            if service_id:
                q = q.filter(Target.service_id == service_id)
            return q.order_by(Target.name).all()

    def update_target(self, target_id: str, *, name: str = None, api_url: str = None,
                      api_key: str = None, target_extra_params: Dict[str, Any] = None,
                      include_path_in_filename: bool = None,
                      schedule_start: str = None, schedule_end: str = None,
                      pages_per_batch: int = None, access_level: str = None) -> Optional[Target]:
        with self.get_session() as s:
            t = s.query(Target).filter(Target.id == target_id).first()
            if not t:
                return None
            if name is not None:
                t.name = name
            if api_url is not None:
                t.api_url = api_url
            if api_key is not None and api_key.strip():
                t.api_key = self._cipher.encrypt(api_key)
            if target_extra_params is not None:
                t.target_extra_params = target_extra_params
            if include_path_in_filename is not None:
                t.include_path_in_filename = bool(include_path_in_filename)
            if schedule_start is not None:
                t.schedule_start = schedule_start or None
            if schedule_end is not None:
                t.schedule_end = schedule_end or None
            if pages_per_batch is not None:
                t.pages_per_batch = pages_per_batch
            if access_level is not None:
                t.access_level = access_level
            s.flush()
            s.refresh(t)
            return t

    def set_target_remote_id(self, target_id: str, remote_id: str) -> None:
        with self.get_session() as s:
            s.query(Target).filter(Target.id == target_id).update({"remote_target_id": remote_id})

    def delete_target_row(self, target_id: str) -> None:
        with self.get_session() as s:
            t = s.query(Target).filter(Target.id == target_id).first()
            if t:
                s.delete(t)

    def count_target_subscriptions_for_target(self, target_id: str) -> int:
        with self.get_session() as s:
            return s.query(TargetSubscription).filter(
                TargetSubscription.target_id == target_id
            ).count()

    # ----- target-subscription link -----
    def get_target_subscription(self, target_id: str, subscription_id: str) -> Optional[TargetSubscription]:
        with self.get_session() as s:
            return s.query(TargetSubscription).filter(
                TargetSubscription.target_id == target_id,
                TargetSubscription.subscription_id == subscription_id,
            ).first()

    def list_target_subscriptions(self, target_id: str) -> List[TargetSubscription]:
        with self.get_session() as s:
            return s.query(TargetSubscription).filter(
                TargetSubscription.target_id == target_id
            ).all()

    def list_targets_for_subscription(self, sub_id: str) -> List[TargetSubscription]:
        with self.get_session() as s:
            return s.query(TargetSubscription).filter(
                TargetSubscription.subscription_id == sub_id
            ).all()

    def link_target_subscriptions(self, target_id: str, sub_ids: List[str], status: str = "ENQUEUED") -> None:
        with self.get_session() as s:
            for sid in sub_ids:
                existing = s.query(TargetSubscription).filter(
                    TargetSubscription.target_id == target_id,
                    TargetSubscription.subscription_id == sid,
                ).first()
                if not existing:
                    s.add(TargetSubscription(
                        target_id=target_id, subscription_id=sid,
                        status=status,
                        last_updated=datetime.now(timezone.utc),
                    ))
            self._notify_target(target_id)

    def set_target_subscriptions_status(self, target_id: str, sub_ids: List[str], status: str,
                                        message: str = None) -> None:
        now = datetime.now(timezone.utc)
        with self.get_session() as s:
            for sid in sub_ids:
                row = s.query(TargetSubscription).filter(
                    TargetSubscription.target_id == target_id,
                    TargetSubscription.subscription_id == sid,
                ).first()
                if row:
                    row.status = status
                    row.last_updated = now
                    if message is not None:
                        row.last_message = message
            self._notify_target(target_id)

    def set_target_subscription_status(self, target_id: str, sub_id: str, status: str,
                                       message: str = None) -> None:
        now = datetime.now(timezone.utc)
        with self.get_session() as s:
            row = s.query(TargetSubscription).filter(
                TargetSubscription.target_id == target_id,
                TargetSubscription.subscription_id == sub_id,
            ).first()
            if row:
                row.status = status
                row.last_updated = now
                if message is not None:
                    row.last_message = message
            self._notify_target(target_id)

    def delete_target_subscription(self, target_id: str, sub_id: str) -> None:
        with self.get_session() as s:
            row = s.query(TargetSubscription).filter(
                TargetSubscription.target_id == target_id,
                TargetSubscription.subscription_id == sub_id,
            ).first()
            if row:
                s.delete(row)

    def delete_target_subscriptions_for_target(self, target_id: str) -> None:
        with self.get_session() as s:
            s.query(TargetSubscription).filter(
                TargetSubscription.target_id == target_id
            ).delete()

    # ----- akb_datafile -----
    def get_or_create_datafile(self, sub_id: str, path: str, size: int, mtime: float, datafile_hash: str) -> AKBDatafile:
        from datetime import timezone as tz
        mtime_dt = datetime.fromtimestamp(mtime, tz=tz.utc)
        with self.get_session() as s:
            existing = s.query(AKBDatafile).filter(AKBDatafile.path == path).first()
            if existing:
                existing.size = size
                existing.mtime = mtime_dt
                existing.hash = datafile_hash
                existing.last_checked = datetime.now(tz=tz.utc)
                s.flush()
                return existing
            df = AKBDatafile(
                id=str(uuid4()), subscription_id=sub_id, path=path,
                size=size, mtime=mtime_dt, hash=datafile_hash,
            )
            s.add(df)
            s.flush()
            return df

    def get_datafile(self, datafile_id: str) -> Optional[AKBDatafile]:
        with self.get_session() as s:
            return s.query(AKBDatafile).filter(AKBDatafile.id == datafile_id).first()

    def get_datafile_by_path(self, path: str) -> Optional[AKBDatafile]:
        with self.get_session() as s:
            return s.query(AKBDatafile).filter(AKBDatafile.path == path).first()

    def update_datafile_stats(self, datafile_id: str, size: int, mtime: float, datafile_hash: str, checked_at=None) -> None:
        from datetime import timezone as tz
        with self.get_session() as s:
            df = s.query(AKBDatafile).filter(AKBDatafile.id == datafile_id).first()
            if df:
                df.size = size
                df.mtime = datetime.fromtimestamp(mtime, tz=tz.utc)
                df.hash = datafile_hash
                df.last_checked = checked_at if checked_at is not None else datetime.now(tz=tz.utc)

    def update_datafile_last_checked(self, datafile_id: str, checked_at=None) -> None:
        from datetime import timezone as tz
        with self.get_session() as s:
            s.query(AKBDatafile).filter(AKBDatafile.id == datafile_id).update(
                {"last_checked": checked_at if checked_at is not None else datetime.now(tz=tz.utc)}
            )

    def set_datafiles_last_checked_batch(self, sub_id: str, checked_at) -> None:
        with self.get_session() as s:
            s.query(AKBDatafile).filter(AKBDatafile.subscription_id == sub_id).update(
                {"last_checked": checked_at}
            )

    def delete_datafile(self, datafile_id: str) -> None:
        with self.get_session() as s:
            df = s.query(AKBDatafile).filter(AKBDatafile.id == datafile_id).first()
            if df:
                s.delete(df)

    def list_datafiles_for_subscription(self, sub_id: str) -> List[AKBDatafile]:
        with self.get_session() as s:
            return s.query(AKBDatafile).filter(AKBDatafile.subscription_id == sub_id).all()

    def list_datafiles_for_target(self, target_id: str) -> List[TargetDatafile]:
        with self.get_session() as s:
            return s.query(TargetDatafile).filter(
                TargetDatafile.target_id == target_id
            ).all()

    def list_datafiles_for_target_subscription(self, target_id: str, sub_id: str) -> List[TargetDatafile]:
        with self.get_session() as s:
            return s.query(TargetDatafile).join(
                AKBDatafile, AKBDatafile.id == TargetDatafile.datafile_id
            ).filter(
                TargetDatafile.target_id == target_id,
                AKBDatafile.subscription_id == sub_id,
            ).all()

    # ----- target_datafile -----
    def get_target_datafile(self, target_id: str, datafile_id: str) -> Optional[TargetDatafile]:
        with self.get_session() as s:
            return s.query(TargetDatafile).filter(
                TargetDatafile.target_id == target_id,
                TargetDatafile.datafile_id == datafile_id,
            ).first()

    def insert_target_datafile(self, target_id: str, datafile_id: str, remote_id: str, datafile_hash: str) -> None:
        with self.get_session() as s:
            s.add(TargetDatafile(
                target_id=target_id, datafile_id=datafile_id,
                remote_datafile_id=remote_id, hash=datafile_hash,
            ))
            self._notify_target(target_id)

    def update_target_datafile_hash(self, target_id: str, datafile_id: str, new_hash: str) -> None:
        with self.get_session() as s:
            s.query(TargetDatafile).filter(
                TargetDatafile.target_id == target_id,
                TargetDatafile.datafile_id == datafile_id,
            ).update({"hash": new_hash})

    def update_target_datafile_remote_id(self, target_id: str, datafile_id: str, new_remote_id: str) -> None:
        with self.get_session() as s:
            s.query(TargetDatafile).filter(
                TargetDatafile.target_id == target_id,
                TargetDatafile.datafile_id == datafile_id,
            ).update({"remote_datafile_id": new_remote_id})

    def delete_target_datafile(self, target_id: str, datafile_id: str) -> None:
        with self.get_session() as s:
            s.query(TargetDatafile).filter(
                TargetDatafile.target_id == target_id,
                TargetDatafile.datafile_id == datafile_id,
            ).delete()

    def delete_target_datafiles_for_target(self, target_id: str) -> None:
        with self.get_session() as s:
            s.query(TargetDatafile).filter(
                TargetDatafile.target_id == target_id
            ).delete()

    # ----- Target notify -----
    def _notify_target(self, target_id: str) -> None:
        try:
            payload = json.dumps({"type": "target", "target_id": target_id}, sort_keys=True, separators=(",", ":"))
            with self.get_session() as s:
                s.execute(text(f"SELECT pg_notify('{NOTIFY_CHANNEL}', :payload)"), {"payload": payload})
        except Exception:
            pass

    def decrypt_target_api_key(self, t: Target) -> str:
        if not t.api_key:
            return ""
        return self._cipher.decrypt(t.api_key)

    # ----- helpers -----
    def decrypt_config(self, sub: Subscription, password_field_names: List[str]) -> Dict[str, Any]:
        return decrypt_password_fields(sub.config or {}, password_field_names, self._cipher)

    def mask_config(self, sub: Subscription, password_field_names: List[str]) -> Dict[str, Any]:
        cfg = sub.config or {}
        return {k: v for k, v in cfg.items() if k not in password_field_names}

    def health_check(self) -> bool:
        try:
            with self.get_session() as s:
                s.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Migration runner
# ---------------------------------------------------------------------------
def run_migrations(database_url: str) -> None:
    """Apply all pending Alembic migrations.

    Called by the Manager and Worker at startup. Uses the alembic config
    from the Manager package but overrides the database URL at runtime so
    credentials are never read from a config file on disk.
    """
    import os

    from alembic import command
    from alembic.config import Config

    manager_dir = os.path.join(os.path.dirname(__file__), "..", "manager")
    config = Config(os.path.join(manager_dir, "alembic.ini"))
    config.set_main_option("script_location", os.path.join(manager_dir, "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


__all__ = [
    "DatabaseManager", "Base", "Subscription", "EventLog", "PluginRegistryState",
    "Sink", "Target", "TargetSubscription", "AKBDatafile", "TargetDatafile",
    "run_migrations",
]
