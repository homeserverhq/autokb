"""Initial schema migration.

Creates the ``subscriptions``, ``event_log``, and ``plugin_registry_state``
tables per the spec.
"""

from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def _table_exists(inspector, name: str) -> bool:
    return name in inspector.get_table_names()


def _index_exists(inspector, table: str, name: str) -> bool:
    return any(idx["name"] == name for idx in inspector.get_indexes(table))


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if not _table_exists(inspector, "subscriptions"):
        op.create_table(
            "subscriptions",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("plugin_id", sa.String(length=255), nullable=False, index=True),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("config", sa.JSON, nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False, server_default="ENABLED"),
            sa.Column("last_updated", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_error", sa.Text, nullable=True),
            sa.Column("last_message", sa.Text, nullable=True),
            sa.Column("access_level", sa.String(length=7), nullable=False, server_default="PRIVATE"),
            sa.Column("progress", sa.Integer, nullable=False, server_default="0"),
            sa.Column("sub_type", sa.String(length=32), nullable=False, server_default="SCHEDULED"),
            sa.Column("cron", sa.String(length=255), nullable=True),
            sa.Column("description", sa.Text, nullable=True),
            sa.Column("password_schema_hash", sa.String(length=128), nullable=True),
            sa.UniqueConstraint("plugin_id", "name", name="uq_subscriptions_plugin_id_name"),
        )
    if not _index_exists(inspector, "subscriptions", "ix_subscriptions_status"):
        op.create_index("ix_subscriptions_status", "subscriptions", ["status"])
    if not _index_exists(inspector, "subscriptions", "ix_subscriptions_plugin_id"):
        op.create_index("ix_subscriptions_plugin_id", "subscriptions", ["plugin_id"])

    if not _table_exists(inspector, "event_log"):
        op.create_table(
            "event_log",
            sa.Column("id", sa.String(length=36), primary_key=True),
            sa.Column("subscription_id", sa.String(length=36), sa.ForeignKey("subscriptions.id", ondelete="CASCADE"), nullable=False),
            sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column("exit_code", sa.Integer, nullable=False),
            sa.Column("exit_string", sa.String(length=255), nullable=False, server_default=""),
        )
    if not _index_exists(inspector, "event_log", "ix_event_log_subscription_id"):
        op.create_index("ix_event_log_subscription_id", "event_log", ["subscription_id"])

    if not _table_exists(inspector, "plugin_registry_state"):
        op.create_table(
            "plugin_registry_state",
            sa.Column("plugin_id", sa.String(length=255), primary_key=True),
            sa.Column("schema_hash", sa.String(length=128), nullable=False),
            sa.Column("last_loaded", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        )


def downgrade() -> None:
    op.drop_table("plugin_registry_state")
    op.drop_table("event_log")
    op.drop_table("subscriptions")
