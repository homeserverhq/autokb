"""0003: Rename DKB tables to Sink/Target.

Tables renamed with data-preserving ALTER TABLE:
  dkb_service          → sink
  dkb_datastore        → target
  datastore_subscriptions → target_subscriptions
  datastore_datafile   → target_datafile

Columns renamed:
  remote_datastore_id  → remote_target_id
  ds_extra_params      → target_extra_params
"""

from alembic import op


revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE dkb_service RENAME TO sink")
    op.execute("ALTER TABLE dkb_datastore RENAME TO target")
    op.execute("ALTER TABLE datastore_subscriptions RENAME TO target_subscriptions")
    op.execute("ALTER TABLE datastore_datafile RENAME TO target_datafile")

    op.execute("ALTER TABLE target RENAME COLUMN remote_datastore_id TO remote_target_id")
    op.execute("ALTER TABLE target RENAME COLUMN ds_extra_params TO target_extra_params")

    op.execute("ALTER TABLE target_subscriptions RENAME COLUMN datastore_id TO target_id")
    op.execute("ALTER TABLE target_datafile RENAME COLUMN datastore_id TO target_id")


def downgrade() -> None:
    op.execute("ALTER TABLE target_datafile RENAME COLUMN target_id TO datastore_id")
    op.execute("ALTER TABLE target_subscriptions RENAME COLUMN target_id TO datastore_id")
    op.execute("ALTER TABLE target RENAME COLUMN remote_target_id TO remote_datastore_id")
    op.execute("ALTER TABLE target RENAME COLUMN target_extra_params TO ds_extra_params")

    op.execute("ALTER TABLE target_datafile RENAME TO datastore_datafile")
    op.execute("ALTER TABLE target_subscriptions RENAME TO datastore_subscriptions")
    op.execute("ALTER TABLE target RENAME TO dkb_datastore")
    op.execute("ALTER TABLE sink RENAME TO dkb_service")
