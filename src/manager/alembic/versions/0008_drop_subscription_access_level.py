"""0008: Drop access_level column from subscriptions.

Access Level moved to the destination side (``target.access_level``, see 0007);
the plugin/subscription-side column is now dead and is removed.
"""

from alembic import op


revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE subscriptions DROP COLUMN access_level")


def downgrade() -> None:
    op.execute(
        "ALTER TABLE subscriptions ADD COLUMN access_level VARCHAR(7) NOT NULL DEFAULT 'PRIVATE'"
    )