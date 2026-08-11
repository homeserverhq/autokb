"""0007: Add access_level column to target.

Access Level (PRIVATE/PUBLIC) moved from subscriptions (plugin-side) to the
destination side. It is a first-class String column on ``target``, not inside
target_extra_params. Defaults to PRIVATE.
"""

from alembic import op


revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE target ADD COLUMN access_level VARCHAR(7) NOT NULL DEFAULT 'PRIVATE'"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE target DROP COLUMN access_level")