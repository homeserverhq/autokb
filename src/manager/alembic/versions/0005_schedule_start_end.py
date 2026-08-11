"""0005: Add schedule_start/schedule_end columns to target.

Each Data Target can optionally restrict file upserts (add/update) to a
daily time window. Stored as first-class varchar "HH:MM" columns, not
inside target_extra_params. Both NULL = no scheduling.
"""

from alembic import op


revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE target ADD COLUMN schedule_start VARCHAR(5)")
    op.execute("ALTER TABLE target ADD COLUMN schedule_end VARCHAR(5)")


def downgrade() -> None:
    op.execute("ALTER TABLE target DROP COLUMN schedule_start")
    op.execute("ALTER TABLE target DROP COLUMN schedule_end")