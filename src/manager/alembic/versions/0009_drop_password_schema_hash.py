"""0009: Drop the unused password_schema_hash column from subscriptions.

The column was an intended "schema hash at encryption time" field that was
never read or written anywhere; it is dead and is removed.
"""

from alembic import op


revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE subscriptions DROP COLUMN IF EXISTS password_schema_hash")


def downgrade() -> None:
    op.execute("ALTER TABLE subscriptions ADD COLUMN password_schema_hash VARCHAR(128) NULL")