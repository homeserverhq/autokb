"""0006: Add pages_per_batch column to target.

LightRAG (and future) sinks group file upserts into batches of estimated
pages; a target controls the batch size with a first-class integer column
(min 1, max 100, default 10), not inside target_extra_params.
"""

from alembic import op


revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE target ADD COLUMN pages_per_batch INTEGER NOT NULL DEFAULT 10"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE target DROP COLUMN pages_per_batch")