"""0004: Add include_path_in_filename column to target.

Each Data Target can optionally prefix the remote filename with the
full directory structure (relative to /output). Stored as a first-class
boolean column, not inside target_extra_params.
"""

from alembic import op


revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE target ADD COLUMN include_path_in_filename "
        "BOOLEAN NOT NULL DEFAULT FALSE"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE target DROP COLUMN include_path_in_filename")
