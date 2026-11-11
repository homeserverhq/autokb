"""0010: Persist per-document sink outcomes.

Adds ``state`` / ``last_error`` / ``last_attempt`` to ``target_datafile`` and
allows ``remote_datafile_id`` to be NULL so a duplicate-blocked add can leave a
durable row before it has a remote counterpart.

Also repairs links that were incorrectly flipped to ERROR by the historical
disable-during-recon race (``last_message = 'link_disabled'``).
"""

from alembic import op


revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE target_datafile "
        "ALTER COLUMN remote_datafile_id DROP NOT NULL"
    )
    op.execute(
        "ALTER TABLE target_datafile "
        "ADD COLUMN IF NOT EXISTS state VARCHAR(32) NOT NULL DEFAULT 'SYNCED'"
    )
    op.execute(
        "ALTER TABLE target_datafile "
        "ADD COLUMN IF NOT EXISTS last_error TEXT"
    )
    op.execute(
        "ALTER TABLE target_datafile "
        "ADD COLUMN IF NOT EXISTS last_attempt TIMESTAMP WITH TIME ZONE"
    )

    # The old Pass III handler caught SinkCancelledError as a generic remote
    # read failure and wrote ERROR/link_disabled over an operator's DISABLED
    # intent. Restore that deliberate pause; the next Enable/Update retries.
    op.execute(
        "UPDATE target_subscriptions "
        "SET status = 'DISABLED', "
        "    last_message = 'link_disabled (repaired from ERROR)', "
        "    last_updated = NOW() "
        "WHERE status = 'ERROR' AND last_message = 'link_disabled'"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE target_datafile DROP COLUMN IF EXISTS last_attempt")
    op.execute("ALTER TABLE target_datafile DROP COLUMN IF EXISTS last_error")
    op.execute("ALTER TABLE target_datafile DROP COLUMN IF EXISTS state")
    op.execute(
        "UPDATE target_datafile SET remote_datafile_id = '' "
        "WHERE remote_datafile_id IS NULL"
    )
    op.execute(
        "ALTER TABLE target_datafile "
        "ALTER COLUMN remote_datafile_id SET NOT NULL"
    )
