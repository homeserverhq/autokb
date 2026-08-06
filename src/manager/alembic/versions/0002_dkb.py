"""0002: Add DKB (Downstream Knowledge Base) tables.

Tables:
  - dkb_service           — service types (OpenWebUI, Cognee, …)
  - dkb_datastore         — per-service datastore instances
  - datastore_subscriptions — many-to-many link + per-link status
  - akb_datafile          — per-subscription local file inventory
  - datastore_datafile    — per-datastore remote file tracking
"""

from datetime import datetime, timezone

from alembic import op
from sqlalchemy import Column, DateTime, ForeignKey, Integer, JSON, String, Text, BigInteger

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # --- dkb_service ---
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dkb_service ("
        "  id          VARCHAR(36)  PRIMARY KEY,"
        "  name        VARCHAR(255) NOT NULL,"
        "  description TEXT"
        ")"
    )

    # --- dkb_datastore ---
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dkb_datastore ("
        "  id                  VARCHAR(36)  PRIMARY KEY,"
        "  service_id          VARCHAR(36)  NOT NULL REFERENCES dkb_service(id) ON DELETE CASCADE,"
        "  name                VARCHAR(255) NOT NULL,"
        "  api_url             TEXT         NOT NULL,"
        "  api_key             TEXT         NOT NULL,"
        "  remote_datastore_id TEXT,"
        "  ds_extra_params     JSON         DEFAULT '{}'::json"
        ")"
    )

    # --- datastore_subscriptions ---
    conn.execute(
        "CREATE TABLE IF NOT EXISTS datastore_subscriptions ("
        "  datastore_id      VARCHAR(36) NOT NULL REFERENCES dkb_datastore(id) ON DELETE CASCADE,"
        "  subscription_id   VARCHAR(36) NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,"
        "  status            VARCHAR(32) NOT NULL DEFAULT 'ENQUEUED',"
        "  last_updated      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),"
        "  last_message      TEXT,"
        "  PRIMARY KEY (datastore_id, subscription_id)"
        ")"
    )

    # --- akb_datafile (auto knowledge base datafile) ---
    conn.execute(
        "CREATE TABLE IF NOT EXISTS akb_datafile ("
        "  id                VARCHAR(36)  PRIMARY KEY,"
        "  subscription_id   VARCHAR(36)  NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,"
        "  path              TEXT         NOT NULL,"
        "  size              BIGINT       NOT NULL,"
        "  mtime             TIMESTAMP WITH TIME ZONE NOT NULL,"
        "  hash              TEXT         NOT NULL,"
        "  last_checked      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),"
        "  UNIQUE (path)"
        ")"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_akb_datafile_sub ON akb_datafile(subscription_id)")

    # --- datastore_datafile ---
    conn.execute(
        "CREATE TABLE IF NOT EXISTS datastore_datafile ("
        "  datastore_id       VARCHAR(36) NOT NULL REFERENCES dkb_datastore(id) ON DELETE CASCADE,"
        "  datafile_id        VARCHAR(36) NOT NULL REFERENCES akb_datafile(id) ON DELETE CASCADE,"
        "  remote_datafile_id TEXT        NOT NULL,"
        "  hash               TEXT        NOT NULL,"
        "  PRIMARY KEY (datastore_id, datafile_id)"
        ")"
    )


def downgrade() -> None:
    conn = op.get_bind()
    for tbl in (
        "datastore_datafile",
        "akb_datafile",
        "datastore_subscriptions",
        "dkb_datastore",
        "dkb_service",
    ):
        conn.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
