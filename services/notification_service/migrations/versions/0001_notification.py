from alembic import op
import sqlalchemy as sa


revision = "0001_notification"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE SCHEMA IF NOT EXISTS notification_service"
    )

    op.create_table(
        "notifications",
        sa.Column(
            "id",
            sa.Integer(),
            primary_key=True,
        ),
        sa.Column(
            "order_id",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "customer_id",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "channel",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text(
                "CURRENT_TIMESTAMP"
            ),
            nullable=False,
        ),
        schema="notification_service",
    )


def downgrade() -> None:
    op.drop_table(
        "notifications",
        schema="notification_service",
    )

    op.execute(
        "DROP SCHEMA IF EXISTS notification_service"
    )