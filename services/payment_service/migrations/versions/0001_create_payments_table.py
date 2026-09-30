from alembic import op
import sqlalchemy as sa


revision = "0001_create_payments_table"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE SCHEMA IF NOT EXISTS payment_service"
    )

    op.create_table(
        "payments",
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
            "amount",
            sa.Numeric(
                precision=12,
                scale=2,
            ),
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
        schema="payment_service",
    )


def downgrade() -> None:
    op.drop_table(
        "payments",
        schema="payment_service",
    )

    op.execute(
        "DROP SCHEMA IF EXISTS payment_service"
    )