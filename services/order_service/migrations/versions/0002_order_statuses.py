from alembic import op
import sqlalchemy as sa


revision = "0002_order_statuses"
down_revision = "0001_create_orders_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "orders",
        sa.Column(
            "payment_status",
            sa.String(length=32),
            server_default="PENDING",
            nullable=False,
        ),
        schema="order_service",
    )

    op.add_column(
        "orders",
        sa.Column(
            "inventory_status",
            sa.String(length=32),
            server_default="PENDING",
            nullable=False,
        ),
        schema="order_service",
    )


def downgrade() -> None:
    op.drop_column(
        "orders",
        "inventory_status",
        schema="order_service",
    )

    op.drop_column(
        "orders",
        "payment_status",
        schema="order_service",
    )