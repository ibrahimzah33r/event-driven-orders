from alembic import op
import sqlalchemy as sa


revision = "0001_create_orders_table"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE SCHEMA IF NOT EXISTS order_service"
    )

    op.create_table(
        "orders",
        sa.Column(
            "id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "customer_id",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "total",
            sa.Numeric(
                precision=12,
                scale=2,
            ),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            server_default="CREATED",
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
        sa.PrimaryKeyConstraint("id"),
        schema="order_service",
    )


def downgrade() -> None:
    op.drop_table(
        "orders",
        schema="order_service",
    )

    op.execute(
        "DROP SCHEMA IF EXISTS order_service"
    )