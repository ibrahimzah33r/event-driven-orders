from alembic import op


revision = "0002_payment_unique"
down_revision = "0001_create_payments_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_payments_order_id",
        "payments",
        ["order_id"],
        schema="payment_service",
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_payments_order_id",
        "payments",
        schema="payment_service",
        type_="unique",
    )