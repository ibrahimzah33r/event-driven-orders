from alembic import op


revision = "0002_inventory_unique"
down_revision = "0001_inventory"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_reservations_order_id",
        "reservations",
        ["order_id"],
        schema="inventory_service",
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_reservations_order_id",
        "reservations",
        schema="inventory_service",
        type_="unique",
    )