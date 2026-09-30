from alembic import op
import sqlalchemy as sa


revision = "0001_inventory"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE SCHEMA IF NOT EXISTS inventory_service"
    )

    op.create_table(
        "reservations",
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
        schema="inventory_service",
    )


def downgrade() -> None:
    op.drop_table(
        "reservations",
        schema="inventory_service",
    )

    op.execute(
        "DROP SCHEMA IF EXISTS inventory_service"
    )