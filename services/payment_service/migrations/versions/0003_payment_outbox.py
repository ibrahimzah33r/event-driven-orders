from alembic import op
import sqlalchemy as sa


revision = "0003_payment_outbox"
down_revision = "0002_payment_unique"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_messages",
        sa.Column(
            "id",
            sa.Integer(),
            primary_key=True,
        ),
        sa.Column(
            "event_id",
            sa.String(length=36),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "topic",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "event_key",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "payload",
            sa.JSON(),
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
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        schema="payment_service",
    )


def downgrade() -> None:
    op.drop_table(
        "outbox_messages",
        schema="payment_service",
    )