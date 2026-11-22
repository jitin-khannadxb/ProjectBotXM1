"""Add kill_switch_active to daily_state

Revision ID: 001_kill_switch_active
Revises: 
Create Date: 2026-11-22 04:55:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "001_kill_switch_active"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "daily_state",
        sa.Column("kill_switch_active", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("daily_state", "kill_switch_active")
