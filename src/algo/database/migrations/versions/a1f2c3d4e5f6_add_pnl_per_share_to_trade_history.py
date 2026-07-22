"""add pnl_per_share to trade_history

Additive analytics column: realized_pnl / lot_size, rounded to two decimals.
Nullable and appended -- realized_pnl and every existing column are unchanged,
so existing rows (which get NULL) and existing readers stay valid.

Revision ID: a1f2c3d4e5f6
Revises: c45ec4abd494
Create Date: 2026-07-22 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'a1f2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'c45ec4abd494'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'trade_history',
        sa.Column('pnl_per_share', sa.Numeric(precision=18, scale=4), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('trade_history', 'pnl_per_share')
