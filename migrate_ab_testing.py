"""
migrate_ab_testing.py

Adds A/B testing infrastructure to existing Railway database:
1. New columns on trades table (strategy, forecast_gap, etc.)
2. Second bankroll_state row for Strategy A (forecast_edge)
3. New directional consensus fields

Run once by swapping railway.json startCommand to:
    python migrate_ab_testing.py
Then restore to: python run.py
"""

import asyncio
import sys
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL env var not set")
    sys.exit(1)

# Each statement executed separately (asyncpg doesn't support multi-statement)
STATEMENTS = [
    # ── New columns on trades table ───────────────────────────────────────────
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS strategy VARCHAR(20) DEFAULT 'sigma'",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS forecast_gap FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS validator_gap FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS same_side_as_forecast BOOLEAN",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS models_directionally_agree BOOLEAN",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS models_on_bet_side_count INTEGER",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS model_count INTEGER",

    # ── Strategy-scoped bankroll ──────────────────────────────────────────────
    "ALTER TABLE bankroll_state ADD COLUMN IF NOT EXISTS strategy VARCHAR(20) DEFAULT 'sigma'",

    # Tag existing row as sigma
    "UPDATE bankroll_state SET strategy = 'sigma' WHERE id = 1 AND (strategy IS NULL OR strategy = 'sigma')",

    # Insert Strategy A bankroll row (id=2) if it doesn't exist
    """INSERT INTO bankroll_state (id, balance, starting_balance, daily_loss_today, strategy)
       SELECT 2, 2000.0, 2000.0, 0.0, 'forecast_edge'
       WHERE NOT EXISTS (SELECT 1 FROM bankroll_state WHERE id = 2)""",
]


async def run():
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text

    db_url = DATABASE_URL
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif db_url.startswith("postgresql://") and "+asyncpg" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    engine = create_async_engine(db_url, echo=True)
    async with engine.begin() as conn:
        for stmt in STATEMENTS:
            await conn.execute(text(stmt))
            print(f"  ✓ {stmt[:80]}...")
    await engine.dispose()
    print("\nMigration done ✓ — A/B testing columns and bankroll ready")


if __name__ == "__main__":
    asyncio.run(run())
