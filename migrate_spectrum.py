"""
migrate_spectrum.py

Adds Strategy C (Spectrum) infrastructure to existing Railway database:
1. New bucket-specific columns on trades table
2. Third bankroll_state row for Strategy C (spectrum)

Run once by swapping railway.json startCommand to:
    python migrate_spectrum.py
Then restore to: python run.py
"""

import asyncio
import sys
import os

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL env var not set")
    sys.exit(1)

STATEMENTS = [
    # ── Bucket-specific trade columns for Strategy C ──────────────────────────
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bucket_low FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bucket_high FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bucket_label VARCHAR(50)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bucket_forecast_prob FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bucket_market_price FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bucket_center FLOAT",

    # ── Strategy C bankroll (id=3) ────────────────────────────────────────────
    """INSERT INTO bankroll_state (id, balance, starting_balance, daily_loss_today, strategy)
       SELECT 3, 2000.0, 2000.0, 0.0, 'spectrum'
       WHERE NOT EXISTS (SELECT 1 FROM bankroll_state WHERE id = 3)""",
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
    print("\nMigration done ✓ — Spectrum columns and bankroll ready")


if __name__ == "__main__":
    asyncio.run(run())
