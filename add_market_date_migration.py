"""
One-time migration: adds market_date column to existing trades table.
Run this once on Railway via:
  python add_market_date_migration.py

Safe to run multiple times — checks if column exists first.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))
from config import DATABASE_URL
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def migrate():
    engine = create_async_engine(DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        # Check if column already exists
        result = await conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='trades' AND column_name='market_date'
        """))
        exists = result.fetchone()
        if exists:
            print("✅ market_date column already exists — nothing to do")
        else:
            await conn.execute(text("""
                ALTER TABLE trades ADD COLUMN market_date VARCHAR(20) NULL
            """))
            print("✅ market_date column added to trades table")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(migrate())
