"""
One-time migration: add V2 columns to trades table.
Run once then revert railway.json startCommand back to 'python run.py'.
"""
import asyncio
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from backend.models.database import engine

ALTER_STATEMENTS = [
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS gfs_forecast FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS ecmwf_forecast FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS models_agreed INTEGER",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS early_window BOOLEAN DEFAULT FALSE",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_number INTEGER DEFAULT 1",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS prior_entry_ev FLOAT",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS crowd_price_at_prior FLOAT",
]

async def run():
    async with engine.begin() as conn:
        for stmt in ALTER_STATEMENTS:
            print(f"Running: {stmt}")
            await conn.execute(__import__('sqlalchemy').text(stmt))
    print("Migration complete.")

asyncio.run(run())
