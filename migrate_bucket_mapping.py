"""
migrate_bucket_mapping.py

Creates the bucket_mapping_diagnostics table on an existing Railway deploy
that already has trades, scan_logs, city_calibration, and bankroll_state.

Run once by swapping railway.json startCommand to:
    python migrate_bucket_mapping.py

Then restore to: uvicorn backend.api.main:app ...
"""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import DATABASE_URL

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS bucket_mapping_diagnostics (
    id                  SERIAL PRIMARY KEY,
    scanned_at          TIMESTAMP DEFAULT NOW(),
    city                VARCHAR(50)  NOT NULL,
    market_date         VARCHAR(20),
    threshold           FLOAT        NOT NULL,
    direction           VARCHAR(5)   NOT NULL,
    synthetic_prob      FLOAT        NOT NULL,
    synthetic_edge      FLOAT        NOT NULL,
    match_type          VARCHAR(20)  NOT NULL,
    is_directly_tradable BOOLEAN     DEFAULT FALSE,
    nearest_bucket_label VARCHAR(100),
    basket_count        INTEGER      DEFAULT 0,
    basket_yes_prob     FLOAT        DEFAULT 0.0,
    prob_gap            FLOAT,
    approximation_note  TEXT,
    polymarket_market_id VARCHAR(100)
);
"""


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
        await conn.execute(text(CREATE_SQL))
    await engine.dispose()
    print("Migration done ✓ — bucket_mapping_diagnostics table ready")


if __name__ == "__main__":
    asyncio.run(run())
