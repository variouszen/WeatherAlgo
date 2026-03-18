#!/usr/bin/env python3
"""
WeatherAlgo v2 — Phase 1C: Database Migration

Adds v2-specific columns to the trades table and creates bankroll rows
for the 5 new strategy variants.

Safe to run multiple times (all statements use IF NOT EXISTS / ON CONFLICT).

Usage:
    # On Railway (uses DATABASE_URL from environment):
    python migrate_v2.py

    # Local with explicit URL:
    DATABASE_URL=postgresql://... python migrate_v2.py
"""

from __future__ import annotations

import asyncio
import os
import sys

# ── Path setup ────────────────────────────────────────────────────────────────
_this_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir = os.path.dirname(_this_dir) if os.path.basename(_this_dir) == "migrations" else _this_dir
sys.path.insert(0, _root_dir)
sys.path.insert(0, os.path.join(_root_dir, "backend"))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def get_database_url() -> str:
    """Get and normalize DATABASE_URL."""
    url = os.getenv("DATABASE_URL", "")
    if not url:
        print("ERROR: DATABASE_URL not set. Set it in your environment or Railway dashboard.")
        sys.exit(1)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://") and "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


# ── New columns for trades table ──────────────────────────────────────────────
# These support ensemble probabilities, CLOB pricing, fill simulation,
# and ladder package tracking per the v2 Master Spec.

TRADES_COLUMNS = [
    # Ensemble signal data
    ("ensemble_prob", "FLOAT"),
    ("ensemble_members_in_bucket", "INT"),
    ("ensemble_total_members", "INT"),
    ("gfs_peak_bucket_index", "INT"),
    ("ecmwf_peak_bucket_index", "INT"),
    ("model_agreement", "BOOLEAN"),
    
    # CLOB pricing data
    ("price_source", "VARCHAR(20)"),       # "order_book", "best_ask", "stale_snapshot"
    ("market_ask", "FLOAT"),               # CLOB best ask at entry
    ("market_midpoint", "FLOAT"),          # For comparison logging only
    ("spread_at_entry", "FLOAT"),
    ("book_depth_at_entry", "FLOAT"),      # Ask shares within 2 ticks
    
    # Fill simulation results
    ("simulated_vwap", "FLOAT"),
    ("simulated_shares", "FLOAT"),
    ("simulated_cost", "FLOAT"),
    ("fill_quality", "VARCHAR(10)"),       # "full", "shallow", "stale"
    
    # Model run tracking
    ("model_run_time", "VARCHAR(20)"),     # e.g. "2026-03-16T12Z"
    
    # Venue tracking
    ("venue", "VARCHAR(20) DEFAULT 'polymarket'"),
    
    # Edge ratio (Sniper YES gate)
    ("edge_ratio", "FLOAT"),
    
    # NO-side trading
    ("no_ask_price", "FLOAT"),             # NO token ask price at entry
    ("trade_side", "VARCHAR(5)"),           # "YES" or "NO"
    ("token_id_traded", "VARCHAR(100)"),    # Which token was actually bought
    
    # Ladder package tracking
    ("ladder_id", "INT"),                  # Groups legs of same ladder
    ("package_cost", "FLOAT"),
    ("package_prob", "FLOAT"),
    ("package_edge", "FLOAT"),
    ("num_legs", "INT"),
    ("leg_index", "INT"),                  # Which leg in the package (0-based)
]


async def run_migration():
    url = get_database_url()
    engine = create_async_engine(url, echo=False)
    
    print("=" * 60)
    print("WeatherAlgo v2 — Database Migration")
    print("=" * 60)
    
    async with engine.begin() as conn:
        # ── Step 1: Add new columns to trades table ───────────────────────
        print("\n--- Step 1: Adding v2 columns to trades table ---")
        
        for col_name, col_type in TRADES_COLUMNS:
            sql = f"ALTER TABLE trades ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            try:
                await conn.execute(text(sql))
                print(f"  OK: {col_name} ({col_type})")
            except Exception as e:
                print(f"  SKIP: {col_name} — {e}")
        
        # ── Step 2: Add bankroll rows for v2 strategies ───────────────────
        print("\n--- Step 2: Creating v2 strategy bankrolls ---")
        
        v2_strategies = [
            # id=1 sigma (legacy), id=2 forecast_edge (legacy), id=3 spectrum (existing)
            # v2 new:
            (4, "sniper_yes", 500.0),
            (5, "sniper_no", 500.0),
            (6, "ladder_3", 500.0),
            (7, "ladder_5", 500.0),
        ]
        
        for strat_id, strat_name, bankroll in v2_strategies:
            sql = text("""
                INSERT INTO bankroll_state (id, balance, starting_balance, daily_loss_today, strategy)
                VALUES (:id, :balance, :starting, 0.0, :strategy)
                ON CONFLICT (id) DO NOTHING
            """)
            try:
                result = await conn.execute(sql, {
                    "id": strat_id,
                    "balance": bankroll,
                    "starting": bankroll,
                    "strategy": strat_name,
                })
                print(f"  OK: {strat_name} (id={strat_id}, ${bankroll:.0f})")
            except Exception as e:
                print(f"  SKIP: {strat_name} — {e}")
        
        # ── Step 3: Update spectrum bankroll to $500 ──────────────────────
        print("\n--- Step 3: Updating spectrum bankroll to $500 ---")
        try:
            await conn.execute(text("""
                UPDATE bankroll_state 
                SET balance = 500.0, starting_balance = 500.0 
                WHERE id = 3 AND starting_balance != 500.0
            """))
            print("  OK: spectrum updated to $500")
        except Exception as e:
            print(f"  SKIP: {e}")
        
        # ── Step 4: Verify ────────────────────────────────────────────────
        print("\n--- Step 4: Verification ---")
        
        # Check bankroll rows
        result = await conn.execute(text(
            "SELECT id, strategy, balance, starting_balance FROM bankroll_state ORDER BY id"
        ))
        rows = result.fetchall()
        print(f"\n  Bankroll rows ({len(rows)}):")
        for row in rows:
            print(f"    id={row[0]} strategy={row[1]:<15} balance=${row[2]:.0f} starting=${row[3]:.0f}")
        
        # Check a sample of new columns exist
        try:
            await conn.execute(text(
                "SELECT ensemble_prob, price_source, fill_quality, venue, ladder_id "
                "FROM trades LIMIT 0"
            ))
            print("\n  V2 columns verified: OK")
        except Exception as e:
            print(f"\n  V2 column check FAILED: {e}")
    
    await engine.dispose()
    
    print(f"\n{'='*60}")
    print("Migration complete.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_migration())
