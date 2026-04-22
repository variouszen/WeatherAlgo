# backend/models/database.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Float, Integer, Boolean, DateTime, Text, func
from datetime import datetime
from typing import Optional
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_URL

engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class BankrollState(Base):
    """Per-strategy bankroll tracking. id=1 for sigma, id=2 for forecast_edge, id=3 for spectrum."""
    __tablename__ = "bankroll_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    balance: Mapped[float] = mapped_column(Float, nullable=False)
    starting_balance: Mapped[float] = mapped_column(Float, nullable=False)
    daily_loss_today: Mapped[float] = mapped_column(Float, default=0.0)
    last_reset_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    strategy: Mapped[Optional[str]] = mapped_column(String(20), default="sigma")
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class Trade(Base):
    """Every paper trade — open and settled."""
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Market info
    city: Mapped[str] = mapped_column(String(50))
    station_id: Mapped[str] = mapped_column(String(10))
    threshold_f: Mapped[float] = mapped_column(Float)          # e.g. 68.0
    direction: Mapped[str] = mapped_column(String(5))           # YES or NO
    market_condition: Mapped[str] = mapped_column(String(100))  # "High ≥ 68°F"
    market_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Polymarket market data (real)
    polymarket_market_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    polymarket_token_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    market_yes_price: Mapped[float] = mapped_column(Float)
    market_volume: Mapped[float] = mapped_column(Float)

    # NOAA forecast data (real)
    noaa_forecast_high: Mapped[float] = mapped_column(Float)
    noaa_sigma: Mapped[float] = mapped_column(Float)
    noaa_true_prob: Mapped[float] = mapped_column(Float)
    noaa_condition: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    forecast_day_offset: Mapped[int] = mapped_column(Integer, default=0)

    # Signal quality
    edge_pct: Mapped[float] = mapped_column(Float)
    confidence: Mapped[float] = mapped_column(Float)
    kelly_raw: Mapped[float] = mapped_column(Float)
    kelly_capped: Mapped[float] = mapped_column(Float)

    # Position (paper)
    position_size_usd: Mapped[float] = mapped_column(Float)
    entry_price: Mapped[float] = mapped_column(Float)
    shares: Mapped[float] = mapped_column(Float)
    bankroll_at_entry: Mapped[float] = mapped_column(Float)

    # Resolution
    status: Mapped[str] = mapped_column(String(20), default="OPEN")
    actual_high_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    gross_pnl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fees_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    net_pnl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bankroll_after: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Multi-model consensus tracking (legacy field kept for backward compat)
    gfs_forecast: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ecmwf_forecast: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    models_agreed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    early_window: Mapped[bool] = mapped_column(Boolean, default=False)

    # Re-entry tracking
    entry_number: Mapped[int] = mapped_column(Integer, default=1)
    prior_entry_edge: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    crowd_price_at_prior: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Calibration tracking
    forecast_error_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # ── A/B Testing fields ────────────────────────────────────────────────────
    strategy: Mapped[Optional[str]] = mapped_column(String(20), default="sigma")
    forecast_gap: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    validator_gap: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    same_side_as_forecast: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    models_directionally_agree: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    models_on_bet_side_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    model_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # ── Strategy C (Spectrum) bucket-specific fields ──────────────────────────
    bucket_low: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bucket_high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bucket_label: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    bucket_forecast_prob: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bucket_market_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bucket_center: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # ── V2 ensemble / fill simulation fields (added by migrate_v2.py) ────────
    ensemble_prob: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ensemble_members_in_bucket: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ensemble_total_members: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    gfs_peak_bucket_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ecmwf_peak_bucket_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    model_agreement: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    price_source: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    market_ask: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    market_midpoint: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    spread_at_entry: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    book_depth_at_entry: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    simulated_vwap: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    simulated_shares: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    simulated_cost: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fill_quality: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    model_run_time: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    venue: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    edge_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Ladder-specific
    ladder_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    package_cost: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    package_prob: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    package_edge: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    num_legs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Timestamps
    opened_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class ScanLog(Base):
    """Log of every scan run."""
    __tablename__ = "scan_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    cities_scanned: Mapped[int] = mapped_column(Integer)
    signals_found: Mapped[int] = mapped_column(Integer)
    trades_opened: Mapped[int] = mapped_column(Integer)
    trades_settled: Mapped[int] = mapped_column(Integer)
    bankroll_snapshot: Mapped[float] = mapped_column(Float)
    errors: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class CityCalibration(Base):
    """Daily NOAA forecast vs actual."""
    __tablename__ = "city_calibration"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    city: Mapped[str] = mapped_column(String(50))
    station_id: Mapped[str] = mapped_column(String(10))
    date: Mapped[str] = mapped_column(String(20))
    forecast_high: Mapped[float] = mapped_column("forecast_high_f", Float)
    actual_high_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    forecast_error_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sigma_used: Mapped[float] = mapped_column(Float)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class BucketMappingDiagnostic(Base):
    """Bucket mapping diagnostics (feature-flagged)."""
    __tablename__ = "bucket_mapping_diagnostics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scanned_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    city: Mapped[str] = mapped_column(String(50))
    market_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    threshold: Mapped[float] = mapped_column(Float)
    direction: Mapped[str] = mapped_column(String(5))
    synthetic_prob: Mapped[float] = mapped_column(Float)
    synthetic_edge: Mapped[float] = mapped_column(Float)
    match_type: Mapped[str] = mapped_column(String(20))
    is_directly_tradable: Mapped[bool] = mapped_column(Boolean, default=False)
    nearest_bucket_label: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    basket_count: Mapped[int] = mapped_column(Integer, default=0)
    basket_yes_prob: Mapped[float] = mapped_column(Float, default=0.0)
    prob_gap: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    approximation_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    polymarket_market_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)


class LogEvent(Base):
    """
    Temporary log drain table — receives Railway log lines via POST /internal/log-drain.
    Intended for short-term monitoring (a day or two). Disable by removing the
    Railway log drain URL. Auto-purges entries older than 48h on each drain call.
    """
    __tablename__ = "log_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    received_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    log_timestamp: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    service: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    matched_term: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    message: Mapped[str] = mapped_column(Text)


async def init_db():
    """Create all tables on startup. Safe to call multiple times."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Ensure bankroll rows exist for all strategies (v1 legacy + v2)
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select

        V2_STARTING = 500.0  # v2 bankroll per strategy (spec Section 8)

        # All strategy rows: id → (starting_balance, strategy_name)
        strategy_rows = {
            1: (2000.0, "sigma"),           # v1 legacy
            2: (2000.0, "forecast_edge"),   # v1 legacy
            3: (V2_STARTING, "spectrum"),
            4: (V2_STARTING, "sniper_yes"),
            5: (V2_STARTING, "sniper_no"),
            6: (V2_STARTING, "ladder_3"),
            7: (V2_STARTING, "ladder_5"),
        }

        for row_id, (starting, strategy_name) in strategy_rows.items():
            result = await session.execute(
                select(BankrollState).where(BankrollState.id == row_id)
            )
            if not result.scalar_one_or_none():
                session.add(BankrollState(
                    id=row_id,
                    balance=starting,
                    starting_balance=starting,
                    daily_loss_today=0.0,
                    strategy=strategy_name,
                ))

        await session.commit()


async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
