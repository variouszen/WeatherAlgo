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
    """Per-strategy bankroll tracking. id=1 for sigma, id=2 for forecast_edge."""
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


async def init_db():
    """Create all tables on startup. Safe to call multiple times."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Ensure bankroll rows exist for both strategies
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select
        from config import STARTING_BANKROLL

        # Strategy B (sigma) — id=1
        result = await session.execute(select(BankrollState).where(BankrollState.id == 1))
        row = result.scalar_one_or_none()
        if not row:
            session.add(BankrollState(
                id=1,
                balance=STARTING_BANKROLL,
                starting_balance=STARTING_BANKROLL,
                daily_loss_today=0.0,
                strategy="sigma",
            ))

        # Strategy A (forecast_edge) — id=2
        result2 = await session.execute(select(BankrollState).where(BankrollState.id == 2))
        row2 = result2.scalar_one_or_none()
        if not row2:
            session.add(BankrollState(
                id=2,
                balance=STARTING_BANKROLL,
                starting_balance=STARTING_BANKROLL,
                daily_loss_today=0.0,
                strategy="forecast_edge",
            ))

        await session.commit()


async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
