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
    """Single-row table tracking current paper bankroll."""
    __tablename__ = "bankroll_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    balance: Mapped[float] = mapped_column(Float, nullable=False)
    starting_balance: Mapped[float] = mapped_column(Float, nullable=False)
    daily_loss_today: Mapped[float] = mapped_column(Float, default=0.0)
    last_reset_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
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
    market_date: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # "2026-03-11" — the temp date being bet on

    # Polymarket market data (real)
    polymarket_market_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    polymarket_token_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    market_yes_price: Mapped[float] = mapped_column(Float)      # Real orderbook price at entry
    market_volume: Mapped[float] = mapped_column(Float)

    # NOAA forecast data (real)
    noaa_forecast_high: Mapped[float] = mapped_column(Float)
    noaa_sigma: Mapped[float] = mapped_column(Float)
    noaa_true_prob: Mapped[float] = mapped_column(Float)        # P(high >= threshold)
    noaa_condition: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    forecast_day_offset: Mapped[int] = mapped_column(Integer, default=0)

    # Signal quality
    edge_pct: Mapped[float] = mapped_column(Float)              # noaa_prob - market_price
    confidence: Mapped[float] = mapped_column(Float)
    kelly_raw: Mapped[float] = mapped_column(Float)
    kelly_capped: Mapped[float] = mapped_column(Float)

    # Position (paper)
    position_size_usd: Mapped[float] = mapped_column(Float)     # Dollars risked
    entry_price: Mapped[float] = mapped_column(Float)           # Price paid per share
    shares: Mapped[float] = mapped_column(Float)                # position_size / entry_price
    bankroll_at_entry: Mapped[float] = mapped_column(Float)

    # Resolution
    status: Mapped[str] = mapped_column(String(20), default="OPEN")  # OPEN, WIN, LOSS
    actual_high_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    gross_pnl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fees_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    net_pnl: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bankroll_after: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Multi-model consensus tracking
    gfs_forecast: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ecmwf_forecast: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    models_agreed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # how many models agreed
    early_window: Mapped[bool] = mapped_column(Boolean, default=False)             # fired in early window

    # Re-entry tracking
    entry_number: Mapped[int] = mapped_column(Integer, default=1)                  # 1=first, 2=re-entry, etc
    prior_entry_ev: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # EV of previous entry
    crowd_price_at_prior: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Calibration tracking
    forecast_error_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # actual - forecast

    # Timestamps
    opened_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class ScanLog(Base):
    """Log of every scan run — for debugging and performance tracking."""
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
    """Daily NOAA forecast vs actual — even when no trade was placed."""
    __tablename__ = "city_calibration"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    city: Mapped[str] = mapped_column(String(50))
    station_id: Mapped[str] = mapped_column(String(10))
    date: Mapped[str] = mapped_column(String(20))               # YYYY-MM-DD
    forecast_high_f: Mapped[float] = mapped_column(Float)
    actual_high_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    forecast_error_f: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sigma_used: Mapped[float] = mapped_column(Float)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


async def init_db():
    """Create all tables on startup. Safe to call multiple times."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Ensure bankroll row exists
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select
        result = await session.execute(select(BankrollState).where(BankrollState.id == 1))
        row = result.scalar_one_or_none()
        if not row:
            from config import STARTING_BANKROLL
            session.add(BankrollState(
                id=1,
                balance=STARTING_BANKROLL,
                starting_balance=STARTING_BANKROLL,
                daily_loss_today=0.0,
            ))
            await session.commit()


async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
