# backend/core/signals.py
import logging
import math
from datetime import datetime, date
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, STARTING_BANKROLL
from models.database import Trade, BankrollState, ScanLog, CityCalibration

logger = logging.getLogger(__name__)


# ── Kelly sizing ──────────────────────────────────────────────────────────────

def compute_kelly_size(
    edge: float,
    entry_price: float,
    confidence: float,
    bankroll: float,
    correlated_yes_count: int = 0,
) -> dict:
    """
    Quarter-Kelly with multiple caps.
    Returns breakdown dict for transparency.
    """
    cfg = BOT_CONFIG

    # Raw Kelly
    kelly_raw = (edge * confidence) / max(0.001, 1 - entry_price)

    # Quarter-Kelly
    kelly_q = kelly_raw * cfg["kelly_fraction"]

    # Correlation discount: if ≥3 correlated YES positions open, halve sizing
    correlation_factor = 0.5 if correlated_yes_count >= cfg["max_correlated_yes"] else 1.0

    # Apply pct cap
    kelly_capped = min(kelly_q * correlation_factor, cfg["max_position_pct"])

    # Dollar size
    size_raw = bankroll * kelly_capped
    size = max(cfg["min_position_usd"], round(size_raw, 2))

    # Hard cap: never more than max_position_pct of bankroll
    size = min(size, bankroll * cfg["max_position_pct"])

    return {
        "kelly_raw": round(kelly_raw, 4),
        "kelly_capped": round(kelly_capped, 4),
        "size_usd": round(size, 2),
        "shares": round(size / max(0.001, entry_price), 4),
        "correlation_factor": correlation_factor,
    }


# ── Signal evaluation ─────────────────────────────────────────────────────────

def evaluate_signal(
    city: str,
    threshold: float,
    noaa_prob: float,
    market_yes_price: float,
    volume: float,
    confidence: float,
    direction: str,
    bankroll: float,
    open_city_positions: list[str],
    open_yes_positions: int,
) -> tuple[bool, str, dict]:
    """
    Run all filters. Returns (should_trade, reason, sizing_info).
    """
    cfg = BOT_CONFIG

    entry_price = market_yes_price if direction == "YES" else (1 - market_yes_price)
    edge = abs(noaa_prob - market_yes_price)

    # Filter 1: Minimum edge
    if edge < cfg["min_edge"]:
        return False, f"Edge {edge:.1%} < min {cfg['min_edge']:.1%}", {}

    # Filter 2: Confidence
    if confidence < cfg["min_confidence"]:
        return False, f"Confidence {confidence:.1%} < min {cfg['min_confidence']:.1%}", {}

    # Filter 3: Volume
    if volume < cfg["min_market_volume"]:
        return False, f"Volume ${volume:,.0f} < min ${cfg['min_market_volume']:,.0f}", {}

    # Filter 4: Price bounds (avoid buying overpriced side)
    if direction == "YES" and market_yes_price > cfg["max_yes_price"]:
        return False, f"YES price {market_yes_price:.2f} > max {cfg['max_yes_price']:.2f}", {}
    if direction == "NO" and market_yes_price < cfg["min_no_price"]:
        return False, f"NO side: YES price {market_yes_price:.2f} < floor {cfg['min_no_price']:.2f}", {}

    # Filter 5: One position per city
    if city in open_city_positions:
        return False, f"Already have open position in {city}", {}

    # Filter 6: Bankroll floor
    if bankroll < cfg["bankroll_floor"]:
        return False, f"Bankroll ${bankroll:.2f} below floor ${cfg['bankroll_floor']:.2f}", {}

    # All passed — compute sizing
    sizing = compute_kelly_size(edge, entry_price, confidence, bankroll, open_yes_positions)

    # Check we can afford it
    if sizing["size_usd"] > bankroll:
        return False, "Insufficient bankroll for minimum position", {}

    return True, "ALL_FILTERS_PASSED", sizing


# ── Database operations ───────────────────────────────────────────────────────

async def get_bankroll(session: AsyncSession) -> BankrollState:
    result = await session.execute(select(BankrollState).where(BankrollState.id == 1))
    state = result.scalar_one_or_none()
    if not state:
        state = BankrollState(id=1, balance=STARTING_BANKROLL, starting_balance=STARTING_BANKROLL)
        session.add(state)
        await session.flush()
    return state


async def get_open_positions(session: AsyncSession) -> list[Trade]:
    result = await session.execute(select(Trade).where(Trade.status == "OPEN"))
    return list(result.scalars().all())


async def open_paper_trade(
    session: AsyncSession,
    city: str,
    station_id: str,
    threshold: float,
    direction: str,
    market_data: dict,
    noaa_data: dict,
    sizing: dict,
    bankroll_state: BankrollState,
) -> Trade:
    """INSERT a new paper trade and deduct from bankroll."""
    size = sizing["size_usd"]
    entry_price = market_data["yes_price"] if direction == "YES" else (1 - market_data["yes_price"])
    noaa_prob = noaa_data["bucket_probs"][threshold]
    edge = abs(noaa_prob - market_data["yes_price"])
    unit = noaa_data.get("unit", "F")

    trade = Trade(
        city=city,
        station_id=station_id,
        threshold_f=threshold,
        direction=direction,
        market_condition=f"High ≥ {threshold}°{unit}",
        polymarket_market_id=market_data.get("market_id"),
        polymarket_token_id=market_data.get("token_id"),
        market_yes_price=market_data["yes_price"],
        market_volume=market_data["volume"],
        noaa_forecast_high=noaa_data["forecast_high_f"],
        noaa_sigma=noaa_data["sigma"],
        noaa_true_prob=noaa_prob,
        noaa_condition=noaa_data.get("condition"),
        forecast_day_offset=noaa_data.get("day_offset", 0),
        edge_pct=round(edge, 4),
        confidence=noaa_data["confidence"],
        kelly_raw=sizing["kelly_raw"],
        kelly_capped=sizing["kelly_capped"],
        position_size_usd=size,
        entry_price=round(entry_price, 4),
        shares=sizing["shares"],
        bankroll_at_entry=bankroll_state.balance,
        status="OPEN",
    )
    session.add(trade)

    # Deduct from bankroll
    bankroll_state.balance = round(bankroll_state.balance - size, 2)

    await session.flush()
    logger.info(
        f"[TRADE] OPEN {city} ≥{threshold}°{unit} {direction} | "
        f"Entry={entry_price:.3f} | Size=${size} | Edge={edge:.1%} | Bankroll→${bankroll_state.balance:.2f}"
    )
    return trade


async def settle_trade(
    session: AsyncSession,
    trade: Trade,
    actual_high_f: float,
    bankroll_state: BankrollState,
) -> dict:
    """
    Resolve a trade against the actual observed temperature.
    Apply Polymarket 2% fee on winnings. Update bankroll.
    """
    cfg = BOT_CONFIG
    won = (
        (trade.direction == "YES" and actual_high_f >= trade.threshold_f) or
        (trade.direction == "NO"  and actual_high_f < trade.threshold_f)
    )

    if won:
        # Gross payout: shares × $1.00
        gross_payout = round(trade.shares * 1.0, 2)
        gross_pnl = round(gross_payout - trade.position_size_usd, 2)
        # Fee: 2% of gross profit
        fees = round(gross_pnl * cfg["polymarket_fee_pct"], 2)
        net_pnl = round(gross_pnl - fees, 2)
        payout_received = round(gross_payout - fees, 2)
        bankroll_state.balance = round(bankroll_state.balance + payout_received, 2)
        status = "WIN"
    else:
        gross_pnl = round(-trade.position_size_usd, 2)
        fees = 0.0
        net_pnl = gross_pnl
        status = "LOSS"
        # Track realized loss for circuit breaker
        bankroll_state.daily_loss_today = round(
            bankroll_state.daily_loss_today + trade.position_size_usd, 2
        )

    forecast_error = round(actual_high_f - trade.noaa_forecast_high, 2)

    # Update trade record
    trade.status = status
    trade.actual_high_f = actual_high_f
    trade.resolved_at = datetime.utcnow()
    trade.gross_pnl = gross_pnl
    trade.fees_usd = fees
    trade.net_pnl = net_pnl
    trade.bankroll_after = bankroll_state.balance
    trade.forecast_error_f = forecast_error

    await session.flush()

    unit = "C" if trade.threshold_f != int(trade.threshold_f * 9/5 + 32) and trade.threshold_f < 50 else "F"

    logger.info(
        f"[SETTLE] {trade.city} ≥{trade.threshold_f}°{unit} {trade.direction} | "
        f"Actual={actual_high_f}°{unit} | {status} | Net P&L=${net_pnl:+.2f} | "
        f"Forecast error={forecast_error:+.1f}°{unit} | Bankroll→${bankroll_state.balance:.2f}"
    )

    return {
        "status": status,
        "net_pnl": net_pnl,
        "fees": fees,
        "forecast_error": forecast_error,
    }


async def log_calibration(
    session: AsyncSession,
    city: str,
    station_id: str,
    forecast_high: float,
    actual_high: Optional[float],
    sigma: float,
):
    """Log daily forecast vs actual for calibration analysis."""
    today = date.today().isoformat()
    
    # Check if we already logged today
    existing = await session.execute(
        select(CityCalibration).where(
            CityCalibration.city == city,
            CityCalibration.date == today,
        )
    )
    if existing.scalar_one_or_none():
        return  # Already logged today

    error = round(actual_high - forecast_high, 2) if actual_high else None
    cal = CityCalibration(
        city=city,
        station_id=station_id,
        date=today,
        forecast_high_f=forecast_high,
        actual_high_f=actual_high,
        forecast_error_f=error,
        sigma_used=sigma,
    )
    session.add(cal)


async def reset_daily_loss(session: AsyncSession, bankroll_state: BankrollState):
    """Reset daily loss counter if it's a new day."""
    today = date.today().isoformat()
    last_reset = bankroll_state.last_reset_date or ""
    if last_reset != today:
        bankroll_state.daily_loss_today = 0.0
        bankroll_state.last_reset_date = today
        logger.info("[BOT] Daily loss counter reset")
