"""
WeatherAlgo v2 — Trade Manager (Phase 3)

Maps TradeSignal / LadderSignal → Trade DB rows.
Handles v2 settlement (bucket-native for all strategies).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from models.database import Trade, BankrollState
from signals import TradeSignal, LadderSignal

logger = logging.getLogger(__name__)

# Polymarket weather markets: feesEnabled=false (Spec Section 7)
POLYMARKET_FEE_PCT = 0.0

# Shared counter for ladder_id (reset per scan)
_next_ladder_id = 1


def reset_ladder_counter():
    global _next_ladder_id
    _next_ladder_id = 1


def _get_next_ladder_id() -> int:
    global _next_ladder_id
    lid = _next_ladder_id
    _next_ladder_id += 1
    return lid


async def open_v2_trade(
    session: AsyncSession,
    signal: TradeSignal,
    bankroll_state: BankrollState,
    city: str,
    market_date: str,
    station_id: str = "",
) -> Trade:
    """
    Open a single v2 paper trade from a TradeSignal.
    Works for Spectrum, Sniper YES, and Sniper NO.
    """
    cost = signal.simulated_cost if signal.simulated_cost > 0 else signal.target_spend

    trade = Trade(
        city=city,
        station_id=station_id,
        threshold_f=0.0,                          # v1 field — not used in v2
        direction=signal.side,                     # "YES" or "NO"
        market_condition=f"v2:{signal.strategy} {signal.bucket_label}",
        market_date=market_date,
        polymarket_token_id=signal.token_id,
        market_yes_price=signal.market_ask,        # ask on traded side
        market_volume=0.0,                         # not tracked per-bucket in v2
        noaa_forecast_high=0.0,                    # v2 uses ensemble, not single forecast
        noaa_sigma=0.0,                            # retired
        noaa_true_prob=signal.ensemble_prob,
        forecast_day_offset=0,
        edge_pct=round(signal.edge, 4),
        confidence=1.0,                            # v2 fixed (no confidence concept)
        kelly_raw=0.0,                             # v2 uses fixed sizing
        kelly_capped=0.0,
        position_size_usd=round(cost, 2),
        entry_price=round(signal.entry_price, 4),
        shares=round(signal.simulated_shares, 4),
        bankroll_at_entry=bankroll_state.balance,
        status="OPEN",
        entry_number=1,
        market_ask=signal.market_ask,
        strategy=signal.strategy,
        # v2 ensemble fields
        ensemble_prob=signal.ensemble_prob,
        ensemble_members_in_bucket=signal.ensemble_members_in_bucket,
        ensemble_total_members=signal.ensemble_total_members,
        gfs_peak_bucket_index=signal.gfs_peak_index,
        ecmwf_peak_bucket_index=signal.ecmwf_peak_index,
        model_agreement=signal.model_agreement,
        # v2 fill sim fields
        price_source=signal.price_source,
        market_midpoint=signal.midpoint_at_entry,
        spread_at_entry=signal.spread_at_entry,
        book_depth_at_entry=signal.book_depth_at_entry,
        simulated_vwap=signal.entry_price,
        simulated_shares=signal.simulated_shares,
        simulated_cost=signal.simulated_cost,
        fill_quality=signal.fill_quality,
        model_run_time=signal.model_run_time,
        venue="polymarket",
        edge_ratio=signal.edge_ratio,
        # Bucket fields
        bucket_label=signal.bucket_label,
        bucket_low=0.0,      # populated by caller if needed
        bucket_high=None,
        bucket_forecast_prob=signal.ensemble_prob,
        bucket_market_price=signal.market_ask,
        # Ladder fields (None for non-ladder trades)
        ladder_id=signal.ladder_id,
        package_cost=signal.package_cost,
        package_prob=signal.package_prob,
        package_edge=signal.package_edge,
        num_legs=signal.num_legs,
    )
    session.add(trade)

    bankroll_state.balance = round(bankroll_state.balance - cost, 2)
    await session.flush()

    logger.info(
        f"[TRADE] OPEN [{signal.strategy}] {city}/{market_date} {signal.bucket_label} "
        f"{signal.side} | Entry={signal.entry_price:.3f} Cost=${cost:.2f} "
        f"Edge={signal.edge:.1%} | Fill={signal.fill_quality} "
        f"| Bankroll->${bankroll_state.balance:.2f}"
    )
    return trade


async def open_v2_ladder(
    session: AsyncSession,
    ladder: LadderSignal,
    bankroll_state: BankrollState,
    city: str,
    market_date: str,
    station_id: str = "",
) -> list[Trade]:
    """
    Open all legs of a ladder package as individual trades with shared ladder_id.
    """
    lid = _get_next_ladder_id()
    trades = []

    for leg in ladder.legs:
        leg.ladder_id = lid
        leg.package_cost = ladder.package_cost
        leg.package_prob = ladder.package_prob
        leg.package_edge = ladder.package_edge
        leg.num_legs = len(ladder.legs)

        trade = await open_v2_trade(
            session=session,
            signal=leg,
            bankroll_state=bankroll_state,
            city=city,
            market_date=market_date,
            station_id=station_id,
        )
        trades.append(trade)

    logger.info(
        f"[LADDER] OPEN [{ladder.strategy}] {city}/{market_date} | "
        f"{len(trades)} legs | Cost=${ladder.package_cost:.2f} "
        f"Prob={ladder.package_prob:.2f} Edge={ladder.package_edge:.1%} "
        f"| Bankroll->${bankroll_state.balance:.2f}"
    )
    return trades


async def settle_v2_trade(
    session: AsyncSession,
    trade: Trade,
    bankroll_state: BankrollState,
    winning_bucket_label: Optional[str] = None,
    actual_high: Optional[float] = None,
) -> dict:
    """
    Settle a v2 trade. All v2 strategies use bucket-native settlement:
    the trade wins if our bucket_label matches the winning bucket.

    For YES trades: win if our bucket is the winning bucket.
    For NO trades: win if our bucket is NOT the winning bucket.
    """
    if winning_bucket_label is None:
        logger.warning(f"[SETTLE] Cannot settle {trade.city} — no winning bucket")
        return {"status": "ERROR", "net_pnl": 0}

    our_bucket = trade.bucket_label or ""
    trade_side = trade.direction or "YES"

    if trade_side == "YES":
        won = (our_bucket == winning_bucket_label)
    else:
        # NO trade: we win if this bucket did NOT win
        won = (our_bucket != winning_bucket_label)

    if won:
        gross_payout = round(trade.shares * 1.0, 2)
        gross_pnl = round(gross_payout - trade.position_size_usd, 2)
        fees = round(gross_pnl * POLYMARKET_FEE_PCT, 2)
        net_pnl = round(gross_pnl - fees, 2)
        payout_received = round(gross_payout - fees, 2)
        bankroll_state.balance = round(bankroll_state.balance + payout_received, 2)
        status = "WIN"
    else:
        gross_pnl = round(-trade.position_size_usd, 2)
        fees = 0.0
        net_pnl = gross_pnl
        status = "LOSS"
        bankroll_state.daily_loss_today = round(
            bankroll_state.daily_loss_today + trade.position_size_usd, 2
        )

    trade.status = status
    trade.actual_high_f = actual_high
    trade.resolved_at = datetime.now(timezone.utc).replace(tzinfo=None)
    trade.gross_pnl = gross_pnl
    trade.fees_usd = fees
    trade.net_pnl = net_pnl
    trade.bankroll_after = bankroll_state.balance

    await session.flush()

    logger.info(
        f"[SETTLE] [{trade.strategy}] {trade.city} {our_bucket} {trade_side} | "
        f"{status} | Net=${net_pnl:+.2f} | Bankroll->${bankroll_state.balance:.2f}"
    )
    return {"status": status, "net_pnl": net_pnl, "fees": fees}
