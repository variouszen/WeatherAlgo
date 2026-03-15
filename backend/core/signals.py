# backend/core/signals.py
import logging
import math
from datetime import datetime, date, timezone
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, FORECAST_EDGE_CONFIG, STARTING_BANKROLL, STRATEGY_BANKROLL_ID
from models.database import Trade, BankrollState, ScanLog, CityCalibration

logger = logging.getLogger(__name__)


# ── Shared utilities ─────────────────────────────────────────────────────────

def compute_kelly_size(edge, entry_price, confidence, bankroll, correlated_yes_count=0, cfg=None):
    if cfg is None:
        cfg = BOT_CONFIG
    kelly_raw = (edge * confidence) / max(0.001, 1 - entry_price)
    kelly_q = kelly_raw * cfg["kelly_fraction"]
    correlation_factor = 0.5 if correlated_yes_count >= cfg.get("max_correlated_yes", 3) else 1.0
    kelly_capped = min(kelly_q * correlation_factor, cfg["max_position_pct"])
    size_raw = bankroll * kelly_capped
    size = max(cfg["min_position_usd"], round(size_raw, 2))
    size = min(size, bankroll * cfg["max_position_pct"])
    return {
        "kelly_raw": round(kelly_raw, 4),
        "kelly_capped": round(kelly_capped, 4),
        "size_usd": round(size, 2),
        "shares": round(size / max(0.001, entry_price), 4),
        "correlation_factor": correlation_factor,
    }


def compute_forecast_analytics(direction, threshold, primary_forecast, gfs_forecast=None, is_celsius=False):
    """Compute analytics fields for any trade (both strategies)."""
    if direction == "YES":
        forecast_gap = primary_forecast - threshold
        same_side = primary_forecast > threshold
    else:
        forecast_gap = threshold - primary_forecast
        same_side = primary_forecast < threshold

    validator_gap = None
    if gfs_forecast is not None:
        validator_gap = (gfs_forecast - threshold) if direction == "YES" else (threshold - gfs_forecast)

    models_on_bet_side = 1 if same_side else 0
    model_count = 1
    models_directionally_agree = same_side

    if gfs_forecast is not None:
        model_count = 2
        gfs_same_side = (gfs_forecast > threshold) if direction == "YES" else (gfs_forecast < threshold)
        if gfs_same_side:
            models_on_bet_side += 1
        models_directionally_agree = same_side and gfs_same_side

    return {
        "forecast_gap": round(forecast_gap, 2),
        "validator_gap": round(validator_gap, 2) if validator_gap is not None else None,
        "same_side_as_forecast": same_side,
        "models_directionally_agree": models_directionally_agree,
        "models_on_bet_side_count": models_on_bet_side,
        "model_count": model_count,
    }


# ── Strategy B: Sigma signal evaluation (with FIXED consensus) ───────────────

def evaluate_signal(
    city, threshold, noaa_prob, market_yes_price, confidence, direction, bankroll,
    open_city_date_positions, open_yes_positions, market_date=None, volume=0.0,
    primary_forecast=None, primary_source=None, is_celsius=False,
    gfs_forecast=None, icon_forecast=None, is_early_window=False,
    entry_number=1, prior_entry_edge=None, crowd_price_at_prior=None,
):
    """Strategy B (Sigma) filter stack with FIXED directional consensus."""
    cfg = BOT_CONFIG
    entry_price = market_yes_price if direction == "YES" else (1 - market_yes_price)
    edge = abs(noaa_prob - market_yes_price)

    # ── Directional probability gate ──────────────────────────────────────────
    if direction == "YES" and noaa_prob < 0.15:
        return False, f"Directional gate: P(>={threshold}) = {noaa_prob:.1%} too low for YES", {}
    if direction == "NO" and noaa_prob > 0.85:
        return False, f"Directional gate: P(>={threshold}) = {noaa_prob:.1%} too high for NO", {}

    # ── Minimum edge ──────────────────────────────────────────────────────────
    effective_min_edge = cfg["min_edge"]
    if entry_number > 1:
        effective_min_edge = cfg["min_edge"] + cfg["reentry_min_edge_premium"]
    if edge < effective_min_edge:
        return False, f"Edge {edge:.1%} < min {effective_min_edge:.1%}", {}

    # ── Confidence ────────────────────────────────────────────────────────────
    effective_min_confidence = cfg["min_confidence"]
    if is_early_window:
        effective_min_confidence = max(0.50, cfg["min_confidence"] - cfg["early_window_confidence_boost"])
    if confidence < effective_min_confidence:
        return False, f"Confidence {confidence:.1%} < min {effective_min_confidence:.1%}", {}

    # ── Price bounds ──────────────────────────────────────────────────────────
    if direction == "YES" and market_yes_price > cfg["max_yes_price"]:
        return False, f"YES price {market_yes_price:.2f} > max {cfg['max_yes_price']:.2f}", {}
    if direction == "NO" and market_yes_price < cfg["min_no_price"]:
        return False, f"NO side: YES price {market_yes_price:.2f} < floor {cfg['min_no_price']:.2f}", {}

    # ── Crowd conviction ──────────────────────────────────────────────────────
    if direction == "NO" and market_yes_price > cfg["max_yes_price_for_no"]:
        return False, f"Crowd conviction too high: YES at {market_yes_price:.2f} > {cfg['max_yes_price_for_no']:.2f}", {}

    # ── Dedup: city-date-threshold (strategy-scoped in scanner) ───────────────
    if (city, market_date, threshold) in open_city_date_positions and entry_number == 1:
        return False, f"Already have sigma position in {city}/{market_date}/{threshold}", {}

    # ── Bankroll floor ────────────────────────────────────────────────────────
    if bankroll < cfg["bankroll_floor"]:
        return False, f"Bankroll ${bankroll:.2f} below floor ${cfg['bankroll_floor']:.2f}", {}

    # ── Re-entry checks ──────────────────────────────────────────────────────
    if entry_number > 1 and cfg.get("reentry_enabled"):
        if crowd_price_at_prior is not None:
            crowd_move = abs(market_yes_price - crowd_price_at_prior)
            if crowd_move < cfg["reentry_min_crowd_move"]:
                return False, f"Re-entry: crowd move {crowd_move:.2f} < min {cfg['reentry_min_crowd_move']:.2f}", {}
        if prior_entry_edge is not None:
            hwm = min(prior_entry_edge, cfg["reentry_edge_hwm_cap"])
            if edge < hwm + cfg["reentry_min_edge_improvement"]:
                return False, f"Re-entry: edge {edge:.1%} doesn't beat HWM {hwm:.1%}", {}
        if entry_number > cfg["reentry_max_per_city"] + 1:
            return False, f"Re-entry: max {cfg['reentry_max_per_city']} re-entries reached for {city}", {}

    # ── FIXED: Directional consensus (replaces old probability-based check) ──
    consensus_factor = 1.0
    spread_note = ""
    models_agreed = 1

    primary_is_gfs = primary_source and "GFS" in primary_source.upper()
    if primary_is_gfs and gfs_forecast is not None:
        independent_validators = [f for f in [icon_forecast] if f is not None]
        all_forecasts = [f for f in [primary_forecast] + independent_validators if f is not None]
        spread_note = " [GFS-primary: validator excluded]"
    else:
        all_forecasts = [f for f in [primary_forecast, gfs_forecast, icon_forecast] if f is not None]

    if len(all_forecasts) >= 2:
        # Directional agreement: all forecasts must be on the bet side of threshold
        # YES → all must be > threshold, NO → all must be < threshold
        # Equality = not on bet side → disagreement
        if direction == "YES":
            dir_agree = all(f > threshold for f in all_forecasts)
            on_bet_side = sum(1 for f in all_forecasts if f > threshold)
        else:
            dir_agree = all(f < threshold for f in all_forecasts)
            on_bet_side = sum(1 for f in all_forecasts if f < threshold)

        models_agreed = on_bet_side

        if not dir_agree:
            return False, (
                f"Models directionally disagree: {on_bet_side}/{len(all_forecasts)} "
                f"on {'above' if direction == 'YES' else 'below'} side of {threshold} "
                f"(forecasts: {[round(f,1) for f in all_forecasts]})"
            ), {}

        # Spread gate (magnitude only — direction already confirmed)
        max_spread = cfg["max_model_spread_c"] if is_celsius else cfg["max_model_spread_f"]
        spread = max(all_forecasts) - min(all_forecasts)
        if spread > max_spread:
            consensus_factor = cfg["consensus_reduced_factor"]
            spread_note = f" [spread={spread:.1f}>{max_spread}->reduced]"
    elif primary_is_gfs:
        consensus_factor = cfg["consensus_reduced_factor"]
        models_agreed = 1
        if not spread_note:
            spread_note = " [1-model: GFS-primary, no independent validator]"

    # ── Compute sizing ────────────────────────────────────────────────────────
    sizing = compute_kelly_size(edge, entry_price, confidence, bankroll, open_yes_positions, cfg)
    sizing["size_usd"] = round(sizing["size_usd"] * consensus_factor, 2)
    sizing["size_usd"] = max(cfg["min_position_usd"], sizing["size_usd"])

    if is_early_window:
        boosted = round(sizing["size_usd"] * cfg["early_window_kelly_boost"], 2)
        max_size = bankroll * cfg["max_position_pct"]
        sizing["size_usd"] = min(boosted, max_size)

    if sizing["size_usd"] > bankroll:
        return False, "Insufficient bankroll for minimum position", {}

    sizing["models_agreed"] = models_agreed
    sizing["consensus_factor"] = consensus_factor
    sizing["spread_note"] = spread_note
    sizing["early_window"] = is_early_window
    sizing["entry_number"] = entry_number
    sizing["strategy"] = "sigma"

    return True, "ALL_FILTERS_PASSED", sizing


# ── Strategy A: Forecast Edge signal evaluation ──────────────────────────────

def evaluate_signal_forecast_edge(
    city, threshold, noaa_prob, market_yes_price, confidence, direction, bankroll,
    open_city_date_positions, open_yes_positions, market_date=None,
    primary_forecast=None, is_celsius=False,
):
    """
    Strategy A (Forecast Edge) — simplified gate stack.
    No consensus, no spread gate, no validator, no directional probability gate.
    The forecast gap IS the margin of safety.
    """
    cfg = FORECAST_EDGE_CONFIG
    entry_price = market_yes_price if direction == "YES" else (1 - market_yes_price)
    edge = abs(noaa_prob - market_yes_price)

    # ── GATE 1: Forecast gap ─────────────────────────────────────────────────
    gap_threshold = cfg["forecast_gap_c"] if is_celsius else cfg["forecast_gap_f"]
    if direction == "YES":
        gap = primary_forecast - threshold
        if gap < gap_threshold:
            return False, f"Forecast gap: {primary_forecast:.1f} only {gap:.1f} above {threshold} (need ≥{gap_threshold})", {}
    else:
        gap = threshold - primary_forecast
        if gap < gap_threshold:
            return False, f"Forecast gap: {primary_forecast:.1f} only {gap:.1f} below {threshold} (need ≥{gap_threshold})", {}

    # ── GATE 2: Minimum edge ─────────────────────────────────────────────────
    if edge < cfg["min_edge"]:
        return False, f"Edge {edge:.1%} < min {cfg['min_edge']:.1%}", {}

    # ── GATE 3: Price bounds ─────────────────────────────────────────────────
    if direction == "YES" and market_yes_price > cfg["max_yes_price"]:
        return False, f"YES price {market_yes_price:.2f} > max {cfg['max_yes_price']:.2f}", {}
    if direction == "NO" and market_yes_price < cfg["min_no_price"]:
        return False, f"NO: YES price {market_yes_price:.2f} < floor {cfg['min_no_price']:.2f}", {}

    # ── GATE 4: Crowd conviction ─────────────────────────────────────────────
    if direction == "NO" and market_yes_price > cfg["max_yes_price_for_no"]:
        return False, f"Crowd too high: YES at {market_yes_price:.2f} > {cfg['max_yes_price_for_no']:.2f}", {}

    # ── GATE 5: Dedup (city-date-threshold, strategy-scoped in scanner) ──────
    if (city, market_date, threshold) in open_city_date_positions:
        return False, f"Already have forecast_edge position in {city}/{market_date}/{threshold}", {}

    # ── GATE 6: Bankroll floor ───────────────────────────────────────────────
    if bankroll < cfg["bankroll_floor"]:
        return False, f"Bankroll ${bankroll:.2f} below floor", {}

    # ── Sizing (sigma for Kelly math only) ───────────────────────────────────
    sizing = compute_kelly_size(edge, entry_price, confidence, bankroll, open_yes_positions, cfg)
    if sizing["size_usd"] > bankroll:
        return False, "Insufficient bankroll", {}

    sizing["models_agreed"] = None
    sizing["consensus_factor"] = 1.0
    sizing["spread_note"] = ""
    sizing["early_window"] = False
    sizing["entry_number"] = 1
    sizing["strategy"] = "forecast_edge"

    return True, "ALL_FILTERS_PASSED", sizing


# ── Database operations ──────────────────────────────────────────────────────

async def get_bankroll(session, strategy="sigma"):
    """Get bankroll state for a specific strategy."""
    bankroll_id = STRATEGY_BANKROLL_ID.get(strategy, 1)
    result = await session.execute(select(BankrollState).where(BankrollState.id == bankroll_id))
    state = result.scalar_one_or_none()
    if not state:
        state = BankrollState(id=bankroll_id, balance=STARTING_BANKROLL, starting_balance=STARTING_BANKROLL, strategy=strategy)
        session.add(state)
        await session.flush()
    return state


async def get_open_positions(session, strategy=None):
    """Get open positions, optionally filtered by strategy."""
    q = select(Trade).where(Trade.status == "OPEN")
    if strategy:
        q = q.where(Trade.strategy == strategy)
    result = await session.execute(q)
    return list(result.scalars().all())


async def open_paper_trade(
    session, city, station_id, threshold, direction, market_data, noaa_data, sizing,
    bankroll_state, strategy="sigma", forecast_analytics=None,
):
    size = sizing["size_usd"]
    entry_price = market_data["yes_price"] if direction == "YES" else (1 - market_data["yes_price"])
    noaa_prob = noaa_data.get("bucket_probs", {}).get(threshold)
    if noaa_prob is None:
        from data.noaa import prob_above
        noaa_prob = round(prob_above(threshold, noaa_data["forecast_high"], noaa_data["sigma"]), 4)
    edge = abs(noaa_prob - market_data["yes_price"])
    unit = noaa_data.get("unit", "F")
    fa = forecast_analytics or {}

    trade = Trade(
        city=city, station_id=station_id, threshold_f=threshold, direction=direction,
        market_condition=f"High >= {threshold}{unit}",
        polymarket_market_id=market_data.get("market_id"),
        polymarket_token_id=market_data.get("token_id"),
        market_yes_price=market_data["yes_price"],
        market_volume=market_data["volume"],
        noaa_forecast_high=noaa_data["forecast_high"],
        noaa_sigma=noaa_data["sigma"],
        noaa_true_prob=noaa_prob,
        noaa_condition=noaa_data.get("condition"),
        forecast_day_offset=noaa_data.get("day_offset", 0),
        edge_pct=round(edge, 4),
        confidence=noaa_data["confidence"],
        kelly_raw=sizing["kelly_raw"], kelly_capped=sizing["kelly_capped"],
        position_size_usd=size, entry_price=round(entry_price, 4),
        shares=sizing["shares"], bankroll_at_entry=bankroll_state.balance,
        status="OPEN",
        gfs_forecast=noaa_data.get("gfs_forecast"),
        ecmwf_forecast=noaa_data.get("ecmwf_forecast"),
        models_agreed=sizing.get("models_agreed"),
        early_window=sizing.get("early_window", False),
        entry_number=sizing.get("entry_number", 1),
        prior_entry_edge=noaa_data.get("prior_entry_edge"),
        crowd_price_at_prior=noaa_data.get("crowd_price_at_prior"),
        market_date=noaa_data.get("market_date"),
        # A/B testing fields
        strategy=strategy,
        forecast_gap=fa.get("forecast_gap"),
        validator_gap=fa.get("validator_gap"),
        same_side_as_forecast=fa.get("same_side_as_forecast"),
        models_directionally_agree=fa.get("models_directionally_agree"),
        models_on_bet_side_count=fa.get("models_on_bet_side_count"),
        model_count=fa.get("model_count"),
    )
    session.add(trade)

    bankroll_state.balance = round(bankroll_state.balance - size, 2)
    await session.flush()

    logger.info(
        f"[TRADE] OPEN [{strategy}] {city} >={threshold}{unit} {direction} | "
        f"Entry={entry_price:.3f} | Size=${size} | Edge={edge:.1%} | "
        f"Gap={fa.get('forecast_gap','?')} | Bankroll->${bankroll_state.balance:.2f}"
    )
    return trade


async def settle_trade(session, trade, bankroll_state, actual_high_f=None, polymarket_won=None):
    """Settle a trade. Unchanged from original except uses strategy-scoped bankroll."""
    cfg = BOT_CONFIG

    if polymarket_won is not None:
        won = polymarket_won
    elif actual_high_f is not None:
        won = (
            (trade.direction == "YES" and actual_high_f >= trade.threshold_f) or
            (trade.direction == "NO"  and actual_high_f < trade.threshold_f)
        )
    else:
        logger.warning(f"[SETTLE] Cannot settle {trade.city} — no resolution source")
        return {"status": "ERROR", "net_pnl": 0, "fees": 0, "forecast_error": None}

    if won:
        gross_payout = round(trade.shares * 1.0, 2)
        gross_pnl = round(gross_payout - trade.position_size_usd, 2)
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
        bankroll_state.daily_loss_today = round(
            bankroll_state.daily_loss_today + trade.position_size_usd, 2
        )

    forecast_error = None
    if actual_high_f is not None:
        forecast_error = round(actual_high_f - trade.noaa_forecast_high, 2)

    trade.status = status
    trade.actual_high_f = actual_high_f
    trade.resolved_at = datetime.now(timezone.utc).replace(tzinfo=None)
    trade.gross_pnl = gross_pnl
    trade.fees_usd = fees
    trade.net_pnl = net_pnl
    trade.bankroll_after = bankroll_state.balance
    trade.forecast_error_f = forecast_error

    await session.flush()

    unit = "C" if str(trade.market_condition or "").endswith("C") else "F"
    actual_str = f"{actual_high_f}{unit}" if actual_high_f is not None else "N/A(poly-resolved)"
    error_str = f"{forecast_error:+.1f}{unit}" if forecast_error is not None else "pending"
    resolution_src = "POLYMARKET" if polymarket_won is not None else "OBSERVATION"
    logger.info(
        f"[SETTLE] [{trade.strategy or 'sigma'}] {trade.city} >={trade.threshold_f}{unit} {trade.direction} | "
        f"Actual={actual_str} | {status} | Net=${net_pnl:+.2f} | Error={error_str} | "
        f"Via={resolution_src} | Bankroll->${bankroll_state.balance:.2f}"
    )
    return {"status": status, "net_pnl": net_pnl, "fees": fees, "forecast_error": forecast_error}


async def log_calibration(session, city, station_id, forecast_high, actual_high, sigma, market_date=None):
    cal_date = market_date or datetime.now(timezone.utc).date().isoformat()
    existing = await session.execute(
        select(CityCalibration).where(CityCalibration.city == city, CityCalibration.date == cal_date)
    )
    if existing.scalar_one_or_none():
        return
    error = round(actual_high - forecast_high, 2) if actual_high else None
    cal = CityCalibration(
        city=city, station_id=station_id, date=cal_date,
        forecast_high=forecast_high, actual_high_f=actual_high,
        forecast_error_f=error, sigma_used=sigma,
    )
    session.add(cal)


async def reset_daily_loss(session, bankroll_state):
    today = datetime.now(timezone.utc).date().isoformat()
    last_reset = bankroll_state.last_reset_date or ""
    if last_reset != today:
        bankroll_state.daily_loss_today = 0.0
        bankroll_state.last_reset_date = today
        logger.info(f"[BOT] Daily loss counter reset [{bankroll_state.strategy or 'sigma'}]")
