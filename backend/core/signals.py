# backend/core/signals.py
import logging
import math
from datetime import datetime, date, timezone
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, STARTING_BANKROLL
from models.database import Trade, BankrollState, ScanLog, CityCalibration

logger = logging.getLogger(__name__)


# ── Kelly sizing ────────────────────────────────────────────────────────────────

def compute_kelly_size(
    edge: float,
    entry_price: float,
    confidence: float,
    bankroll: float,
    correlated_yes_count: int = 0,
) -> dict:
    cfg = BOT_CONFIG

    kelly_raw = (edge * confidence) / max(0.001, 1 - entry_price)
    kelly_q = kelly_raw * cfg["kelly_fraction"]
    correlation_factor = 0.5 if correlated_yes_count >= cfg["max_correlated_yes"] else 1.0
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


# ── Signal evaluation ───────────────────────────────────────────────────────────

def evaluate_signal(
    city: str,
    threshold: float,
    noaa_prob: float,
    market_yes_price: float,
    confidence: float,
    direction: str,
    bankroll: float,
    open_city_date_positions: set,   # set of (city, date_str) tuples
    open_yes_positions: int,
    market_date: str = None,         # YYYY-MM-DD — required for per-date dedup
    volume: float = 0.0,              # kept for API compatibility — not used in filter logic
    # Forecast values for directional gate + consensus
    primary_forecast: float = None,     # NOAA for US, ECMWF for intl
    primary_source: str = None,         # "NOAA/NWS", "ECMWF", or "Open-Meteo-GFS"
    is_celsius: bool = False,
    # Model consensus
    gfs_forecast: float = None,
    ecmwf_forecast: float = None,
    # Timing
    is_early_window: bool = False,
    # Re-entry
    entry_number: int = 1,
    prior_entry_edge: float = None,
    crowd_price_at_prior: float = None,
) -> tuple[bool, str, dict]:
    """
    V2 filter stack. Returns (should_trade, reason, sizing_info).

    Hard gates (no override):
    1. Directional gate — noaa_prob must agree with trade direction
    Then confidence/sizing modifiers:
    2. Core filters — edge, confidence, price bounds, crowd conviction
    3. Multi-model consensus — affects sizing, not a hard veto unless all disagree
    4. Early window boost — slightly relaxes confidence, boosts sizing
    5. Re-entry rules — escalating EV requirement, cooldown, crowd move check
    """
    cfg = BOT_CONFIG

    entry_price = market_yes_price if direction == "YES" else (1 - market_yes_price)
    edge = abs(noaa_prob - market_yes_price)

    # ── HARD GATE 1: Directional gate ────────────────────────────────────────────
    # Use noaa_prob (cumulative CDF), not raw forecast vs threshold.
    # Thresholds: YES requires NOAA >= 15%, NO requires NOAA <= 85%.
    MIN_DIR_PROB_YES = 0.15
    MAX_DIR_PROB_NO  = 0.85
    if direction == "YES" and noaa_prob < MIN_DIR_PROB_YES:
        return False, (
            f"Directional gate: P(>={threshold}) = {noaa_prob:.1%} too low for YES"
        ), {}
    if direction == "NO" and noaa_prob > MAX_DIR_PROB_NO:
        return False, (
            f"Directional gate: P(>={threshold}) = {noaa_prob:.1%} too high for NO"
        ), {}

    # ── Filter 2: Minimum edge ───────────────────────────────────────────────────
    effective_min_edge = cfg["min_edge"]
    if entry_number > 1:
        effective_min_edge = cfg["min_edge"] + cfg["reentry_min_edge_premium"]

    if edge < effective_min_edge:
        return False, f"Edge {edge:.1%} < min {effective_min_edge:.1%}", {}

    # ── Filter 3: Confidence ─────────────────────────────────────────────────────
    effective_min_confidence = cfg["min_confidence"]
    if is_early_window:
        effective_min_confidence = max(0.50, cfg["min_confidence"] - cfg["early_window_confidence_boost"])

    if confidence < effective_min_confidence:
        return False, f"Confidence {confidence:.1%} < min {effective_min_confidence:.1%}", {}

    # ── Filter 4: Price bounds ───────────────────────────────────────────────────
    if direction == "YES" and market_yes_price > cfg["max_yes_price"]:
        return False, f"YES price {market_yes_price:.2f} > max {cfg['max_yes_price']:.2f}", {}
    if direction == "NO" and market_yes_price < cfg["min_no_price"]:
        return False, f"NO side: YES price {market_yes_price:.2f} < floor {cfg['min_no_price']:.2f}", {}

    # ── Filter 5: Crowd conviction ───────────────────────────────────────────────
    if direction == "NO" and market_yes_price > cfg["max_yes_price_for_no"]:
        return False, (
            f"Crowd conviction too high: YES at {market_yes_price:.2f} > "
            f"{cfg['max_yes_price_for_no']:.2f} — skipping NO"
        ), {}

    # ── Filter 6: One position per city-date (unless re-entry enabled) ─────────
    # Same city + same date → blocked unless valid re-entry
    # Same city + different date → allowed (city-wide caps enforced in scanner)
    if (city, market_date) in open_city_date_positions and entry_number == 1:
        return False, f"Already have open position in {city} for {market_date}", {}

    # ── Filter 7: Bankroll floor ─────────────────────────────────────────────────
    if bankroll < cfg["bankroll_floor"]:
        return False, f"Bankroll ${bankroll:.2f} below floor ${cfg['bankroll_floor']:.2f}", {}

    # ── Re-entry checks (only applies when entry_number > 1) ────────────────────
    if entry_number > 1 and cfg.get("reentry_enabled"):
        if crowd_price_at_prior is not None:
            crowd_move = abs(market_yes_price - crowd_price_at_prior)
            if crowd_move < cfg["reentry_min_crowd_move"]:
                return False, (
                    f"Re-entry: crowd move {crowd_move:.2f} < min {cfg['reentry_min_crowd_move']:.2f} "
                    f"— no material repricing"
                ), {}

        if prior_entry_edge is not None:
            hwm = min(prior_entry_edge, cfg["reentry_edge_hwm_cap"])
            if edge < hwm + cfg["reentry_min_edge_improvement"]:
                return False, (
                    f"Re-entry: edge {edge:.1%} doesn't beat HWM {hwm:.1%} + "
                    f"premium {cfg['reentry_min_edge_improvement']:.1%}"
                ), {}

        if entry_number > cfg["reentry_max_per_city"] + 1:
            return False, f"Re-entry: max {cfg['reentry_max_per_city']} re-entries reached for {city}", {}

    # ── Multi-model consensus: determine sizing factor ───────────────────────────
    models_agreed = 1  # primary source always counts
    consensus_factor = 1.0
    spread_note = ""

    # If primary source is GFS-based (intl fallback), exclude GFS validator
    # to prevent pseudo-consensus from GFS agreeing with itself.
    primary_is_gfs = primary_source and "GFS" in primary_source.upper()
    if primary_is_gfs and gfs_forecast is not None:
        # GFS validator is same source family as primary — not independent
        independent_validators = [f for f in [ecmwf_forecast] if f is not None]
        all_forecasts = [f for f in [primary_forecast] + independent_validators if f is not None]
        spread_note = " [GFS-primary: validator excluded]"
    else:
        all_forecasts = [f for f in [primary_forecast, gfs_forecast, ecmwf_forecast] if f is not None]

    if len(all_forecasts) >= 2:
        # Count how many models agree on direction using prob_above, not raw comparison.
        # Reconstruct sigma from confidence:
        #   confidence = clip(1 - (sigma_f - 3.0) / 10, 0.50, 0.95)
        #   => sigma_f = 3.0 + (1 - confidence) * 10
        from data.noaa import prob_above as _prob_above
        sigma_f = 3.0 + (1.0 - confidence) * 10.0
        sigma_for_models = sigma_f * (5 / 9) if is_celsius else sigma_f
        if direction == "YES":
            models_agreed = sum(
                1 for f in all_forecasts
                if _prob_above(threshold, f, sigma_for_models) >= 0.20
            )
        else:
            models_agreed = sum(
                1 for f in all_forecasts
                if _prob_above(threshold, f, sigma_for_models) <= 0.80
            )

        # Check max spread between any two models — wide spread reduces size, not a veto
        max_spread = cfg["max_model_spread_c"] if is_celsius else cfg["max_model_spread_f"]
        spread = max(all_forecasts) - min(all_forecasts)
        if spread > max_spread:
            consensus_factor = cfg["consensus_reduced_factor"]
            spread_note = f" [spread={spread:.1f}>{max_spread}->reduced]"

        total_models = len(all_forecasts)
        if models_agreed == total_models and not spread_note:
            consensus_factor = 1.0
        elif models_agreed >= 2 and not spread_note:
            consensus_factor = cfg["consensus_reduced_factor"]
        elif models_agreed < 2:
            return False, (
                f"No model consensus: only {models_agreed}/{total_models} agree on direction"
            ), {}
    elif primary_is_gfs:
        # Single-model: GFS-fallback primary with no independent validator.
        # Still trade but at reduced size — no second opinion available.
        consensus_factor = cfg["consensus_reduced_factor"]
        models_agreed = 1
        if not spread_note:
            spread_note = " [1-model: GFS-primary, no independent validator]"

    # ── Compute sizing ───────────────────────────────────────────────────────────
    sizing = compute_kelly_size(edge, entry_price, confidence, bankroll, open_yes_positions)

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

    return True, "ALL_FILTERS_PASSED", sizing


# ── Database operations ──────────────────────────────────────────────────────────

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
        kelly_raw=sizing["kelly_raw"],
        kelly_capped=sizing["kelly_capped"],
        position_size_usd=size,
        entry_price=round(entry_price, 4),
        shares=sizing["shares"],
        bankroll_at_entry=bankroll_state.balance,
        status="OPEN",
        gfs_forecast=noaa_data.get("gfs_forecast"),
        ecmwf_forecast=noaa_data.get("ecmwf_forecast"),
        models_agreed=sizing.get("models_agreed"),
        early_window=sizing.get("early_window", False),
        entry_number=sizing.get("entry_number", 1),
        prior_entry_edge=noaa_data.get("prior_entry_edge"),
        crowd_price_at_prior=noaa_data.get("crowd_price_at_prior"),
        market_date=noaa_data.get("market_date"),
    )
    session.add(trade)

    bankroll_state.balance = round(bankroll_state.balance - size, 2)
    await session.flush()

    logger.info(
        f"[TRADE] OPEN {city} >={threshold}{unit} {direction} | "
        f"Entry={entry_price:.3f} | Size=${size} | Edge={edge:.1%} | Bankroll->${bankroll_state.balance:.2f}"
    )
    return trade


async def settle_trade(
    session: AsyncSession,
    trade: Trade,
    bankroll_state: BankrollState,
    actual_high_f: Optional[float] = None,
    polymarket_won: Optional[bool] = None,
) -> dict:
    """
    Settle a trade. Two resolution modes:

    1. Polymarket-resolved (preferred): polymarket_won is set directly from
       which bucket won on Polymarket. actual_high_f is optional — used for
       forecast error tracking only, not for WIN/LOSS determination.

    2. Observation-resolved (legacy fallback): actual_high_f is required,
       WIN/LOSS computed by comparing to threshold.
    """
    cfg = BOT_CONFIG

    # Determine WIN/LOSS
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

    # Forecast error — only if we have a real observed high
    forecast_error = None
    if actual_high_f is not None:
        forecast_error = round(actual_high_f - trade.noaa_forecast_high, 2)

    trade.status = status
    trade.actual_high_f = actual_high_f  # may be None if only Polymarket-resolved
    trade.resolved_at = datetime.now(timezone.utc).replace(tzinfo=None)  # naive to match DB column
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
        f"[SETTLE] {trade.city} >={trade.threshold_f}{unit} {trade.direction} | "
        f"Actual={actual_str} | {status} | Net P&L=${net_pnl:+.2f} | "
        f"Forecast error={error_str} | Via={resolution_src} | Bankroll->${bankroll_state.balance:.2f}"
    )

    return {"status": status, "net_pnl": net_pnl, "fees": fees, "forecast_error": forecast_error}


async def log_calibration(
    session: AsyncSession,
    city: str,
    station_id: str,
    forecast_high: float,
    actual_high: Optional[float],
    sigma: float,
    market_date: str = None,
):
    cal_date = market_date or datetime.now(timezone.utc).date().isoformat()
    existing = await session.execute(
        select(CityCalibration).where(
            CityCalibration.city == city,
            CityCalibration.date == cal_date,
        )
    )
    if existing.scalar_one_or_none():
        return

    error = round(actual_high - forecast_high, 2) if actual_high else None
    cal = CityCalibration(
        city=city,
        station_id=station_id,
        date=cal_date,
        forecast_high=forecast_high,
        actual_high_f=actual_high,
        forecast_error_f=error,
        sigma_used=sigma,
    )
    session.add(cal)


async def reset_daily_loss(session: AsyncSession, bankroll_state: BankrollState):
    today = datetime.now(timezone.utc).date().isoformat()
    last_reset = bankroll_state.last_reset_date or ""
    if last_reset != today:
        bankroll_state.daily_loss_today = 0.0
        bankroll_state.last_reset_date = today
        logger.info("[BOT] Daily loss counter reset")
