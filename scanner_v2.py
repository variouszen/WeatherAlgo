"""
WeatherAlgo v2 — Scanner (Phase 3)

Wires Phase 1 (ensemble + venue adapter) and Phase 2 (evaluators + fill sim)
into a complete scan loop for paper trading.

Scan flow per cycle:
  1. For each city, for day+0 and day+1:
     a. Discover markets via PolymarketAdapter
     b. Fetch ensemble probabilities (GFS + ECMWF)
     c. Evaluate all 5 strategies independently
     d. Open paper trades for passing signals
  2. Settle resolved trades
"""
from __future__ import annotations

import logging
import time
import asyncio
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Optional

from sqlalchemy import select

from config import (
    CITIES, STRATEGY_BANKROLL_ID, DRY_RUN, SCAN_INTERVAL_SECONDS,
    SPECTRUM_V2_CONFIG, SNIPER_YES_CONFIG, SNIPER_NO_CONFIG,
    LADDER_3_CONFIG, LADDER_5_CONFIG,
)
from models.database import AsyncSessionLocal, Trade, BankrollState, ScanLog

# Phase 1 modules
from forecast.ensemble import fetch_ensemble_members

# Phase 2 modules
from signals.spectrum import evaluate_spectrum
from signals.sniper import evaluate_sniper_yes, evaluate_sniper_no
from signals.ladder import evaluate_ladder

# Phase 3 trade persistence
from trade_manager import (
    open_v2_trade, open_v2_ladder, settle_v2_trade, reset_ladder_counter,
)

# Shared utilities from v1 signals (still valid)
from core.signals import get_bankroll, get_open_positions, reset_daily_loss

logger = logging.getLogger(__name__)

SETTLEMENT_ROUNDING = 0.5

# Model suffixes returned by Open-Meteo ensemble API
GFS_SUFFIX = "ncep_gefs_seamless"
ECMWF_SUFFIX = "ecmwf_ifs025_ensemble"

city_by_name = {c["name"]: c for c in CITIES}

# V2 strategy names
V2_STRATEGIES = ["spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"]


# ── GFS model-run scan sync (Spec Section 4) ────────────────────────────────
# GFS runs initialize at 00, 06, 12, 18 UTC.
# Data typically available ~3.5-4.5 hours after initialization.
# Optimal scan windows: ~04:00, ~10:00, ~16:00, ~22:00 UTC.
# Between model runs: scan every 5 minutes for market price changes
# using cached ensemble signal (signal unchanged, prices may move).

GFS_SCAN_WINDOWS = [
    # (window_start_hour, window_end_hour) — UTC
    # Each window opens ~3.5h after GFS init, closes ~2h later
    (3, 6),    # 00Z run → data available ~03:30-05:30 UTC
    (9, 12),   # 06Z run → data available ~09:30-11:30 UTC
    (15, 18),  # 12Z run → data available ~15:30-17:30 UTC
    (21, 24),  # 18Z run → data available ~21:30-23:30 UTC
]


def is_in_gfs_scan_window(utc_now: Optional[datetime] = None) -> tuple[bool, str]:
    """
    Check whether the current UTC time falls inside a GFS model-run scan window.

    Returns:
        (is_eligible, reason) — is_eligible=True means fresh ensemble fetch,
        False means reuse cached ensemble signal (market prices may still move).
    """
    if utc_now is None:
        utc_now = datetime.now(timezone.utc)

    current_hour = utc_now.hour + utc_now.minute / 60.0

    for start_h, end_h in GFS_SCAN_WINDOWS:
        actual_end = end_h if end_h <= 24 else end_h - 24
        if end_h > 24:
            # Window wraps midnight (21-24 → 21:00-00:00)
            if current_hour >= start_h or current_hour < actual_end:
                return True, f"GFS window {start_h:02d}-{end_h:02d}Z"
        else:
            if start_h <= current_hour < end_h:
                return True, f"GFS window {start_h:02d}-{end_h:02d}Z"

    return False, f"between GFS runs (hour={utc_now.hour:02d}:{utc_now.minute:02d}Z)"


# ── Ensemble signal cache ────────────────────────────────────────────────────
# Between GFS windows, reuse the last valid ensemble context per city-date.
# Keyed by (city_name, target_date) → dict with ensemble_probs, peaks, etc.
# Refreshed when a fresh fetch succeeds during a GFS window.

_ensemble_cache: dict[tuple[str, str], dict] = {}


def _clear_ensemble_cache():
    """Clear the ensemble cache."""
    global _ensemble_cache
    _ensemble_cache = {}


# ── Ensemble helpers ─────────────────────────────────────────────────────────

def _extract_day_members(ensemble_result, target_date: str):
    """
    Extract per-model member values for a specific date from ensemble result.

    Returns:
        (all_values, gfs_values, ecmwf_values) — lists of float
        Returns ([], [], []) if target_date not found.
    """
    dates = ensemble_result.dates if hasattr(ensemble_result, 'dates') else []
    members_by_model = (
        ensemble_result.members_by_model
        if hasattr(ensemble_result, 'members_by_model')
        else {}
    )

    # Find day index matching target_date
    day_index = None
    for i, d in enumerate(dates):
        if str(d) == target_date:
            day_index = i
            break

    if day_index is None:
        logger.warning(f"Target date {target_date} not in ensemble dates: {dates}")
        return [], [], []

    all_values = []
    gfs_values = []
    ecmwf_values = []

    for model_key, member_arrays in members_by_model.items():
        for member_arr in member_arrays:
            if day_index < len(member_arr):
                val = member_arr[day_index]
                if val is not None:
                    all_values.append(val)
                    if "gfs" in model_key.lower() or "gefs" in model_key.lower() or "ncep" in model_key.lower():
                        gfs_values.append(val)
                    elif "ecmwf" in model_key.lower():
                        ecmwf_values.append(val)

    return all_values, gfs_values, ecmwf_values


def _compute_bucket_probs_from_members(member_values: list[float], buckets: list) -> dict[str, float]:
    """
    Count ensemble members per bucket with settlement rounding correction.

    Args:
        member_values: list of temperature values from ensemble members
        buckets: list of BucketMarket objects

    Returns:
        dict mapping bucket_label → probability
    """
    total = len(member_values)
    if total == 0:
        return {}

    probs = {}
    for bkt in buckets:
        low = bkt.bucket_low
        high = bkt.bucket_high

        # Apply settlement rounding (Spec Section 4)
        if low is None:
            # Lower tail: "45°F or below" → value < 45.5
            settle_low = float("-inf")
            settle_high = high + SETTLEMENT_ROUNDING if high is not None else float("inf")
        elif high is None:
            # Upper tail: "64°F or higher" → value >= 63.5
            settle_low = low - SETTLEMENT_ROUNDING
            settle_high = float("inf")
        else:
            # Interior: "58-59°F" → 57.5 <= value < 59.5
            settle_low = low - SETTLEMENT_ROUNDING
            settle_high = high + SETTLEMENT_ROUNDING

        count = 0
        for val in member_values:
            if settle_low == float("-inf"):
                in_bucket = val < settle_high
            elif settle_high == float("inf"):
                in_bucket = val >= settle_low
            else:
                in_bucket = settle_low <= val < settle_high
            if in_bucket:
                count += 1

        probs[bkt.bucket_label] = count / total

    return probs


def _find_peak_index(probs: dict[str, float], buckets: list) -> int:
    """Find index of bucket with highest probability."""
    best_idx = 0
    best_prob = 0.0
    for i, bkt in enumerate(buckets):
        p = probs.get(bkt.bucket_label, 0.0)
        if p > best_prob:
            best_prob = p
            best_idx = i
    return best_idx


# ── PolymarketAdapter lazy loader ────────────────────────────────────────────

_adapter = None

def _get_adapter():
    """Lazy-initialize PolymarketAdapter."""
    global _adapter
    if _adapter is None:
        from venue.polymarket_adapter import PolymarketAdapter
        _adapter = PolymarketAdapter(dry_run=DRY_RUN)
    return _adapter


# ── Settlement ───────────────────────────────────────────────────────────────

async def _settle_v2_trades(session, log_fn):
    """Settle all open v2 trades where the market has resolved."""
    from data.polymarket import check_event_resolution

    all_open = await get_open_positions(session)
    v2_open = [t for t in all_open if t.strategy in V2_STRATEGIES]

    if not v2_open:
        return 0

    settled_count = 0
    today = datetime.now(timezone.utc).date()

    # Group bankrolls
    bankrolls = {}
    for strat in V2_STRATEGIES:
        bankrolls[strat] = await get_bankroll(session, strat)

    for trade in v2_open:
        # Skip future-dated trades
        if trade.market_date:
            try:
                trade_date = datetime.strptime(trade.market_date, "%Y-%m-%d").date()
            except Exception:
                trade_date = today
        else:
            trade_date = trade.opened_at.date() if trade.opened_at else today

        if trade_date > today:
            continue

        # Check resolution
        try:
            resolution = await check_event_resolution(
                city=trade.city,
                market_date_str=trade.market_date or trade_date.isoformat(),
            )
        except Exception as e:
            log_fn(f"Settlement check failed {trade.city}/{trade.market_date}: {e}", "WARN")
            continue

        if resolution is None or not resolution.get("resolved", False):
            if trade_date < today:
                log_fn(f"STALE? [{trade.strategy}] {trade.city} {trade.bucket_label} | Date={trade_date}", "WARN")
            continue

        # Extract winning bucket label from resolution
        winning_label = resolution.get("winning_bucket_label")
        if not winning_label:
            # Try to reconstruct from low/high
            wb_low = resolution.get("winning_bucket_low")
            wb_high = resolution.get("winning_bucket_high")
            if wb_low is not None and wb_high is not None:
                winning_label = f"{int(wb_low)}-{int(wb_high)}°F"
            elif wb_low is not None:
                winning_label = f"{int(wb_low)}"

        # Get actual high for logging
        actual_high = resolution.get("estimated_high")

        bs = bankrolls.get(trade.strategy)
        if not bs:
            continue

        result = await settle_v2_trade(
            session=session,
            trade=trade,
            bankroll_state=bs,
            winning_bucket_label=winning_label,
            actual_high=actual_high,
        )
        settled_count += 1
        log_fn(f"SETTLED [{trade.strategy}] {trade.city} {trade.bucket_label} {trade.direction} | "
               f"{result['status']} | Net=${result['net_pnl']:+.2f}")

    return settled_count


# ── Main scan loop ───────────────────────────────────────────────────────────

async def run_scan_v2():
    """
    Full v2 scan cycle — 5 strategy evaluation.

    For each city × day:
      1. Discover Polymarket bucket markets
      2. Fetch ensemble forecasts (GFS + ECMWF)
      3. Compute ensemble bucket probabilities
      4. Evaluate Spectrum, Sniper YES, Sniper NO, Ladder 3, Ladder 5
      5. Open paper trades for passing signals
    Then settle resolved trades.
    """
    start_ms = int(time.time() * 1000)
    scan_result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "cities_scanned": 0, "signals_found": 0,
        "trades_opened": 0, "trades_settled": 0,
        "errors": [], "log_lines": [],
    }

    def log(msg, level="INFO"):
        logger.info(msg) if level == "INFO" else logger.warning(msg)
        scan_result["log_lines"].append(f"[{level}] {msg}")

    adapter = _get_adapter()
    reset_ladder_counter()

    # ── GFS scan sync: decide fresh ensemble vs cached signal ────────
    from config import SCAN_SYNC_GFS_RUNS
    gfs_eligible, gfs_reason = is_in_gfs_scan_window()
    refresh_ensemble = gfs_eligible or not SCAN_SYNC_GFS_RUNS
    if refresh_ensemble:
        log(f"FULL scan — fresh ensemble ({gfs_reason})")
    else:
        log(f"CACHED-SIGNAL scan — reusing last ensemble ({gfs_reason})")

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # ── Load bankrolls for all 5 v2 strategies ───────────────────
            bankrolls = {}
            for strat in V2_STRATEGIES:
                bankrolls[strat] = await get_bankroll(session, strat)
                await reset_daily_loss(session, bankrolls[strat])

            # ── Daily loss cap check — build blocked set ────────────────
            config_map = {
                "spectrum": SPECTRUM_V2_CONFIG,
                "sniper_yes": SNIPER_YES_CONFIG,
                "sniper_no": SNIPER_NO_CONFIG,
                "ladder_3": LADDER_3_CONFIG,
                "ladder_5": LADDER_5_CONFIG,
            }
            blocked_strategies = set()
            for strat, bs in bankrolls.items():
                cfg = config_map[strat]
                cap = cfg.get("max_daily_loss", 50.0)
                if bs.daily_loss_today >= cap:
                    blocked_strategies.add(strat)
                    log(f"CIRCUIT BREAKER [{strat}]: Daily loss ${bs.daily_loss_today:.2f} >= cap ${cap:.2f} — BLOCKED for this scan", "WARN")

            # ── Build per-strategy dedup sets ────────────────────────────
            all_open = await get_open_positions(session)
            dedup = {strat: set() for strat in V2_STRATEGIES}
            for t in all_open:
                s = t.strategy or ""
                if s in dedup:
                    dedup[s].add((t.city, t.market_date))

            # Cross-ladder dedup: union of ladder_3 and ladder_5
            ladder_cross_dedup = dedup["ladder_3"] | dedup["ladder_5"]

            # ── Scan each city × day (always — use cache between windows) ─
            utc_today = datetime.now(timezone.utc).date()
            cities_scanned = set()

            for city_cfg in CITIES:
                city = city_cfg["name"]
                lat = city_cfg["lat"]
                lon = city_cfg["lon"]
                station = city_cfg["station"]
                is_celsius = city_cfg.get("celsius", False)
                tz_name = city_cfg.get("timezone", "UTC")

                for day_offset in [0, 1]:
                    target_date = (utc_today + timedelta(days=day_offset)).isoformat()

                    # ── Noon local-time guard (day+0 only) ───────────────
                    if day_offset == 0:
                        try:
                            local_now = datetime.now(timezone.utc).astimezone(ZoneInfo(tz_name))
                            if local_now.hour >= 12:
                                log(f"SKIP {city}/{target_date} | Day-0 past noon local ({local_now.strftime('%H:%M')} {tz_name})")
                                continue
                        except Exception:
                            pass

                    # ── Step 1: Discover markets ─────────────────────────
                    try:
                        buckets = await adapter.discover_markets(city_cfg, target_date)
                    except Exception as e:
                        log(f"Market discovery failed {city}/{target_date}: {e}", "WARN")
                        scan_result["errors"].append(f"Discovery {city}/{target_date}: {e}")
                        continue

                    if not buckets:
                        log(f"No buckets for {city}/{target_date}")
                        continue

                    cities_scanned.add(city)

                    # ── Step 2+3: Ensemble signal (fresh or cached) ──────
                    cache_key = (city, target_date)

                    if refresh_ensemble:
                        # GFS window: fetch fresh ensemble and cache it
                        try:
                            ensemble_result = await fetch_ensemble_members(
                                lat=lat, lon=lon,
                                models=["gfs_seamless", "ecmwf_ifs025"],
                                forecast_days=2,
                                timezone=tz_name,
                            )
                        except Exception as e:
                            log(f"Ensemble fetch failed {city}/{target_date}: {e}", "WARN")
                            scan_result["errors"].append(f"Ensemble {city}: {e}")
                            continue

                        all_members, gfs_members, ecmwf_members = _extract_day_members(
                            ensemble_result, target_date
                        )

                        if not all_members:
                            log(f"No ensemble members for {city}/{target_date}")
                            continue

                        total_members = len(all_members)
                        log(f"Ensemble {city}/{target_date}: {len(gfs_members)} GFS + {len(ecmwf_members)} ECMWF = {total_members} (fresh)")

                        # Compute bucket probabilities
                        ensemble_probs = _compute_bucket_probs_from_members(all_members, buckets)
                        gfs_probs = _compute_bucket_probs_from_members(gfs_members, buckets) if gfs_members else {}
                        ecmwf_probs = _compute_bucket_probs_from_members(ecmwf_members, buckets) if ecmwf_members else {}

                        gfs_peak = _find_peak_index(gfs_probs, buckets) if gfs_probs else 0
                        ecmwf_peak = _find_peak_index(ecmwf_probs, buckets) if ecmwf_probs else 0

                        model_agreement = abs(gfs_peak - ecmwf_peak) <= 2
                        log(f"Peaks {city}/{target_date}: GFS={gfs_peak} ECMWF={ecmwf_peak} Agreement={model_agreement}")

                        # Cache the signal context for between-window scans
                        _ensemble_cache[cache_key] = {
                            "ensemble_probs": ensemble_probs,
                            "gfs_probs": gfs_probs,
                            "ecmwf_probs": ecmwf_probs,
                            "gfs_peak": gfs_peak,
                            "ecmwf_peak": ecmwf_peak,
                            "total_members": total_members,
                        }

                    else:
                        # Between GFS windows: reuse cached signal
                        if cache_key not in _ensemble_cache:
                            log(f"SKIP {city}/{target_date} — no cached ensemble yet (between GFS windows)")
                            continue

                        cached = _ensemble_cache[cache_key]
                        ensemble_probs = cached["ensemble_probs"]
                        gfs_probs = cached["gfs_probs"]
                        ecmwf_probs = cached["ecmwf_probs"]
                        gfs_peak = cached["gfs_peak"]
                        ecmwf_peak = cached["ecmwf_peak"]
                        total_members = cached["total_members"]
                        log(f"Using cached ensemble for {city}/{target_date} (signal unchanged, checking market prices)")

                    # ── Step 4: Evaluate strategies (skip blocked) ─────────

                    # 4a. Spectrum
                    if "spectrum" not in blocked_strategies:
                        try:
                            sig_spectrum = await evaluate_spectrum(
                                buckets=buckets,
                                ensemble_probs=ensemble_probs,
                                gfs_peak_index=gfs_peak,
                                ecmwf_peak_index=ecmwf_peak,
                                bankroll=bankrolls["spectrum"].balance,
                                open_positions=dedup["spectrum"],
                                venue_adapter=adapter,
                                city=city,
                                market_date=target_date,
                                config=SPECTRUM_V2_CONFIG,
                                ensemble_total_members=total_members,
                            )
                            if sig_spectrum:
                                scan_result["signals_found"] += 1
                                await open_v2_trade(
                                    session, sig_spectrum, bankrolls["spectrum"],
                                    city, target_date, station,
                                )
                                dedup["spectrum"].add((city, target_date))
                                scan_result["trades_opened"] += 1
                                log(f"TRADE [{sig_spectrum.strategy}] {city}/{target_date} "
                                    f"{sig_spectrum.bucket_label} {sig_spectrum.side} | Edge={sig_spectrum.edge:.1%}")
                        except Exception as e:
                            log(f"Spectrum eval error {city}/{target_date}: {e}", "WARN")

                    # 4b. Sniper YES
                    if "sniper_yes" not in blocked_strategies:
                        try:
                            sig_sniper_y = await evaluate_sniper_yes(
                                buckets=buckets,
                                ensemble_probs=ensemble_probs,
                                gfs_peak_index=gfs_peak,
                                ecmwf_peak_index=ecmwf_peak,
                                gfs_bucket_probs=gfs_probs,
                                ecmwf_bucket_probs=ecmwf_probs,
                                bankroll=bankrolls["sniper_yes"].balance,
                                open_positions=dedup["sniper_yes"],
                                venue_adapter=adapter,
                                city=city,
                                market_date=target_date,
                                config=SNIPER_YES_CONFIG,
                                ensemble_total_members=total_members,
                            )
                            if sig_sniper_y:
                                scan_result["signals_found"] += 1
                                await open_v2_trade(
                                    session, sig_sniper_y, bankrolls["sniper_yes"],
                                    city, target_date, station,
                                )
                                dedup["sniper_yes"].add((city, target_date))
                                scan_result["trades_opened"] += 1
                                log(f"TRADE [{sig_sniper_y.strategy}] {city}/{target_date} "
                                    f"{sig_sniper_y.bucket_label} YES | Edge={sig_sniper_y.edge:.1%}")
                        except Exception as e:
                            log(f"Sniper YES eval error {city}/{target_date}: {e}", "WARN")

                    # 4c. Sniper NO
                    if "sniper_no" not in blocked_strategies:
                        try:
                            sig_sniper_n = await evaluate_sniper_no(
                                buckets=buckets,
                                ensemble_probs=ensemble_probs,
                                gfs_peak_index=gfs_peak,
                                ecmwf_peak_index=ecmwf_peak,
                                gfs_bucket_probs=gfs_probs,
                                ecmwf_bucket_probs=ecmwf_probs,
                                bankroll=bankrolls["sniper_no"].balance,
                                open_positions=dedup["sniper_no"],
                                venue_adapter=adapter,
                                city=city,
                                market_date=target_date,
                                config=SNIPER_NO_CONFIG,
                                ensemble_total_members=total_members,
                            )
                            if sig_sniper_n:
                                scan_result["signals_found"] += 1
                                await open_v2_trade(
                                    session, sig_sniper_n, bankrolls["sniper_no"],
                                    city, target_date, station,
                                )
                                dedup["sniper_no"].add((city, target_date))
                                scan_result["trades_opened"] += 1
                                log(f"TRADE [{sig_sniper_n.strategy}] {city}/{target_date} "
                                    f"{sig_sniper_n.bucket_label} NO | Edge={sig_sniper_n.edge:.1%}")
                        except Exception as e:
                            log(f"Sniper NO eval error {city}/{target_date}: {e}", "WARN")

                    # 4d. Ladder 3
                    if "ladder_3" not in blocked_strategies:
                        try:
                            sig_l3 = await evaluate_ladder(
                                buckets=buckets,
                                ensemble_probs=ensemble_probs,
                                gfs_peak_index=gfs_peak,
                                ecmwf_peak_index=ecmwf_peak,
                                bankroll=bankrolls["ladder_3"].balance,
                                open_positions=dedup["ladder_3"],
                                ladder_open_positions=dedup["ladder_5"],  # cross-ladder dedup
                                venue_adapter=adapter,
                                city=city,
                                market_date=target_date,
                                width=3,
                                config=LADDER_3_CONFIG,
                                ensemble_total_members=total_members,
                            )
                            if sig_l3:
                                scan_result["signals_found"] += 1
                                await open_v2_ladder(
                                    session, sig_l3, bankrolls["ladder_3"],
                                    city, target_date, station,
                                )
                                dedup["ladder_3"].add((city, target_date))
                                ladder_cross_dedup.add((city, target_date))
                                scan_result["trades_opened"] += len(sig_l3.legs)
                                log(f"LADDER [ladder_3] {city}/{target_date} | "
                                    f"{len(sig_l3.legs)} legs Cost=${sig_l3.package_cost:.2f} Edge={sig_l3.package_edge:.1%}")
                        except Exception as e:
                            log(f"Ladder 3 eval error {city}/{target_date}: {e}", "WARN")

                    # 4e. Ladder 5
                    if "ladder_5" not in blocked_strategies:
                        try:
                            sig_l5 = await evaluate_ladder(
                                buckets=buckets,
                                ensemble_probs=ensemble_probs,
                                gfs_peak_index=gfs_peak,
                                ecmwf_peak_index=ecmwf_peak,
                                bankroll=bankrolls["ladder_5"].balance,
                                open_positions=dedup["ladder_5"],
                                ladder_open_positions=dedup["ladder_3"],  # cross-ladder dedup
                                venue_adapter=adapter,
                                city=city,
                                market_date=target_date,
                                width=5,
                                config=LADDER_5_CONFIG,
                                ensemble_total_members=total_members,
                            )
                            if sig_l5:
                                scan_result["signals_found"] += 1
                                await open_v2_ladder(
                                    session, sig_l5, bankrolls["ladder_5"],
                                    city, target_date, station,
                                )
                                dedup["ladder_5"].add((city, target_date))
                                ladder_cross_dedup.add((city, target_date))
                                scan_result["trades_opened"] += len(sig_l5.legs)
                                log(f"LADDER [ladder_5] {city}/{target_date} | "
                                    f"{len(sig_l5.legs)} legs Cost=${sig_l5.package_cost:.2f} Edge={sig_l5.package_edge:.1%}")
                        except Exception as e:
                            log(f"Ladder 5 eval error {city}/{target_date}: {e}", "WARN")

                    # Brief delay between city-dates to avoid API throttle
                    await asyncio.sleep(0.5)

            scan_result["cities_scanned"] = len(cities_scanned)

            # ── Step 5: Settle resolved trades ───────────────────────────
            log("Checking settlements...")
            try:
                settled = await _settle_v2_trades(session, log)
                scan_result["trades_settled"] = settled
            except Exception as e:
                log(f"Settlement error: {e}", "WARN")
                scan_result["errors"].append(f"Settlement: {e}")

            # ── Step 6: Log scan ─────────────────────────────────────────
            duration_ms = int(time.time() * 1000) - start_ms
            total_bankroll = sum(bs.balance for bs in bankrolls.values())
            scan_log = ScanLog(
                cities_scanned=scan_result["cities_scanned"],
                signals_found=scan_result["signals_found"],
                trades_opened=scan_result["trades_opened"],
                trades_settled=scan_result["trades_settled"],
                bankroll_snapshot=total_bankroll,
                errors="; ".join(scan_result["errors"]) if scan_result["errors"] else None,
                duration_ms=duration_ms,
            )
            session.add(scan_log)

    scan_result["duration_ms"] = duration_ms
    bankroll_summary = " | ".join(f"{s}=${bankrolls[s].balance:.2f}" for s in V2_STRATEGIES)
    log(f"Scan complete in {duration_ms}ms | {bankroll_summary}")
    return scan_result
