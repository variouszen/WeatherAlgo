# backend/core/scanner.py
import logging
import asyncio
import time
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, CITIES, TEMP_THRESHOLDS_F, TEMP_THRESHOLDS_C, TEMP_THRESHOLDS
from data.noaa import fetch_all_cities, get_nws_daily_high, get_openmeteo_daily_high
from data.polymarket import build_market_map
from core.signals import (
    get_bankroll, get_open_positions,
    open_paper_trade, settle_trade,
    evaluate_signal, log_calibration,
    reset_daily_loss,
)
from models.database import AsyncSessionLocal, ScanLog

logger = logging.getLogger(__name__)

city_names = [c["name"] for c in CITIES]
city_by_name = {c["name"]: c for c in CITIES}

# ── Signal persistence buffer ─────────────────────────────────────────────────
# A signal must survive 2 consecutive scans before a position opens.
# Key: (city, threshold, direction)  →  signal_info dict
_pending_signals: dict[tuple, dict] = {}


async def run_scan() -> dict:
    """
    Full scan cycle:
    1. Reset daily loss / circuit breaker
    2. Fetch NOAA forecasts
    3. Settle open positions (next-day, after 20:00 UTC)
    4. Fetch Polymarket prices
    5. Evaluate signals with stability filters
    6. Confirm signals that appeared in 2 consecutive scans → open trades
    7. Log to Postgres
    """
    global _pending_signals

    start_ms = int(time.time() * 1000)
    scan_result = {
        "started_at": datetime.utcnow().isoformat(),
        "cities_scanned": 0,
        "signals_found": 0,
        "trades_opened": 0,
        "trades_settled": 0,
        "errors": [],
        "log_lines": [],
    }

    def log(msg: str, level: str = "INFO"):
        logger.info(msg) if level == "INFO" else logger.warning(msg)
        scan_result["log_lines"].append(f"[{level}] {msg}")

    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session)
            await reset_daily_loss(session, bankroll_state)

            # ── Step 1: Circuit breaker ───────────────────────────────────────
            cfg = BOT_CONFIG
            daily_loss_cap = bankroll_state.starting_balance * cfg["daily_loss_cap_pct"]
            if bankroll_state.daily_loss_today >= daily_loss_cap:
                log(f"CIRCUIT BREAKER: Daily loss ${bankroll_state.daily_loss_today:.2f} >= cap ${daily_loss_cap:.2f}", "WARN")
                scan_result["errors"].append("Daily loss cap hit")
                return scan_result

            # ── Step 2: Fetch NOAA ────────────────────────────────────────────
            log("Fetching NOAA forecasts...")
            try:
                forecasts = await fetch_all_cities(day_offset=0)
                scan_result["cities_scanned"] = len(forecasts)
                log(f"NOAA: Got forecasts for {len(forecasts)} cities")
            except Exception as e:
                log(f"NOAA fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"NOAA: {e}")
                return scan_result

            # ── Step 3: Settle open positions ─────────────────────────────────
            open_positions = await get_open_positions(session)
            log(f"Open positions: {len(open_positions)}")

            now_utc = datetime.utcnow()
            today = now_utc.date()
            settlement_hour_utc = 20

            for trade in open_positions:
                trade_date = trade.opened_at.date()

                if trade_date >= today:
                    log(f"Keeping {trade.city} ≥{trade.threshold_f} — opened today, not yet eligible")
                    continue

                if now_utc.hour < settlement_hour_utc:
                    log(f"Keeping {trade.city} ≥{trade.threshold_f} — before settlement window (need 20:00 UTC)")
                    continue

                city_cfg = city_by_name.get(trade.city)
                if not city_cfg:
                    log(f"No city config for {trade.city} — skipping", "WARN")
                    continue

                is_celsius = city_cfg.get("celsius", False)
                unit = "C" if is_celsius else "F"

                if is_celsius:
                    actual_high = await get_openmeteo_daily_high(
                        city_cfg["lat"], city_cfg["lon"], trade_date
                    )
                else:
                    actual_high = await get_nws_daily_high(
                        city_cfg["station"], trade_date
                    )

                if actual_high is None:
                    log(f"No daily high for {trade.city} on {trade_date} — keeping open", "WARN")
                    continue

                # Pass city_cfg so settle_trade knows the unit correctly
                result = await settle_trade(session, trade, actual_high, bankroll_state, city_cfg)
                scan_result["trades_settled"] += 1
                log(
                    f"SETTLED {trade.city} ≥{trade.threshold_f}°{unit} {trade.direction} | "
                    f"Date={trade_date} | Actual={actual_high}°{unit} | "
                    f"{result['status']} | Net=${result['net_pnl']:+.2f}"
                )

                # Calibration keyed to trade_date (not today) using the real daily high
                await log_calibration(
                    session, trade.city, city_cfg["station"],
                    trade.noaa_forecast_high, actual_high, trade.noaa_sigma,
                    trade_date=trade_date,
                )

            # ── Step 4: Fetch Polymarket ──────────────────────────────────────
            log("Fetching Polymarket market prices...")
            try:
                market_map = await build_market_map(city_names, TEMP_THRESHOLDS_F + TEMP_THRESHOLDS_C)
                log(f"Polymarket: {len(market_map)} city/threshold markets found")
            except Exception as e:
                log(f"Polymarket fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"Polymarket: {e}")
                market_map = {}

            # ── Step 5: Evaluate signals ──────────────────────────────────────
            open_after_settle = await get_open_positions(session)
            open_city_set = {t.city for t in open_after_settle}
            open_yes_count = sum(1 for t in open_after_settle if t.direction == "YES")

            # Signals that pass all filters this scan
            this_scan_signals: dict[tuple, dict] = {}

            for f in forecasts:
                city = f["city"]
                if city in open_city_set:
                    log(f"Skip {city} — position already open")
                    continue

                city_cfg = city_by_name.get(city, {})
                is_celsius = city_cfg.get("celsius", False)
                thresholds_for_city = TEMP_THRESHOLDS_C if is_celsius else TEMP_THRESHOLDS_F
                forecast_high = f["forecast_high_f"]
                sigma = f.get("sigma", 3.5)
                unit = f.get("unit", "F")

                # Max threshold distance: only evaluate within ±2σ of forecast
                max_distance = cfg.get("max_threshold_distance_sigma", 2.0) * sigma

                candidates = []

                for threshold in thresholds_for_city:
                    # ── Distance filter ───────────────────────────────────────
                    distance = abs(threshold - forecast_high)
                    if distance > max_distance:
                        continue

                    mkt_key = (city, threshold)
                    if mkt_key not in market_map:
                        continue

                    market_data = market_map[mkt_key]
                    yes_price = market_data["yes_price"]
                    noaa_prob = f["bucket_probs"].get(threshold, 0)

                    direction = "YES" if noaa_prob > yes_price else "NO"
                    edge = abs(noaa_prob - yes_price)

                    # ── Threshold sanity filter ───────────────────────────────
                    # YES trades above forecast need extra edge as a penalty
                    above_forecast = threshold > forecast_high
                    extra_edge_required = 0.0
                    if direction == "YES" and above_forecast:
                        extra_edge_required = (threshold - forecast_high) * cfg.get(
                            "above_forecast_edge_penalty", 0.02
                        )

                    effective_min_edge = cfg["min_edge"] + extra_edge_required

                    should_trade, reason, sizing = evaluate_signal(
                        city=city,
                        threshold=threshold,
                        noaa_prob=noaa_prob,
                        market_yes_price=yes_price,
                        volume=market_data["volume"],
                        confidence=f["confidence"],
                        direction=direction,
                        bankroll=bankroll_state.balance,
                        open_city_positions=list(open_city_set),
                        open_yes_positions=open_yes_count,
                        min_edge_override=effective_min_edge,
                    )

                    candidates.append({
                        "threshold": threshold,
                        "direction": direction,
                        "noaa_prob": noaa_prob,
                        "yes_price": yes_price,
                        "edge": edge,
                        "effective_min_edge": effective_min_edge,
                        "above_forecast": above_forecast,
                        "kelly_capped": sizing.get("kelly_capped", 0) if should_trade else 0,
                        "should_trade": should_trade,
                        "reason": reason,
                        "sizing": sizing,
                        "market_data": market_data,
                    })

                # ── Signal audit: log top 3 candidates regardless of filter ───
                sorted_cands = sorted(candidates, key=lambda x: x["edge"], reverse=True)[:3]
                for c in sorted_cands:
                    flag = "✓" if c["should_trade"] else "✗"
                    above_note = (
                        f" [ABOVE FORECAST +{c['threshold'] - forecast_high:.1f}°]"
                        if c["above_forecast"] and c["direction"] == "YES" else ""
                    )
                    log(
                        f"CANDIDATE {flag} {city} ≥{c['threshold']}°{unit} {c['direction']} | "
                        f"NOAA={c['noaa_prob']:.1%} Mkt={c['yes_price']:.2f} "
                        f"Edge={c['edge']:.1%}(min {c['effective_min_edge']:.1%}) "
                        f"Kelly={c['kelly_capped']:.4f}{above_note} | {c['reason']}"
                    )

                # ── Pick best by kelly_capped (not raw edge) ──────────────────
                tradeable = [c for c in candidates if c["should_trade"]]
                if not tradeable:
                    continue

                best = max(tradeable, key=lambda x: x["kelly_capped"])

                scan_result["signals_found"] += 1
                sig_key = (city, best["threshold"], best["direction"])

                this_scan_signals[sig_key] = {
                    "city": city,
                    "threshold": best["threshold"],
                    "direction": best["direction"],
                    "edge": best["edge"],
                    "kelly_capped": best["kelly_capped"],
                    "noaa_prob": best["noaa_prob"],
                    "yes_price": best["yes_price"],
                    "sizing": best["sizing"],
                    "market_data": best["market_data"],
                    "forecast": f,
                }

                log(
                    f"SIGNAL {city} ≥{best['threshold']}°{unit} {best['direction']} | "
                    f"NOAA={best['noaa_prob']:.1%} Mkt={best['yes_price']:.2f} "
                    f"Edge={best['edge']:.1%} Kelly={best['kelly_capped']:.4f}"
                )

            # ── Step 6: Persistence check — confirm signals seen 2 scans ──────
            confirmed_signals: dict[str, dict] = {}

            for sig_key, sig in this_scan_signals.items():
                city = sig_key[0]
                if city in open_city_set:
                    continue
                if sig_key in _pending_signals:
                    # Confirmed — appeared in both last scan and this scan
                    log(
                        f"CONFIRMED (2-scan) {city} ≥{sig_key[1]}°"
                        f"{sig['forecast'].get('unit','F')} {sig_key[2]}"
                    )
                    if city not in confirmed_signals or \
                       sig["kelly_capped"] > confirmed_signals[city]["kelly_capped"]:
                        confirmed_signals[city] = sig
                else:
                    log(
                        f"PENDING (scan 1/2) {city} ≥{sig_key[1]}°"
                        f"{sig['forecast'].get('unit','F')} {sig_key[2]} — awaiting confirmation"
                    )

            # Roll pending forward
            _pending_signals = this_scan_signals

            # ── Step 7: Open paper trades ─────────────────────────────────────
            for city, sig in confirmed_signals.items():
                city_cfg = city_by_name[city]
                await open_paper_trade(
                    session=session,
                    city=city,
                    station_id=city_cfg["station"],
                    threshold=sig["threshold"],
                    direction=sig["direction"],
                    market_data=sig["market_data"],
                    noaa_data=sig["forecast"],
                    sizing=sig["sizing"],
                    bankroll_state=bankroll_state,
                )
                open_city_set.add(city)
                if sig["direction"] == "YES":
                    open_yes_count += 1
                scan_result["trades_opened"] += 1
                unit = sig["forecast"].get("unit", "F")
                log(
                    f"TRADE OPENED: {city} ≥{sig['threshold']}°{unit} {sig['direction']} | "
                    f"${sig['sizing']['size_usd']} | Bankroll→${bankroll_state.balance:.2f}"
                )

            # ── Step 8: Log scan ──────────────────────────────────────────────
            duration_ms = int(time.time() * 1000) - start_ms
            scan_log = ScanLog(
                cities_scanned=scan_result["cities_scanned"],
                signals_found=scan_result["signals_found"],
                trades_opened=scan_result["trades_opened"],
                trades_settled=scan_result["trades_settled"],
                bankroll_snapshot=bankroll_state.balance,
                errors="; ".join(scan_result["errors"]) if scan_result["errors"] else None,
                duration_ms=duration_ms,
            )
            session.add(scan_log)

    scan_result["duration_ms"] = duration_ms
    log(f"Scan complete in {duration_ms}ms | Bankroll: ${bankroll_state.balance:.2f}")
    return scan_result
