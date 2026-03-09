# backend/core/scanner.py
import logging
import asyncio
import time
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, CITIES, TEMP_THRESHOLDS_F, TEMP_THRESHOLDS_C, TEMP_THRESHOLDS
from data.noaa import fetch_all_cities, get_latest_observation
import httpx
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


async def run_scan() -> dict:
    """
    Full scan cycle:
    1. Fetch NOAA forecasts (real)
    2. Fetch Polymarket market prices (real)
    3. Settle open positions if observations available
    4. Evaluate new signals
    5. Open paper trades
    6. Log everything to Postgres
    """
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

            # ── Step 1: Check daily circuit breaker ───────────────────────────
            cfg = BOT_CONFIG
            daily_loss_cap = bankroll_state.starting_balance * cfg["daily_loss_cap_pct"]
            if bankroll_state.daily_loss_today >= daily_loss_cap:
                log(f"CIRCUIT BREAKER: Daily loss ${bankroll_state.daily_loss_today:.2f} >= cap ${daily_loss_cap:.2f}", "WARN")
                scan_result["errors"].append("Daily loss cap hit")
                return scan_result

            # ── Step 2: Fetch NOAA (real API) ─────────────────────────────────
            log("Fetching NOAA forecasts...")
            try:
                forecasts = await fetch_all_cities(day_offset=0)
                scan_result["cities_scanned"] = len(forecasts)
                log(f"NOAA: Got forecasts for {len(forecasts)} cities")
            except Exception as e:
                log(f"NOAA fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"NOAA: {e}")
                return scan_result

            forecast_map = {f["city"]: f for f in forecasts}

            # ── Step 3: Settle open positions ──────────────────────────────────
            open_positions = await get_open_positions(session)
            log(f"Open positions: {len(open_positions)}")

            for trade in open_positions:
                city_cfg = city_by_name.get(trade.city)
                if not city_cfg:
                    continue

                # Get current observation for this city's NWS station
                async with httpx.AsyncClient() as client:
                    from data.noaa import get_latest_observation
                    actual_temp = await get_latest_observation(city_cfg["station"], client)

                if actual_temp is None:
                    log(f"No observation for {trade.city} ({city_cfg['station']}) — keeping open")
                    continue

                # Only settle if it's end of market day (past 8pm local) or temp is definitive
                # Simple heuristic: if actual_temp is well above or below threshold, settle now
                # In production you'd check the market's end_date
                gap = abs(actual_temp - trade.threshold_f)
                if gap > trade.noaa_sigma * 2:
                    # Temp is more than 2σ from threshold — result is clear
                    result = await settle_trade(session, trade, actual_temp, bankroll_state)
                    scan_result["trades_settled"] += 1
                    log(
                        f"SETTLED {trade.city} ≥{trade.threshold_f}°F | "
                        f"Actual={actual_temp}°F | {result['status']} | Net=${result['net_pnl']:+.2f}"
                    )
                else:
                    log(f"Keeping {trade.city} ≥{trade.threshold_f}°F open — too close to call (actual={actual_temp}°F)")

            # ── Step 4: Log calibration data ───────────────────────────────────
            for f in forecasts:
                city_cfg = city_by_name.get(f["city"])
                if city_cfg:
                    await log_calibration(
                        session, f["city"], city_cfg["station"],
                        f["forecast_high_f"], f.get("current_obs_f"), f["sigma"]
                    )

            # ── Step 5: Fetch Polymarket prices (real API) ────────────────────
            log("Fetching Polymarket market prices...")
            try:
            # Pass all thresholds (F and C) — polymarket.py matches per-city unit
                market_map = await build_market_map(city_names, TEMP_THRESHOLDS_F + TEMP_THRESHOLDS_C)
                log(f"Polymarket: {len(market_map)} city/threshold markets found")
            except Exception as e:
                log(f"Polymarket fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"Polymarket: {e}")
                market_map = {}

            # ── Step 6: Evaluate signals ───────────────────────────────────────
            open_after_settle = await get_open_positions(session)
            open_city_set = {t.city for t in open_after_settle}
            open_yes_count = sum(1 for t in open_after_settle if t.direction == "YES")

            # Track best signal per city (avoid multiple trades same city)
            best_per_city: dict[str, dict] = {}

            for f in forecasts:
                city = f["city"]
                if city in open_city_set:
                    log(f"Skip {city} — position already open")
                    continue

                city_cfg = city_by_name.get(city, {})
                is_celsius = city_cfg.get("celsius", False)
                thresholds_for_city = TEMP_THRESHOLDS_C if is_celsius else TEMP_THRESHOLDS_F

                for threshold in thresholds_for_city:
                    mkt_key = (city, threshold)
                    if mkt_key not in market_map:
                        continue

                    market_data = market_map[mkt_key]
                    yes_price = market_data["yes_price"]
                    noaa_prob = f["bucket_probs"].get(threshold, 0)

                    # Determine direction
                    if noaa_prob > yes_price:
                        direction = "YES"
                    else:
                        direction = "NO"

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
                    )

                    edge = abs(noaa_prob - yes_price)

                    unit = f.get("unit", "F")
                    if should_trade:
                        scan_result["signals_found"] += 1
                        signal_info = {
                            "city": city,
                            "threshold": threshold,
                            "direction": direction,
                            "edge": edge,
                            "noaa_prob": noaa_prob,
                            "yes_price": yes_price,
                            "sizing": sizing,
                            "market_data": market_data,
                            "forecast": f,
                        }
                        # Keep only best edge signal per city
                        if city not in best_per_city or edge > best_per_city[city]["edge"]:
                            best_per_city[city] = signal_info
                        log(
                            f"SIGNAL {city} ≥{threshold}°{unit} {direction} | "
                            f"NOAA={noaa_prob:.1%} Mkt={yes_price:.2f} Edge={edge:.1%} | ${sizing.get('size_usd', 0)}"
                        )
                    else:
                        if edge >= 0.05:  # Only log near-misses
                            log(f"SKIP {city} ≥{threshold}°{unit} | {reason} | Edge={edge:.1%}")

            # ── Step 7: Open paper trades ─────────────────────────────────────
            for city, sig in best_per_city.items():
                city_cfg = city_by_name[city]
                trade = await open_paper_trade(
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
                log(
                    f"TRADE OPENED: {city} ≥{sig['threshold']}°{sig['forecast'].get('unit','F')} {sig['direction']} | "
                    f"${sig['sizing']['size_usd']} | Bankroll→${bankroll_state.balance:.2f}"
                )

            # ── Step 8: Log scan to DB ────────────────────────────────────────
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
