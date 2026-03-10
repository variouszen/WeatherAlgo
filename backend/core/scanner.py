# backend/core/scanner.py
import logging
import asyncio
import time
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, CITIES, TEMP_THRESHOLDS_F, TEMP_THRESHOLDS_C, TEMP_THRESHOLDS
from data.noaa import fetch_all_cities, get_nws_daily_high, get_openmeteo_daily_high, get_openmeteo_forecast_high
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


async def fetch_openmeteo_forecasts() -> dict[str, float]:
    """
    Fetch today's forecast high from Open-Meteo for all cities.
    Returns dict of {city_name: forecast_high} in native unit (F or C).
    Used as second source for consensus filtering.
    """
    import httpx
    results = {}
    async with httpx.AsyncClient(timeout=15.0) as client:
        tasks = []
        city_order = []
        for city_cfg in CITIES:
            tasks.append(
                get_openmeteo_forecast_high(
                    city_cfg["lat"],
                    city_cfg["lon"],
                    day_offset=0,
                    celsius=city_cfg.get("celsius", False),
                )
            )
            city_order.append(city_cfg["name"])

        forecasts = await asyncio.gather(*tasks, return_exceptions=True)

        for city_name, forecast in zip(city_order, forecasts):
            if isinstance(forecast, Exception) or forecast is None:
                logger.warning(f"[OM] Failed to get forecast for {city_name}: {forecast}")
                results[city_name] = None
            else:
                results[city_name] = forecast
                logger.info(f"[OM] {city_name}: {forecast:.1f}")

    return results


async def run_scan() -> dict:
    """
    Full scan cycle v2:
    1. Fetch NOAA forecasts
    2. Fetch Open-Meteo forecasts (second source for consensus)
    3. Fetch Polymarket prices
    4. Settle open positions
    5. Evaluate signals with multi-source consensus filter
    6. Open paper trades (2-scan persistence still applies)
    7. Log to Postgres
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

            # ── Circuit breaker ───────────────────────────────────────────────
            cfg = BOT_CONFIG
            daily_loss_cap = bankroll_state.starting_balance * cfg["daily_loss_cap_pct"]
            if bankroll_state.daily_loss_today >= daily_loss_cap:
                log(f"CIRCUIT BREAKER: Daily loss ${bankroll_state.daily_loss_today:.2f} >= cap ${daily_loss_cap:.2f}", "WARN")
                scan_result["errors"].append("Daily loss cap hit")
                return scan_result

            # ── Step 1: Fetch NOAA ────────────────────────────────────────────
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

            # ── Step 2: Fetch Open-Meteo (second source) ──────────────────────
            log("Fetching Open-Meteo forecasts (consensus source)...")
            try:
                om_forecasts = await fetch_openmeteo_forecasts()
                om_count = sum(1 for v in om_forecasts.values() if v is not None)
                log(f"Open-Meteo: Got forecasts for {om_count}/{len(CITIES)} cities")
            except Exception as e:
                log(f"Open-Meteo fetch failed: {e} — will skip consensus filter", "WARN")
                om_forecasts = {}

            # ── Step 3: Settle open positions ─────────────────────────────────
            open_positions = await get_open_positions(session)
            log(f"Open positions: {len(open_positions)}")

            now_utc = datetime.utcnow()
            today = now_utc.date()
            settlement_hour_utc = 20

            for trade in open_positions:
                trade_date = trade.opened_at.date()

                if trade_date >= today:
                    log(f"Keeping {trade.city} >={trade.threshold_f} — opened today, not yet eligible")
                    continue

                if trade_date < today and now_utc.hour < settlement_hour_utc:
                    log(f"Keeping {trade.city} >={trade.threshold_f} — before settlement window")
                    continue

                city_cfg = city_by_name.get(trade.city)
                if not city_cfg:
                    log(f"No city config for {trade.city} — skipping", "WARN")
                    continue

                is_celsius = city_cfg.get("celsius", False)
                unit = "C" if is_celsius else "F"

                if is_celsius:
                    actual_high = await get_openmeteo_daily_high(city_cfg["lat"], city_cfg["lon"], trade_date)
                else:
                    actual_high = await get_nws_daily_high(city_cfg["station"], trade_date)

                if actual_high is None:
                    log(f"No daily high data for {trade.city} on {trade_date} — keeping open", "WARN")
                    continue

                result = await settle_trade(session, trade, actual_high, bankroll_state)
                scan_result["trades_settled"] += 1
                log(
                    f"SETTLED {trade.city} >={trade.threshold_f}{unit} | "
                    f"Date={trade_date} | Actual={actual_high}{unit} | "
                    f"{result['status']} | Net=${result['net_pnl']:+.2f}"
                )

                await log_calibration(
                    session, trade.city, city_cfg["station"],
                    trade.noaa_forecast_high, actual_high, trade.noaa_sigma
                )

            # ── Step 4: Fetch Polymarket prices ───────────────────────────────
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

            best_per_city: dict[str, dict] = {}

            for f in forecasts:
                city = f["city"]
                if city in open_city_set:
                    log(f"Skip {city} — position already open")
                    continue

                city_cfg = city_by_name.get(city, {})
                is_celsius = city_cfg.get("celsius", False)
                thresholds_for_city = TEMP_THRESHOLDS_C if is_celsius else TEMP_THRESHOLDS_F

                # Get Open-Meteo forecast for this city (second source)
                om_forecast = om_forecasts.get(city)
                noaa_raw = f.get("forecast_high_f")  # raw NOAA forecast high

                for threshold in thresholds_for_city:
                    mkt_key = (city, threshold)
                    if mkt_key not in market_map:
                        continue

                    market_data = market_map[mkt_key]
                    yes_price = market_data["yes_price"]
                    noaa_prob = f["bucket_probs"].get(threshold, 0)

                    direction = "YES" if noaa_prob > yes_price else "NO"

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
                        # Multi-source consensus args
                        noaa_forecast=noaa_raw,
                        openmeteo_forecast=om_forecast,
                        is_celsius=is_celsius,
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
                            "noaa_raw": noaa_raw,
                            "om_raw": om_forecast,
                        }
                        if city not in best_per_city or edge > best_per_city[city]["edge"]:
                            best_per_city[city] = signal_info
                        log(
                            f"SIGNAL {city} >={threshold}{unit} {direction} | "
                            f"NOAA={noaa_raw:.1f} OM={f'{om_forecast:.1f}' if om_forecast is not None else 'N/A'} | "
                            f"NOAA_prob={noaa_prob:.1%} Mkt={yes_price:.2f} Edge={edge:.1%} | ${sizing.get('size_usd', 0)}"
                        )
                    else:
                        if edge >= 0.05:
                            log(f"SKIP {city} >={threshold}{unit} | {reason} | Edge={edge:.1%}")

            # ── Step 6: Open paper trades (2-scan persistence in main.py) ─────
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
                    f"TRADE OPENED: {city} >={sig['threshold']}{sig['forecast'].get('unit','F')} {sig['direction']} | "
                    f"NOAA={sig['noaa_raw']:.1f} OM={f\"{sig['om_raw']:.1f}\" if sig['om_raw'] is not None else 'N/A'} | "
                    f"${sig['sizing']['size_usd']} | Bankroll->${bankroll_state.balance:.2f}"
                )

            # ── Step 7: Log scan ──────────────────────────────────────────────
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
