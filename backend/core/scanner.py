# backend/core/scanner.py
import logging
import asyncio
import time
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, CITIES, TEMP_THRESHOLDS_F, TEMP_THRESHOLDS_C, TEMP_THRESHOLDS
from data.noaa import (
    fetch_all_cities, get_nws_daily_high, get_openmeteo_daily_high,
    get_openmeteo_forecast_high, fetch_gfs_forecast_high, fetch_ecmwf_forecast_high
)
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

# ── Re-entry session state ────────────────────────────────────────────────────
# Tracks per-city state within a single day: entry count, high-water mark EV,
# last entry price (for crowd move check), last entry time (for cooldown)
# Resets at midnight UTC
_reentry_state: dict[str, dict] = {}
_reentry_state_date: str = ""


def _get_reentry_state(city: str) -> dict:
    """Get or init re-entry state for a city today."""
    global _reentry_state, _reentry_state_date
    today = datetime.now(timezone.utc).date().isoformat()
    if _reentry_state_date != today:
        _reentry_state = {}
        _reentry_state_date = today
    if city not in _reentry_state:
        _reentry_state[city] = {
            "entry_count": 0,
            "ev_hwm": 0.0,
            "last_crowd_price": None,
            "last_entry_time": None,
        }
    return _reentry_state[city]


def _is_early_window(end_date_str: str) -> bool:
    """
    Returns True if market is within the early window (< 6h after open).
    Markets open ~2 days before end_date, so we estimate open time as
    end_date minus 2 days. If now is within early_window_hours of that, it's fresh.
    """
    if not end_date_str:
        return False
    try:
        cfg = BOT_CONFIG
        if "T" in str(end_date_str):
            end_dt = datetime.fromisoformat(str(end_date_str).replace("Z", "+00:00"))
        else:
            parts = str(end_date_str)[:10].split("-")
            end_dt = datetime(*[int(x) for x in parts], 23, 59, 59, tzinfo=timezone.utc)

        estimated_open = end_dt - timedelta(days=2)
        now_utc = datetime.now(timezone.utc)
        hours_since_open = (now_utc - estimated_open).total_seconds() / 3600
        return 0 <= hours_since_open <= cfg["early_window_hours"]
    except Exception:
        return False


def _is_too_late_for_reentry(end_date_str: str) -> bool:
    """Returns True if market closes within reentry_no_late_entry_hours."""
    if not end_date_str:
        return False
    try:
        cfg = BOT_CONFIG
        if "T" in str(end_date_str):
            end_dt = datetime.fromisoformat(str(end_date_str).replace("Z", "+00:00"))
        else:
            parts = str(end_date_str)[:10].split("-")
            end_dt = datetime(*[int(x) for x in parts], 23, 59, 59, tzinfo=timezone.utc)
        hours_until_close = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
        return hours_until_close < cfg["reentry_no_late_entry_hours"]
    except Exception:
        return False


async def fetch_validator_forecasts(city_cfg: dict, day_offset: int = 0) -> dict:
    """
    Fetch GFS and ECMWF validator forecasts for a city.
    Serialized with small stagger to avoid 429s.
    Returns {"gfs": float|None, "ecmwf": float|None}
    """
    is_celsius = city_cfg.get("celsius", False)
    lat, lon = city_cfg["lat"], city_cfg["lon"]
    tz = city_cfg.get("timezone", "UTC")

    gfs = await fetch_gfs_forecast_high(lat, lon, day_offset, is_celsius, tz)
    await asyncio.sleep(0.5)
    ecmwf = await fetch_ecmwf_forecast_high(lat, lon, day_offset, is_celsius, tz)
    return {"gfs": gfs, "ecmwf": ecmwf}


async def run_scan() -> dict:
    """
    Full scan cycle V2:
    1. Fetch primary forecasts (NOAA for US, ECMWF for intl via fetch_all_cities)
    2. Fetch GFS + ECMWF validator forecasts per city
    3. Fetch Polymarket prices
    4. Settle open positions
    5. Evaluate signals with directional gate + buffer + consensus + timing + re-entry
    6. Open paper trades
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

            cfg = BOT_CONFIG
            daily_loss_cap = bankroll_state.starting_balance * cfg["daily_loss_cap_pct"]
            if bankroll_state.daily_loss_today >= daily_loss_cap:
                log(f"CIRCUIT BREAKER: Daily loss ${bankroll_state.daily_loss_today:.2f} >= cap ${daily_loss_cap:.2f}", "WARN")
                scan_result["errors"].append("Daily loss cap hit")
                return scan_result

            # ── Step 1: Fetch primary forecasts ───────────────────────────────
            log("Fetching primary forecasts (NOAA/ECMWF)...")
            try:
                forecasts = await fetch_all_cities(day_offset=0)
                scan_result["cities_scanned"] = len(forecasts)
                log(f"Primary forecasts: {len(forecasts)} cities")
            except Exception as e:
                log(f"Primary forecast fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"Forecast: {e}")
                return scan_result

            forecast_map = {f["city"]: f for f in forecasts}

            # ── Step 2: Fetch GFS + ECMWF validators (serialized) ─────────────
            log("Fetching GFS + ECMWF validator forecasts...")
            validator_map: dict[str, dict] = {}
            for i, city_cfg in enumerate(CITIES):
                if i > 0:
                    await asyncio.sleep(0.5)
                try:
                    validators = await fetch_validator_forecasts(city_cfg, day_offset=0)
                    validator_map[city_cfg["name"]] = validators
                    gfs_val = f"{validators['gfs']:.1f}" if validators['gfs'] else "N/A"
                    ecmwf_val = f"{validators['ecmwf']:.1f}" if validators['ecmwf'] else "N/A"
                    log(f"Validators {city_cfg['name']}: GFS={gfs_val} ECMWF={ecmwf_val}")
                except Exception as e:
                    log(f"Validator fetch failed for {city_cfg['name']}: {e}", "WARN")
                    validator_map[city_cfg["name"]] = {"gfs": None, "ecmwf": None}

            # ── Step 3: Settle open positions ─────────────────────────────────
            open_positions = await get_open_positions(session)
            log(f"Open positions: {len(open_positions)}")

            now_utc = datetime.utcnow()
            today = now_utc.date()
            settlement_hour_utc = 20

            for trade in open_positions:
                trade_date = trade.opened_at.date()
                if trade_date >= today:
                    continue
                if trade_date < today and now_utc.hour < settlement_hour_utc:
                    continue

                city_cfg = city_by_name.get(trade.city)
                if not city_cfg:
                    continue

                is_celsius = city_cfg.get("celsius", False)
                unit = "C" if is_celsius else "F"

                if is_celsius:
                    actual_high = await get_openmeteo_daily_high(city_cfg["lat"], city_cfg["lon"], trade_date)
                else:
                    actual_high = await get_nws_daily_high(city_cfg["station"], trade_date)

                if actual_high is None:
                    log(f"No daily high for {trade.city} on {trade_date} — keeping open", "WARN")
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
            log("Fetching Polymarket prices...")
            try:
                market_map = await build_market_map(city_names, TEMP_THRESHOLDS_F + TEMP_THRESHOLDS_C)
                log(f"Polymarket: {len(market_map)} city/threshold markets")
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
                city_cfg_item = city_by_name.get(city, {})
                is_celsius = city_cfg_item.get("celsius", False)
                thresholds_for_city = TEMP_THRESHOLDS_C if is_celsius else TEMP_THRESHOLDS_F
                unit = "C" if is_celsius else "F"

                primary_forecast = f.get("forecast_high_f")
                validators = validator_map.get(city, {"gfs": None, "ecmwf": None})
                gfs_forecast = validators.get("gfs")
                ecmwf_forecast = validators.get("ecmwf")

                # Re-entry state for this city
                reentry = _get_reentry_state(city)
                entry_number = reentry["entry_count"] + 1
                prior_ev = reentry["ev_hwm"] if reentry["entry_count"] > 0 else None
                last_crowd_price = reentry["last_crowd_price"]

                for threshold in thresholds_for_city:
                    mkt_key = (city, threshold)
                    if mkt_key not in market_map:
                        continue

                    market_data = market_map[mkt_key]
                    yes_price = market_data["yes_price"]
                    noaa_prob = f["bucket_probs"].get(threshold, 0)
                    direction = "YES" if noaa_prob > yes_price else "NO"

                    end_date_str = market_data.get("end_date", "")
                    is_early = _is_early_window(end_date_str)
                    too_late_reentry = _is_too_late_for_reentry(end_date_str) if entry_number > 1 else False

                    # Check cooldown for re-entry
                    if entry_number > 1 and reentry["last_entry_time"]:
                        mins_since = (datetime.now(timezone.utc) - reentry["last_entry_time"]).total_seconds() / 60
                        if mins_since < cfg["reentry_cooldown_minutes"]:
                            continue

                    if too_late_reentry:
                        continue

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
                        primary_forecast=primary_forecast,
                        is_celsius=is_celsius,
                        gfs_forecast=gfs_forecast,
                        ecmwf_forecast=ecmwf_forecast,
                        is_early_window=is_early,
                        entry_number=entry_number,
                        prior_entry_ev=prior_ev,
                        crowd_price_at_prior=last_crowd_price,
                    )

                    edge = abs(noaa_prob - yes_price)

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
                            "primary_forecast": primary_forecast,
                            "gfs_forecast": gfs_forecast,
                            "ecmwf_forecast": ecmwf_forecast,
                            "is_early_window": is_early,
                            "entry_number": entry_number,
                            "prior_entry_ev": prior_ev,
                            "crowd_price_at_prior": last_crowd_price,
                        }
                        if city not in best_per_city or edge > best_per_city[city]["edge"]:
                            best_per_city[city] = signal_info
                        log(
                            f"SIGNAL {city} >={threshold}{unit} {direction} | "
                            f"Primary={primary_forecast:.1f} GFS={f'{gfs_forecast:.1f}' if gfs_forecast else 'N/A'} "
                            f"ECMWF={f'{ecmwf_forecast:.1f}' if ecmwf_forecast else 'N/A'} | "
                            f"Edge={edge:.1%} Models={sizing.get('models_agreed','?')} "
                            f"{'🌅EARLY' if is_early else ''} {'🔁RE-ENTRY' if entry_number > 1 else ''}"
                        )
                    else:
                        if edge >= 0.05:
                            log(f"SKIP {city} >={threshold}{unit} | {reason} | Edge={edge:.1%}")

            # ── Step 6: Open paper trades ─────────────────────────────────────
            for city, sig in best_per_city.items():
                city_cfg_item = city_by_name[city]
                is_celsius = city_cfg_item.get("celsius", False)
                unit = "C" if is_celsius else "F"

                # Inject extra data into forecast dict for open_paper_trade
                sig["forecast"]["gfs_forecast"] = sig.get("gfs_forecast")
                sig["forecast"]["ecmwf_forecast"] = sig.get("ecmwf_forecast")
                sig["forecast"]["prior_entry_ev"] = sig.get("prior_entry_ev")
                sig["forecast"]["crowd_price_at_prior"] = sig.get("crowd_price_at_prior")

                trade = await open_paper_trade(
                    session=session,
                    city=city,
                    station_id=city_cfg_item["station"],
                    threshold=sig["threshold"],
                    direction=sig["direction"],
                    market_data=sig["market_data"],
                    noaa_data=sig["forecast"],
                    sizing=sig["sizing"],
                    bankroll_state=bankroll_state,
                )

                # Update re-entry state
                reentry = _get_reentry_state(city)
                reentry["entry_count"] += 1
                reentry["ev_hwm"] = max(reentry["ev_hwm"], sig["edge"])
                reentry["last_crowd_price"] = sig["yes_price"]
                reentry["last_entry_time"] = datetime.now(timezone.utc)

                if sig["direction"] == "YES":
                    open_yes_count += 1
                scan_result["trades_opened"] += 1

                models = sig["sizing"].get("models_agreed", "?")
                log(
                    f"TRADE OPENED: {city} >={sig['threshold']}{unit} {sig['direction']} | "
                    f"Primary={sig['primary_forecast']:.1f} | "
                    f"Models={models} | ${sig['sizing']['size_usd']} | "
                    f"{'🌅EARLY ' if sig['is_early_window'] else ''}"
                    f"{'🔁#' + str(sig['entry_number']) + ' ' if sig['entry_number'] > 1 else ''}"
                    f"Bankroll->${bankroll_state.balance:.2f}"
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


