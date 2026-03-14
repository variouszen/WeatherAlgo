# backend/core/scanner.py
import logging
import asyncio
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy.ext.asyncio import AsyncSession
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, CITIES
from data.noaa import (
    get_nws_daily_high, get_openmeteo_daily_high,
    fetch_gfs_forecast_high, prob_above as _prob_above_fn
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
async def _get_reentry_state_from_db(session, city: str, market_date: str) -> dict:
    """
    Derive re-entry state for a city/date from existing DB trades.
    This is persistent across restarts and deploys.
    Falls back to zero state if no trades exist.
    """
    from sqlalchemy import select
    from models.database import Trade as TradeModel
    today_str = market_date or datetime.now(timezone.utc).date().isoformat()

    result = await session.execute(
        select(TradeModel)
        .where(TradeModel.city == city)
        .where(TradeModel.market_date == today_str)
        .where(TradeModel.status == "OPEN")
        .order_by(TradeModel.opened_at.desc())
    )
    trades = result.scalars().all()

    if not trades:
        return {
            "entry_count": 0,
            "edge_hwm": 0.0,
            "last_crowd_price": None,
            "last_entry_time": None,
        }

    # Derive state from existing trades
    entry_count = len(trades)
    edge_hwm = max((t.edge_pct for t in trades), default=0.0)
    latest = trades[0]  # most recent
    last_crowd_price = latest.market_yes_price
    last_entry_time = latest.opened_at.replace(tzinfo=timezone.utc) if latest.opened_at.tzinfo is None else latest.opened_at

    return {
        "entry_count": entry_count,
        "edge_hwm": edge_hwm,
        "last_crowd_price": last_crowd_price,
        "last_entry_time": last_entry_time,
    }


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
    Fetch GFS validator forecast for a city.
    All tiers use GFS as the independent validator:
      US cities: NOAA primary + GFS validator
      Europe:    ICON primary + GFS validator
      East Asia: JMA primary  + GFS validator
    Returns {"gfs": float|None, "icon": None}
    """
    is_celsius = city_cfg.get("celsius", False)
    lat, lon = city_cfg["lat"], city_cfg["lon"]
    tz = city_cfg.get("timezone", "UTC")

    gfs = await fetch_gfs_forecast_high(lat, lon, day_offset, is_celsius, tz)

    return {"gfs": gfs, "icon": None}


async def run_scan() -> dict:
    """
    Full scan cycle V3 (multi-day):
    1. Fetch Polymarket prices — today + tomorrow per city (day+2 as fallback only)
    2. Fetch primary forecasts per city-date pair (NOAA for US, ICON for Europe, JMA for East Asia)
    3. Fetch GFS validator forecasts per city-date pair
    4. Settle open positions via Polymarket resolution
    5. Evaluate signals with directional gate + consensus + timing + re-entry + city caps
    6. Open paper trades (best signal per city-date, re-check city caps at open time)
    7. Log to Postgres
    """
    start_ms = int(time.time() * 1000)
    scan_result = {
        "started_at": datetime.now(timezone.utc).isoformat(),
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

            # ── Step 1: Fetch Polymarket prices FIRST ────────────────────────
            # Must happen before forecasts so we know which city-date pairs
            # have active markets and can fetch forecasts for each one.
            log("Fetching Polymarket prices...")
            try:
                market_map, city_date_map = await build_market_map(city_names)
                cities_found = len(set(c for c, _ in city_date_map))
                log(f"Polymarket: {len(market_map)} direct city/date/threshold entries across {cities_found} cities, {len(city_date_map)} city-date pairs")
            except Exception as e:
                log(f"Polymarket fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"Polymarket: {e}")
                market_map = {}
                city_date_map = set()

            # ── Group city-date pairs by city and compute day offsets ──────
            utc_today = datetime.now(timezone.utc).date()
            from collections import defaultdict
            city_dates = defaultdict(list)  # city -> [date_str, ...]
            city_date_offset = {}           # (city, date_str) -> int

            for city_name, date_str in city_date_map:
                city_dates[city_name].append(date_str)
                try:
                    mkt_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    offset = max(0, min((mkt_date - utc_today).days, 6))
                except Exception:
                    offset = 0
                city_date_offset[(city_name, date_str)] = offset

            for city_name in city_dates:
                city_dates[city_name].sort()  # chronological within each city

            log(f"City-date offsets: { {f'{c}/{d}': o for (c, d), o in city_date_offset.items()} }")

            # ── Step 2: Fetch primary forecasts — sequential per city, shared client ─
            log("Fetching primary forecasts (NOAA/ICON/JMA)...")
            import httpx as _httpx
            from data.noaa import fetch_city_forecast as _fetch_city_forecast
            forecast_map: dict[tuple[str, str], dict] = {}  # (city, date_str) -> forecast

            try:
                async with _httpx.AsyncClient() as _client:
                    fetch_idx = 0
                    for city_name, date_strs in city_dates.items():
                        city_cfg = city_by_name.get(city_name)
                        if not city_cfg:
                            continue
                        is_intl = city_cfg.get("celsius", False)
                        for date_str in date_strs:
                            # Stagger Open-Meteo calls to avoid 429s.
                            # International primary hits Open-Meteo (ICON/GFS);
                            # US primary hits NWS (different API, no shared rate limit).
                            if fetch_idx > 0 and is_intl:
                                await asyncio.sleep(1.5)
                            offset = city_date_offset.get((city_name, date_str), 0)
                            try:
                                result = await _fetch_city_forecast(
                                    city_cfg, offset, _client, target_date=date_str,
                                )
                                if isinstance(result, dict):
                                    forecast_map[(city_name, date_str)] = result
                                    unit = result.get("unit", "?")
                                    source = result.get("source", "unknown")
                                    high = result.get("forecast_high")
                                    log(f"Primary {city_name}/{date_str}: {high}°{unit} via {source}")
                                else:
                                    log(f"Forecast failed for {city_name}/{date_str}: {result}", "WARN")
                            except Exception as e:
                                log(f"Forecast failed for {city_name}/{date_str}: {e}", "WARN")
                            fetch_idx += 1

                scan_result["cities_scanned"] = len(set(c for c, _ in forecast_map))
                log(f"Primary forecasts: {len(forecast_map)} city-date pairs across {scan_result['cities_scanned']} cities")
            except Exception as e:
                log(f"Primary forecast fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"Forecast: {e}")
                return scan_result

            # ── Step 3: Fetch GFS validator forecasts per city-date pair ─────
            log("Fetching GFS validator forecasts...")
            validator_map: dict[tuple[str, str], dict] = {}  # (city, date_str) -> {gfs, icon}
            fetch_count = 0
            for city_name, date_strs in city_dates.items():
                city_cfg = city_by_name.get(city_name)
                if not city_cfg:
                    continue
                for date_str in date_strs:
                    if fetch_count > 0:
                        await asyncio.sleep(1.5)  # stagger to avoid Open-Meteo 429s
                    offset = city_date_offset.get((city_name, date_str), 0)
                    try:
                        validators = await fetch_validator_forecasts(city_cfg, day_offset=offset)
                        validator_map[(city_name, date_str)] = validators
                        gfs_val = f"{validators['gfs']:.1f}" if validators['gfs'] is not None else "N/A"
                        log(f"Validator {city_name}/{date_str} (offset={offset}): GFS={gfs_val}")
                    except Exception as e:
                        log(f"Validator fetch failed for {city_name}/{date_str}: {e}", "WARN")
                        validator_map[(city_name, date_str)] = {"gfs": None, "icon": None}
                    fetch_count += 1

            # ── Step 4: Settle open positions via Polymarket resolution ────
            open_positions = await get_open_positions(session)
            log(f"Open positions: {len(open_positions)}")

            from data.polymarket import check_event_resolution

            for trade in open_positions:
                city_cfg = city_by_name.get(trade.city)
                if not city_cfg:
                    continue

                is_celsius = city_cfg.get("celsius", False)
                unit = "C" if is_celsius else "F"

                # Skip future-dated trades — Polymarket can't have resolved yet
                if trade.market_date:
                    try:
                        trade_date = datetime.strptime(trade.market_date, "%Y-%m-%d").date()
                    except Exception:
                        trade_date = trade.opened_at.date()
                else:
                    trade_date = trade.opened_at.date()

                now_utc = datetime.now(timezone.utc)
                today = now_utc.date()
                if trade_date > today:
                    continue

                # ── Primary: check Polymarket for resolution ──────────────
                resolution = await check_event_resolution(
                    city=trade.city,
                    market_date_str=trade.market_date or trade_date.isoformat(),
                )

                if resolution is None or not resolution.get("resolved", False):
                    if trade_date < today:
                        log(f"STALE? {trade.city} >={trade.threshold_f}{unit} | "
                            f"Date={trade_date} | Polymarket not resolved yet — keeping open", "WARN")
                    continue

                # ── Determine WIN/LOSS from winning bucket ────────────────
                winning_low = resolution["winning_bucket_low"]
                # If winning bucket starts at or above threshold → actual high >= threshold → YES wins
                polymarket_won = (
                    (trade.direction == "YES" and winning_low >= trade.threshold_f) or
                    (trade.direction == "NO"  and winning_low < trade.threshold_f)
                )

                # ── Fetch observed high for calibration (best-effort) ─────
                actual_high = None
                try:
                    if is_celsius:
                        actual_high = await get_openmeteo_daily_high(
                            city_cfg["lat"], city_cfg["lon"], trade_date
                        )
                    else:
                        actual_high = await get_nws_daily_high(
                            city_cfg["station"], trade_date
                        )
                except Exception as e:
                    log(f"Observation fetch failed for {trade.city} (non-fatal): {e}", "WARN")

                # If no observation available, use Polymarket bucket estimate
                if actual_high is None:
                    actual_high = resolution.get("estimated_high")
                    if actual_high is not None:
                        log(f"Using Polymarket estimate for {trade.city}: {actual_high}{unit} "
                            f"(from bucket '{resolution['winning_label']}')")

                # ── Settle ────────────────────────────────────────────────
                result = await settle_trade(
                    session, trade, bankroll_state,
                    actual_high_f=actual_high,
                    polymarket_won=polymarket_won,
                )
                scan_result["trades_settled"] += 1
                log(
                    f"SETTLED {trade.city} >={trade.threshold_f}{unit} | "
                    f"Date={trade_date} | Winner='{resolution['winning_label']}' | "
                    f"Actual={actual_high if actual_high is not None else 'N/A'}{unit} | "
                    f"{result['status']} | Net=${result['net_pnl']:+.2f}"
                )

                # ── Calibration data (always log if we have observation) ──
                if actual_high is not None:
                    await log_calibration(
                        session, trade.city, city_cfg["station"],
                        trade.noaa_forecast_high, actual_high, trade.noaa_sigma,
                        market_date=trade.market_date,
                    )

            # ── Step 5: Evaluate signals ──────────────────────────────────────
            open_after_settle = await get_open_positions(session)

            # Snapshot bankroll BEFORE opening any new trades this scan.
            # Used for all exposure cap calculations so caps don't shrink
            # as trades open within the same scan cycle.
            scan_start_bankroll = bankroll_state.balance

            # Build position tracking structures for multi-day caps
            open_city_date_set = {(t.city, t.market_date) for t in open_after_settle}
            open_yes_count = sum(1 for t in open_after_settle if t.direction == "YES")

            # City-wide aggregates (across all dates)
            open_city_total: dict[str, int] = {}    # city -> count
            open_city_exposure: dict[str, float] = {} # city -> sum of position_size_usd
            for t in open_after_settle:
                open_city_total[t.city] = open_city_total.get(t.city, 0) + 1
                open_city_exposure[t.city] = open_city_exposure.get(t.city, 0.0) + t.position_size_usd

            best_per_city_date: dict[tuple[str, str], dict] = {}

            # Iterate all city-date pairs from market_map
            for city, market_date_str in sorted(city_date_map):
                city_cfg_item = city_by_name.get(city, {})
                if not city_cfg_item:
                    continue
                is_celsius = city_cfg_item.get("celsius", False)
                unit = "C" if is_celsius else "F"

                # ── Local-time guard: skip day-0 if past noon in city's timezone ──
                # After noon local time, the daily high has likely occurred and
                # the forecast no longer has edge over what the market already knows.
                # This prevents betting on near-resolution markets with stale forecasts.
                # Only applies to day-0 (market_date == today UTC). Day+1/+2 unaffected.
                try:
                    mkt_date = datetime.strptime(market_date_str, "%Y-%m-%d").date()
                    if mkt_date == utc_today:
                        city_tz = city_cfg_item.get("timezone", "UTC")
                        city_local_now = datetime.now(timezone.utc).astimezone(ZoneInfo(city_tz))
                        if city_local_now.hour >= 12:
                            log(f"SKIP {city}/{market_date_str} | Day-0 past noon local time "
                                f"({city_local_now.strftime('%H:%M')} {city_tz}) — forecast edge unreliable")
                            continue
                except Exception:
                    pass  # If timezone lookup fails, don't block — let other filters handle it

                # ── Dynamic thresholds: use what Polymarket actually offers ──
                # Polymarket is the sole source of tradeable thresholds.
                # No static TEMP_THRESHOLDS_F/C filtering.
                thresholds_for_city = sorted(
                    thresh for (c, d, thresh) in market_map
                    if c == city and d == market_date_str
                )

                # ── City-wide hard cap check (before evaluating any threshold) ──
                if open_city_total.get(city, 0) >= cfg["max_positions_per_city"]:
                    log(f"SKIP {city}/{market_date_str} | City cap: {open_city_total[city]} open >= max {cfg['max_positions_per_city']}")
                    continue

                city_exposure = open_city_exposure.get(city, 0.0)
                max_city_exposure = scan_start_bankroll * cfg["max_city_exposure_pct"]
                if city_exposure >= max_city_exposure:
                    log(f"SKIP {city}/{market_date_str} | City exposure ${city_exposure:.2f} >= max ${max_city_exposure:.2f} ({cfg['max_city_exposure_pct']:.0%})")
                    continue

                f = forecast_map.get((city, market_date_str))
                if f is None:
                    continue

                primary_forecast = f.get("forecast_high")
                validators = validator_map.get((city, market_date_str), {"gfs": None, "icon": None})
                gfs_forecast = validators.get("gfs")
                icon_forecast = validators.get("icon")

                for threshold in thresholds_for_city:
                    mkt_key = (city, market_date_str, threshold)
                    market_data = market_map[mkt_key]
                    yes_price = market_data["yes_price"]
                    # Compute probability on the fly from continuous forecast distribution.
                    # Works for ANY threshold — not limited to pre-computed bucket_probs.
                    noaa_prob = round(_prob_above_fn(threshold, primary_forecast, f["sigma"]), 4)
                    direction = "YES" if noaa_prob > yes_price else "NO"

                    # ── Raw directional gate (day-0 only) ─────────────────────────
                    # Day-0: forecast must support the bet direction.
                    # YES requires forecast ABOVE threshold.
                    # NO requires forecast BELOW threshold.
                    # Equality = no conviction → skip.
                    # Day+1/+2: uses the existing probability-based gate in signals.py.
                    is_day0 = f.get("day_offset", 0) == 0
                    forecast_vs_threshold = round(primary_forecast - threshold, 2)

                    if is_day0:
                        if direction == "YES" and primary_forecast <= threshold:
                            log(f"SKIP {city} | MarketDate={market_date_str} | Bucket=>={threshold}{unit} | "
                                f"Day-0 raw gate: forecast {primary_forecast} not above threshold {threshold} "
                                f"(diff={forecast_vs_threshold}) — cannot support YES | gate_mode=raw_day0")
                            continue
                        if direction == "NO" and primary_forecast >= threshold:
                            log(f"SKIP {city} | MarketDate={market_date_str} | Bucket=>={threshold}{unit} | "
                                f"Day-0 raw gate: forecast {primary_forecast} not below threshold {threshold} "
                                f"(diff={forecast_vs_threshold}) — cannot support NO | gate_mode=raw_day0")
                            continue

                    end_date_str = market_data.get("end_date", "")
                    is_early = _is_early_window(end_date_str)

                    # ── Two-level liquidity gate ──────────────────────────────
                    # Level 1: event-level volume (total traded on this event)
                    event_vol = market_data.get("event_volume", 0.0)
                    if event_vol < cfg["min_event_volume"]:
                        log(f"SKIP {city} | MarketDate={market_date_str} | Bucket=>={threshold}{unit} | "
                            f"EventVol ${event_vol:,.0f} < min ${cfg['min_event_volume']:,.0f}")
                        continue

                    # Level 2: bucket-level volume (volume on the matched bucket)
                    matched_bucket = next(
                        (b for b in market_data.get("buckets", []) if b["low"] == threshold),
                        None,
                    )
                    if matched_bucket is None:
                        bucket_vol = None
                        log(f"SKIP {city} | MarketDate={market_date_str} | Bucket=>={threshold}{unit} | "
                            f"BucketVol unavailable — no bucket with low=={threshold}")
                        continue
                    else:
                        bucket_vol = matched_bucket.get("bucket_volume", 0.0)
                        if bucket_vol < cfg["min_bucket_volume"]:
                            log(f"SKIP {city} | MarketDate={market_date_str} | Bucket=>={threshold}{unit} | "
                                f"BucketVol ${bucket_vol:,.0f} < min ${cfg['min_bucket_volume']:,.0f}")
                            continue

                    # Re-entry state from DB keyed by city + actual market date
                    reentry = await _get_reentry_state_from_db(session, city, market_date_str)
                    entry_number = reentry["entry_count"] + 1
                    prior_ev = reentry["edge_hwm"] if reentry["entry_count"] > 0 else None
                    last_crowd_price = reentry["last_crowd_price"]

                    too_late_reentry = _is_too_late_for_reentry(end_date_str) if entry_number > 1 else False

                    # Check cooldown for re-entry (using DB-derived last_entry_time)
                    if entry_number > 1 and reentry["last_entry_time"]:
                        last_t = reentry["last_entry_time"]
                        if last_t.tzinfo is None:
                            last_t = last_t.replace(tzinfo=timezone.utc)
                        mins_since = (datetime.now(timezone.utc) - last_t).total_seconds() / 60
                        if mins_since < cfg["reentry_cooldown_minutes"]:
                            continue

                    if too_late_reentry:
                        continue

                    should_trade, reason, sizing = evaluate_signal(
                        city=city,
                        threshold=threshold,
                        noaa_prob=noaa_prob,
                        market_yes_price=yes_price,
                        confidence=f["confidence"],
                        direction=direction,
                        bankroll=bankroll_state.balance,
                        open_city_date_positions=open_city_date_set,
                        open_yes_positions=open_yes_count,
                        market_date=market_date_str,
                        primary_forecast=primary_forecast,
                        primary_source=f.get("source"),
                        is_celsius=is_celsius,
                        gfs_forecast=gfs_forecast,
                        icon_forecast=icon_forecast,
                        is_early_window=is_early,
                        entry_number=entry_number,
                        prior_entry_edge=prior_ev,
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
                            "icon_forecast": icon_forecast,
                            "is_early_window": is_early,
                            "entry_number": entry_number,
                            "prior_entry_edge": prior_ev,
                            "crowd_price_at_prior": last_crowd_price,
                            "market_date": market_date_str,
                        }
                        # Rank by edge * size_usd — better than raw edge alone
                        # as it accounts for consensus factor and kelly sizing
                        score = edge * sizing.get("size_usd", 0)
                        cd_key = (city, market_date_str)
                        if cd_key not in best_per_city_date or score > best_per_city_date[cd_key].get("score", 0):
                            signal_info["score"] = score
                            best_per_city_date[cd_key] = signal_info
                        log(
                            f"SIGNAL {city} | MarketDate={market_date_str} | Bucket=>={threshold}{unit} {direction} | "
                            f"{f.get('source','Primary')}={primary_forecast:.1f} GFS={f'{gfs_forecast:.1f}' if gfs_forecast is not None else 'N/A'} | "
                            f"Edge={edge:.1%} Models={sizing.get('models_agreed','?')} "
                            f"EventVol=${event_vol:,.0f} BucketVol=${bucket_vol:,.0f} "
                            f"{sizing.get('spread_note','')} "
                            f"{'🌅EARLY' if is_early else ''} {'🔁RE-ENTRY' if entry_number > 1 else ''}"
                        )
                        # ── Bucket mapping diagnostics (feature-flagged, DB write) ──
                        # Never blocks trades. Enable via env var BUCKET_MAPPING=1.
                        try:
                            from data.bucket_mapping import BUCKET_MAPPING_ENABLED, store_bucket_mapping
                            if BUCKET_MAPPING_ENABLED:
                                await store_bucket_mapping(
                                    session=session,
                                    city=city,
                                    threshold=threshold,
                                    direction=direction,
                                    synthetic_prob=noaa_prob,
                                    synthetic_edge=edge,
                                    market_data=market_data,
                                    is_celsius=is_celsius,
                                    market_date=market_date_str,
                                )
                        except Exception:
                            pass
                    else:
                        if edge >= 0.05:
                            log(f"SKIP {city} | MarketDate={market_date_str} | Bucket=>={threshold}{unit} | {reason} | Edge={edge:.1%} | EventVol=${event_vol:,.0f} BucketVol=${bucket_vol:,.0f}")

            # ── Step 6: Open paper trades ─────────────────────────────────────
            for (city, mkt_date), sig in best_per_city_date.items():
                city_cfg_item = city_by_name[city]
                is_celsius = city_cfg_item.get("celsius", False)
                unit = "C" if is_celsius else "F"

                # Re-check city-wide caps at trade opening time
                # (a same-scan trade for an earlier date may have already opened)
                current_city_count = open_city_total.get(city, 0)
                if current_city_count >= cfg["max_positions_per_city"]:
                    log(f"TRADE BLOCKED (city cap): {city}/{mkt_date} >={sig['threshold']}{unit} {sig['direction']}")
                    continue
                current_city_exposure = open_city_exposure.get(city, 0.0)
                max_city_exp = scan_start_bankroll * cfg["max_city_exposure_pct"]
                if current_city_exposure + sig["sizing"]["size_usd"] > max_city_exp:
                    log(f"TRADE BLOCKED (exposure cap): {city}/{mkt_date} | "
                        f"existing ${current_city_exposure:.2f} + ${sig['sizing']['size_usd']:.2f} > ${max_city_exp:.2f}")
                    continue

                # Inject extra data into forecast dict for open_paper_trade
                sig["forecast"]["gfs_forecast"] = sig.get("gfs_forecast")
                # DB column is still named ecmwf_forecast (no migration), but stores ICON value
                sig["forecast"]["ecmwf_forecast"] = sig.get("icon_forecast")
                sig["forecast"]["prior_entry_edge"] = sig.get("prior_entry_edge")
                sig["forecast"]["crowd_price_at_prior"] = sig.get("crowd_price_at_prior")
                sig["forecast"]["market_date"] = sig.get("market_date")

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

                # Update in-memory tracking for subsequent trades in same scan
                open_city_total[city] = current_city_count + 1
                open_city_exposure[city] = current_city_exposure + sig["sizing"]["size_usd"]
                open_city_date_set.add((city, mkt_date))

                if sig["direction"] == "YES":
                    open_yes_count += 1
                scan_result["trades_opened"] += 1

                models = sig["sizing"].get("models_agreed", "?")
                log(
                    f"TRADE OPENED: {city} | MarketDate={sig['market_date']} | Bucket=>={sig['threshold']}{unit} {sig['direction']} | "
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
