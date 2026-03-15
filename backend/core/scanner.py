# backend/core/scanner.py
import logging
import asyncio
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy.ext.asyncio import AsyncSession
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, FORECAST_EDGE_CONFIG, CITIES, STRATEGY_BANKROLL_ID
from data.noaa import (
    get_nws_daily_high, get_openmeteo_daily_high,
    fetch_gfs_forecast_high, prob_above as _prob_above_fn
)
from data.polymarket import build_market_map
from core.signals import (
    get_bankroll, get_open_positions,
    open_paper_trade, settle_trade,
    evaluate_signal, evaluate_signal_forecast_edge,
    compute_forecast_analytics,
    log_calibration, reset_daily_loss,
)
from models.database import AsyncSessionLocal, ScanLog

logger = logging.getLogger(__name__)

city_names = [c["name"] for c in CITIES]
city_by_name = {c["name"]: c for c in CITIES}


# ── Re-entry state (Strategy B only — Strategy A has no re-entry) ────────────

async def _get_reentry_state_from_db(session, city, market_date, threshold=None, strategy="sigma"):
    """Derive re-entry state from existing DB trades. Now includes threshold + strategy."""
    from sqlalchemy import select
    from models.database import Trade as TradeModel
    today_str = market_date or datetime.now(timezone.utc).date().isoformat()

    q = (
        select(TradeModel)
        .where(TradeModel.city == city)
        .where(TradeModel.market_date == today_str)
        .where(TradeModel.status == "OPEN")
        .where(TradeModel.strategy == strategy)
    )
    if threshold is not None:
        q = q.where(TradeModel.threshold_f == threshold)
    q = q.order_by(TradeModel.opened_at.desc())

    result = await session.execute(q)
    trades = result.scalars().all()

    if not trades:
        return {"entry_count": 0, "edge_hwm": 0.0, "last_crowd_price": None, "last_entry_time": None}

    entry_count = len(trades)
    edge_hwm = max((t.edge_pct for t in trades), default=0.0)
    latest = trades[0]
    last_crowd_price = latest.market_yes_price
    last_entry_time = latest.opened_at.replace(tzinfo=timezone.utc) if latest.opened_at.tzinfo is None else latest.opened_at

    return {
        "entry_count": entry_count,
        "edge_hwm": edge_hwm,
        "last_crowd_price": last_crowd_price,
        "last_entry_time": last_entry_time,
    }


def _is_early_window(end_date_str):
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


def _is_too_late_for_reentry(end_date_str):
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


async def fetch_validator_forecasts(city_cfg, day_offset=0):
    is_celsius = city_cfg.get("celsius", False)
    lat, lon = city_cfg["lat"], city_cfg["lon"]
    tz = city_cfg.get("timezone", "UTC")
    gfs = await fetch_gfs_forecast_high(lat, lon, day_offset, is_celsius, tz)
    return {"gfs": gfs, "icon": None}


async def run_scan():
    """
    Full scan cycle — dual strategy execution.
    Shared pipeline: market fetch, forecast fetch, settlement.
    Then fork: evaluate Strategy B (sigma) and Strategy A (forecast_edge) independently.
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

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # ── Get BOTH bankroll states ──────────────────────────────────────
            bankroll_b = await get_bankroll(session, "sigma")
            bankroll_a = await get_bankroll(session, "forecast_edge")
            await reset_daily_loss(session, bankroll_b)
            await reset_daily_loss(session, bankroll_a)

            cfg = BOT_CONFIG
            cfg_a = FORECAST_EDGE_CONFIG

            # Daily loss cap check (effectively disabled for paper at 100%)
            for label, bs in [("sigma", bankroll_b), ("forecast_edge", bankroll_a)]:
                cap = max(cfg.get("daily_loss_cap_floor_usd", 50), bs.balance * cfg["daily_loss_cap_pct"])
                if bs.daily_loss_today >= cap:
                    log(f"CIRCUIT BREAKER [{label}]: Daily loss ${bs.daily_loss_today:.2f} >= cap ${cap:.2f}", "WARN")
                    # Don't return — other strategy may still trade

            # ── Step 1: Fetch Polymarket prices (SHARED) ─────────────────────
            log("Fetching Polymarket prices...")
            try:
                market_map, city_date_map = await build_market_map(city_names)
                cities_found = len(set(c for c, _ in city_date_map))
                log(f"Polymarket: {len(market_map)} entries across {cities_found} cities")
            except Exception as e:
                log(f"Polymarket fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"Polymarket: {e}")
                market_map = {}
                city_date_map = set()

            # ── Group city-date pairs ─────────────────────────────────────────
            utc_today = datetime.now(timezone.utc).date()
            from collections import defaultdict
            city_dates = defaultdict(list)
            city_date_offset = {}

            for city_name, date_str in city_date_map:
                city_dates[city_name].append(date_str)
                try:
                    mkt_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    offset = max(0, min((mkt_date - utc_today).days, 6))
                except Exception:
                    offset = 0
                city_date_offset[(city_name, date_str)] = offset

            for city_name in city_dates:
                city_dates[city_name].sort()

            # ── Step 2: Fetch primary forecasts (SHARED) ─────────────────────
            log("Fetching primary forecasts (NOAA/ICON/JMA)...")
            import httpx as _httpx
            from data.noaa import fetch_city_forecast as _fetch_city_forecast
            forecast_map = {}

            try:
                async with _httpx.AsyncClient() as _client:
                    fetch_idx = 0
                    for city_name, date_strs in city_dates.items():
                        city_cfg = city_by_name.get(city_name)
                        if not city_cfg:
                            continue
                        is_intl = city_cfg.get("celsius", False)
                        for date_str in date_strs:
                            if fetch_idx > 0 and is_intl:
                                await asyncio.sleep(1.5)
                            offset = city_date_offset.get((city_name, date_str), 0)
                            try:
                                result = await _fetch_city_forecast(city_cfg, offset, _client, target_date=date_str)
                                if isinstance(result, dict):
                                    forecast_map[(city_name, date_str)] = result
                                    log(f"Primary {city_name}/{date_str}: {result.get('forecast_high')}°{result.get('unit','?')} via {result.get('source','?')}")
                                else:
                                    log(f"Forecast failed for {city_name}/{date_str}: {result}", "WARN")
                            except Exception as e:
                                log(f"Forecast failed for {city_name}/{date_str}: {e}", "WARN")
                            fetch_idx += 1
                scan_result["cities_scanned"] = len(set(c for c, _ in forecast_map))
            except Exception as e:
                log(f"Primary forecast fetch failed: {e}", "WARN")
                scan_result["errors"].append(f"Forecast: {e}")
                return scan_result

            # ── Step 3: Fetch GFS validator forecasts (SHARED) ────────────────
            log("Fetching GFS validator forecasts...")
            validator_map = {}
            fetch_count = 0
            for city_name, date_strs in city_dates.items():
                city_cfg = city_by_name.get(city_name)
                if not city_cfg:
                    continue
                if city_cfg.get("single_model", False):
                    for date_str in date_strs:
                        validator_map[(city_name, date_str)] = {"gfs": None, "icon": None}
                    log(f"Validator SKIP {city_name} — single-model city")
                    continue
                for date_str in date_strs:
                    if fetch_count > 0:
                        await asyncio.sleep(1.5)
                    offset = city_date_offset.get((city_name, date_str), 0)
                    try:
                        validators = await fetch_validator_forecasts(city_cfg, day_offset=offset)
                        validator_map[(city_name, date_str)] = validators
                        gfs_val = f"{validators['gfs']:.1f}" if validators['gfs'] is not None else "N/A"
                        log(f"Validator {city_name}/{date_str}: GFS={gfs_val}")
                    except Exception as e:
                        log(f"Validator fetch failed for {city_name}/{date_str}: {e}", "WARN")
                        validator_map[(city_name, date_str)] = {"gfs": None, "icon": None}
                    fetch_count += 1

            # ── Step 4: Settle open positions (BOTH strategies) ──────────────
            all_open = await get_open_positions(session)  # all strategies
            log(f"Open positions: {len(all_open)} (sigma: {sum(1 for t in all_open if (t.strategy or 'sigma') == 'sigma')}, forecast_edge: {sum(1 for t in all_open if t.strategy == 'forecast_edge')})")

            from data.polymarket import check_event_resolution

            for trade in all_open:
                city_cfg = city_by_name.get(trade.city)
                if not city_cfg:
                    continue
                is_celsius = city_cfg.get("celsius", False)
                unit = "C" if is_celsius else "F"

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

                resolution = await check_event_resolution(city=trade.city, market_date_str=trade.market_date or trade_date.isoformat())

                if resolution is None or not resolution.get("resolved", False):
                    if trade_date < today:
                        log(f"STALE? [{trade.strategy or 'sigma'}] {trade.city} >={trade.threshold_f}{unit} | Date={trade_date}", "WARN")
                    continue

                winning_low = resolution["winning_bucket_low"]
                polymarket_won = (
                    (trade.direction == "YES" and winning_low >= trade.threshold_f) or
                    (trade.direction == "NO"  and winning_low < trade.threshold_f)
                )

                actual_high = None
                try:
                    if is_celsius:
                        actual_high = await get_openmeteo_daily_high(city_cfg["lat"], city_cfg["lon"], trade_date)
                    else:
                        actual_high = await get_nws_daily_high(city_cfg["station"], trade_date)
                except Exception as e:
                    log(f"Observation fetch failed for {trade.city}: {e}", "WARN")

                if actual_high is None:
                    actual_high = resolution.get("estimated_high")

                # Settle using the CORRECT strategy's bankroll
                trade_strategy = trade.strategy or "sigma"
                settle_bankroll = bankroll_b if trade_strategy == "sigma" else bankroll_a

                result = await settle_trade(session, trade, settle_bankroll, actual_high_f=actual_high, polymarket_won=polymarket_won)
                scan_result["trades_settled"] += 1
                log(f"SETTLED [{trade_strategy}] {trade.city} >={trade.threshold_f}{unit} | {result['status']} | Net=${result['net_pnl']:+.2f}")

                if actual_high is not None:
                    await log_calibration(session, trade.city, city_cfg["station"], trade.noaa_forecast_high, actual_high, trade.noaa_sigma, market_date=trade.market_date)

            # ── Step 5: Evaluate signals — DUAL STRATEGY ─────────────────────
            open_after_settle = await get_open_positions(session)

            # Build STRATEGY-SCOPED position tracking
            # Strategy B (sigma)
            open_b_trades = [t for t in open_after_settle if (t.strategy or "sigma") == "sigma"]
            open_b_set = {(t.city, t.market_date, t.threshold_f) for t in open_b_trades}
            open_b_yes = sum(1 for t in open_b_trades if t.direction == "YES")
            open_b_city_total = {}
            open_b_city_exposure = {}
            for t in open_b_trades:
                open_b_city_total[t.city] = open_b_city_total.get(t.city, 0) + 1
                open_b_city_exposure[t.city] = open_b_city_exposure.get(t.city, 0.0) + t.position_size_usd

            # Strategy A (forecast_edge)
            open_a_trades = [t for t in open_after_settle if t.strategy == "forecast_edge"]
            open_a_set = {(t.city, t.market_date, t.threshold_f) for t in open_a_trades}
            open_a_yes = sum(1 for t in open_a_trades if t.direction == "YES")
            open_a_city_total = {}
            open_a_city_exposure = {}
            for t in open_a_trades:
                open_a_city_total[t.city] = open_a_city_total.get(t.city, 0) + 1
                open_a_city_exposure[t.city] = open_a_city_exposure.get(t.city, 0.0) + t.position_size_usd

            scan_start_bankroll_b = bankroll_b.balance
            scan_start_bankroll_a = bankroll_a.balance

            best_per_city_date_b = {}  # Strategy B signals
            best_per_city_date_a = {}  # Strategy A signals

            for city, market_date_str in sorted(city_date_map):
                city_cfg_item = city_by_name.get(city, {})
                if not city_cfg_item:
                    continue
                is_celsius = city_cfg_item.get("celsius", False)
                unit = "C" if is_celsius else "F"

                # ── Noon local-time guard (SHARED — both strategies) ──────────
                try:
                    mkt_date = datetime.strptime(market_date_str, "%Y-%m-%d").date()
                    if mkt_date == utc_today:
                        city_tz = city_cfg_item.get("timezone", "UTC")
                        city_local_now = datetime.now(timezone.utc).astimezone(ZoneInfo(city_tz))
                        if city_local_now.hour >= 12:
                            log(f"SKIP {city}/{market_date_str} | Day-0 past noon local ({city_local_now.strftime('%H:%M')} {city_tz})")
                            continue
                except Exception:
                    pass

                thresholds_for_city = sorted(
                    thresh for (c, d, thresh) in market_map if c == city and d == market_date_str
                )

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
                    noaa_prob = round(_prob_above_fn(threshold, primary_forecast, f["sigma"]), 4)
                    direction = "YES" if noaa_prob > yes_price else "NO"

                    # ── Shared pre-filters: liquidity ─────────────────────────
                    event_vol = market_data.get("event_volume", 0.0)
                    if event_vol < cfg["min_event_volume"]:
                        continue
                    matched_bucket = next((b for b in market_data.get("buckets", []) if b["low"] == threshold), None)
                    if matched_bucket is None:
                        continue
                    bucket_vol = matched_bucket.get("bucket_volume", 0.0)
                    if bucket_vol < cfg["min_bucket_volume"]:
                        continue

                    end_date_str = market_data.get("end_date", "")
                    is_early = _is_early_window(end_date_str)

                    # Compute forecast analytics ONCE (shared by both strategies)
                    fa = compute_forecast_analytics(direction, threshold, primary_forecast, gfs_forecast, is_celsius)
                    edge = abs(noaa_prob - yes_price)

                    # ── Raw directional gate (day-0 only, Strategy B only) ────
                    is_day0 = f.get("day_offset", 0) == 0

                    # ══════════════════════════════════════════════════════════
                    # STRATEGY B: Sigma evaluation
                    # ══════════════════════════════════════════════════════════
                    b_blocked_day0 = False
                    if is_day0:
                        if direction == "YES" and primary_forecast <= threshold:
                            b_blocked_day0 = True
                        if direction == "NO" and primary_forecast >= threshold:
                            b_blocked_day0 = True

                    if not b_blocked_day0:
                        # City cap check for Strategy B
                        b_city_ok = (
                            open_b_city_total.get(city, 0) < cfg["max_positions_per_city"] and
                            open_b_city_exposure.get(city, 0.0) < scan_start_bankroll_b * cfg["max_city_exposure_pct"]
                        )

                        if b_city_ok and bankroll_b.balance > 0:
                            reentry = await _get_reentry_state_from_db(session, city, market_date_str, threshold, "sigma")
                            entry_number = reentry["entry_count"] + 1
                            prior_ev = reentry["edge_hwm"] if reentry["entry_count"] > 0 else None
                            last_crowd_price = reentry["last_crowd_price"]

                            too_late = _is_too_late_for_reentry(end_date_str) if entry_number > 1 else False
                            cooldown_ok = True
                            if entry_number > 1 and reentry["last_entry_time"]:
                                last_t = reentry["last_entry_time"]
                                if last_t.tzinfo is None:
                                    last_t = last_t.replace(tzinfo=timezone.utc)
                                mins_since = (datetime.now(timezone.utc) - last_t).total_seconds() / 60
                                if mins_since < cfg["reentry_cooldown_minutes"]:
                                    cooldown_ok = False

                            if cooldown_ok and not too_late:
                                should_b, reason_b, sizing_b = evaluate_signal(
                                    city=city, threshold=threshold, noaa_prob=noaa_prob,
                                    market_yes_price=yes_price, confidence=f["confidence"],
                                    direction=direction, bankroll=bankroll_b.balance,
                                    open_city_date_positions=open_b_set,
                                    open_yes_positions=open_b_yes, market_date=market_date_str,
                                    primary_forecast=primary_forecast, primary_source=f.get("source"),
                                    is_celsius=is_celsius, gfs_forecast=gfs_forecast,
                                    icon_forecast=icon_forecast, is_early_window=is_early,
                                    entry_number=entry_number, prior_entry_edge=prior_ev,
                                    crowd_price_at_prior=last_crowd_price,
                                )

                                if should_b:
                                    scan_result["signals_found"] += 1
                                    score = edge * sizing_b.get("size_usd", 0)
                                    cd_key = (city, market_date_str, "sigma")
                                    sig_b = {
                                        "city": city, "threshold": threshold, "direction": direction,
                                        "edge": edge, "sizing": sizing_b, "market_data": market_data,
                                        "forecast": f, "primary_forecast": primary_forecast,
                                        "gfs_forecast": gfs_forecast, "icon_forecast": icon_forecast,
                                        "is_early_window": is_early, "entry_number": entry_number,
                                        "prior_entry_edge": prior_ev, "crowd_price_at_prior": last_crowd_price,
                                        "market_date": market_date_str, "score": score,
                                        "forecast_analytics": fa, "strategy": "sigma",
                                    }
                                    if cd_key not in best_per_city_date_b or score > best_per_city_date_b[cd_key].get("score", 0):
                                        best_per_city_date_b[cd_key] = sig_b
                                    log(f"SIGNAL [sigma] {city}/{market_date_str} >={threshold}{unit} {direction} | Edge={edge:.1%} Gap={fa['forecast_gap']}")

                    # ══════════════════════════════════════════════════════════
                    # STRATEGY A: Forecast Edge evaluation
                    # ══════════════════════════════════════════════════════════
                    a_city_ok = (
                        open_a_city_total.get(city, 0) < cfg_a["max_positions_per_city"] and
                        open_a_city_exposure.get(city, 0.0) < scan_start_bankroll_a * cfg_a["max_city_exposure_pct"]
                    )

                    if a_city_ok and bankroll_a.balance > 0:
                        should_a, reason_a, sizing_a = evaluate_signal_forecast_edge(
                            city=city, threshold=threshold, noaa_prob=noaa_prob,
                            market_yes_price=yes_price, confidence=f["confidence"],
                            direction=direction, bankroll=bankroll_a.balance,
                            open_city_date_positions=open_a_set,
                            open_yes_positions=open_a_yes, market_date=market_date_str,
                            primary_forecast=primary_forecast, is_celsius=is_celsius,
                        )

                        if should_a:
                            scan_result["signals_found"] += 1
                            score_a = edge * sizing_a.get("size_usd", 0)
                            cd_key_a = (city, market_date_str, "forecast_edge")
                            sig_a = {
                                "city": city, "threshold": threshold, "direction": direction,
                                "edge": edge, "sizing": sizing_a, "market_data": market_data,
                                "forecast": f, "primary_forecast": primary_forecast,
                                "gfs_forecast": gfs_forecast, "icon_forecast": icon_forecast,
                                "is_early_window": False, "entry_number": 1,
                                "prior_entry_edge": None, "crowd_price_at_prior": None,
                                "market_date": market_date_str, "score": score_a,
                                "forecast_analytics": fa, "strategy": "forecast_edge",
                            }
                            if cd_key_a not in best_per_city_date_a or score_a > best_per_city_date_a[cd_key_a].get("score", 0):
                                best_per_city_date_a[cd_key_a] = sig_a
                            log(f"SIGNAL [forecast_edge] {city}/{market_date_str} >={threshold}{unit} {direction} | Edge={edge:.1%} Gap={fa['forecast_gap']}")

            # ── Step 6: Open paper trades — Strategy B ────────────────────────
            for cd_key, sig in best_per_city_date_b.items():
                city = sig["city"]
                city_cfg_item = city_by_name[city]
                is_celsius = city_cfg_item.get("celsius", False)
                unit = "C" if is_celsius else "F"

                current_city_count = open_b_city_total.get(city, 0)
                if current_city_count >= cfg["max_positions_per_city"]:
                    continue
                current_city_exposure = open_b_city_exposure.get(city, 0.0)
                max_city_exp = scan_start_bankroll_b * cfg["max_city_exposure_pct"]
                if current_city_exposure + sig["sizing"]["size_usd"] > max_city_exp:
                    continue

                sig["forecast"]["gfs_forecast"] = sig.get("gfs_forecast")
                sig["forecast"]["ecmwf_forecast"] = sig.get("icon_forecast")
                sig["forecast"]["prior_entry_edge"] = sig.get("prior_entry_edge")
                sig["forecast"]["crowd_price_at_prior"] = sig.get("crowd_price_at_prior")
                sig["forecast"]["market_date"] = sig.get("market_date")

                trade = await open_paper_trade(
                    session=session, city=city, station_id=city_cfg_item["station"],
                    threshold=sig["threshold"], direction=sig["direction"],
                    market_data=sig["market_data"], noaa_data=sig["forecast"],
                    sizing=sig["sizing"], bankroll_state=bankroll_b,
                    strategy="sigma", forecast_analytics=sig.get("forecast_analytics"),
                )

                open_b_city_total[city] = current_city_count + 1
                open_b_city_exposure[city] = current_city_exposure + sig["sizing"]["size_usd"]
                open_b_set.add((city, sig["market_date"], sig["threshold"]))
                if sig["direction"] == "YES":
                    open_b_yes += 1
                scan_result["trades_opened"] += 1
                log(f"TRADE OPENED [sigma] {city}/{sig['market_date']} >={sig['threshold']}{unit} {sig['direction']} | ${sig['sizing']['size_usd']} | Bankroll->${bankroll_b.balance:.2f}")

            # ── Step 6b: Open paper trades — Strategy A ───────────────────────
            for cd_key, sig in best_per_city_date_a.items():
                city = sig["city"]
                city_cfg_item = city_by_name[city]
                is_celsius = city_cfg_item.get("celsius", False)
                unit = "C" if is_celsius else "F"

                current_city_count = open_a_city_total.get(city, 0)
                if current_city_count >= cfg_a["max_positions_per_city"]:
                    continue
                current_city_exposure = open_a_city_exposure.get(city, 0.0)
                max_city_exp = scan_start_bankroll_a * cfg_a["max_city_exposure_pct"]
                if current_city_exposure + sig["sizing"]["size_usd"] > max_city_exp:
                    continue

                sig["forecast"]["gfs_forecast"] = sig.get("gfs_forecast")
                sig["forecast"]["ecmwf_forecast"] = sig.get("icon_forecast")
                sig["forecast"]["market_date"] = sig.get("market_date")

                trade = await open_paper_trade(
                    session=session, city=city, station_id=city_cfg_item["station"],
                    threshold=sig["threshold"], direction=sig["direction"],
                    market_data=sig["market_data"], noaa_data=sig["forecast"],
                    sizing=sig["sizing"], bankroll_state=bankroll_a,
                    strategy="forecast_edge", forecast_analytics=sig.get("forecast_analytics"),
                )

                open_a_city_total[city] = current_city_count + 1
                open_a_city_exposure[city] = current_city_exposure + sig["sizing"]["size_usd"]
                open_a_set.add((city, sig["market_date"], sig["threshold"]))
                if sig["direction"] == "YES":
                    open_a_yes += 1
                scan_result["trades_opened"] += 1
                log(f"TRADE OPENED [forecast_edge] {city}/{sig['market_date']} >={sig['threshold']}{unit} {sig['direction']} | ${sig['sizing']['size_usd']} | Gap={sig.get('forecast_analytics',{}).get('forecast_gap','?')} | Bankroll->${bankroll_a.balance:.2f}")

            # ── Step 7: Log scan ─────────────────────────────────────────────
            duration_ms = int(time.time() * 1000) - start_ms
            scan_log = ScanLog(
                cities_scanned=scan_result["cities_scanned"],
                signals_found=scan_result["signals_found"],
                trades_opened=scan_result["trades_opened"],
                trades_settled=scan_result["trades_settled"],
                bankroll_snapshot=bankroll_b.balance + bankroll_a.balance,
                errors="; ".join(scan_result["errors"]) if scan_result["errors"] else None,
                duration_ms=duration_ms,
            )
            session.add(scan_log)

    scan_result["duration_ms"] = duration_ms
    log(f"Scan complete in {duration_ms}ms | Sigma=${bankroll_b.balance:.2f} ForecastEdge=${bankroll_a.balance:.2f}")
    return scan_result
