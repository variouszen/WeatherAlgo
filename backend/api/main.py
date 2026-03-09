# backend/api/main.py
import logging
import asyncio
from datetime import datetime, date
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, func, desc
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, STARTING_BANKROLL, DRY_RUN
from models.database import (
    init_db, AsyncSessionLocal,
    Trade, BankrollState, ScanLog, CityCalibration
)
from core.signals import get_bankroll
from core.scanner import run_scan
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Weather Arb Bot", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_scan_running = False
_last_scan_result = None


@app.on_event("startup")
async def startup():
    logger.info("Starting Weather Arb Bot...")
    await init_db()
    logger.info(f"DB initialized | DRY_RUN={DRY_RUN} | Starting bankroll=${STARTING_BANKROLL}")

    # Start background scheduler
    asyncio.create_task(scan_scheduler())


async def scan_scheduler():
    """Run scan every N seconds in background."""
    interval = BOT_CONFIG["scan_interval_seconds"]
    logger.info(f"Scanner starting — interval={interval}s")
    await asyncio.sleep(10)  # Initial delay after startup
    while True:
        try:
            await trigger_scan()
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
        await asyncio.sleep(interval)


async def trigger_scan():
    global _scan_running, _last_scan_result
    if _scan_running:
        logger.warning("Scan already running, skipping")
        return
    _scan_running = True
    try:
        _last_scan_result = await run_scan()
    finally:
        _scan_running = False


# ── API Endpoints ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the dashboard HTML directly from Railway."""
    import os
    this_file = os.path.abspath(__file__)  # backend/api/main.py
    api_dir = os.path.dirname(this_file)   # backend/api/
    backend_dir = os.path.dirname(api_dir) # backend/
    repo_dir = os.path.dirname(backend_dir) # repo root

    # Search in likely locations
    candidates = [
        os.path.join(backend_dir, "weather-arb-dashboard.html"),
        os.path.join(repo_dir, "weather-arb-dashboard.html"),
        os.path.join(api_dir, "weather-arb-dashboard.html"),
    ]
    for html_path in candidates:
        if os.path.exists(html_path):
            with open(html_path, "r") as f:
                return HTMLResponse(content=f.read())

    # Debug: show what we searched
    searched = ", ".join(candidates)
    return HTMLResponse(
        content=f"<h1>Dashboard not found</h1><p>Searched: {searched}</p>",
        status_code=404
    )


@app.post("/api/admin/reset-daily-loss")
async def reset_daily_loss_endpoint():
    """Force-reset the daily loss counter. Use when counter is stuck."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            from core.signals import get_bankroll
            from datetime import date
            bankroll_state = await get_bankroll(session)
            old_val = bankroll_state.daily_loss_today
            bankroll_state.daily_loss_today = 0.0
            bankroll_state.last_reset_date = date.today().isoformat()
    return {"status": "reset", "previous_daily_loss": old_val, "now": 0.0}


@app.post("/api/admin/purge-all-open-trades")
async def purge_all_open_trades():
    """Delete ALL open trades and refund their position sizes to bankroll."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session)

            result = await session.execute(
                select(Trade).where(Trade.status == "OPEN")
            )
            open_trades = result.scalars().all()

            purged = []
            for trade in open_trades:
                bankroll_state.balance = round(
                    bankroll_state.balance + trade.position_size_usd, 2
                )
                purged.append({
                    "city": trade.city,
                    "threshold": trade.threshold_f,
                    "direction": trade.direction,
                    "size": trade.position_size_usd,
                })
                await session.delete(trade)

            logger.info(f"[PURGE-ALL] Deleted {len(purged)} open trades, bankroll restored to ${bankroll_state.balance}")

    return {
        "status": "done",
        "purged_count": len(purged),
        "bankroll_after": bankroll_state.balance,
        "purged_trades": purged,
    }
async def purge_stale_trades():
    """
    Delete open trades that were entered against expired/stale Polymarket markets.
    A trade is stale if its market_condition references a date before today.
    Also refunds the position size back to bankroll.
    """
    from datetime import datetime, timezone, timedelta
    import re

    today = datetime.now(timezone.utc).date()

    def extract_date_from_condition(condition: str):
        """Parse date from market_condition or polymarket title."""
        if not condition:
            return None
        m = re.search(
            r'\b(january|february|march|april|may|june|july|august|'
            r'september|october|november|december)\s+(\d{1,2})\b',
            condition.lower()
        )
        if m:
            try:
                year = datetime.now(timezone.utc).year
                dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {year}", "%B %d %Y")
                return dt.date()
            except ValueError:
                pass
        return None

    purged = []
    kept = []

    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session)

            result = await session.execute(
                select(Trade).where(Trade.status == "OPEN")
            )
            open_trades = result.scalars().all()

            for trade in open_trades:
                # Check opened_at date — trades opened before today are candidates
                trade_open_date = trade.opened_at.date() if trade.opened_at else None

                # Also check if the market_condition has an explicit old date
                condition_date = extract_date_from_condition(trade.market_condition or "")

                is_stale = False
                reason = ""

                if condition_date and condition_date < today:
                    is_stale = True
                    reason = f"market_condition date {condition_date} < today {today}"
                elif trade_open_date and trade_open_date < today:
                    # Opened before today — check if it has a valid today/tomorrow market
                    # For safety, flag trades opened more than 1 day ago
                    if trade_open_date < today - timedelta(days=1):
                        is_stale = True
                        reason = f"opened {trade_open_date}, more than 1 day old"

                if is_stale:
                    # Refund position size to bankroll
                    bankroll_state.balance = round(
                        bankroll_state.balance + trade.position_size_usd, 2
                    )
                    purged.append({
                        "id": trade.id,
                        "city": trade.city,
                        "threshold": trade.threshold_f,
                        "direction": trade.direction,
                        "size": trade.position_size_usd,
                        "reason": reason,
                    })
                    await session.delete(trade)
                    logger.info(
                        f"[PURGE] Deleted stale trade: {trade.city} "
                        f">={trade.threshold_f} {trade.direction} | "
                        f"${trade.position_size_usd} refunded | {reason}"
                    )
                else:
                    kept.append(f"{trade.city} >={trade.threshold_f} {trade.direction}")

    return {
        "status": "done",
        "purged_count": len(purged),
        "kept_count": len(kept),
        "bankroll_after": bankroll_state.balance,
        "purged_trades": purged,
        "kept_trades": kept,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "dry_run": DRY_RUN, "timestamp": datetime.utcnow().isoformat()}


@app.post("/api/scan")
async def manual_scan(background_tasks: BackgroundTasks):
    """Trigger a manual scan (runs in background)."""
    if _scan_running:
        return {"status": "already_running"}
    background_tasks.add_task(trigger_scan)
    return {"status": "started"}


@app.get("/api/dashboard")
async def dashboard():
    """All data needed for frontend in one call."""
    async with AsyncSessionLocal() as session:
        # Bankroll
        bankroll = await get_bankroll(session)

        # Trades
        all_trades_result = await session.execute(
            select(Trade).order_by(desc(Trade.opened_at)).limit(200)
        )
        all_trades = all_trades_result.scalars().all()

        open_trades = [t for t in all_trades if t.status == "OPEN"]
        settled = [t for t in all_trades if t.status in ("WIN", "LOSS")]
        wins = [t for t in settled if t.status == "WIN"]
        losses = [t for t in settled if t.status == "LOSS"]

        # Stats
        total_net_pnl = sum(t.net_pnl or 0 for t in settled)
        total_fees = sum(t.fees_usd or 0 for t in settled)
        win_rate = (len(wins) / len(settled) * 100) if settled else 0
        avg_edge = (sum(t.edge_pct for t in settled) / len(settled) * 100) if settled else 0

        # Average win/loss
        avg_win = (sum(t.net_pnl for t in wins) / len(wins)) if wins else 0
        avg_loss = (sum(t.net_pnl for t in losses) / len(losses)) if losses else 0

        # Expected value per trade
        ev_per_trade = (
            (win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)
        ) if settled else 0

        # Profit factor
        gross_wins = sum(t.gross_pnl for t in wins if t.gross_pnl)
        gross_losses = abs(sum(t.gross_pnl for t in losses if t.gross_pnl))
        profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else 0

        # Max drawdown
        running_bal = STARTING_BANKROLL
        peak = STARTING_BANKROLL
        max_dd = 0
        equity_curve = [STARTING_BANKROLL]
        for t in sorted(settled, key=lambda x: x.resolved_at or datetime.min):
            running_bal += (t.net_pnl or 0)
            equity_curve.append(round(running_bal, 2))
            if running_bal > peak:
                peak = running_bal
            dd = (peak - running_bal) / peak * 100
            if dd > max_dd:
                max_dd = dd

        # Sharpe (simplified: mean/std of net_pnls)
        import numpy as np
        pnls = [t.net_pnl for t in settled if t.net_pnl is not None]
        sharpe = (np.mean(pnls) / np.std(pnls)) if len(pnls) > 1 else 0

        # Calibration
        cal_result = await session.execute(
            select(CityCalibration)
            .where(CityCalibration.actual_high_f.isnot(None))
            .order_by(desc(CityCalibration.recorded_at))
            .limit(100)
        )
        cal_rows = cal_result.scalars().all()
        cal_errors = [abs(r.forecast_error_f) for r in cal_rows if r.forecast_error_f is not None]
        mean_abs_error = round(float(np.mean(cal_errors)), 2) if cal_errors else None

        # By-city breakdown
        city_stats = {}
        for t in settled:
            c = t.city
            if c not in city_stats:
                city_stats[c] = {"trades": 0, "wins": 0, "pnl": 0.0, "edge_sum": 0.0}
            city_stats[c]["trades"] += 1
            city_stats[c]["pnl"] = round(city_stats[c]["pnl"] + (t.net_pnl or 0), 2)
            city_stats[c]["edge_sum"] += t.edge_pct
            if t.status == "WIN":
                city_stats[c]["wins"] += 1
        for c in city_stats:
            s = city_stats[c]
            s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1)
            s["avg_edge"] = round(s["edge_sum"] / s["trades"] * 100, 1)
            del s["edge_sum"]

        # Recent scan logs
        scan_log_result = await session.execute(
            select(ScanLog).order_by(desc(ScanLog.scanned_at)).limit(20)
        )
        scan_logs = scan_log_result.scalars().all()

        return {
            "bankroll": {
                "current": round(bankroll.balance, 2),
                "starting": bankroll.starting_balance,
                "pnl": round(bankroll.balance - bankroll.starting_balance, 2),
                "pnl_pct": round((bankroll.balance - bankroll.starting_balance) / bankroll.starting_balance * 100, 2),
                "daily_loss_today": round(bankroll.daily_loss_today, 2),
            },
            "performance": {
                "total_trades": len(settled),
                "open_positions": len(open_trades),
                "win_rate": round(win_rate, 1),
                "avg_edge_pct": round(avg_edge, 1),
                "total_net_pnl": round(total_net_pnl, 2),
                "total_fees": round(total_fees, 2),
                "avg_win": round(avg_win, 2),
                "avg_loss": round(avg_loss, 2),
                "ev_per_trade": round(ev_per_trade, 2),
                "profit_factor": round(profit_factor, 2),
                "max_drawdown_pct": round(max_dd, 2),
                "sharpe": round(float(sharpe), 3),
                "mean_abs_forecast_error_f": mean_abs_error,
            },
            "equity_curve": equity_curve,
            "open_positions": [_trade_to_dict(t) for t in open_trades],
            "trade_history": [_trade_to_dict(t) for t in settled[:50]],
            "city_stats": city_stats,
            "scan_logs": [_scan_log_to_dict(s) for s in scan_logs],
            "config": BOT_CONFIG,
            "dry_run": DRY_RUN,
            "scan_running": _scan_running,
            "last_scan": _last_scan_result,
        }


@app.get("/api/trades")
async def get_trades(status: str = None, city: str = None, limit: int = 100):
    async with AsyncSessionLocal() as session:
        q = select(Trade).order_by(desc(Trade.opened_at))
        if status:
            q = q.where(Trade.status == status.upper())
        if city:
            q = q.where(Trade.city == city)
        result = await session.execute(q.limit(limit))
        trades = result.scalars().all()
        return [_trade_to_dict(t) for t in trades]


@app.get("/api/calibration")
async def get_calibration(city: str = None, limit: int = 60):
    async with AsyncSessionLocal() as session:
        q = select(CityCalibration).order_by(desc(CityCalibration.recorded_at))
        if city:
            q = q.where(CityCalibration.city == city)
        result = await session.execute(q.limit(limit))
        rows = result.scalars().all()
        return [
            {
                "city": r.city,
                "date": r.date,
                "forecast_high_f": r.forecast_high_f,
                "actual_high_f": r.actual_high_f,
                "forecast_error_f": r.forecast_error_f,
                "sigma_used": r.sigma_used,
            }
            for r in rows
        ]


@app.get("/api/stats/by-city")
async def stats_by_city():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Trade).where(Trade.status.in_(["WIN", "LOSS"]))
        )
        trades = result.scalars().all()
        breakdown = {}
        for t in trades:
            c = t.city
            if c not in breakdown:
                breakdown[c] = {"city": c, "trades": 0, "wins": 0, "pnl": 0.0, "edges": []}
            breakdown[c]["trades"] += 1
            breakdown[c]["pnl"] = round(breakdown[c]["pnl"] + (t.net_pnl or 0), 2)
            breakdown[c]["edges"].append(t.edge_pct)
            if t.status == "WIN":
                breakdown[c]["wins"] += 1
        for c in breakdown:
            s = breakdown[c]
            s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] else 0
            s["avg_edge_pct"] = round(sum(s["edges"]) / len(s["edges"]) * 100, 1) if s["edges"] else 0
            del s["edges"]
        return list(breakdown.values())


def _trade_to_dict(t: Trade) -> dict:
    return {
        "id": t.id,
        "city": t.city,
        "station": t.station_id,
        "threshold_f": t.threshold_f,
        "direction": t.direction,
        "market_condition": t.market_condition,
        "market_yes_price": t.market_yes_price,
        "market_volume": t.market_volume,
        "noaa_forecast_high": t.noaa_forecast_high,
        "noaa_sigma": t.noaa_sigma,
        "noaa_true_prob": t.noaa_true_prob,
        "noaa_condition": t.noaa_condition,
        "edge_pct": round(t.edge_pct * 100, 1),
        "confidence": round(t.confidence * 100, 1),
        "kelly_raw": t.kelly_raw,
        "kelly_capped": t.kelly_capped,
        "position_size_usd": t.position_size_usd,
        "entry_price": t.entry_price,
        "shares": t.shares,
        "bankroll_at_entry": t.bankroll_at_entry,
        "status": t.status,
        "actual_high_f": t.actual_high_f,
        "gross_pnl": t.gross_pnl,
        "fees_usd": t.fees_usd,
        "net_pnl": t.net_pnl,
        "bankroll_after": t.bankroll_after,
        "forecast_error_f": t.forecast_error_f,
        "opened_at": t.opened_at.isoformat() if t.opened_at else None,
        "resolved_at": t.resolved_at.isoformat() if t.resolved_at else None,
        "polymarket_market_id": t.polymarket_market_id,
    }


def _scan_log_to_dict(s: ScanLog) -> dict:
    return {
        "id": s.id,
        "scanned_at": s.scanned_at.isoformat() if s.scanned_at else None,
        "cities_scanned": s.cities_scanned,
        "signals_found": s.signals_found,
        "trades_opened": s.trades_opened,
        "trades_settled": s.trades_settled,
        "bankroll_snapshot": s.bankroll_snapshot,
        "errors": s.errors,
        "duration_ms": s.duration_ms,
    }


@app.get("/api/debug/markets")
async def debug_markets():
    """Fetch raw Gamma API events to inspect temperature market structure."""
    results = {}
    async with httpx.AsyncClient() as client:
        # Check events endpoint
        for tag in ["daily-temperature", "weather"]:
            try:
                r = await client.get(
                    "https://gamma-api.polymarket.com/events",
                    params={"active": "true", "closed": "false", "tag_slug": tag, "limit": 2},
                    headers={"User-Agent": "WeatherArbBot/1.0", "Accept": "application/json"},
                    timeout=15.0,
                )
                data = r.json()
                events = data if isinstance(data, list) else data.get("events", [])
                results[f"events_tag_{tag}"] = {
                    "status": r.status_code,
                    "count": len(events),
                    "sample": events[:1]
                }
            except Exception as e:
                results[f"events_tag_{tag}"] = {"error": str(e)}

        # Also check raw markets with daily-temperature tag
        try:
            r = await client.get(
                "https://gamma-api.polymarket.com/markets",
                params={"active": "true", "closed": "false", "tag_slug": "daily-temperature", "limit": 2},
                headers={"User-Agent": "WeatherArbBot/1.0", "Accept": "application/json"},
                timeout=15.0,
            )
            data = r.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            results["markets_tag_daily_temperature"] = {
                "status": r.status_code,
                "count": len(markets),
                "sample": markets[:1]
            }
        except Exception as e:
            results["markets_tag_daily_temperature"] = {"error": str(e)}

    return results
