# backend/api/main.py
import logging
import asyncio
from datetime import datetime, date, timezone, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, func, desc
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import BOT_CONFIG, SPECTRUM_CONFIG, STARTING_BANKROLL, DRY_RUN, CITY_MODEL_TIER, STRATEGY_BANKROLL_ID
from models.database import (
    init_db, AsyncSessionLocal,
    Trade, BankrollState, ScanLog, CityCalibration,
    BucketMappingDiagnostic,
)
from core.signals import get_bankroll
from core.scanner import run_scan
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Weather Arb Bot", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_scan_running = False
_last_scan_result = None


@app.on_event("startup")
async def startup():
    logger.info("Starting Weather Arb Bot (A/B/C mode — Sigma, Edge, Spectrum)...")
    await init_db()
    logger.info(f"DB initialized | DRY_RUN={DRY_RUN} | Starting bankroll=${STARTING_BANKROLL}/strategy")
    await _purge_old_bucket_diagnostics()
    asyncio.create_task(scan_scheduler())


async def scan_scheduler():
    interval = BOT_CONFIG["scan_interval_seconds"]
    logger.info(f"Scanner starting — interval={interval}s")
    await asyncio.sleep(10)
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


# ── Dashboard & Static ────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    import os
    this_file = os.path.abspath(__file__)
    api_dir = os.path.dirname(this_file)
    backend_dir = os.path.dirname(api_dir)
    repo_dir = os.path.dirname(backend_dir)
    for html_path in [
        os.path.join(backend_dir, "weather-arb-dashboard.html"),
        os.path.join(repo_dir, "weather-arb-dashboard.html"),
        os.path.join(api_dir, "weather-arb-dashboard.html"),
    ]:
        if os.path.exists(html_path):
            with open(html_path, "r") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Dashboard not found</h1>", status_code=404)


@app.get("/analysis", response_class=HTMLResponse)
async def serve_analysis():
    import os
    this_file = os.path.abspath(__file__)
    api_dir = os.path.dirname(this_file)
    backend_dir = os.path.dirname(api_dir)
    repo_dir = os.path.dirname(backend_dir)
    for html_path in [
        os.path.join(backend_dir, "weather-analysis.html"),
        os.path.join(repo_dir, "weather-analysis.html"),
        os.path.join(api_dir, "weather-analysis.html"),
    ]:
        if os.path.exists(html_path):
            with open(html_path, "r") as f:
                return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Analysis dashboard not found</h1>", status_code=404)


# ── Admin endpoints ───────────────────────────────────────────────────────────

@app.post("/api/admin/reset-bankroll")
async def reset_bankroll_endpoint(strategy: str = "sigma"):
    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session, strategy)
            old_balance = bankroll_state.balance
            bankroll_state.balance = STARTING_BANKROLL
            bankroll_state.daily_loss_today = 0.0
            bankroll_state.last_reset_date = datetime.now(timezone.utc).date().isoformat()
    return {"status": "reset", "strategy": strategy, "previous_balance": old_balance, "new_balance": STARTING_BANKROLL}


@app.post("/api/admin/reset-daily-loss")
async def reset_daily_loss_endpoint(strategy: str = "sigma"):
    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session, strategy)
            old_val = bankroll_state.daily_loss_today
            bankroll_state.daily_loss_today = 0.0
            bankroll_state.last_reset_date = datetime.now(timezone.utc).date().isoformat()
    return {"status": "reset", "strategy": strategy, "previous_daily_loss": old_val, "now": 0.0}


@app.post("/api/admin/purge-all-open-trades")
async def purge_all_open_trades(strategy: str = None):
    async with AsyncSessionLocal() as session:
        async with session.begin():
            q = select(Trade).where(Trade.status == "OPEN")
            if strategy:
                q = q.where(Trade.strategy == strategy)
            result = await session.execute(q)
            open_trades = result.scalars().all()

            purged = []
            for trade in open_trades:
                trade_strategy = trade.strategy or "sigma"
                bs = await get_bankroll(session, trade_strategy)
                bs.balance = round(bs.balance + trade.position_size_usd, 2)
                purged.append({"city": trade.city, "threshold": trade.threshold_f, "direction": trade.direction, "size": trade.position_size_usd, "strategy": trade_strategy})
                await session.delete(trade)

    return {"status": "done", "purged_count": len(purged), "purged_trades": purged}


@app.post("/api/admin/purge-stale-trades")
async def purge_stale_trades():
    today = datetime.now(timezone.utc).date()
    purged = []
    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(select(Trade).where(Trade.status == "OPEN"))
            open_trades = result.scalars().all()
            for trade in open_trades:
                is_stale = False
                if trade.market_date:
                    try:
                        mkt_date = datetime.strptime(trade.market_date, "%Y-%m-%d").date()
                        if mkt_date < today:
                            is_stale = True
                    except Exception:
                        pass
                if not is_stale and not trade.market_date:
                    if trade.opened_at and trade.opened_at.date() < today:
                        is_stale = True
                if is_stale:
                    trade_strategy = trade.strategy or "sigma"
                    bs = await get_bankroll(session, trade_strategy)
                    bs.balance = round(bs.balance + trade.position_size_usd, 2)
                    purged.append({"id": trade.id, "city": trade.city, "strategy": trade_strategy})
                    await session.delete(trade)
    return {"status": "done", "purged_count": len(purged), "purged_trades": purged}


# ── Core API ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "dry_run": DRY_RUN, "mode": "abc_testing", "strategies": ["sigma", "forecast_edge", "spectrum"], "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/api/scan")
async def manual_scan(background_tasks: BackgroundTasks):
    if _scan_running:
        return {"status": "already_running"}
    background_tasks.add_task(trigger_scan)
    return {"status": "started"}


@app.get("/api/trades")
async def get_trades(status: str = None, city: str = None, strategy: str = None, limit: int = 100):
    async with AsyncSessionLocal() as session:
        q = select(Trade).order_by(desc(Trade.opened_at))
        if status:
            q = q.where(Trade.status == status.upper())
        if city:
            q = q.where(Trade.city == city)
        if strategy:
            q = q.where(Trade.strategy == strategy)
        result = await session.execute(q.limit(limit))
        trades = result.scalars().all()
        return [_trade_to_dict(t) for t in trades]


@app.get("/api/dashboard")
async def dashboard(strategy: str = None):
    """Dashboard data with optional strategy filter. No filter = all trades combined."""
    async with AsyncSessionLocal() as session:
        bankroll_b = await get_bankroll(session, "sigma")
        bankroll_a = await get_bankroll(session, "forecast_edge")
        bankroll_c = await get_bankroll(session, "spectrum")

        # Trades query with optional strategy filter
        q = select(Trade).order_by(desc(Trade.opened_at)).limit(200)
        if strategy:
            q = q.where(Trade.strategy == strategy)
        all_trades_result = await session.execute(q)
        all_trades = all_trades_result.scalars().all()

        open_trades = [t for t in all_trades if t.status == "OPEN"]
        settled = [t for t in all_trades if t.status in ("WIN", "LOSS")]
        wins = [t for t in settled if t.status == "WIN"]
        losses = [t for t in settled if t.status == "LOSS"]

        total_net_pnl = sum(t.net_pnl or 0 for t in settled)
        total_fees = sum(t.fees_usd or 0 for t in settled)
        win_rate = (len(wins) / len(settled) * 100) if settled else 0
        avg_edge = (sum(t.edge_pct for t in settled) / len(settled) * 100) if settled else 0
        avg_win = (sum(t.net_pnl for t in wins) / len(wins)) if wins else 0
        avg_loss = (sum(t.net_pnl for t in losses) / len(losses)) if losses else 0
        ev_per_trade = ((win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)) if settled else 0

        gross_wins = sum(t.gross_pnl for t in wins if t.gross_pnl)
        gross_losses = abs(sum(t.gross_pnl for t in losses if t.gross_pnl))
        profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else 0

        # Equity curve — use appropriate starting bankroll
        if strategy == "sigma":
            start_bal = bankroll_b.starting_balance
        elif strategy == "forecast_edge":
            start_bal = bankroll_a.starting_balance
        elif strategy == "spectrum":
            start_bal = bankroll_c.starting_balance
        else:
            start_bal = bankroll_b.starting_balance + bankroll_a.starting_balance + bankroll_c.starting_balance

        running_bal = start_bal
        peak = start_bal
        max_dd = 0
        equity_curve = [start_bal]
        for t in sorted(settled, key=lambda x: x.resolved_at or datetime.min):
            running_bal += (t.net_pnl or 0)
            equity_curve.append(round(running_bal, 2))
            if running_bal > peak:
                peak = running_bal
            dd = (peak - running_bal) / peak * 100
            if dd > max_dd:
                max_dd = dd

        import numpy as np
        pnls = [t.net_pnl for t in settled if t.net_pnl is not None]
        sharpe = (np.mean(pnls) / np.std(pnls)) if len(pnls) > 1 else 0

        cal_result = await session.execute(
            select(CityCalibration).where(CityCalibration.actual_high_f.isnot(None)).order_by(desc(CityCalibration.recorded_at)).limit(100)
        )
        cal_rows = cal_result.scalars().all()
        cal_errors = [abs(r.forecast_error_f) for r in cal_rows if r.forecast_error_f is not None]
        mean_abs_error = round(float(np.mean(cal_errors)), 2) if cal_errors else None

        city_stats = {}
        for t in settled:
            c = t.city
            if c not in city_stats:
                city_stats[c] = {"trades": 0, "wins": 0, "pnl": 0.0, "edge_sum": 0.0, "model_tier": CITY_MODEL_TIER.get(c, "unknown")}
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

        scan_log_result = await session.execute(select(ScanLog).order_by(desc(ScanLog.scanned_at)).limit(20))
        scan_logs = scan_log_result.scalars().all()

        # ── Strategy comparison bar (always returned) ─────────────────────────
        comparison = await _build_strategy_comparison(session)

        return {
            "bankroll": {
                "sigma": {"current": round(bankroll_b.balance, 2), "starting": bankroll_b.starting_balance, "pnl": round(bankroll_b.balance - bankroll_b.starting_balance, 2)},
                "forecast_edge": {"current": round(bankroll_a.balance, 2), "starting": bankroll_a.starting_balance, "pnl": round(bankroll_a.balance - bankroll_a.starting_balance, 2)},
                "spectrum": {"current": round(bankroll_c.balance, 2), "starting": bankroll_c.starting_balance, "pnl": round(bankroll_c.balance - bankroll_c.starting_balance, 2)},
                "combined": {"current": round(bankroll_b.balance + bankroll_a.balance + bankroll_c.balance, 2), "starting": bankroll_b.starting_balance + bankroll_a.starting_balance + bankroll_c.starting_balance},
                "daily_loss_sigma": round(bankroll_b.daily_loss_today, 2),
                "daily_loss_forecast_edge": round(bankroll_a.daily_loss_today, 2),
                "daily_loss_spectrum": round(bankroll_c.daily_loss_today, 2),
            },
            "performance": {
                "total_trades": len(settled), "open_positions": len(open_trades),
                "win_rate": round(win_rate, 1), "avg_edge_pct": round(avg_edge, 1),
                "total_net_pnl": round(total_net_pnl, 2), "total_fees": round(total_fees, 2),
                "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
                "ev_per_trade": round(ev_per_trade, 2), "profit_factor": round(profit_factor, 2),
                "max_drawdown_pct": round(max_dd, 2), "sharpe": round(float(sharpe), 3),
                "mean_abs_forecast_error_f": mean_abs_error,
            },
            "strategy_comparison": comparison,
            "equity_curve": equity_curve,
            "open_positions": [_trade_to_dict(t) for t in open_trades],
            "trade_history": [_trade_to_dict(t) for t in settled[:50]],
            "city_stats": city_stats,
            "scan_logs": [_scan_log_to_dict(s) for s in scan_logs],
            "config": BOT_CONFIG,
            "dry_run": DRY_RUN,
            "scan_running": _scan_running,
            "last_scan": _last_scan_result,
            "filter": strategy or "all",
        }


async def _build_strategy_comparison(session):
    """Build side-by-side strategy comparison data."""
    result = {}
    for strat in ["sigma", "forecast_edge", "spectrum"]:
        q = select(Trade).where(Trade.status.in_(["WIN", "LOSS"]), Trade.strategy == strat)
        trades_result = await session.execute(q)
        trades = trades_result.scalars().all()
        wins = [t for t in trades if t.status == "WIN"]
        losses = [t for t in trades if t.status == "LOSS"]
        total_pnl = sum(t.net_pnl or 0 for t in trades)
        win_rate = (len(wins) / len(trades) * 100) if trades else 0
        pnl_per_trade = (total_pnl / len(trades)) if trades else 0

        # Open count
        open_q = await session.execute(select(Trade).where(Trade.status == "OPEN", Trade.strategy == strat))
        open_count = len(open_q.scalars().all())

        result[strat] = {
            "settled_trades": len(trades), "wins": len(wins), "losses": len(losses),
            "open_positions": open_count,
            "win_rate": round(win_rate, 1),
            "total_pnl": round(total_pnl, 2),
            "pnl_per_trade": round(pnl_per_trade, 2),
        }
    return result


@app.get("/api/calibration")
async def get_calibration(city: str = None, limit: int = 60):
    async with AsyncSessionLocal() as session:
        q = select(CityCalibration).order_by(desc(CityCalibration.recorded_at))
        if city:
            q = q.where(CityCalibration.city == city)
        result = await session.execute(q.limit(limit))
        rows = result.scalars().all()
        return [{"city": r.city, "date": r.date, "forecast_high": r.forecast_high, "actual_high_f": r.actual_high_f, "forecast_error_f": r.forecast_error_f, "sigma_used": r.sigma_used} for r in rows]


@app.get("/api/stats/by-city")
async def stats_by_city(strategy: str = None):
    async with AsyncSessionLocal() as session:
        q = select(Trade).where(Trade.status.in_(["WIN", "LOSS"]))
        if strategy:
            q = q.where(Trade.strategy == strategy)
        result = await session.execute(q)
        trades = result.scalars().all()
        breakdown = {}
        for t in trades:
            c = t.city
            if c not in breakdown:
                breakdown[c] = {"city": c, "model_tier": CITY_MODEL_TIER.get(c, "unknown"), "trades": 0, "wins": 0, "pnl": 0.0, "edges": []}
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


@app.get("/api/stats/by-model")
async def stats_by_model(strategy: str = None):
    async with AsyncSessionLocal() as session:
        q = select(Trade).where(Trade.status.in_(["WIN", "LOSS"]))
        if strategy:
            q = q.where(Trade.strategy == strategy)
        result = await session.execute(q)
        trades = result.scalars().all()
        breakdown = {}
        for t in trades:
            tier = CITY_MODEL_TIER.get(t.city, "unknown")
            if tier not in breakdown:
                breakdown[tier] = {"model_tier": tier, "cities": set(), "trades": 0, "wins": 0, "pnl": 0.0, "edges": [], "forecast_errors": []}
            breakdown[tier]["cities"].add(t.city)
            breakdown[tier]["trades"] += 1
            breakdown[tier]["pnl"] = round(breakdown[tier]["pnl"] + (t.net_pnl or 0), 2)
            breakdown[tier]["edges"].append(t.edge_pct)
            if t.forecast_error_f is not None:
                breakdown[tier]["forecast_errors"].append(abs(t.forecast_error_f))
            if t.status == "WIN":
                breakdown[tier]["wins"] += 1
        for tier in breakdown:
            s = breakdown[tier]
            s["cities"] = sorted(s["cities"])
            s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] else 0
            s["avg_edge_pct"] = round(sum(s["edges"]) / len(s["edges"]) * 100, 1) if s["edges"] else 0
            s["mean_abs_forecast_error"] = round(sum(s["forecast_errors"]) / len(s["forecast_errors"]), 2) if s["forecast_errors"] else None
            del s["edges"]
            del s["forecast_errors"]
        return list(breakdown.values())


@app.get("/api/stats/by-strategy")
async def stats_by_strategy():
    """Direct strategy comparison endpoint."""
    async with AsyncSessionLocal() as session:
        return await _build_strategy_comparison(session)


# ── Trade dict helper ─────────────────────────────────────────────────────────

def _trade_to_dict(t: Trade) -> dict:
    return {
        "id": t.id, "city": t.city, "station": t.station_id,
        "model_tier": CITY_MODEL_TIER.get(t.city, "unknown"),
        "threshold_f": t.threshold_f, "direction": t.direction,
        "market_condition": t.market_condition, "market_date": t.market_date,
        "market_yes_price": t.market_yes_price, "market_volume": t.market_volume,
        "noaa_forecast_high": t.noaa_forecast_high,
        "gfs_forecast": t.gfs_forecast,
        "icon_forecast": t.ecmwf_forecast,
        "models_agreed": t.models_agreed,
        "noaa_sigma": t.noaa_sigma, "noaa_true_prob": t.noaa_true_prob,
        "noaa_condition": t.noaa_condition,
        "edge_pct": round(t.edge_pct * 100, 1),
        "confidence": round(t.confidence * 100, 1),
        "kelly_raw": t.kelly_raw, "kelly_capped": t.kelly_capped,
        "position_size_usd": t.position_size_usd, "entry_price": t.entry_price,
        "shares": t.shares, "bankroll_at_entry": t.bankroll_at_entry,
        "status": t.status, "actual_high_f": t.actual_high_f,
        "gross_pnl": t.gross_pnl, "fees_usd": t.fees_usd,
        "net_pnl": t.net_pnl, "bankroll_after": t.bankroll_after,
        "forecast_error_f": t.forecast_error_f,
        "forecast_day_offset": t.forecast_day_offset,
        "entry_number": t.entry_number,
        "opened_at": t.opened_at.isoformat() if t.opened_at else None,
        "resolved_at": t.resolved_at.isoformat() if t.resolved_at else None,
        "polymarket_market_id": t.polymarket_market_id,
        # A/B testing fields
        "strategy": t.strategy or "sigma",
        "forecast_gap": t.forecast_gap,
        "validator_gap": t.validator_gap,
        "same_side_as_forecast": t.same_side_as_forecast,
        "models_directionally_agree": t.models_directionally_agree,
        "models_on_bet_side_count": t.models_on_bet_side_count,
        "model_count": t.model_count,
        # Strategy C (Spectrum) bucket fields
        "bucket_low": t.bucket_low,
        "bucket_high": t.bucket_high,
        "bucket_label": t.bucket_label,
        "bucket_forecast_prob": t.bucket_forecast_prob,
        "bucket_market_price": t.bucket_market_price,
        "bucket_center": t.bucket_center,
    }


def _scan_log_to_dict(s: ScanLog) -> dict:
    return {
        "id": s.id,
        "scanned_at": s.scanned_at.isoformat() if s.scanned_at else None,
        "cities_scanned": s.cities_scanned, "signals_found": s.signals_found,
        "trades_opened": s.trades_opened, "trades_settled": s.trades_settled,
        "bankroll_snapshot": s.bankroll_snapshot, "errors": s.errors, "duration_ms": s.duration_ms,
    }


# ── Bucket mapping diagnostics (unchanged) ───────────────────────────────────

async def _purge_old_bucket_diagnostics():
    from sqlalchemy import delete
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=7)
    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(delete(BucketMappingDiagnostic).where(BucketMappingDiagnostic.scanned_at < cutoff))
                if result.rowcount:
                    logger.info(f"[BUCKET] Purged {result.rowcount} diagnostic rows older than 7 days")
    except Exception as e:
        logger.warning(f"[BUCKET] Purge failed (non-fatal): {e}")


@app.get("/api/debug/markets")
async def debug_markets():
    from data.polymarket import build_slug, fetch_event_by_slug, CITY_SLUGS, MAX_FORWARD_DAYS
    utc_today = datetime.now(timezone.utc).date()
    results = {}
    async with httpx.AsyncClient() as client:
        for city in CITY_SLUGS:
            event = None; slug = None; found_date = None
            rejection = f"No valid event in next {MAX_FORWARD_DAYS} days"
            for day_offset in range(MAX_FORWARD_DAYS):
                target_date = utc_today + timedelta(days=day_offset)
                slug = build_slug(city, target_date)
                event, rejection = await fetch_event_by_slug(city, target_date, client)
                if event:
                    found_date = target_date.isoformat()
                    break
            if event:
                markets = event.get("markets", [])
                results[city] = {
                    "status": "found", "market_date": found_date, "slug": slug,
                    "title": event.get("title", ""), "bucket_count": len(markets),
                    "event_volume": float(event.get("volumeNum") or event.get("volume") or 0),
                }
            else:
                results[city] = {"status": "not_found", "slug": slug, "rejection": rejection}
    found = sum(1 for v in results.values() if v["status"] == "found")
    return {"utc_now": datetime.now(timezone.utc).isoformat(), "cities_found": found, "results": results}
