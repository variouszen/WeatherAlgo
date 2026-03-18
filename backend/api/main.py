# backend/api/main.py — WeatherAlgo v2 Phase 4
import logging
import asyncio
from datetime import datetime, date, timezone, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, func, desc, delete
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DRY_RUN, CITY_MODEL_TIER, STRATEGY_BANKROLL_ID, SCAN_INTERVAL_SECONDS
from config import SPECTRUM_V2_CONFIG, SNIPER_YES_CONFIG, SNIPER_NO_CONFIG, LADDER_3_CONFIG, LADDER_5_CONFIG
from models.database import (
    init_db, AsyncSessionLocal,
    Trade, BankrollState, ScanLog, CityCalibration,
    BucketMappingDiagnostic,
)
from core.signals import get_bankroll
from scanner_v2 import run_scan_v2
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Weather Arb Bot", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_scan_running = False
_last_scan_result = None

# ── Constants ────────────────────────────────────────────────────────────────

V2_STRATEGIES = ["spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"]

V2_CONFIGS = {
    "spectrum": SPECTRUM_V2_CONFIG,
    "sniper_yes": SNIPER_YES_CONFIG,
    "sniper_no": SNIPER_NO_CONFIG,
    "ladder_3": LADDER_3_CONFIG,
    "ladder_5": LADDER_5_CONFIG,
}

V2_KILL_CRITERIA = {
    "spectrum":   {"min_pnl_per_trade": -0.10, "min_win_rate": 30, "max_drawdown_pct": 30},
    "sniper_yes": {"min_pnl_per_trade": -0.10, "min_win_rate": 30, "max_drawdown_pct": 30},
    "sniper_no":  {"min_pnl_per_trade": -0.10, "min_win_rate": 30, "max_drawdown_pct": 30},
    "ladder_3":   {"min_pnl_per_trade": -0.10, "min_win_rate": 40, "max_drawdown_pct": 30},
    "ladder_5":   {"min_pnl_per_trade": -0.10, "min_win_rate": 40, "max_drawdown_pct": 30},
}


# ── Startup & Scheduler ─────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info("Starting Weather Arb Bot v2 (5-strategy paper trading)...")
    await init_db()
    logger.info(f"DB initialized | DRY_RUN={DRY_RUN} | $500/strategy × 5 strategies")
    await _purge_old_bucket_diagnostics()
    asyncio.create_task(scan_scheduler())


async def scan_scheduler():
    interval = SCAN_INTERVAL_SECONDS
    logger.info(f"Scanner starting (v2) — interval={interval}s")
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
        _last_scan_result = await run_scan_v2()
    finally:
        _scan_running = False


# ══════════════════════════════════════════════════════════════════════════════
# HTML ROUTES — v2 is the primary product
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_html(filename: str) -> str | None:
    """Find an HTML file in known locations."""
    this_file = os.path.abspath(__file__)
    api_dir = os.path.dirname(this_file)
    backend_dir = os.path.dirname(api_dir)
    repo_dir = os.path.dirname(backend_dir)
    for d in [os.path.join(repo_dir, "templates"), backend_dir, repo_dir, api_dir]:
        path = os.path.join(d, filename)
        if os.path.exists(path):
            with open(path, "r") as f:
                return f.read()
    return None


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Main v2 dashboard — the primary product."""
    content = _resolve_html("dashboard_v2.html")
    if content:
        return HTMLResponse(content=content)
    return HTMLResponse(content="<h1>Dashboard not found</h1>", status_code=404)


@app.get("/analysis", response_class=HTMLResponse)
async def serve_analysis():
    """Main v2 analysis page."""
    content = _resolve_html("analysis_v2.html")
    if content:
        return HTMLResponse(content=content)
    return HTMLResponse(content="<h1>Analysis not found</h1>", status_code=404)


# Aliases for /v2 and /v2/analysis
@app.get("/v2", response_class=HTMLResponse)
async def v2_alias():
    return RedirectResponse(url="/")


@app.get("/v2/analysis", response_class=HTMLResponse)
async def v2_analysis_alias():
    return RedirectResponse(url="/analysis")


# ══════════════════════════════════════════════════════════════════════════════
# V2 API — Clean v2-only contracts. New pages consume ONLY these.
# ══════════════════════════════════════════════════════════════════════════════

# ── V2 Trade serializer ──────────────────────────────────────────────────────

def _v2_trade_to_dict(t: Trade) -> dict:
    """Full v2 trade serialization with ensemble, fill sim, and ladder fields."""
    return {
        "id": t.id, "city": t.city, "station": t.station_id,
        "strategy": t.strategy or "unknown",
        "direction": t.direction,
        "market_date": t.market_date,
        "bucket_label": t.bucket_label,
        "bucket_low": t.bucket_low,
        "bucket_high": t.bucket_high,
        # Pricing
        "entry_price": t.entry_price,
        "market_ask": t.market_ask,
        "market_midpoint": t.market_midpoint,
        "spread_at_entry": t.spread_at_entry,
        "book_depth_at_entry": t.book_depth_at_entry,
        "price_source": t.price_source,
        # Fill simulation
        "simulated_vwap": t.simulated_vwap,
        "simulated_shares": t.simulated_shares,
        "simulated_cost": t.simulated_cost,
        "fill_quality": t.fill_quality,
        # Ensemble signal
        "ensemble_prob": t.ensemble_prob,
        "ensemble_members_in_bucket": t.ensemble_members_in_bucket,
        "ensemble_total_members": t.ensemble_total_members,
        "gfs_peak_bucket_index": t.gfs_peak_bucket_index,
        "ecmwf_peak_bucket_index": t.ecmwf_peak_bucket_index,
        "model_agreement": t.model_agreement,
        "model_run_time": t.model_run_time,
        # Edge
        "edge_pct": round(t.edge_pct * 100, 1) if t.edge_pct else 0,
        "edge_ratio": t.edge_ratio,
        # Position
        "position_size_usd": t.position_size_usd,
        "shares": t.shares,
        "bankroll_at_entry": t.bankroll_at_entry,
        # Ladder
        "ladder_id": t.ladder_id,
        "package_cost": t.package_cost,
        "package_prob": t.package_prob,
        "package_edge": t.package_edge,
        "num_legs": t.num_legs,
        # Resolution
        "status": t.status,
        "actual_high_f": t.actual_high_f,
        "gross_pnl": t.gross_pnl,
        "fees_usd": t.fees_usd,
        "net_pnl": t.net_pnl,
        "bankroll_after": t.bankroll_after,
        # NO-side fields for normalized scatter
        "no_ask_price": getattr(t, 'market_ask', None),  # market_ask stores the traded-side ask
        # Timestamps
        "opened_at": t.opened_at.isoformat() if t.opened_at else None,
        "resolved_at": t.resolved_at.isoformat() if t.resolved_at else None,
        "venue": t.venue,
    }


# ── /api/v2/trades ───────────────────────────────────────────────────────────

@app.get("/api/v2/trades")
async def get_v2_trades(strategy: str = None, status: str = None, city: str = None, limit: int = 200):
    """All v2 trades with full ensemble/fill/ladder fields."""
    async with AsyncSessionLocal() as session:
        q = select(Trade).where(Trade.strategy.in_(V2_STRATEGIES)).order_by(desc(Trade.opened_at))
        if strategy:
            q = q.where(Trade.strategy == strategy)
        if status:
            q = q.where(Trade.status == status.upper())
        if city:
            q = q.where(Trade.city == city)
        result = await session.execute(q.limit(limit))
        trades = result.scalars().all()
        return [_v2_trade_to_dict(t) for t in trades]


# ── /api/v2/stats ────────────────────────────────────────────────────────────

def _compute_max_drawdown(trades, starting_balance):
    """Compute max drawdown % from a sorted list of settled trades."""
    running = starting_balance
    peak = running
    max_dd_pct = 0.0
    for t in sorted(trades, key=lambda x: x.resolved_at or datetime.min):
        running += (t.net_pnl or 0)
        if running > peak:
            peak = running
        dd = ((peak - running) / peak * 100) if peak > 0 else 0
        if dd > max_dd_pct:
            max_dd_pct = dd
    return max_dd_pct


def _build_ladder_package_stats(trades):
    """
    Group ladder trades by (strategy, ladder_id) into packages.
    Returns package-level metrics: count, wins, pnl, avg cost, avg edge.
    """
    packages = {}
    for t in trades:
        if t.ladder_id is None:
            continue
        key = (t.strategy, t.ladder_id)
        if key not in packages:
            packages[key] = {
                "legs": [],
                "package_cost": t.package_cost,
                "package_prob": t.package_prob,
                "package_edge": t.package_edge,
                "num_legs_expected": t.num_legs,
                "total_pnl": 0.0,
                "any_win": False,
                "all_settled": True,
            }
        packages[key]["legs"].append(t)
        packages[key]["total_pnl"] = round(packages[key]["total_pnl"] + (t.net_pnl or 0), 2)
        if t.status == "WIN":
            packages[key]["any_win"] = True
        if t.status not in ("WIN", "LOSS"):
            packages[key]["all_settled"] = False

    # Aggregate
    settled_pkgs = [p for p in packages.values() if p["all_settled"]]
    pkg_wins = [p for p in settled_pkgs if p["any_win"]]
    total_pkg_pnl = sum(p["total_pnl"] for p in settled_pkgs)
    pkg_count = len(settled_pkgs)
    pkg_win_rate = (len(pkg_wins) / pkg_count * 100) if pkg_count else 0
    pnl_per_pkg = (total_pkg_pnl / pkg_count) if pkg_count else 0

    return {
        "settled_packages": pkg_count,
        "open_packages": len(packages) - pkg_count,
        "package_wins": len(pkg_wins),
        "package_win_rate": round(pkg_win_rate, 1),
        "total_pnl": round(total_pkg_pnl, 2),
        "pnl_per_package": round(pnl_per_pkg, 2),
        "raw_packages": packages,
    }


@app.get("/api/v2/stats")
async def get_v2_stats():
    """Per-strategy stats with kill criteria flags. Ladder uses package-level metrics."""
    async with AsyncSessionLocal() as session:
        result = {}
        for strat in V2_STRATEGIES:
            bs = await get_bankroll(session, strat)
            cfg = V2_CONFIGS[strat]
            is_ladder = strat in ("ladder_3", "ladder_5")

            # All trades for this strategy
            all_q = await session.execute(
                select(Trade).where(Trade.strategy == strat)
            )
            all_trades = all_q.scalars().all()
            settled = [t for t in all_trades if t.status in ("WIN", "LOSS")]
            open_trades = [t for t in all_trades if t.status == "OPEN"]
            wins = [t for t in settled if t.status == "WIN"]
            losses = [t for t in settled if t.status == "LOSS"]

            total_pnl = sum(t.net_pnl or 0 for t in settled)
            max_dd_pct = _compute_max_drawdown(settled, bs.starting_balance)
            roc = ((bs.balance - bs.starting_balance) / bs.starting_balance * 100) if bs.starting_balance > 0 else 0

            # Fill quality breakdown (all trades including open)
            fill_counts = {"full": 0, "shallow": 0, "stale": 0, "unknown": 0}
            for t in all_trades:
                fq = (t.fill_quality or "unknown").lower()
                fill_counts[fq] = fill_counts.get(fq, 0) + 1

            # ── Build performance dict ────────────────────────────────────
            if is_ladder:
                # Package-level metrics for ladder strategies
                pkg_stats = _build_ladder_package_stats(settled + open_trades)
                pkg_settled = pkg_stats["settled_packages"]
                pnl_per_unit = pkg_stats["pnl_per_package"]
                win_rate = pkg_stats["package_win_rate"]
                unit_label = "package"

                performance = {
                    "settled_packages": pkg_settled,
                    "open_packages": pkg_stats["open_packages"],
                    "package_wins": pkg_stats["package_wins"],
                    "package_win_rate": round(win_rate, 1),
                    "total_pnl": round(total_pnl, 2),
                    "pnl_per_package": round(pnl_per_unit, 2),
                    "settled_legs": len(settled),
                    "open_legs": len(open_trades),
                    "max_drawdown_pct": round(max_dd_pct, 1),
                    "return_on_capital": round(roc, 1),
                }
            else:
                # Trade-level metrics for non-ladder strategies
                win_rate = (len(wins) / len(settled) * 100) if settled else 0
                pnl_per_unit = (total_pnl / len(settled)) if settled else 0
                avg_win = (sum(t.net_pnl or 0 for t in wins) / len(wins)) if wins else 0
                avg_loss = (sum(t.net_pnl or 0 for t in losses) / len(losses)) if losses else 0
                unit_label = "trade"

                performance = {
                    "settled_trades": len(settled),
                    "open_positions": len(open_trades),
                    "wins": len(wins),
                    "losses": len(losses),
                    "win_rate": round(win_rate, 1),
                    "total_pnl": round(total_pnl, 2),
                    "pnl_per_trade": round(pnl_per_unit, 2),
                    "avg_win": round(avg_win, 2),
                    "avg_loss": round(avg_loss, 2),
                    "max_drawdown_pct": round(max_dd_pct, 1),
                    "return_on_capital": round(roc, 1),
                }

            # ── Kill criteria check ───────────────────────────────────────
            kills = V2_KILL_CRITERIA[strat]
            kill_flags = []
            min_sample = 10 if not is_ladder else 5
            sample_count = pkg_stats["settled_packages"] if is_ladder else len(settled)

            if sample_count >= min_sample:
                if pnl_per_unit < kills["min_pnl_per_trade"]:
                    kill_flags.append(f"PnL/{unit_label} ${pnl_per_unit:.2f} < ${kills['min_pnl_per_trade']}")
                if win_rate < kills["min_win_rate"]:
                    kill_flags.append(f"Win rate {win_rate:.1f}% < {kills['min_win_rate']}%")
                if max_dd_pct > kills["max_drawdown_pct"]:
                    kill_flags.append(f"Drawdown {max_dd_pct:.1f}% > {kills['max_drawdown_pct']}%")
                total_fill = sum(fill_counts.values())
                if total_fill > 0 and (fill_counts.get("stale", 0) / total_fill) > 0.5:
                    kill_flags.append(f"Stale fills > 50%")

            result[strat] = {
                "bankroll": {
                    "current": round(bs.balance, 2),
                    "starting": bs.starting_balance,
                    "pnl": round(bs.balance - bs.starting_balance, 2),
                    "daily_loss": round(bs.daily_loss_today, 2),
                    "daily_loss_cap": cfg.get("max_daily_loss", 50),
                },
                "performance": performance,
                "fill_quality": fill_counts,
                "kill_flags": kill_flags,
                "kill_triggered": len(kill_flags) > 0,
                "is_ladder": is_ladder,
            }
        return result


# ── /api/v2/comparison ───────────────────────────────────────────────────────

@app.get("/api/v2/comparison")
async def get_v2_comparison():
    """Cross-strategy comparison with normalized scatter data and ladder packages."""
    async with AsyncSessionLocal() as session:
        # All settled v2 trades
        q = select(Trade).where(
            Trade.status.in_(["WIN", "LOSS"]),
            Trade.strategy.in_(V2_STRATEGIES),
        )
        result = await session.execute(q)
        trades = result.scalars().all()

        # ── Normalized scatter: traded-side prob vs traded-side ask ────
        # YES trades: x = ensemble_prob, y = market_ask (which IS the yes_ask)
        # NO trades:  x = 1 - ensemble_prob, y = market_ask (which IS the no_ask for NO trades)
        scatter = []
        for t in trades:
            if t.ensemble_prob is not None and t.market_ask is not None:
                if t.direction == "YES":
                    traded_prob = t.ensemble_prob
                    traded_ask = t.market_ask
                else:
                    traded_prob = 1.0 - t.ensemble_prob
                    traded_ask = t.market_ask
                scatter.append({
                    "traded_prob": round(traded_prob, 4),
                    "traded_ask": round(traded_ask, 4),
                    "status": t.status,
                    "strategy": t.strategy,
                    "direction": t.direction,
                    "city": t.city,
                    "bucket_label": t.bucket_label,
                    "edge_pct": round(t.edge_pct * 100, 1) if t.edge_pct else 0,
                    "net_pnl": t.net_pnl,
                })

        # ── By-city breakdown ─────────────────────────────────────────
        city_stats = {}
        for t in trades:
            c = t.city
            if c not in city_stats:
                city_stats[c] = {"city": c, "trades": 0, "wins": 0, "pnl": 0.0}
            city_stats[c]["trades"] += 1
            city_stats[c]["pnl"] = round(city_stats[c]["pnl"] + (t.net_pnl or 0), 2)
            if t.status == "WIN":
                city_stats[c]["wins"] += 1
        for c in city_stats:
            s = city_stats[c]
            s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] else 0

        # ── Ladder package grouping ───────────────────────────────────
        # Include open ladder trades too for display
        all_ladder_q = await session.execute(
            select(Trade).where(
                Trade.strategy.in_(["ladder_3", "ladder_5"]),
                Trade.ladder_id.isnot(None),
            )
        )
        all_ladder = all_ladder_q.scalars().all()

        packages = {}
        for t in all_ladder:
            key = (t.strategy, t.ladder_id)
            if key not in packages:
                packages[key] = {
                    "strategy": t.strategy, "ladder_id": t.ladder_id,
                    "city": t.city, "market_date": t.market_date,
                    "package_cost": t.package_cost, "package_prob": t.package_prob,
                    "package_edge": t.package_edge, "num_legs": t.num_legs,
                    "legs": [], "total_pnl": 0.0, "all_settled": True,
                    "won": False,
                }
            packages[key]["legs"].append({
                "bucket_label": t.bucket_label, "direction": t.direction,
                "entry_price": t.entry_price, "status": t.status,
                "net_pnl": t.net_pnl,
            })
            packages[key]["total_pnl"] = round(packages[key]["total_pnl"] + (t.net_pnl or 0), 2)
            if t.status not in ("WIN", "LOSS"):
                packages[key]["all_settled"] = False
            if t.status == "WIN":
                packages[key]["won"] = True

        return {
            "scatter": scatter,
            "city_stats": list(city_stats.values()),
            "ladder_packages": list(packages.values()),
        }


# ── /api/v2/dashboard ────────────────────────────────────────────────────────

@app.get("/api/v2/dashboard")
async def get_v2_dashboard():
    """V2-only dashboard summary: equity curve + scan logs. No legacy mixing."""
    async with AsyncSessionLocal() as session:
        # Equity curve from all settled v2 trades
        q = select(Trade).where(
            Trade.status.in_(["WIN", "LOSS"]),
            Trade.strategy.in_(V2_STRATEGIES),
        ).order_by(Trade.resolved_at)
        result = await session.execute(q)
        settled = result.scalars().all()

        starting = 500.0 * len(V2_STRATEGIES)  # $2500 total
        running = starting
        peak = starting
        max_dd = 0.0
        equity_curve = [starting]
        for t in settled:
            running += (t.net_pnl or 0)
            equity_curve.append(round(running, 2))
            if running > peak:
                peak = running
            dd = ((peak - running) / peak * 100) if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        # Scan logs (most recent)
        scan_q = await session.execute(
            select(ScanLog).order_by(desc(ScanLog.scanned_at)).limit(20)
        )
        scan_logs = scan_q.scalars().all()

        return {
            "equity_curve": equity_curve,
            "max_drawdown_pct": round(max_dd, 1),
            "total_settled": len(settled),
            "scan_logs": [
                {
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
                for s in scan_logs
            ],
            "scan_running": _scan_running,
            "last_scan": _last_scan_result,
        }


# ══════════════════════════════════════════════════════════════════════════════
# ADMIN ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    return {
        "status": "ok", "dry_run": DRY_RUN, "mode": "v2_paper",
        "strategies": V2_STRATEGIES,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/scan")
async def manual_scan(background_tasks: BackgroundTasks):
    if _scan_running:
        return {"status": "already_running"}
    background_tasks.add_task(trigger_scan)
    return {"status": "started"}


@app.post("/api/admin/reset-bankroll")
async def reset_bankroll_endpoint(strategy: str = "spectrum"):
    """Reset a single strategy's bankroll to starting balance."""
    cfg = V2_CONFIGS.get(strategy)
    if not cfg:
        raise HTTPException(status_code=400, detail=f"Unknown strategy: {strategy}")
    starting = cfg.get("starting_bankroll", 500.0)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session, strategy)
            old_balance = bankroll_state.balance
            bankroll_state.balance = starting
            bankroll_state.daily_loss_today = 0.0
            bankroll_state.last_reset_date = datetime.now(timezone.utc).date().isoformat()
    return {"status": "reset", "strategy": strategy, "previous_balance": old_balance, "new_balance": starting}


@app.post("/api/admin/reset-daily-loss")
async def reset_daily_loss_endpoint(strategy: str = "spectrum"):
    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session, strategy)
            old_val = bankroll_state.daily_loss_today
            bankroll_state.daily_loss_today = 0.0
            bankroll_state.last_reset_date = datetime.now(timezone.utc).date().isoformat()
    return {"status": "reset", "strategy": strategy, "previous_daily_loss": old_val, "now": 0.0}


@app.post("/api/admin/full-reset")
async def full_reset_endpoint(confirm: str = ""):
    """
    Full system wipe for paper-trading resets.
    Deletes ALL trades (v1 + v2), resets bankrolls, clears scan logs,
    clears runtime caches. Refuses to run if a scan is in progress.
    Requires confirm=YES to execute.
    """
    global _last_scan_result

    if confirm != "YES":
        return {
            "status": "dry_run",
            "message": "Add ?confirm=YES to actually execute. This will DELETE all trades and reset all bankrolls.",
        }

    # Refuse if scan is actively running — avoid DB race conditions
    if _scan_running:
        return {
            "status": "refused",
            "message": "A scan is currently running. Wait for it to finish before resetting.",
        }

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # 1. Delete ALL trades (v1 legacy + v2 — we no longer preserve v1 rows)
            del_trades = await session.execute(delete(Trade))
            trades_deleted = del_trades.rowcount

            # 2. Delete scan logs
            del_scans = await session.execute(delete(ScanLog))
            scans_deleted = del_scans.rowcount

            # 3. Reset all v2 bankrolls
            for strat in V2_STRATEGIES:
                bs = await get_bankroll(session, strat)
                starting = V2_CONFIGS[strat].get("starting_bankroll", 500.0)
                bs.balance = starting
                bs.daily_loss_today = 0.0
                bs.last_reset_date = datetime.now(timezone.utc).date().isoformat()

    # 4. Clear runtime state
    _last_scan_result = None
    try:
        from scanner_v2 import _clear_ensemble_cache
        _clear_ensemble_cache()
    except ImportError:
        pass

    logger.info(f"[ADMIN] Full reset: {trades_deleted} trades deleted, {scans_deleted} scan logs deleted, bankrolls reset, caches cleared")
    return {
        "status": "reset_complete",
        "trades_deleted": trades_deleted,
        "scan_logs_deleted": scans_deleted,
        "bankrolls_reset": V2_STRATEGIES,
        "caches_cleared": True,
    }


@app.post("/api/admin/purge-all-open-trades")
async def purge_all_open_trades(strategy: str = None):
    """Purge open trades and refund bankrolls."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            q = select(Trade).where(Trade.status == "OPEN")
            if strategy:
                q = q.where(Trade.strategy == strategy)
            result = await session.execute(q)
            open_trades = result.scalars().all()

            purged = []
            for trade in open_trades:
                trade_strategy = trade.strategy or "unknown"
                bs = await get_bankroll(session, trade_strategy)
                bs.balance = round(bs.balance + trade.position_size_usd, 2)
                purged.append({"id": trade.id, "city": trade.city, "strategy": trade_strategy})
                await session.delete(trade)

    return {"status": "done", "purged_count": len(purged), "purged_trades": purged}


@app.post("/api/admin/purge-stale-trades")
async def purge_stale_trades():
    """Purge trades whose market date has passed without settlement."""
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
                    trade_strategy = trade.strategy or "unknown"
                    bs = await get_bankroll(session, trade_strategy)
                    bs.balance = round(bs.balance + trade.position_size_usd, 2)
                    purged.append({"id": trade.id, "city": trade.city, "strategy": trade_strategy})
                    await session.delete(trade)
    return {"status": "done", "purged_count": len(purged), "purged_trades": purged}


# ══════════════════════════════════════════════════════════════════════════════
# LEGACY API ENDPOINTS — kept for backward compat, not used by new pages
# ══════════════════════════════════════════════════════════════════════════════

def _trade_to_dict(t: Trade) -> dict:
    """Legacy trade serializer (v1 field names)."""
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
        "edge_pct": round(t.edge_pct * 100, 1) if t.edge_pct else 0,
        "position_size_usd": t.position_size_usd, "entry_price": t.entry_price,
        "shares": t.shares, "bankroll_at_entry": t.bankroll_at_entry,
        "status": t.status, "actual_high_f": t.actual_high_f,
        "gross_pnl": t.gross_pnl, "fees_usd": t.fees_usd,
        "net_pnl": t.net_pnl, "bankroll_after": t.bankroll_after,
        "strategy": t.strategy or "unknown",
        "bucket_label": t.bucket_label,
        "opened_at": t.opened_at.isoformat() if t.opened_at else None,
        "resolved_at": t.resolved_at.isoformat() if t.resolved_at else None,
    }


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
                breakdown[c] = {"city": c, "model_tier": CITY_MODEL_TIER.get(c, "unknown"), "trades": 0, "wins": 0, "pnl": 0.0}
            breakdown[c]["trades"] += 1
            breakdown[c]["pnl"] = round(breakdown[c]["pnl"] + (t.net_pnl or 0), 2)
            if t.status == "WIN":
                breakdown[c]["wins"] += 1
        for c in breakdown:
            s = breakdown[c]
            s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] else 0
        return list(breakdown.values())


# ── Bucket mapping diagnostics ───────────────────────────────────────────────

async def _purge_old_bucket_diagnostics():
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
