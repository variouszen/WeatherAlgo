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
    Trade, BankrollState, ScanLog, CityCalibration,
    BucketMappingDiagnostic,
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
    await _purge_old_bucket_diagnostics()

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


@app.post("/api/admin/reset-bankroll")
async def reset_bankroll_endpoint():
    """Hard reset bankroll to STARTING_BANKROLL. Use when balance is corrupted."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session)
            old_balance = bankroll_state.balance
            bankroll_state.balance = STARTING_BANKROLL
            bankroll_state.daily_loss_today = 0.0
            bankroll_state.last_reset_date = date.today().isoformat()
    return {
        "status": "reset",
        "previous_balance": old_balance,
        "new_balance": STARTING_BANKROLL,
    }


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


@app.post("/api/admin/purge-stale-trades")
async def purge_stale_trades():
    """
    Delete open trades whose market_date is before today.
    Uses trade.market_date directly — no title/condition parsing.
    Also refunds position size back to bankroll.
    """
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).date()
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
                # Primary: use market_date field (the actual date being bet on)
                is_stale = False
                reason = ""

                if trade.market_date:
                    try:
                        mkt_date = datetime.strptime(trade.market_date, "%Y-%m-%d").date()
                        if mkt_date < today:
                            is_stale = True
                            reason = f"market_date {mkt_date} < today {today}"
                    except Exception:
                        pass

                # Fallback: if market_date missing, use opened_at
                if not is_stale and not trade.market_date:
                    if trade.opened_at and trade.opened_at.date() < today:
                        is_stale = True
                        reason = f"no market_date, opened_at {trade.opened_at.date()} < today {today}"

                if is_stale:
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
                        f"[PURGE-STALE] {trade.city} >={trade.threshold_f} {trade.direction} | "
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


@app.post("/api/admin/delete-trades")
async def delete_specific_trades():
    """
    One-time cleanup — delete specific bad trades by ID and refund bankroll.
    Hardcoded to IDs: 78, 80, 81, 82, 83
    """
    trade_ids = [78, 80, 81, 82, 83]
    async with AsyncSessionLocal() as session:
        async with session.begin():
            bankroll_state = await get_bankroll(session)
            result = await session.execute(
                select(Trade).where(Trade.id.in_(trade_ids))
            )
            trades = result.scalars().all()
            refunded = 0.0
            deleted = []
            for trade in trades:
                if trade.status == "OPEN":
                    bankroll_state.balance = round(
                        bankroll_state.balance + trade.position_size_usd, 2
                    )
                    refunded += trade.position_size_usd
                deleted.append({
                    "id": trade.id,
                    "city": trade.city,
                    "threshold": trade.threshold_f,
                    "direction": trade.direction,
                    "size": trade.position_size_usd,
                })
                await session.delete(trade)
                logger.info(
                    f"[DELETE-TRADE] #{trade.id} {trade.city} >={trade.threshold_f} "
                    f"{trade.direction} | ${trade.position_size_usd} refunded"
                )

    return {
        "status": "done",
        "deleted_count": len(deleted),
        "refunded": round(refunded, 2),
        "bankroll_after": bankroll_state.balance,
        "deleted_trades": deleted,
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
                "forecast_high": r.forecast_high,
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
        "market_date": t.market_date,
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


async def _purge_old_bucket_diagnostics():
    """Delete bucket_mapping_diagnostics rows older than 7 days. Safe to call on startup."""
    from datetime import timedelta
    from sqlalchemy import delete
    cutoff = datetime.utcnow() - timedelta(days=7)
    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    delete(BucketMappingDiagnostic).where(
                        BucketMappingDiagnostic.scanned_at < cutoff
                    )
                )
                deleted = result.rowcount
                if deleted:
                    logger.info(f"[BUCKET] Purged {deleted} diagnostic rows older than 7 days")
    except Exception as e:
        logger.warning(f"[BUCKET] Purge failed (non-fatal): {e}")


@app.get("/api/debug/bucket-mapping/summary")
async def bucket_mapping_summary():
    """
    Daily summary of bucket mapping diagnostics.
    Returns counts by match type, avg prob gap, and top 10 mismatches.
    """
    from datetime import timezone

    async with AsyncSessionLocal() as session:
        now_utc = datetime.now(timezone.utc)
        today_str = now_utc.date().isoformat()
        day_start = datetime(now_utc.year, now_utc.month, now_utc.day, tzinfo=timezone.utc)

        result = await session.execute(
            select(BucketMappingDiagnostic)
            .where(BucketMappingDiagnostic.scanned_at >= day_start)
            .order_by(desc(BucketMappingDiagnostic.scanned_at))
        )
        rows = result.scalars().all()

        if not rows:
            return {"today": today_str, "mapped_candidates": 0, "message": "No bucket diagnostics today. Is BUCKET_MAPPING=1 set?"}

        counts = {"exact": 0, "nearest": 0, "basket_only": 0, "parse_fail": 0}
        gaps = []
        for r in rows:
            mt = r.match_type if r.match_type in counts else "parse_fail"
            counts[mt] += 1
            if r.prob_gap is not None:
                gaps.append((r.prob_gap, r))

        avg_gap = round(sum(g[0] for g in gaps) / len(gaps), 4) if gaps else None
        top_mismatches = sorted(gaps, key=lambda x: x[0], reverse=True)[:10]

        return {
            "today": today_str,
            "mapped_candidates": len(rows),
            "exact": counts["exact"],
            "nearest": counts["nearest"],
            "basket_only": counts["basket_only"],
            "parse_fail": counts["parse_fail"],
            "avg_gap_synthetic_vs_basket": avg_gap,
            "top_mismatches": [
                {
                    "city": r.city,
                    "threshold": r.threshold,
                    "direction": r.direction,
                    "synthetic_prob": r.synthetic_prob,
                    "basket_yes_prob": r.basket_yes_prob,
                    "prob_gap": r.prob_gap,
                    "match_type": r.match_type,
                    "note": r.approximation_note,
                }
                for _, r in top_mismatches
            ],
        }


@app.get("/api/debug/bucket-mapping/detail")
async def bucket_mapping_detail(limit: int = 100):
    """
    Last N bucket mapping diagnostic rows, newest first.
    Default 100, max 200.
    """
    limit = min(limit, 200)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(BucketMappingDiagnostic)
            .order_by(desc(BucketMappingDiagnostic.scanned_at))
            .limit(limit)
        )
        rows = result.scalars().all()
        return [
            {
                "id": r.id,
                "scanned_at": r.scanned_at.isoformat() if r.scanned_at else None,
                "city": r.city,
                "market_date": r.market_date,
                "threshold": r.threshold,
                "direction": r.direction,
                "synthetic_prob": r.synthetic_prob,
                "synthetic_edge": r.synthetic_edge,
                "match_type": r.match_type,
                "is_directly_tradable": r.is_directly_tradable,
                "nearest_bucket_label": r.nearest_bucket_label,
                "basket_count": r.basket_count,
                "basket_yes_prob": r.basket_yes_prob,
                "prob_gap": r.prob_gap,
                "approximation_note": r.approximation_note,
                "polymarket_market_id": r.polymarket_market_id,
            }
            for r in rows
        ]


@app.get("/api/debug/markets")
async def debug_markets():
    """
    Test the slug-based event discovery path — mirrors MAX_FORWARD_DAYS logic.
    Tries today, +1, +2 for each city and reports the first valid event found.
    Shows exactly what build_market_map would find.
    """
    from data.polymarket import build_slug, fetch_event_by_slug, CITY_SLUGS, MAX_FORWARD_DAYS
    from datetime import timezone, timedelta

    utc_today = datetime.now(timezone.utc).date()
    results = {}

    async with httpx.AsyncClient() as client:
        for city in CITY_SLUGS:
            event = None
            slug = None
            rejection = f"No valid event in next {MAX_FORWARD_DAYS} days"
            found_date = None

            for day_offset in range(MAX_FORWARD_DAYS):
                target_date = utc_today + timedelta(days=day_offset)
                slug = build_slug(city, target_date)
                event, rejection = await fetch_event_by_slug(city, target_date, client)
                if event:
                    found_date = target_date.isoformat()
                    break

            if event:
                markets = event.get("markets", [])
                summed_bucket_vol = sum(
                    float(m.get("volumeNum") or m.get("volume") or 0)
                    for m in markets
                )
                sample_buckets = [
                    {
                        "label": m.get("groupItemTitle") or m.get("question", ""),
                        "outcomePrices": m.get("outcomePrices"),
                        "bucket_volume": float(m.get("volumeNum") or m.get("volume") or 0),
                    }
                    for m in markets[:3]
                ]
                results[city] = {
                    "status": "found",
                    "market_date": found_date,
                    "slug": slug,
                    "title": event.get("title", ""),
                    "endDate": event.get("endDate") or event.get("end_date"),
                    "active": event.get("active"),
                    "closed": event.get("closed"),
                    "bucket_count": len(markets),
                    "event_volume": float(event.get("volumeNum") or event.get("volume") or 0),
                    "summed_bucket_volume": round(summed_bucket_vol, 2),
                    "sample_buckets": sample_buckets,
                }
            else:
                results[city] = {
                    "status": "not_found",
                    "slug": slug,
                    "rejection": rejection,
                }

    found = sum(1 for v in results.values() if v["status"] == "found")
    return {
        "utc_now": datetime.now(timezone.utc).isoformat(),
        "cities_found": found,
        "cities_checked": len(CITY_SLUGS),
        "results": results,
    }
