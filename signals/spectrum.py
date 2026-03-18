"""
WeatherAlgo v2 — Spectrum Evaluator (Phase 2B.1)

Spectrum — Native Bucket Benchmark ($500 bankroll)

Evaluates YES and NO edge on every bucket. Picks the single best edge
per city-date across both sides. Fixed $2.00 per trade.

Gate order (from Master Spec Section 3A):
  1. Minimum edge >= 0.08
  2. Minimum ensemble prob >= 0.05 (for the side being traded)
  3. Maximum ask price <= 0.50 (for the side being traded)
  4. Peak proximity: |bucket_index - peak_index| <= 4 (YES only; NO has no proximity gate)
  5. Fillability: fill simulation passes on the token being traded
  6. City-date dedup: no existing Spectrum position on this city-date
  7. Bankroll floor: bankroll > 0

Phase 5A:
  - All quote values normalized through _safe_float() before comparison
  - Per-side tradable flags checked (yes_tradable / no_tradable)
  - No Gamma/stale pricing in fill paths — live CLOB only
  - OrderBook dataclass handling in _fetch_quote_context
"""
from __future__ import annotations

import logging
from typing import Optional

from signals import TradeSignal, FillResult
from signals.fill_simulator import resolve_fill, compute_book_depth

logger = logging.getLogger(__name__)


# ── Safe numeric normalization ───────────────────────────────────────────────

def _safe_float(value, default: float = 0.0) -> float:
    """
    Normalize a quote-like value to a safe float for numeric comparison.
    Returns default if value is None, non-numeric, or conversion fails.
    Never returns None.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ── Helper: fetch quote context for a token ──────────────────────────────────

async def _fetch_quote_context(token_id: str, venue_adapter) -> dict:
    """
    Fetch ask, bid, spread, midpoint, and book depth for a specific token.
    All fields reference the same token — no cross-side contamination.
    Returns a dict with logging fields. Falls back to sentinels on error.

    All values normalized through _safe_float().
    Handles OrderBook dataclass returns from adapter.
    """
    result = {"ask": 0.0, "bid": 0.0, "spread": 0.0, "midpoint": 0.0, "depth": 0.0}

    try:
        raw_ask = await venue_adapter.get_ask_price(token_id)
        result["ask"] = _safe_float(raw_ask)
    except Exception:
        pass

    try:
        order_book = await venue_adapter.get_order_book(token_id)
        if order_book is not None:
            if hasattr(order_book, 'asks'):
                ask_dicts = [{"price": a.price, "size": a.size} for a in (order_book.asks or [])]
                bid_dicts = [{"price": b.price, "size": b.size} for b in (order_book.bids or [])]
            elif isinstance(order_book, dict):
                ask_dicts = order_book.get("asks", [])
                bid_dicts = order_book.get("bids", [])
            else:
                ask_dicts = []
                bid_dicts = []

            result["depth"] = compute_book_depth(ask_dicts)
            if ask_dicts and result["ask"] == 0.0:
                first = ask_dicts[0]
                result["ask"] = _safe_float(first.get("price") if isinstance(first, dict) else getattr(first, 'price', 0))
            if bid_dicts:
                first = bid_dicts[0]
                result["bid"] = _safe_float(first.get("price") if isinstance(first, dict) else getattr(first, 'price', 0))
    except Exception:
        pass

    if result["ask"] > 0 and result["bid"] > 0:
        result["spread"] = result["ask"] - result["bid"]
        result["midpoint"] = (result["ask"] + result["bid"]) / 2
    elif result["ask"] > 0:
        result["spread"] = result["ask"]  # No bid available
        result["midpoint"] = result["ask"]

    return result


async def evaluate_spectrum(
    buckets: list,
    ensemble_probs: dict,
    gfs_peak_index: int,
    ecmwf_peak_index: int,
    bankroll: float,
    open_positions: set,
    venue_adapter,
    city: str = "",
    market_date: str = "",
    config: Optional[dict] = None,
    ensemble_total_members: int = 82,
    model_run_time: Optional[str] = None,
) -> Optional[TradeSignal]:
    """
    Evaluate Spectrum strategy on a set of buckets for one city-date.

    Returns:
        TradeSignal (with side="YES" or side="NO") or None
    """
    if config is None:
        config = {}

    min_edge = config.get("min_edge", 0.08)
    min_ensemble_prob = config.get("min_ensemble_prob", 0.05)
    max_ask = config.get("max_ask", 0.50)
    max_peak_distance = config.get("max_peak_distance", 4)
    trade_size = config.get("trade_size", 2.00)

    # ── Gate 7: Bankroll floor ───────────────────────────────────────────
    if bankroll <= 0:
        return None

    # ── Gate 6: City-date dedup ──────────────────────────────────────────
    if (city, market_date) in open_positions:
        return None

    # ── Find combined peak index for proximity check ─────────────────────
    combined_peak_index = 0
    best_combined_prob = 0.0
    for i, bkt in enumerate(buckets):
        p = ensemble_probs.get(bkt.bucket_label, 0.0)
        if p > best_combined_prob:
            best_combined_prob = p
            combined_peak_index = i

    # ── Evaluate all candidates (YES and NO) ─────────────────────────────
    candidates = []

    for i, bkt in enumerate(buckets):
        prob = ensemble_probs.get(bkt.bucket_label, 0.0)
        members_in = round(prob * ensemble_total_members)

        # --- YES side evaluation ---
        yes_ask = _safe_float(bkt.ask_price)

        # Skip if YES side has no live CLOB data
        if not bkt.yes_tradable:
            yes_ask = 0.0

        if yes_ask > 0:
            yes_edge = prob - yes_ask

            # Gate 1: Minimum edge
            if yes_edge >= min_edge:
                # Gate 2: Minimum ensemble prob (YES)
                if prob >= min_ensemble_prob:
                    # Gate 3: Maximum ask price
                    if yes_ask <= max_ask:
                        # Gate 4: Peak proximity (YES only)
                        if abs(i - combined_peak_index) <= max_peak_distance:
                            candidates.append({
                                "side": "YES",
                                "bucket": bkt,
                                "bucket_index": i,
                                "edge": yes_edge,
                                "prob": prob,
                                "ask": yes_ask,
                                "token_id": bkt.yes_token_id,
                                "members_in": members_in,
                            })

        # --- NO side evaluation ---
        # Skip if NO side has no live CLOB data
        if not bkt.no_tradable:
            continue

        no_prob = 1.0 - prob
        # Use the NO ask that the adapter already resolved from live CLOB
        # (get_price OR order book best ask). Do not re-fetch — the adapter
        # already determined tradability and set no_ask_price from live data.
        no_ask = _safe_float(bkt.no_ask_price)

        if no_ask > 0 and no_ask < 1.0:
            no_edge = no_prob - no_ask

            # Gate 1: Minimum edge
            if no_edge >= min_edge:
                # Gate 2: Minimum ensemble prob (NO side)
                if no_prob >= min_ensemble_prob:
                    # Gate 3: Maximum ask price (actual NO ask)
                    if no_ask <= max_ask:
                        # Gate 4: NO side has no peak proximity gate
                        candidates.append({
                            "side": "NO",
                            "bucket": bkt,
                            "bucket_index": i,
                            "edge": no_edge,
                            "prob": prob,
                            "ask": no_ask,
                            "token_id": bkt.no_token_id,
                            "members_in": members_in,
                        })

    if not candidates:
        return None

    # ── Rank by edge, pick best ──────────────────────────────────────────
    candidates.sort(key=lambda c: c["edge"], reverse=True)

    # ── Gate 5: Fillability — try candidates in order ────────────────────
    for cand in candidates:
        fill = await resolve_fill(
            token_id=cand["token_id"],
            target_spend_usd=trade_size,
            venue_adapter=venue_adapter,
        )

        if not fill.filled:
            continue  # Try next candidate

        bkt = cand["bucket"]

        # Fetch quote context for the TRADED token (YES or NO)
        quote = await _fetch_quote_context(cand["token_id"], venue_adapter)

        return TradeSignal(
            strategy="spectrum",
            side=cand["side"],
            token_id=cand["token_id"],
            bucket_label=bkt.bucket_label,
            bucket_index=cand["bucket_index"],
            edge=cand["edge"],
            ensemble_prob=cand["prob"],
            ensemble_members_in_bucket=cand["members_in"],
            ensemble_total_members=ensemble_total_members,
            gfs_peak_index=gfs_peak_index,
            ecmwf_peak_index=ecmwf_peak_index,
            model_agreement=abs(gfs_peak_index - ecmwf_peak_index) <= 2,
            entry_price=fill.vwap,
            market_ask=cand["ask"],
            market_bid=quote["bid"],
            spread_at_entry=quote["spread"],
            midpoint_at_entry=quote["midpoint"],
            book_depth_at_entry=quote["depth"],
            simulated_shares=fill.total_shares,
            simulated_cost=fill.total_cost,
            fill_quality=fill.fill_quality,
            price_source=fill.price_source,
            levels_swept=fill.levels_swept,
            target_spend=trade_size,
            model_run_time=model_run_time,
        )

    # All candidates failed fill simulation
    return None
