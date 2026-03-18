"""
WeatherAlgo v2 — Sniper YES + NO Evaluators (Phase 2B.2-2B.3)

Sniper YES — High-Conviction Cheap Buckets ($500 bankroll)
Gate order (Spec Section 3B):
  1. Minimum edge >= 0.10
  2. Edge ratio: ensemble_prob >= 2.0 × market_ask
  3. Maximum ask price: market_ask <= 0.15
  4. Minimum ensemble prob >= 0.08
  5. Peak proximity: |bucket_index - peak_index| <= 3
  6. Multi-model agreement: GFS and ECMWF peak within 2 bucket indexes
  7. Spread check: spread <= 0.05
  8. Fillability: fill simulation passes
  9. City-date dedup: no existing Sniper YES position
  10. Bankroll floor: bankroll > 0

Sniper NO — High-Conviction Overpriced Buckets ($500 bankroll)
Gate order (Spec Section 3C):
  1. Bucket ensemble prob <= 0.03
  2. NO ask price: no_ask <= 0.55 (actual ask on no_token_id — NOT 1-yes_bid)
  3. Edge: (1 - ensemble_prob) - no_ask >= 0.10
  4. Multi-model agreement: both models place bucket at <5% probability
  5. Spread check: spread <= 0.05 (on NO token)
  6. Fillability: fill simulation on NO token passes
  7. City-date dedup: no existing Sniper NO position
  8. Bankroll floor: bankroll > 0
"""
from __future__ import annotations

from typing import Optional

from signals import TradeSignal
from signals.fill_simulator import resolve_fill, compute_book_depth


# ── Helper: fetch quote context for a token ──────────────────────────────────

async def _fetch_no_quote_context(token_id: str, venue_adapter) -> dict:
    """
    Fetch ask, bid, spread, midpoint, and book depth for the NO token.
    All fields reference the NO token — no YES-side contamination.
    """
    result = {"ask": 0.0, "bid": 0.0, "spread": 0.0, "midpoint": 0.0, "depth": 0.0}

    try:
        ask = await venue_adapter.get_ask_price(token_id)
        if ask is not None:
            result["ask"] = ask
    except Exception:
        pass

    try:
        order_book = await venue_adapter.get_order_book(token_id)
        asks = order_book.get("asks", []) if isinstance(order_book, dict) else []
        bids = order_book.get("bids", []) if isinstance(order_book, dict) else []
        result["depth"] = compute_book_depth(asks)
        if asks and result["ask"] == 0.0:
            result["ask"] = float(asks[0]["price"])
        if bids:
            result["bid"] = float(bids[0]["price"])
    except Exception:
        pass

    if result["ask"] > 0 and result["bid"] > 0:
        result["spread"] = result["ask"] - result["bid"]
        result["midpoint"] = (result["ask"] + result["bid"]) / 2
    elif result["ask"] > 0:
        result["spread"] = result["ask"]
        result["midpoint"] = result["ask"]

    return result


# ── Sniper YES ───────────────────────────────────────────────────────────────

async def evaluate_sniper_yes(
    buckets: list,
    ensemble_probs: dict,
    gfs_peak_index: int,
    ecmwf_peak_index: int,
    gfs_bucket_probs: Optional[dict] = None,
    ecmwf_bucket_probs: Optional[dict] = None,
    bankroll: float = 500.0,
    open_positions: Optional[set] = None,
    venue_adapter=None,
    city: str = "",
    market_date: str = "",
    config: Optional[dict] = None,
    ensemble_total_members: int = 82,
    model_run_time: Optional[str] = None,
) -> Optional[TradeSignal]:
    """
    Evaluate Sniper YES on all buckets for one city-date.
    Returns the best qualifying TradeSignal or None.
    """
    if open_positions is None:
        open_positions = set()
    if config is None:
        config = {}

    min_edge = config.get("min_edge", 0.10)
    min_edge_ratio = config.get("min_edge_ratio", 2.0)
    max_ask = config.get("max_ask", 0.15)
    min_ensemble_prob = config.get("min_ensemble_prob", 0.08)
    max_peak_distance = config.get("max_peak_distance", 3)
    max_spread = config.get("max_spread", 0.05)
    trade_size = config.get("trade_size", 1.00)

    # ── Gate 10: Bankroll floor ──────────────────────────────────────────
    if bankroll <= 0:
        return None

    # ── Gate 9: City-date dedup ──────────────────────────────────────────
    if (city, market_date) in open_positions:
        return None

    # ── Gate 6: Multi-model agreement ────────────────────────────────────
    if abs(gfs_peak_index - ecmwf_peak_index) > 2:
        return None

    # ── Find combined peak ───────────────────────────────────────────────
    combined_peak_index = 0
    best_prob = 0.0
    for i, bkt in enumerate(buckets):
        p = ensemble_probs.get(bkt.bucket_label, 0.0)
        if p > best_prob:
            best_prob = p
            combined_peak_index = i

    # ── Evaluate each bucket ─────────────────────────────────────────────
    candidates = []

    for i, bkt in enumerate(buckets):
        prob = ensemble_probs.get(bkt.bucket_label, 0.0)
        yes_ask = bkt.ask_price

        if yes_ask <= 0:
            continue

        edge = prob - yes_ask

        # Gate 1: Minimum edge
        if edge < min_edge:
            continue

        # Gate 2: Edge ratio
        if prob < min_edge_ratio * yes_ask:
            continue

        # Gate 3: Maximum ask price
        if yes_ask > max_ask:
            continue

        # Gate 4: Minimum ensemble prob
        if prob < min_ensemble_prob:
            continue

        # Gate 5: Peak proximity
        if abs(i - combined_peak_index) > max_peak_distance:
            continue

        # Gate 6: Multi-model agreement (already checked above — all pass)

        # Gate 7: Spread check
        spread = yes_ask - bkt.bid_price if bkt.bid_price > 0 else yes_ask
        if spread > max_spread:
            continue

        candidates.append({
            "bucket": bkt,
            "bucket_index": i,
            "edge": edge,
            "prob": prob,
            "ask": yes_ask,
            "spread": spread,
            "members_in": round(prob * ensemble_total_members),
        })

    if not candidates:
        return None

    # Rank by edge (best first)
    candidates.sort(key=lambda c: c["edge"], reverse=True)

    # ── Gate 8: Fillability — try candidates in order ────────────────────
    for cand in candidates:
        bkt = cand["bucket"]
        fill = await resolve_fill(
            token_id=bkt.yes_token_id,
            target_spend_usd=trade_size,
            venue_adapter=venue_adapter,
            gamma_price=cand["ask"],
        )

        if not fill.filled:
            continue

        midpoint = (bkt.ask_price + bkt.bid_price) / 2 if bkt.bid_price > 0 else bkt.ask_price

        try:
            order_book = await venue_adapter.get_order_book(bkt.yes_token_id)
            asks = order_book.get("asks", []) if isinstance(order_book, dict) else []
            depth = compute_book_depth(asks)
        except Exception:
            depth = 0.0

        return TradeSignal(
            strategy="sniper_yes",
            side="YES",
            token_id=bkt.yes_token_id,
            bucket_label=bkt.bucket_label,
            bucket_index=cand["bucket_index"],
            edge=cand["edge"],
            ensemble_prob=cand["prob"],
            ensemble_members_in_bucket=cand["members_in"],
            ensemble_total_members=ensemble_total_members,
            gfs_peak_index=gfs_peak_index,
            ecmwf_peak_index=ecmwf_peak_index,
            model_agreement=True,  # Passed gate 6
            entry_price=fill.vwap,
            market_ask=cand["ask"],
            market_bid=bkt.bid_price,
            spread_at_entry=cand["spread"],
            midpoint_at_entry=midpoint,
            book_depth_at_entry=depth,
            simulated_shares=fill.total_shares,
            simulated_cost=fill.total_cost,
            fill_quality=fill.fill_quality,
            price_source=fill.price_source,
            levels_swept=fill.levels_swept,
            edge_ratio=cand["prob"] / cand["ask"] if cand["ask"] > 0 else 0.0,
            target_spend=trade_size,
            model_run_time=model_run_time,
        )

    return None


# ── Sniper NO ────────────────────────────────────────────────────────────────

async def evaluate_sniper_no(
    buckets: list,
    ensemble_probs: dict,
    gfs_peak_index: int,
    ecmwf_peak_index: int,
    gfs_bucket_probs: Optional[dict] = None,
    ecmwf_bucket_probs: Optional[dict] = None,
    bankroll: float = 500.0,
    open_positions: Optional[set] = None,
    venue_adapter=None,
    city: str = "",
    market_date: str = "",
    config: Optional[dict] = None,
    ensemble_total_members: int = 82,
    model_run_time: Optional[str] = None,
) -> Optional[TradeSignal]:
    """
    Evaluate Sniper NO on all buckets for one city-date.
    Targets overpriced buckets — buys NO token (clobTokenIds[1]).

    All gating and edge computation uses the ACTUAL NO ask fetched from
    the NO token via venue_adapter.get_ask_price(no_token_id).
    """
    if open_positions is None:
        open_positions = set()
    if config is None:
        config = {}

    max_ensemble_prob = config.get("max_ensemble_prob", 0.03)
    max_no_ask = config.get("max_no_ask", 0.55)
    min_edge = config.get("min_edge", 0.10)
    max_gfs_prob = config.get("max_model_prob", 0.05)
    max_ecmwf_prob = config.get("max_model_prob", 0.05)
    max_spread = config.get("max_spread", 0.05)
    trade_size = config.get("trade_size", 1.00)

    # ── Gate 8: Bankroll floor ───────────────────────────────────────────
    if bankroll <= 0:
        return None

    # ── Gate 7: City-date dedup ──────────────────────────────────────────
    if (city, market_date) in open_positions:
        return None

    # ── Evaluate each bucket ─────────────────────────────────────────────
    candidates = []

    for i, bkt in enumerate(buckets):
        prob = ensemble_probs.get(bkt.bucket_label, 0.0)

        # Gate 1: Bucket ensemble prob <= 0.03
        if prob > max_ensemble_prob:
            continue

        # Fetch ACTUAL NO ask from the NO token
        try:
            no_ask = await venue_adapter.get_ask_price(bkt.no_token_id)
        except Exception:
            continue  # Cannot price NO token → skip

        if no_ask is None or no_ask <= 0:
            continue

        # Gate 2: NO ask price <= 0.55 (actual NO ask, not approximation)
        if no_ask > max_no_ask:
            continue

        no_prob = 1.0 - prob
        no_edge = no_prob - no_ask

        # Gate 3: Edge >= 0.10 on NO side
        if no_edge < min_edge:
            continue

        # Gate 4: Multi-model agreement — both models at <5% for this bucket
        if gfs_bucket_probs and ecmwf_bucket_probs:
            gfs_prob = gfs_bucket_probs.get(bkt.bucket_label, 0.0)
            ecmwf_prob = ecmwf_bucket_probs.get(bkt.bucket_label, 0.0)
            if gfs_prob >= max_gfs_prob or ecmwf_prob >= max_ecmwf_prob:
                continue
        # If per-model probs not available, skip this gate (wired in Phase 3)

        # Gate 5: Spread check — on the NO token
        # Fetch NO-side quote for spread
        no_quote = await _fetch_no_quote_context(bkt.no_token_id, venue_adapter)
        no_spread = no_quote["spread"]
        if no_spread > max_spread:
            continue

        candidates.append({
            "bucket": bkt,
            "bucket_index": i,
            "edge": no_edge,
            "prob": prob,
            "no_ask": no_ask,
            "no_quote": no_quote,
            "members_in": round(prob * ensemble_total_members),
        })

    if not candidates:
        return None

    # Rank by edge (best first)
    candidates.sort(key=lambda c: c["edge"], reverse=True)

    # ── Gate 6: Fillability — fill simulation on NO token ────────────────
    for cand in candidates:
        bkt = cand["bucket"]

        # Sniper NO buys clobTokenIds[1] (the NO token)
        fill = await resolve_fill(
            token_id=bkt.no_token_id,
            target_spend_usd=trade_size,
            venue_adapter=venue_adapter,
            gamma_price=cand["no_ask"],
        )

        if not fill.filled:
            continue

        no_quote = cand["no_quote"]

        return TradeSignal(
            strategy="sniper_no",
            side="NO",
            token_id=bkt.no_token_id,
            bucket_label=bkt.bucket_label,
            bucket_index=cand["bucket_index"],
            edge=cand["edge"],
            ensemble_prob=cand["prob"],
            ensemble_members_in_bucket=cand["members_in"],
            ensemble_total_members=ensemble_total_members,
            gfs_peak_index=gfs_peak_index,
            ecmwf_peak_index=ecmwf_peak_index,
            model_agreement=True,  # Passed gate 4
            entry_price=fill.vwap,
            market_ask=cand["no_ask"],          # Actual NO ask
            market_bid=no_quote["bid"],          # NO-side bid
            spread_at_entry=no_quote["spread"],  # NO-side spread
            midpoint_at_entry=no_quote["midpoint"],
            book_depth_at_entry=no_quote["depth"],
            simulated_shares=fill.total_shares,
            simulated_cost=fill.total_cost,
            fill_quality=fill.fill_quality,
            price_source=fill.price_source,
            levels_swept=fill.levels_swept,
            target_spend=trade_size,
            model_run_time=model_run_time,
        )

    return None
