"""
WeatherAlgo v2 — Ladder 3 + Ladder 5 Evaluators (Phase 2B.4-2B.5)

Ladder 3: 3-bucket contiguous package (peak ± 1), 10 shares per bucket
Ladder 5: 5-bucket contiguous package (peak ± 2), 10 shares per bucket

Package math (Spec Section 3D):
  package_cost = sum(ask_price × shares for each bucket in window)
  package_prob = sum(ensemble_prob for each bucket in window)
  package_ev = (package_prob × $1.00 × shares_per_bucket) - package_cost
  package_edge = package_ev / package_cost

Gate order (Spec Section 3D):
  1. Minimum package edge >= 0.15
  2. Minimum package prob >= 0.60
  3. Maximum package cost <= $10.00
  3b. Minimum package cost >= $1.00
  4. All buckets fillable (each passes fill simulation independently)
     — Per-leg minimum ask price >= $0.03 (rejects penny buckets)
  5. No internal gaps (all buckets contiguous)
  6. Multi-model agreement: peak within 2 buckets between models
  7. City-date dedup: no existing Ladder on this city-date
  8. Bankroll floor: bankroll >= package_cost

Partial fill rules (Spec Section 3D):
  - 1 tail bucket unfillable → proceed with reduced ladder, recalculate
  - Peak bucket unfillable → REJECT entire ladder
  - 2+ buckets unfillable → REJECT entire ladder

Ladder dedup: Two Ladders (3 and 5) on same city-date NOT allowed.

Phase 5A:
  - No Gamma/stale pricing in fill paths — live CLOB only
  - Per-side tradable flag checked (ladders are YES only)
  - Safe float normalization on ask_price
"""
from __future__ import annotations

import logging
from typing import Optional

from signals import TradeSignal, LadderSignal
from signals.fill_simulator import resolve_fill, compute_book_depth

logger = logging.getLogger(__name__)


SHARES_PER_BUCKET = 10


def _safe_float(value, default: float = 0.0) -> float:
    """Normalize a quote-like value to a safe float. Never returns None."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


async def evaluate_ladder(
    buckets: list,
    ensemble_probs: dict,
    gfs_peak_index: int,
    ecmwf_peak_index: int,
    bankroll: float = 500.0,
    open_positions: Optional[set] = None,
    ladder_open_positions: Optional[set] = None,
    venue_adapter=None,
    city: str = "",
    market_date: str = "",
    width: int = 3,
    config: Optional[dict] = None,
    ensemble_total_members: int = 82,
    model_run_time: Optional[str] = None,
    hours_to_close: Optional[float] = None,
) -> Optional[LadderSignal]:
    """
    Evaluate a Ladder package for one city-date.

    Args:
        buckets: list of BucketMarket objects (ordered)
        ensemble_probs: bucket_label → probability
        gfs_peak_index: GFS peak bucket index
        ecmwf_peak_index: ECMWF peak bucket index
        bankroll: current ladder bankroll
        open_positions: (city, date) pairs for THIS ladder variant
        ladder_open_positions: (city, date) pairs for the OTHER ladder variant
                               (for cross-ladder dedup)
        venue_adapter: VenueAdapter for fill simulation
        city: city name
        market_date: date string
        width: 3 or 5 (number of buckets)
        config: strategy config dict
        ensemble_total_members: total ensemble members
        model_run_time: GFS run identifier

    Returns:
        LadderSignal or None
    """
    if open_positions is None:
        open_positions = set()
    if ladder_open_positions is None:
        ladder_open_positions = set()
    if config is None:
        config = {}

    strategy_name = f"ladder_{width}"
    half_width = width // 2  # 1 for ladder_3, 2 for ladder_5

    min_package_edge = config.get("min_package_edge", 0.15)
    min_package_prob = config.get("min_package_prob", 0.60)
    max_package_cost = config.get("max_package_cost", 10.00)
    min_package_cost = config.get("min_package_cost", 1.00)
    min_leg_ask = config.get("min_leg_ask", 0.03)
    shares_per_bucket = config.get("shares_per_bucket", SHARES_PER_BUCKET)

    # ── Gate 8: Bankroll floor (check early, refined after cost calc) ────
    if bankroll <= 0:
        return None

    # ── Horizon outlier guard (backstop — per-city schedule is the primary fix) ──
    # Hard-blocks only extreme outliers indicating misconfiguration or timezone bugs.
    # The per-city entry window in scanner_v2.py is the real control mechanism.
    # This guard is belt-and-suspenders, not the primary horizon filter.
    if hours_to_close is not None:
        if hours_to_close > 48:
            logger.warning(
                f"HORIZON-OUTLIER [hard-block] {city}/{market_date} "
                f"h={hours_to_close:.1f}h — blocked (>48h ceiling)"
            )
            return None
        if hours_to_close < 12:
            logger.info(
                f"HORIZON-SHORT [log-only] {city}/{market_date} "
                f"h={hours_to_close:.1f}h — market near close, proceeding"
            )
        elif not (26.0 <= hours_to_close <= 34.0):
            logger.info(
                f"HORIZON-DRIFT [info] {city}/{market_date} "
                f"h={hours_to_close:.1f}h — outside 26-34h core, proceeding"
            )

    # ── Gate 7: City-date dedup (own + cross-ladder) ─────────────────────
    if (city, market_date) in open_positions:
        return None
    # Two Ladders on same city-date not allowed
    if (city, market_date) in ladder_open_positions:
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

    # ── Build window: peak ± half_width ──────────────────────────────────
    window_start = max(0, combined_peak_index - half_width)
    window_end = min(len(buckets) - 1, combined_peak_index + half_width)

    # Gate 5: No internal gaps — window must have exactly `width` contiguous buckets
    window_buckets = buckets[window_start:window_end + 1]
    if len(window_buckets) < width:
        # Not enough buckets at edge of range — try with what we have
        # but at minimum need 2 for a valid reduced ladder
        if len(window_buckets) < 2:
            return None

    # ── Gate 4: Fillability — test each bucket independently ─────────────
    # Ladders are YES only — skip any bucket where YES side is dead
    leg_fills = []
    unfillable_indices = []

    for offset, bkt in enumerate(window_buckets):
        abs_index = window_start + offset

        # If YES side has no live CLOB data, mark as unfillable
        if not bkt.yes_tradable:
            leg_fills.append({
                "bucket": bkt,
                "abs_index": abs_index,
                "fill": None,
                "offset": offset,
            })
            unfillable_indices.append(offset)
            continue

        ask_price = _safe_float(bkt.ask_price)

        # Gate: Per-leg minimum ask price — reject penny buckets
        if ask_price < min_leg_ask:
            leg_fills.append({
                "bucket": bkt,
                "abs_index": abs_index,
                "fill": None,
                "offset": offset,
            })
            unfillable_indices.append(offset)
            continue

        target_spend = shares_per_bucket * ask_price if ask_price > 0 else 0.0

        if target_spend <= 0:
            leg_fills.append({
                "bucket": bkt,
                "abs_index": abs_index,
                "fill": None,
                "offset": offset,
            })
            unfillable_indices.append(offset)
            continue

        fill = await resolve_fill(
            token_id=bkt.yes_token_id,  # Ladders are YES only
            target_spend_usd=target_spend,
            venue_adapter=venue_adapter,
        )

        leg_fills.append({
            "bucket": bkt,
            "abs_index": abs_index,
            "fill": fill,
            "offset": offset,
        })

        if not fill.filled:
            unfillable_indices.append(offset)

    # ── Partial fill rules ───────────────────────────────────────────────
    peak_offset = combined_peak_index - window_start

    if len(unfillable_indices) >= 2:
        # 2+ buckets unfillable → REJECT
        return None

    if len(unfillable_indices) == 1:
        unfillable = unfillable_indices[0]

        if unfillable == peak_offset:
            # Peak bucket unfillable → REJECT
            return None

        # 1 tail bucket unfillable → proceed with reduced ladder
        leg_fills = [lf for lf in leg_fills if lf["offset"] != unfillable]

    if not leg_fills:
        return None

    # Filter out any entries with no fill (shouldn't happen after above, but defensive)
    leg_fills = [lf for lf in leg_fills if lf["fill"] is not None and lf["fill"].filled]
    if not leg_fills:
        return None

    # ── Package math ─────────────────────────────────────────────────────
    package_cost = 0.0
    package_prob = 0.0
    legs = []

    for lf in leg_fills:
        bkt = lf["bucket"]
        fill = lf["fill"]
        prob = ensemble_probs.get(bkt.bucket_label, 0.0)
        members_in = round(prob * ensemble_total_members)

        package_cost += fill.total_cost
        package_prob += prob

        ask_price = _safe_float(bkt.ask_price)
        bid_price = _safe_float(bkt.bid_price)
        spread = ask_price - bid_price if bid_price > 0 else 0.0
        midpoint = (ask_price + bid_price) / 2 if bid_price > 0 else ask_price

        try:
            order_book = await venue_adapter.get_order_book(bkt.yes_token_id)
            if order_book is not None and hasattr(order_book, 'asks'):
                ask_dicts = [{"price": a.price, "size": a.size} for a in (order_book.asks or [])]
            elif isinstance(order_book, dict):
                ask_dicts = order_book.get("asks", [])
            else:
                ask_dicts = []
            depth = compute_book_depth(ask_dicts)
        except Exception:
            depth = 0.0

        legs.append(TradeSignal(
            strategy=strategy_name,
            side="YES",
            token_id=bkt.yes_token_id,
            bucket_label=bkt.bucket_label,
            bucket_index=lf["abs_index"],
            edge=0.0,           # Set at package level
            ensemble_prob=prob,
            ensemble_members_in_bucket=members_in,
            ensemble_total_members=ensemble_total_members,
            gfs_peak_index=gfs_peak_index,
            ecmwf_peak_index=ecmwf_peak_index,
            model_agreement=True,  # Passed gate 6
            entry_price=fill.vwap,
            market_ask=ask_price,
            market_bid=bid_price,
            spread_at_entry=spread,
            midpoint_at_entry=midpoint,
            book_depth_at_entry=depth,
            simulated_shares=fill.total_shares,
            simulated_cost=fill.total_cost,
            fill_quality=fill.fill_quality,
            price_source=fill.price_source,
            levels_swept=fill.levels_swept,
            target_spend=fill.total_cost,
            model_run_time=model_run_time,
        ))

    # package_ev = (package_prob × $1.00 × shares_per_bucket) - package_cost
    package_ev = (package_prob * 1.00 * shares_per_bucket) - package_cost
    package_edge = package_ev / package_cost if package_cost > 0 else 0.0

    # ── Gate 1: Minimum package edge ─────────────────────────────────────
    if package_edge < min_package_edge:
        return None

    # ── Gate 2: Minimum package prob ─────────────────────────────────────
    if package_prob < min_package_prob:
        return None

    # ── Gate 3: Maximum package cost ─────────────────────────────────────
    if package_cost > max_package_cost:
        return None

    # ── Gate 3b: Minimum package cost ────────────────────────────────────
    if package_cost < min_package_cost:
        return None

    # ── Gate 8 (refined): Bankroll >= package_cost ───────────────────────
    if bankroll < package_cost:
        return None

    # ── Stamp package-level fields on each leg ───────────────────────────
    for leg in legs:
        leg.package_cost = package_cost
        leg.package_prob = package_prob
        leg.package_edge = package_edge
        leg.num_legs = len(legs)
        leg.edge = package_edge  # Use package edge for logging

    return LadderSignal(
        strategy=strategy_name,
        width=width,
        legs=legs,
        package_cost=package_cost,
        package_prob=package_prob,
        package_edge=package_edge,
        peak_index=combined_peak_index,
        model_agreement=True,
        gfs_peak_index=gfs_peak_index,
        ecmwf_peak_index=ecmwf_peak_index,
    )
