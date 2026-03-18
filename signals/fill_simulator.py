"""
WeatherAlgo v2 — Fill Simulation (Phase 2A)

Implements the price hierarchy and fill simulation from Master Spec Section 5:
  1. get_order_book() → full fill simulation with sanity check
  2. get_price(BUY) → shallow fill (no depth data)
  3. outcomePrices from Gamma API → stale snapshot with penalty
  4. No price → skip

Rejection gates:
  - total_shares < 5 (Polymarket minimum) → REJECT
  - vwap > best_ask × 1.10 (10% max slippage) → REJECT
  - total_shares < 80% of desired → WARN (degraded fill)
"""
from __future__ import annotations

from typing import Optional

from signals import FillResult


# ── Constants ────────────────────────────────────────────────────────────────

MIN_SHARES = 5              # Polymarket minimum order size
MAX_SLIPPAGE = 0.10         # 10% max VWAP deviation from best ask
DEGRADED_FILL_PCT = 0.80    # Warn if < 80% of desired shares filled
SANITY_CHECK_PCT = 0.20     # Order book best_ask must be within 20% of get_price
STALE_PENALTY_MIN = 0.01    # Minimum penalty on stale snapshot prices
STALE_PENALTY_PCT = 0.05    # 5% penalty on stale snapshot prices


# ── Core fill simulation ─────────────────────────────────────────────────────

def simulate_fill(
    target_spend_usd: float,
    asks_sorted_ascending: list[dict],
) -> FillResult:
    """
    Simulate filling a market order against the ask side of the order book.

    Args:
        target_spend_usd: Dollar amount to spend (e.g. $2.00 for Spectrum)
        asks_sorted_ascending: List of {"price": float, "size": float} sorted
                               by price ascending (best ask first)

    Returns:
        FillResult with vwap, shares, cost, and quality assessment.
    """
    if not asks_sorted_ascending or target_spend_usd <= 0:
        return FillResult(
            filled=False,
            reject_reason="no_asks" if not asks_sorted_ascending else "zero_spend",
        )

    total_cost = 0.0
    total_shares = 0.0
    fills = []

    for ask in asks_sorted_ascending:
        if total_cost >= target_spend_usd:
            break

        price = float(ask["price"])
        size = float(ask["size"])

        if price <= 0:
            continue

        remaining = target_spend_usd - total_cost
        shares = min(size, remaining / price)
        cost = shares * price

        total_cost += cost
        total_shares += shares
        fills.append({"price": price, "shares": shares})

    # ── Rejection gate 1: No shares filled ───────────────────────────────
    if total_shares == 0:
        return FillResult(
            filled=False,
            reject_reason="zero_shares",
        )

    vwap = total_cost / total_shares
    best_ask = float(asks_sorted_ascending[0]["price"])

    # ── Rejection gate 2: Minimum shares (Polymarket min = 5) ────────────
    if total_shares < MIN_SHARES:
        return FillResult(
            filled=False,
            vwap=vwap,
            total_shares=total_shares,
            total_cost=total_cost,
            levels_swept=len(fills),
            reject_reason=f"min_shares ({total_shares:.1f} < {MIN_SHARES})",
        )

    # ── Rejection gate 3: Max slippage (VWAP > best_ask × 1.10) ─────────
    if vwap > best_ask * (1 + MAX_SLIPPAGE):
        return FillResult(
            filled=False,
            vwap=vwap,
            total_shares=total_shares,
            total_cost=total_cost,
            levels_swept=len(fills),
            reject_reason=f"slippage ({vwap:.4f} > {best_ask * (1 + MAX_SLIPPAGE):.4f})",
        )

    # ── Warning: Degraded fill (< 80% of desired shares) ────────────────
    desired_shares = target_spend_usd / best_ask if best_ask > 0 else 0
    warnings = []
    if desired_shares > 0 and total_shares < desired_shares * DEGRADED_FILL_PCT:
        warnings.append(
            f"degraded_fill ({total_shares:.1f}/{desired_shares:.1f} = "
            f"{total_shares / desired_shares:.0%})"
        )

    return FillResult(
        filled=True,
        vwap=vwap,
        total_shares=total_shares,
        total_cost=total_cost,
        levels_swept=len(fills),
        fill_quality="full",
        price_source="order_book",
        warnings=warnings,
    )


# ── Price hierarchy orchestrator ─────────────────────────────────────────────

async def resolve_fill(
    token_id: str,
    target_spend_usd: float,
    venue_adapter,
    gamma_price: Optional[float] = None,
) -> FillResult:
    """
    Resolve the best available fill using the spec's price hierarchy:
      1. Order book → full fill simulation (with sanity check)
      2. get_price(BUY) → shallow fill
      3. Gamma outcomePrices → stale snapshot with penalty
      4. No price → skip

    Args:
        token_id: The token to fill (YES or NO clobTokenId)
        target_spend_usd: Dollar amount to spend
        venue_adapter: VenueAdapter instance with get_order_book / get_ask_price
        gamma_price: Optional stale price from Gamma API discovery

    Returns:
        FillResult with pricing source and quality flags
    """
    # ── Level 1: Order book fill simulation ──────────────────────────────
    try:
        order_book = await venue_adapter.get_order_book(token_id)
        asks = order_book.get("asks", []) if isinstance(order_book, dict) else []

        if asks:
            # Sanity check: best_ask from book within 20% of get_price(BUY)
            book_best_ask = float(asks[0]["price"])
            try:
                live_best_ask = await venue_adapter.get_ask_price(token_id)
                if live_best_ask > 0 and abs(book_best_ask - live_best_ask) / live_best_ask > SANITY_CHECK_PCT:
                    # Book is stale — fall through to Level 2
                    pass
                else:
                    # Book passes sanity check — run full simulation
                    result = simulate_fill(target_spend_usd, asks)
                    if result.filled:
                        return result
                    # Fill sim rejected — fall through to try best_ask
            except Exception:
                # get_ask_price failed — still use book if it has data
                result = simulate_fill(target_spend_usd, asks)
                if result.filled:
                    return result
    except Exception:
        pass  # Order book unavailable — fall through

    # ── Level 2: Best ask (shallow fill) ─────────────────────────────────
    try:
        best_ask = await venue_adapter.get_ask_price(token_id)
        if best_ask and best_ask > 0:
            shares = target_spend_usd / best_ask
            if shares >= MIN_SHARES:
                return FillResult(
                    filled=True,
                    vwap=best_ask,
                    total_shares=shares,
                    total_cost=target_spend_usd,
                    levels_swept=1,
                    fill_quality="shallow",
                    price_source="best_ask",
                    warnings=["no_depth_data"],
                )
            else:
                return FillResult(
                    filled=False,
                    vwap=best_ask,
                    total_shares=shares,
                    reject_reason=f"min_shares_shallow ({shares:.1f} < {MIN_SHARES})",
                )
    except Exception:
        pass  # get_ask_price failed — fall through

    # ── Level 3: Stale snapshot with penalty ─────────────────────────────
    if gamma_price and gamma_price > 0:
        penalty = max(STALE_PENALTY_MIN, gamma_price * STALE_PENALTY_PCT)
        penalized_price = gamma_price + penalty
        shares = target_spend_usd / penalized_price

        if shares >= MIN_SHARES:
            return FillResult(
                filled=True,
                vwap=penalized_price,
                total_shares=shares,
                total_cost=target_spend_usd,
                levels_swept=1,
                fill_quality="stale",
                price_source="stale_snapshot",
                warnings=[f"stale_penalty={penalty:.4f}"],
            )
        else:
            return FillResult(
                filled=False,
                vwap=penalized_price,
                total_shares=shares,
                reject_reason=f"min_shares_stale ({shares:.1f} < {MIN_SHARES})",
            )

    # ── Level 4: No price → skip ─────────────────────────────────────────
    return FillResult(
        filled=False,
        reject_reason="no_price_available",
    )


# ── Utility: compute book depth within N ticks ──────────────────────────────

def compute_book_depth(asks: list[dict], ticks: int = 2) -> float:
    """
    Sum ask-side shares within `ticks` price levels of best ask.
    Used for book_depth_at_entry logging.
    """
    if not asks:
        return 0.0

    best_price = float(asks[0]["price"])
    # Polymarket tick size is 0.001
    max_price = best_price + (ticks * 0.001)
    depth = 0.0

    for ask in asks:
        price = float(ask["price"])
        if price > max_price:
            break
        depth += float(ask["size"])

    return depth
