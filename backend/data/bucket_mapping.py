# backend/data/bucket_mapping.py
"""
Bucket mapping layer — diagnostic/logging only.

Feature-flagged OFF by default. When enabled, logs how each synthetic
threshold (e.g. >=62F) maps onto the real Polymarket bucket structure.

DOES NOT affect trade execution, sizing, signals, or settlement.
Safe to enable/disable at any time — all paths are wrapped in try/except.

Match types:
  exact        — a bucket lower-bound aligns exactly with the threshold
                 (e.g. threshold=62, bucket "62F or higher" → perfect)
  nearest      — no exact boundary match; closest bucket identified,
                 with basket as the recommended interpretation
  basket_only  — threshold falls inside a bucket range; cannot be expressed
                 as a single bucket, basket is the only interpretation
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Feature flag ──────────────────────────────────────────────────────────────
# Set to True (or set env var BUCKET_MAPPING=1) to enable diagnostic logging.
import os
BUCKET_MAPPING_ENABLED: bool = os.getenv("BUCKET_MAPPING", "0") == "1"


# ── Public API ────────────────────────────────────────────────────────────────

def extract_buckets(market_data: dict) -> list[dict]:
    """
    Return enriched bucket list from market_data already fetched by build_market_map.

    Each bucket:
      label      str    — original Polymarket label e.g. "62-63°F"
      low        float  — lower bound (float('-inf') for open-lower)
      high       float|None — upper bound (None for open-upper)
      yes_price  float  — price of YES outcome (0–1)
      no_price   float  — 1 - yes_price
      token_id   str|None — CLOB token id if available
      end_date   str|None — market end date from event

    Returns [] on any failure — caller must handle empty list.
    """
    try:
        raw = market_data.get("buckets", [])
        end_date = market_data.get("end_date")
        enriched = []
        for b in raw:
            yes_price = b.get("price")
            if yes_price is None:
                continue
            enriched.append({
                "label":     b.get("label", ""),
                "low":       b.get("low", float("-inf")),
                "high":      b.get("high"),
                "yes_price": round(float(yes_price), 4),
                "no_price":  round(1.0 - float(yes_price), 4),
                "token_id":  b.get("token_id"),
                "end_date":  end_date,
            })
        return enriched
    except Exception as e:
        logger.debug(f"[BUCKET] extract_buckets failed: {e}")
        return []


def map_threshold_to_buckets(
    threshold: float,
    buckets: list[dict],
    is_celsius: bool = False,
) -> dict:
    """
    Given a synthetic threshold (e.g. 62.0 meaning >=62F), map it to real buckets.

    Returns dict:
      match_type        "exact" | "nearest" | "basket_only"
      exact_buckets     list — buckets whose lower bound == threshold (may be empty)
      nearest_bucket    dict|None — single closest bucket by lower-bound distance
      basket            list — all buckets fully at or above threshold
      basket_yes_prob   float — sum of yes_prices in basket (unnormalized; informational)
      approximation_note str — human-readable explanation of any approximation
      is_directly_tradable bool — True only for exact match
    """
    unit = "C" if is_celsius else "F"
    result = {
        "match_type": "basket_only",
        "exact_buckets": [],
        "nearest_bucket": None,
        "basket": [],
        "basket_yes_prob": 0.0,
        "approximation_note": "",
        "is_directly_tradable": False,
    }

    if not buckets:
        result["approximation_note"] = "No buckets available"
        return result

    # ── Exact match: bucket lower-bound == threshold AND open-upper ───────────
    # This is the ideal case: "62F or higher" maps perfectly to >=62
    exact = [b for b in buckets if b["low"] == threshold and b["high"] is None]
    if exact:
        result["match_type"] = "exact"
        result["exact_buckets"] = exact
        result["is_directly_tradable"] = True
        result["approximation_note"] = (
            f"Exact match: threshold >={threshold:.0f}{unit} maps to "
            f"'{exact[0]['label']}' (yes={exact[0]['yes_price']:.3f})"
        )

    # ── Also catch exact lower-bound match with bounded upper (less ideal) ────
    # e.g. threshold=62, bucket "62-63F" — boundary aligns but range is bounded
    elif any(b["low"] == threshold for b in buckets):
        exact_bounded = [b for b in buckets if b["low"] == threshold]
        result["match_type"] = "exact"
        result["exact_buckets"] = exact_bounded
        result["is_directly_tradable"] = False  # bounded upper — basket needed
        labels = ", ".join(f"'{b['label']}'" for b in exact_bounded)
        result["approximation_note"] = (
            f"Lower-bound match: threshold >={threshold:.0f}{unit} aligns with {labels}, "
            f"but upper bound is not open — use basket for full >=threshold coverage"
        )

    # ── Threshold falls inside a bucket range — can't split ──────────────────
    elif any(
        b["low"] != float("-inf") and b["high"] is not None
        and b["low"] < threshold < b["high"]
        for b in buckets
    ):
        containing = next(
            b for b in buckets
            if b["low"] != float("-inf") and b["high"] is not None
            and b["low"] < threshold < b["high"]
        )
        result["match_type"] = "basket_only"
        result["approximation_note"] = (
            f"Threshold {threshold:.0f}{unit} falls inside bucket '{containing['label']}' "
            f"({containing['low']:.0f}–{containing['high']:.0f}{unit}). "
            f"Cannot express as single bucket — basket interpretation only."
        )

    # ── No boundary alignment — find nearest ──────────────────────────────────
    else:
        lower_bounds = [b["low"] for b in buckets if b["low"] != float("-inf")]
        if lower_bounds:
            nearest_low = min(lower_bounds, key=lambda x: abs(x - threshold))
            nearest = next(b for b in buckets if b["low"] == nearest_low)
            result["match_type"] = "nearest"
            result["nearest_bucket"] = nearest
            diff = nearest_low - threshold
            direction_str = f"+{diff:.1f}" if diff >= 0 else f"{diff:.1f}"
            result["approximation_note"] = (
                f"No exact boundary for {threshold:.0f}{unit}. "
                f"Nearest: '{nearest['label']}' (lower={nearest_low:.0f}{unit}, "
                f"off by {direction_str}{unit}, yes={nearest['yes_price']:.3f}). "
                f"Use basket for best approximation."
            )

    # ── Basket: all buckets fully at or above threshold ───────────────────────
    # A bucket is "fully above" if its low >= threshold.
    # Open-upper buckets (high=None) count if low >= threshold.
    basket = [
        b for b in buckets
        if b["low"] != float("-inf") and b["low"] >= threshold
    ]
    result["basket"] = basket
    result["basket_yes_prob"] = round(sum(b["yes_price"] for b in basket), 4)

    return result


async def store_bucket_mapping(
    session,
    city: str,
    threshold: float,
    direction: str,
    synthetic_prob: float,
    synthetic_edge: float,
    market_data: dict,
    is_celsius: bool,
    market_date: str = None,
) -> None:
    """
    Write one diagnostic row to bucket_mapping_diagnostics and log one compact line.
    Never raises — all exceptions are swallowed.
    Only runs when BUCKET_MAPPING_ENABLED=True.

    Call this AFTER evaluate_signal returns should_trade=True.
    No effect on trade execution.
    """
    try:
        from models.database import BucketMappingDiagnostic

        unit = "C" if is_celsius else "F"
        prefix = f"[BUCKET] {city} >={threshold:.0f}{unit} {direction}"

        buckets = extract_buckets(market_data)
        if not buckets:
            logger.info(f"{prefix} | parse_fail — no buckets extracted")
            row = BucketMappingDiagnostic(
                city=city, market_date=market_date, threshold=threshold,
                direction=direction, synthetic_prob=round(synthetic_prob, 4),
                synthetic_edge=round(synthetic_edge, 4),
                match_type="parse_fail", is_directly_tradable=False,
                basket_count=0, basket_yes_prob=0.0,
                approximation_note="No buckets extracted from market_data",
                polymarket_market_id=market_data.get("market_id"),
            )
            session.add(row)
            return

        mapping = map_threshold_to_buckets(threshold, buckets, is_celsius)
        match_type = mapping["match_type"]
        basket_yes_prob = mapping["basket_yes_prob"]
        prob_gap = round(abs(synthetic_prob - basket_yes_prob), 4) if basket_yes_prob else None
        nearest_label = mapping["nearest_bucket"]["label"] if mapping["nearest_bucket"] else None

        # One compact log line
        tradable = "✓DIRECT" if mapping["is_directly_tradable"] else "~APPROX"
        logger.info(
            f"{prefix} | {match_type.upper()} {tradable} "
            f"synth={synthetic_prob:.3f} basket={basket_yes_prob:.3f} "
            f"gap={prob_gap:.3f if prob_gap is not None else 'N/A'} "
            f"buckets={len(mapping['basket'])}/{len(buckets)}"
        )

        row = BucketMappingDiagnostic(
            city=city,
            market_date=market_date,
            threshold=threshold,
            direction=direction,
            synthetic_prob=round(synthetic_prob, 4),
            synthetic_edge=round(synthetic_edge, 4),
            match_type=match_type,
            is_directly_tradable=mapping["is_directly_tradable"],
            nearest_bucket_label=nearest_label,
            basket_count=len(mapping["basket"]),
            basket_yes_prob=round(basket_yes_prob, 4),
            prob_gap=prob_gap,
            approximation_note=mapping["approximation_note"][:500] if mapping["approximation_note"] else None,
            polymarket_market_id=market_data.get("market_id"),
        )
        session.add(row)

    except Exception as e:
        logger.debug(f"[BUCKET] store_bucket_mapping error (non-fatal): {e}")
