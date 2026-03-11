# backend/data/polymarket.py
"""
Polymarket temperature market fetcher — v3 (slug-based, date-keyed, direct-threshold only).

Four key design decisions vs v2:
  1. Key is (city, market_date_str, threshold) — prevents today/tomorrow collision.
  2. Only thresholds that directly align with a real bucket lower-bound are populated.
     No synthetic interpolation across unsupported thresholds ever fires a signal.
  3. The kill-switch is per-bucket (skip that bucket) not per-event.
     Legitimate tail buckets can be cheap; skip the bucket, not the whole event.
  4. build_market_map() returns (market_map, city_date_map) so scanner knows which
     market_date was selected per city without scanning keys.

No tag-based fallback. If slug fetch fails or validation fails → city/date skipped entirely.
"""

import httpx
import re
import json
import logging
from typing import Optional
from datetime import datetime, date, timezone, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import GAMMA_API_BASE, CLOB_API_BASE, USER_AGENT

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
}

# ── City → Polymarket slug token ──────────────────────────────────────────────
CITY_SLUGS: dict[str, str] = {
    "New York":     "nyc",
    "Chicago":      "chicago",
    "Seattle":      "seattle",
    "Atlanta":      "atlanta",
    "Dallas":       "dallas",
    "Miami":        "miami",
    "Boston":       "boston",
    "Philadelphia": "philadelphia",
    "London":       "london",
    "Seoul":        "seoul",
    "Paris":        "paris",
    "Toronto":      "toronto",
}

CITY_TITLE_TOKENS: dict[str, list[str]] = {
    "New York":     ["nyc", "new york"],
    "Chicago":      ["chicago"],
    "Seattle":      ["seattle"],
    "Atlanta":      ["atlanta"],
    "Dallas":       ["dallas"],
    "Miami":        ["miami"],
    "Boston":       ["boston"],
    "Philadelphia": ["philadelphia", "philly"],
    "London":       ["london"],
    "Seoul":        ["seoul"],
    "Paris":        ["paris"],
    "Toronto":      ["toronto"],
}

CITY_CELSIUS: set[str] = {"London", "Seoul", "Paris", "Toronto"}

SLIPPAGE_HAIRCUT = 0.02
MIN_VALID_BUCKETS = 3
MIN_PRICE_MASS = 0.50
MIN_BUCKET_PRICE = 0.02   # per-bucket floor — skip bucket, not event
MAX_FORWARD_DAYS = 3      # try today, +1, +2


# ── Slug construction ─────────────────────────────────────────────────────────

def build_slug(city: str, target_date: date) -> Optional[str]:
    """
    "New York" + 2026-03-11  →  "highest-temperature-in-nyc-on-march-11-2026"
    Returns None if city has no slug mapping.
    """
    city_slug = CITY_SLUGS.get(city)
    if not city_slug:
        return None
    month = target_date.strftime("%B").lower()
    day   = target_date.day
    year  = target_date.year
    return f"highest-temperature-in-{city_slug}-on-{month}-{day}-{year}"


# ── Event fetch + strict validation ──────────────────────────────────────────

async def fetch_event_by_slug(
    city: str,
    target_date: date,
    client: httpx.AsyncClient,
) -> tuple[Optional[dict], str]:
    """
    Fetch and fully validate the Polymarket temperature event for city + target_date.
    Returns (event_dict, "") on success; (None, reason) on failure.
    Never substitutes another market.
    """
    slug = build_slug(city, target_date)
    if not slug:
        return None, f"No slug mapping for city '{city}'"

    url = f"{GAMMA_API_BASE}/events/slug/{slug}"
    try:
        r = await client.get(url, headers=HEADERS, timeout=15.0)
    except Exception as e:
        return None, f"HTTP request failed: {e}"

    if r.status_code == 404:
        return None, f"Slug not found (404): {slug}"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code} for slug: {slug}"

    try:
        event = r.json()
        if isinstance(event, list):
            event = event[0] if event else {}
    except Exception as e:
        return None, f"JSON parse error: {e}"

    if not isinstance(event, dict) or not event:
        return None, f"Empty or malformed response for slug: {slug}"

    ok, reason = _validate_event(event, city, target_date)
    if not ok:
        return None, reason

    return event, ""


def _validate_event(event: dict, city: str, target_date: date) -> tuple[bool, str]:
    """
    All gates must pass — any failure rejects the event entirely.
    """
    title       = (event.get("title") or "").strip()
    title_lower = title.lower()

    # 1. Title must reference the city
    city_tokens = CITY_TITLE_TOKENS.get(city, [city.lower()])
    if not any(tok in title_lower for tok in city_tokens):
        return False, f"Title '{title[:60]}' does not mention city '{city}'"

    # 2. Title must be a temperature market
    if "temperature" not in title_lower:
        return False, f"Title '{title[:60]}' is not a temperature market"

    # 3. Active and not closed
    if not event.get("active", False):
        return False, "Event is not active"
    if event.get("closed", True):
        return False, "Event is closed"

    # 4. endDate: must exist, parseable, not expired, not closing < 3h, match target_date
    end_date_raw = event.get("endDate") or event.get("end_date")
    if not end_date_raw:
        return False, "Event has no endDate"

    try:
        end_date_str = str(end_date_raw).strip()
        if "T" in end_date_str:
            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        else:
            parts = end_date_str[:10].split("-")
            end_dt = datetime(
                int(parts[0]), int(parts[1]), int(parts[2]),
                23, 59, 59, tzinfo=timezone.utc,
            )
    except Exception as e:
        return False, f"Cannot parse endDate '{end_date_raw}': {e}"

    now_utc = datetime.now(timezone.utc)
    if end_dt <= now_utc:
        return False, f"Event already closed (endDate={end_date_raw})"

    mins_left = (end_dt - now_utc).total_seconds() / 60
    if mins_left < 180:
        return False, f"Event closes too soon ({int(mins_left)}min left, need ≥180min)"

    diff = abs((end_dt.date() - target_date).days)
    if diff > 1:
        return False, (
            f"endDate {end_dt.date()} doesn't match target_date {target_date} (diff={diff}d)"
        )

    # 5. Must have nested bucket markets
    markets = event.get("markets", [])
    if not markets:
        return False, "Event has no nested markets"

    # 6. Every nested market label must parse as a temperature bucket
    for m in markets:
        label = (
            m.get("groupItemTitle") or m.get("question") or m.get("title") or ""
        ).strip()
        if not label:
            return False, "A nested market has an empty label"
        if parse_bucket_range(label) is None:
            return False, f"Nested market '{label[:50]}' is not a parseable temperature bucket"

    return True, ""


# ── Bucket label parser ───────────────────────────────────────────────────────

def parse_bucket_range(label: str) -> Optional[tuple]:
    """
    Parse bucket outcome label into (low, high).
    Open-upper: (lo, None). Open-lower: (float('-inf'), hi). Range: (lo, hi).
    Returns None if not parseable.
    """
    s = label.strip().lower()

    # "below X" / "under X" / "less than X"
    m = re.search(r"(?:below|under|less than)\s*(-?\d+(?:\.\d+)?)", s)
    if m:
        return (float("-inf"), float(m.group(1)))

    # "X or below" / "X or lower" / "X and below" / "X and under"
    m = re.search(
        r"(-?\d+(?:\.\d+)?)\s*(?:°?[fc])?\s*"
        r"(?:or below|or lower|or under|and below|and under)",
        s,
    )
    if m:
        return (float("-inf"), float(m.group(1)))

    # "X or higher" / "X+" / "X and above" / "X or above"
    m = re.search(
        r"(-?\d+(?:\.\d+)?)\s*(?:°?[fc])?\s*"
        r"(?:or higher|or above|and above|\+|&\s*above)",
        s,
    )
    if m:
        return (float(m.group(1)), None)

    # "X-Y" range
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)", s)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        return (min(lo, hi), max(lo, hi))

    # Single value "46°f"
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*°?[fc]", s)
    if m:
        v = float(m.group(1))
        return (v, v)

    # Bare number
    m = re.search(r"^(-?\d+(?:\.\d+)?)$", s.strip())
    if m:
        v = float(m.group(1))
        return (v, v)

    return None


# ── Direct-threshold extraction ───────────────────────────────────────────────

def get_direct_thresholds(buckets: list, candidate_thresholds: list) -> list:
    """
    Return only the thresholds from candidate_thresholds that exactly equal a
    real bucket lower-bound. Everything else requires synthetic interpolation
    and is excluded to prevent fake edges.

    Example:
        Buckets: [(-inf,56), (56,57), (57,58), (58,59), (59,None)]
        Candidates: [55, 56, 57, 58, 59, 60, 65]
        Returns:    [56, 57, 58, 59]
    """
    bucket_lower_bounds = {
        b["low"] for b in buckets
        if b["low"] != float("-inf")
    }
    return [t for t in candidate_thresholds if t in bucket_lower_bounds]


# ── Price helpers ─────────────────────────────────────────────────────────────

async def get_token_midpoint(token_id: str, client: httpx.AsyncClient) -> Optional[float]:
    """Fetch CLOB midpoint for a token. Returns None on any failure."""
    try:
        r = await client.get(
            f"{CLOB_API_BASE}/midpoint",
            params={"token_id": token_id},
            headers=HEADERS,
            timeout=8.0,
        )
        if r.status_code == 200:
            mid = r.json().get("mid")
            return float(mid) if mid is not None else None
    except Exception:
        pass
    return None


def _parse_json_field(raw) -> list:
    """Safely parse JSON string or list field."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            pass
    return []


async def _extract_bucket_price(
    nested_market: dict,
    client: httpx.AsyncClient,
) -> Optional[float]:
    """
    Get the best YES price for a bucket.
    Priority: CLOB midpoint > outcomePrices snapshot.
    Returns None if price is missing, invalid, or below MIN_BUCKET_PRICE.
    Note: MIN_BUCKET_PRICE rejection skips this bucket only, not the event.
    """
    price = None

    clob_ids = _parse_json_field(nested_market.get("clobTokenIds", "[]"))
    if clob_ids:
        clob_price = await get_token_midpoint(clob_ids[0], client)
        if clob_price is not None:
            price = clob_price

    if price is None:
        outcome_prices = _parse_json_field(nested_market.get("outcomePrices", "[]"))
        if outcome_prices:
            try:
                price = float(outcome_prices[0])
            except (ValueError, TypeError):
                pass

    if price is None or price <= 0 or price >= 1:
        return None

    if price < MIN_BUCKET_PRICE:
        return None   # skip this bucket; caller decides if remaining buckets are still valid

    return price


# ── Bucket quality gate ───────────────────────────────────────────────────────

def validate_bucket_set(buckets: list) -> tuple[bool, str]:
    """
    Quality check on price-filtered bucket set.
    Returns (is_valid, reason).
    """
    if len(buckets) < MIN_VALID_BUCKETS:
        return False, f"Only {len(buckets)} valid buckets after price filtering (need {MIN_VALID_BUCKETS})"

    total = sum(b["price"] for b in buckets)
    if total < MIN_PRICE_MASS:
        return False, f"Price mass {total:.2f} too low (need {MIN_PRICE_MASS})"

    has_bounded = any(
        b["low"] != float("-inf") and b["high"] is not None
        for b in buckets
    )
    if not has_bounded:
        return False, "No bounded-range buckets found"

    return True, "OK"


# ── Cumulative probability ────────────────────────────────────────────────────

def compute_cumulative_prob(buckets: list, threshold: float) -> Optional[float]:
    """
    Compute P(outcome >= threshold) from bucket prices.
    Called ONLY for thresholds confirmed as real bucket lower-bounds
    (via get_direct_thresholds). The partial-bucket interpolation branch
    only fires when the matching bucket is bounded (not open-upper), which
    is sound because the boundary alignment is exact.
    """
    if not buckets:
        return None

    total = sum(b["price"] for b in buckets)
    if total < 0.05:
        return None

    prob = 0.0
    for b in buckets:
        lo, hi = b["low"], b["high"]
        p = b["price"] / total

        if hi is None:
            if lo >= threshold:
                prob += p
        elif lo == float("-inf"):
            pass
        else:
            if lo >= threshold:
                prob += p
            elif hi > threshold:
                width = hi - lo
                if width > 0:
                    prob += p * (hi - threshold) / width

    prob_with_slippage = prob * (1 - SLIPPAGE_HAIRCUT)
    return round(min(max(prob_with_slippage, 0.01), 0.99), 4)


# ── Main entry point ──────────────────────────────────────────────────────────

async def build_market_map(
    cities: list[str],
    thresholds: list[float],
) -> tuple[dict, dict]:
    """
    Returns:
        market_map    {(city, market_date_str, threshold): market_data}
        city_date_map {city: market_date_str}

    Key uses 3-tuple to prevent date collision. Only direct-threshold entries
    are added — no synthetic interpolation fires from this map.
    Takes soonest valid market per city (breaks after first success).
    """
    utc_today = datetime.now(timezone.utc).date()
    market_map: dict = {}
    city_date_map: dict[str, str] = {}

    async with httpx.AsyncClient() as client:
        for city in cities:
            if city not in CITY_SLUGS:
                logger.warning(f"[POLY] No slug mapping for '{city}' — skipping")
                continue

            unit = "C" if city in CITY_CELSIUS else "F"

            for day_offset in range(MAX_FORWARD_DAYS):
                target_date = utc_today + timedelta(days=day_offset)
                event, rejection = await fetch_event_by_slug(city, target_date, client)

                if event is None:
                    logger.info(f"[POLY] SKIP {city}/{target_date} | {rejection}")
                    continue

                title           = event.get("title", "").strip()
                end_date        = event.get("endDate") or event.get("end_date")
                event_id        = str(event.get("id", ""))
                slug            = build_slug(city, target_date)
                market_date_str = target_date.isoformat()
                nested_markets  = event.get("markets", [])

                # ── Parse buckets ─────────────────────────────────────────────
                buckets: list = []
                total_volume = 0.0

                for nm in nested_markets:
                    label = (
                        nm.get("groupItemTitle")
                        or nm.get("question")
                        or nm.get("title")
                        or ""
                    ).strip()
                    if not label:
                        continue

                    parsed = parse_bucket_range(label)
                    if parsed is None:
                        logger.debug(f"[POLY] Unparseable bucket '{label}' — skip bucket")
                        continue

                    lo, hi = parsed
                    price = await _extract_bucket_price(nm, client)
                    if price is None:
                        logger.debug(
                            f"[POLY] No valid price for '{label}' ({city}/{target_date}) — skip bucket"
                        )
                        continue

                    clob_ids = _parse_json_field(nm.get("clobTokenIds", "[]"))
                    token_id = clob_ids[0] if clob_ids else None
                    vol      = float(nm.get("volumeNum") or nm.get("volume") or 0)
                    total_volume += vol

                    buckets.append({
                        "label":    label,
                        "low":      lo,
                        "high":     hi,
                        "price":    price,
                        "token_id": token_id,
                    })

                # ── Quality gate ──────────────────────────────────────────────
                is_valid, reason = validate_bucket_set(buckets)
                if not is_valid:
                    logger.warning(
                        f"[POLY] BUCKET FAIL {city}/{target_date} | {reason} | '{title[:55]}'"
                    )
                    continue

                # ── Direct-threshold filtering ────────────────────────────────
                direct_thresholds = get_direct_thresholds(buckets, thresholds)
                if not direct_thresholds:
                    logger.warning(
                        f"[POLY] No direct threshold matches for {city}/{target_date} "
                        f"| bucket bounds: "
                        f"{sorted(b['low'] for b in buckets if b['low'] != float('-inf'))} "
                        f"| candidates (first 8): {thresholds[:8]}"
                    )
                    continue

                logger.info(
                    f"[POLY] ✓ {city}/{market_date_str} | {len(buckets)} buckets | "
                    f"{len(direct_thresholds)} direct thresholds | "
                    f"EventVol=${event_volume:,.0f} BucketsVol=${total_volume:,.0f} | slug={slug}"
                )

                # ── Populate market_map ───────────────────────────────────────
                for thresh in direct_thresholds:
                    cum_prob = compute_cumulative_prob(buckets, thresh)
                    if cum_prob is None:
                        continue
                    key = (city, market_date_str, thresh)
                    market_map[key] = {
                        "market_id":         event_id,
                        "event_slug":        slug,
                        "event_title":       title,
                        "yes_price":         cum_prob,
                        "volume":            total_volume,
                        "unit":              unit,
                        "end_date":          end_date,
                        "bucket_count":      len(buckets),
                        "buckets":           buckets,
                        "price_source":      "CLOB+Gamma/slug",
                        "direct_thresholds": direct_thresholds,
                    }

                city_date_map[city] = market_date_str
                break   # soonest valid date wins — never try later offsets for this city

            if city not in city_date_map:
                logger.info(
                    f"[POLY] No valid event for '{city}' in next {MAX_FORWARD_DAYS} days"
                )

    logger.info(
        f"[POLY] Market map: {len(market_map)} entries across {len(city_date_map)} cities"
    )
    if not market_map:
        logger.warning("[POLY] 0 markets — verify slug patterns and threshold config")

    return market_map, city_date_map
