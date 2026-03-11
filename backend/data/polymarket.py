# backend/data/polymarket.py
"""
Polymarket temperature market fetcher.

Temperature markets on Polymarket are structured as EVENTS with multiple
outcome markets (buckets). Each event like "Highest temperature in NYC on March 9?"
contains outcomes like "46-47°F", "48-49°F", "52°F or higher".

The correct API approach is to query the /events endpoint with tag filtering,
then extract the nested markets (outcomes) from each event.
"""

import httpx
import re
import logging
from typing import Optional
from datetime import datetime, timezone, timedelta
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import GAMMA_API_BASE, CLOB_API_BASE, USER_AGENT

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
}

CITY_ALIASES = {
    "New York":      ["new york", "nyc", "new york city"],
    "Chicago":       ["chicago"],
    "Seattle":       ["seattle"],
    "Atlanta":       ["atlanta"],
    "Dallas":        ["dallas"],
    "Miami":         ["miami"],
    "Boston":        ["boston"],
    "Philadelphia":  ["philadelphia", "philly"],
    "London":        ["london"],
    "Seoul":         ["seoul"],
    "Paris":         ["paris"],
    "Toronto":       ["toronto"],
}

CITY_CELSIUS = {"London", "Seoul", "Paris", "Toronto"}

# Slippage haircut: assume you get 2% worse than displayed midpoint
# Makes paper trading closer to live reality on thin markets
SLIPPAGE_HAIRCUT = 0.02

# Minimum number of valid buckets required to trust reconstruction
MIN_VALID_BUCKETS = 3

# Minimum total price mass for a bucket set to be considered liquid
MIN_PRICE_MASS = 0.50


async def fetch_temperature_events(client: httpx.AsyncClient) -> list:
    """
    Fetch temperature events from Gamma API /events endpoint.
    """
    all_events = []
    seen_ids = set()

    queries = [
        {"tag_slug": "daily-temperature", "limit": 200},
        {"tag_slug": "weather", "limit": 200},
        {"tag": "temperature", "limit": 200},
    ]

    for params in queries:
        try:
            r = await client.get(
                f"{GAMMA_API_BASE}/events",
                params={"active": "true", "closed": "false", **params},
                headers=HEADERS,
                timeout=15.0,
            )
            if r.status_code != 200:
                logger.warning(f"[POLY] Events query {params} → HTTP {r.status_code}")
                continue
            data = r.json()
            events = data if isinstance(data, list) else data.get("events", [])
            new = [e for e in events if e.get("id") not in seen_ids and _is_temp_event(e)]
            all_events.extend(new)
            seen_ids.update(e.get("id") for e in new)
            logger.info(f"[POLY] Events query {params} → {len(events)} total, {len(new)} temp events new")
        except Exception as e:
            logger.warning(f"[POLY] Events query {params} failed: {e}")

    # Also try markets endpoint directly
    try:
        r = await client.get(
            f"{GAMMA_API_BASE}/markets",
            params={"active": "true", "closed": "false", "tag_slug": "daily-temperature", "limit": 200},
            headers=HEADERS,
            timeout=15.0,
        )
        if r.status_code == 200:
            data = r.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            logger.info(f"[POLY] Direct markets tag=daily-temperature → {len(markets)} markets")
            for m in markets:
                title = m.get("question") or m.get("groupItemTitle") or ""
                if _is_temp_title(title):
                    all_events.append({"_raw_market": True, **m})
    except Exception as e:
        logger.warning(f"[POLY] Direct markets fetch failed: {e}")

    logger.info(f"[POLY] Total temperature events/markets: {len(all_events)}")
    return all_events


def _is_temp_event(event: dict) -> bool:
    title = (event.get("title") or event.get("question") or "").lower()
    return _is_temp_title(title)


def _is_temp_title(title: str) -> bool:
    tl = title.lower()
    return ("highest temperature" in tl or "daily temperature" in tl or "high temp" in tl)


def extract_market_date(title: str) -> Optional[datetime.date]:
    """
    Extract date from market title like 'Highest temperature in NYC on March 9?'
    """
    m = re.search(
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2})\b',
        title.lower()
    )
    if m:
        month_str, day_str = m.group(1), m.group(2)
        try:
            year = datetime.now(timezone.utc).year
            dt = datetime.strptime(f"{month_str} {day_str} {year}", "%B %d %Y")
            return dt.date()
        except ValueError:
            pass
    return None


def is_valid_market_date(title: str, end_date: str = None) -> bool:
    """
    Return True only if market is still valid to enter.

    Priority:
    1. If endDate exists: use it as the hard filter
       - Reject if already past
       - Reject if within 3 hours of closing (too late to enter)
    2. If endDate missing/malformed: fall back to title date parsing
    3. If both exist: log a warning if they disagree by more than 1 day
    """
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    min_entry_cutoff = now_utc + timedelta(hours=3)

    # Primary: endDate from Polymarket
    if end_date:
        try:
            end_date_str = str(end_date).strip()
            if "T" in end_date_str:
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            else:
                end_dt = datetime(
                    *[int(x) for x in end_date_str[:10].split("-")],
                    23, 59, 59, tzinfo=timezone.utc
                )

            if end_dt <= now_utc:
                logger.debug(f"[POLY] Rejecting expired market (endDate={end_date_str}): '{title[:55]}'")
                return False

            if end_dt <= min_entry_cutoff:
                mins_left = int((end_dt - now_utc).total_seconds() // 60)
                logger.debug(
                    f"[POLY] Rejecting market closing too soon "
                    f"({mins_left}min left, need 3h): '{title[:55]}'"
                )
                return False

            # Sanity-check against title date
            title_date = extract_market_date(title)
            if title_date:
                end_date_only = end_dt.date()
                day_diff = abs((end_date_only - title_date).days)
                if day_diff > 1:
                    logger.warning(
                        f"[POLY] Date mismatch: title says {title_date} but "
                        f"endDate implies {end_date_only} (diff={day_diff}d): '{title[:55]}'"
                    )

            return True

        except Exception as e:
            logger.warning(f"[POLY] Could not parse endDate '{end_date}': {e} — falling back to title")

    # Fallback: title date parsing
    market_date = extract_market_date(title)
    if market_date is None:
        logger.debug(f"[POLY] Could not parse date from title: '{title[:60]}'")
        return False

    max_date = today + timedelta(days=5)
    valid = today <= market_date <= max_date
    if not valid:
        logger.debug(f"[POLY] Skipping stale/future market: '{title[:60]}' (date={market_date})")
    return valid


def match_city(title: str) -> Optional[str]:
    tl = title.lower()
    for city, aliases in CITY_ALIASES.items():
        if any(alias in tl for alias in aliases):
            return city
    return None


def parse_bucket_range(label: str) -> Optional[tuple]:
    """
    Parse bucket outcome label into (low, high).
    Returns (low, None) for open upper, (float('-inf'), high) for open lower.
    """
    s = label.strip().lower()

    # "below X" / "under X" / "less than X"
    m = re.search(r"(?:below|under|less than)\s*(-?\d+(?:\.\d+)?)", s)
    if m:
        return (float("-inf"), float(m.group(1)))

    # "X or higher" / "X+" / "X and above"
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*(?:°?[fc])?\s*(?:or higher|or above|and above|\+|&\s*above)", s)
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


def validate_bucket_set(buckets: list) -> tuple[bool, str]:
    """
    Quality check before trusting a synthetic cumulative reconstruction.
    Returns (is_valid, reason).
    """
    if len(buckets) < MIN_VALID_BUCKETS:
        return False, f"Only {len(buckets)} buckets (need {MIN_VALID_BUCKETS})"

    total = sum(b["price"] for b in buckets)
    if total < MIN_PRICE_MASS:
        return False, f"Price mass {total:.2f} too low (need {MIN_PRICE_MASS})"

    # Check for at least one bounded range bucket (not just open-ended)
    has_bounded = any(
        b["low"] != float("-inf") and b["high"] is not None
        for b in buckets
    )
    if not has_bounded:
        return False, "No bounded-range buckets found"

    return True, "OK"


def compute_cumulative_prob(buckets: list, threshold: float) -> Optional[float]:
    """
    Compute P(outcome >= threshold) from Polymarket bucket prices.
    Applies slippage haircut to the final probability to model real fill cost.
    """
    if not buckets:
        return None

    total = sum(b["price"] for b in buckets)
    if total < 0.05:
        return None

    prob = 0.0
    for b in buckets:
        lo, hi = b["low"], b["high"]
        p = b["price"] / total  # normalize

        if hi is None:  # open upper bucket: everything above lo counts
            if lo >= threshold:
                prob += p
        elif lo == float("-inf"):  # open lower bucket: nothing above threshold here
            pass
        else:
            if lo >= threshold:
                prob += p  # full bucket above threshold
            elif hi >= threshold:
                # Partial bucket: assume uniform distribution within range
                width = hi - lo
                if width > 0:
                    prob += p * (hi - threshold) / width

    # Apply slippage haircut: the side you're buying gets slightly worse price
    # YES trade: prob is slightly lower than displayed (market asks more)
    # NO trade: handled in scanner via (1 - prob)
    # We conservatively haircut the displayed YES probability
    prob_with_slippage = prob * (1 - SLIPPAGE_HAIRCUT)

    return round(min(max(prob_with_slippage, 0.01), 0.99), 4)


async def get_token_midpoint(token_id: str, client: httpx.AsyncClient) -> Optional[float]:
    try:
        r = await client.get(
            f"{CLOB_API_BASE}/midpoint",
            params={"token_id": token_id},
            headers=HEADERS,
            timeout=8.0,
        )
        if r.status_code == 200:
            mid = r.json().get("mid")
            return float(mid) if mid else None
    except Exception:
        pass
    return None


async def build_market_map(cities: list, thresholds: list) -> dict:
    """
    Build {(city, threshold): market_data} from Polymarket temperature events.
    """
    async with httpx.AsyncClient() as client:
        events = await fetch_temperature_events(client)

        market_map = {}

        for event in events:
            title = event.get("title") or event.get("question") or ""
            if not _is_temp_title(title):
                continue

            city = match_city(title)
            if not city or city not in cities:
                continue

            end_date = event.get("endDate") or event.get("end_date_iso") or event.get("end_date")
            if not is_valid_market_date(title, end_date=end_date):
                continue

            unit = "C" if city in CITY_CELSIUS else "F"

            nested_markets = event.get("markets", [])
            if event.get("_raw_market"):
                nested_markets = [event]

            if not nested_markets:
                logger.debug(f"[POLY] Event '{title[:50]}' has no nested markets")
                continue

            buckets = []
            total_volume = 0.0
            market_id = event.get("id", "")
            end_date = event.get("endDate", "")

            for nm in nested_markets:
                bucket_label = nm.get("groupItemTitle") or nm.get("question") or nm.get("title") or ""
                if not bucket_label:
                    continue

                parsed = parse_bucket_range(bucket_label)
                if parsed is None:
                    logger.debug(f"[POLY] Unparseable bucket: '{bucket_label}'")
                    continue

                lo, hi = parsed

                # Get price from outcomePrices or CLOB
                price = None
                outcome_prices = nm.get("outcomePrices", "[]")
                if isinstance(outcome_prices, str):
                    import json
                    try:
                        outcome_prices = json.loads(outcome_prices)
                    except Exception:
                        outcome_prices = []

                if outcome_prices and len(outcome_prices) > 0:
                    try:
                        price = float(outcome_prices[0])
                    except (ValueError, TypeError):
                        pass

                clob_ids = nm.get("clobTokenIds", "[]")
                if isinstance(clob_ids, str):
                    import json
                    try:
                        clob_ids = json.loads(clob_ids)
                    except Exception:
                        clob_ids = []

                if clob_ids:
                    clob_price = await get_token_midpoint(clob_ids[0], client)
                    if clob_price is not None:
                        price = clob_price

                if price is None or price <= 0 or price >= 1:
                    continue

                vol = float(nm.get("volumeNum") or nm.get("volume") or 0)
                total_volume += vol

                buckets.append({
                    "label": bucket_label,
                    "low": lo,
                    "high": hi,
                    "price": price,
                })

            # ── Bucket quality check before using this market ─────────────────
            is_valid, reason = validate_bucket_set(buckets)
            if not is_valid:
                logger.warning(f"[POLY] Skipping '{title[:50]}' — {reason}")
                continue

            logger.info(
                f"[POLY] Matched: {city} | {len(buckets)} buckets | "
                f"Vol=${total_volume:,.0f} | {title[:55]}"
            )

            for thresh in thresholds:
                cum_prob = compute_cumulative_prob(buckets, thresh)
                if cum_prob is None:
                    continue
                key = (city, thresh)
                if key in market_map and market_map[key]["volume"] >= total_volume:
                    continue
                market_map[key] = {
                    "market_id": str(market_id),
                    "yes_price": cum_prob,
                    "volume": total_volume,
                    "title": title,
                    "buckets": buckets,
                    "unit": unit,
                    "price_source": "CLOB+Gamma",
                    "end_date": end_date,
                    "bucket_count": len(buckets),
                }

        logger.info(f"[POLY] Market map: {len(market_map)} city/threshold pairs")
        if not market_map:
            logger.warning("[POLY] 0 markets matched — check API structure")

        return market_map
