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
}

CITY_CELSIUS = {"London", "Seoul"}


async def fetch_temperature_events(client: httpx.AsyncClient) -> list:
    """
    Fetch temperature events from Gamma API /events endpoint.
    Events contain nested markets (the individual bucket outcomes).
    """
    all_events = []
    seen_ids = set()

    # Query events endpoint with temperature-related tags
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

    # Also try fetching markets directly with negRisk flag (temperature markets use negRisk)
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
            # Wrap each market as a pseudo-event for unified processing
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


def match_city(title: str) -> Optional[str]:
    tl = title.lower()
    for city, aliases in CITY_ALIASES.items():
        if any(alias in tl for alias in aliases):
            return city
    return None


def parse_bucket_range(label: str) -> Optional[tuple]:
    """
    Parse bucket outcome label into (low, high).
    Returns (low, None) for open upper, (None, high) would be open lower.
    """
    s = label.strip().lower()

    # "below X" / "under X"
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

    # Single value "46°f" or "46°c"
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


def compute_cumulative_prob(buckets: list, threshold: float) -> Optional[float]:
    if not buckets:
        return None
    total = sum(b["price"] for b in buckets)
    if total < 0.05:
        return None

    prob = 0.0
    for b in buckets:
        lo, hi = b["low"], b["high"]
        p = b["price"] / total

        if hi is None:  # open upper bucket
            if lo >= threshold:
                prob += p
        elif lo == float("-inf"):  # open lower bucket
            pass
        else:
            if lo >= threshold:
                prob += p
            elif hi >= threshold:
                width = hi - lo
                if width > 0:
                    prob += p * (hi - threshold) / width

    return round(min(max(prob, 0.01), 0.99), 4)


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
            # Get the event title
            title = event.get("title") or event.get("question") or ""
            if not _is_temp_title(title):
                continue

            city = match_city(title)
            if not city or city not in cities:
                continue

            unit = "C" if city in CITY_CELSIUS else "F"

            # Get nested markets (outcomes/buckets) from event
            nested_markets = event.get("markets", [])

            # If this is a raw market (not an event), treat it differently
            if event.get("_raw_market"):
                nested_markets = [event]

            if not nested_markets:
                logger.debug(f"[POLY] Event '{title[:50]}' has no nested markets")
                continue

            # Each nested market IS a bucket outcome
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

                # Get price from outcomePrices[0] (Yes price) or clobTokenIds
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

                # Try CLOB for better price
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

            if len(buckets) < 2:
                logger.debug(f"[POLY] '{title[:50]}' — only {len(buckets)} valid buckets, skipping")
                continue

            logger.info(f"[POLY] Matched: {city} | {len(buckets)} buckets | Vol=${total_volume:,.0f} | {title[:55]}")

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
                }

        logger.info(f"[POLY] Market map: {len(market_map)} city/threshold pairs")
        if not market_map:
            logger.warning("[POLY] 0 markets matched — temperature events may use a different API structure today")

        return market_map
