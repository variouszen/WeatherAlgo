# backend/data/polymarket.py
"""
Polymarket temperature market fetcher.

Polymarket structures temperature markets as BUCKET RANGE markets, not simple
yes/no thresholds. Each market is a group of outcomes like:
  "Highest temperature in NYC on March 8?"
  Outcomes: ["46-47°F", "48-49°F", "50-51°F", "52°F or higher", ...]

Each outcome is a separate token with its own yes/no price.
We fetch all outcomes, parse their bucket ranges, and compute the implied
cumulative probability of the high being >= a given threshold by summing
bucket probabilities for all buckets >= that threshold.

Resolution source: Weather Underground (Wunderground), NOT NWS.
"""

import httpx
import asyncio
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

# How Polymarket titles cities in their weather markets
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

# Whether the city's markets quote in °C (international) or °F (US)
CITY_CELSIUS = {"London", "Seoul"}


# ── Gamma API fetching ────────────────────────────────────────────────────────

async def fetch_weather_markets(client: httpx.AsyncClient) -> list[dict]:
    """
    Fetch all open temperature/weather markets from Gamma API.
    Tries multiple queries to maximize coverage.
    """
    all_markets: list[dict] = []
    seen_ids: set = set()

    search_queries = [
        {"tag_slug": "weather", "limit": 200},
        {"keyword": "highest temperature", "limit": 100},
        {"keyword": "daily temperature", "limit": 100},
    ]

    for params in search_queries:
        try:
            r = await client.get(
                f"{GAMMA_API_BASE}/markets",
                params={"active": "true", "closed": "false", **params},
                headers=HEADERS,
                timeout=15.0,
            )
            r.raise_for_status()
            data = r.json()
            markets = data if isinstance(data, list) else data.get("markets", [])
            new = [m for m in markets if m.get("id") not in seen_ids]
            all_markets.extend(new)
            seen_ids.update(m.get("id") for m in new)
        except Exception as e:
            logger.warning(f"[POLY] Query {params} failed: {e}")

    logger.info(f"[POLY] Total weather markets fetched: {len(all_markets)}")
    return all_markets


# ── City / market matching ────────────────────────────────────────────────────

def match_city(title: str) -> Optional[str]:
    """Match a market title to one of our tracked cities."""
    tl = title.lower()
    for city, aliases in CITY_ALIASES.items():
        if any(alias in tl for alias in aliases):
            return city
    return None


def is_temperature_market(title: str) -> bool:
    """Is this a daily high temperature market (not precipitation, wind, etc)?"""
    tl = title.lower()
    positive = any(kw in tl for kw in [
        "highest temperature", "high temperature", "daily temperature",
        "temperature", "degrees", "°f", "°c",
    ])
    negative = any(kw in tl for kw in [
        "precipitation", "rainfall", "snowfall", "wind", "humidity",
        "monthly", "weekly", "average", "mean",
    ])
    return positive and not negative


# ── Bucket range parsing ──────────────────────────────────────────────────────

def parse_bucket_range(outcome_label: str) -> Optional[tuple]:
    """
    Parse a bucket outcome label into (low, high) temperature bounds.
    Returns (low, None) for open-ended upper buckets ("52°F or higher", "30°C+").
    Returns None if unparseable.

    Examples:
      "46-47°F"       → (46.0, 47.0)
      "48-49°F"       → (48.0, 49.0)
      "52°F or higher" → (52.0, None)
      "Below 40°F"    → (float('-inf'), 40.0)
      "10-12°C"       → (10.0, 12.0)
      "30°C or higher" → (30.0, None)
    """
    label = outcome_label.strip().lower()

    # "below X°F/°C" or "under X"
    m = re.search(r"(?:below|under|less than)\s*(-?\d+(?:\.\d+)?)\s*°?[fc]?", label)
    if m:
        return (float("-inf"), float(m.group(1)))

    # "X or higher / X+" open upper bucket
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*°?[fc]?\s*(?:or higher|or above|\+)", label)
    if m:
        return (float(m.group(1)), None)

    # "X-Y°F" range  (handles negative with optional minus)
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*[-–]\s*(-?\d+(?:\.\d+)?)\s*°?[fc]?", label)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        return (min(lo, hi), max(lo, hi))

    # Single value "46°F" (exact bucket)
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*°?[fc]", label)
    if m:
        v = float(m.group(1))
        return (v, v)

    return None


def compute_cumulative_prob(buckets: list, threshold: float) -> Optional[float]:
    """
    Given a list of parsed bucket dicts with keys {low, high, price},
    compute P(high_temp >= threshold) by summing prices of buckets that
    are entirely at or above the threshold.

    bucket["price"] = market Yes price = market-implied P(this bucket wins).
    """
    if not buckets:
        return None

    total = sum(b["price"] for b in buckets)
    if total < 0.1:
        return None

    prob = 0.0
    for b in buckets:
        lo, hi = b["low"], b["high"]
        p = b["price"] / total  # normalized

        if hi is None:
            # Open upper bucket: entirely >= lo
            if lo >= threshold:
                prob += p
        elif lo == float("-inf"):
            # Sub-threshold (below) bucket: contributes 0
            pass
        else:
            # Normal range bucket [lo, hi]
            if lo >= threshold:
                prob += p  # entire bucket is above threshold
            elif hi >= threshold:
                # Bucket straddles threshold — partial credit
                width = hi - lo
                if width > 0:
                    frac = (hi - threshold) / width
                    prob += p * frac

    return round(min(max(prob, 0.01), 0.99), 4)


# ── CLOB price fetching ───────────────────────────────────────────────────────

async def get_token_midpoint(token_id: str, client: httpx.AsyncClient) -> Optional[float]:
    """Fetch real-time CLOB midpoint price for a token."""
    try:
        r = await client.get(
            f"{CLOB_API_BASE}/midpoint",
            params={"token_id": token_id},
            headers=HEADERS,
            timeout=10.0,
        )
        r.raise_for_status()
        mid = r.json().get("mid")
        return float(mid) if mid else None
    except Exception:
        return None


# ── Main market map builder ───────────────────────────────────────────────────

async def build_market_map(cities: list, thresholds: list) -> dict:
    """
    Fetch all Polymarket weather markets and return:
    {
      (city, threshold): {
        "market_id": str,
        "yes_price": float,      # implied P(high >= threshold) from buckets
        "volume": float,
        "title": str,
        "buckets": list,
        "unit": "F" or "C",
        "price_source": str,
        "end_date": str,
      }
    }

    For US cities thresholds are °F; for London/Seoul they're °C.
    The scanner passes the appropriate list per city.
    """
    async with httpx.AsyncClient() as client:
        markets = await fetch_weather_markets(client)

        market_map: dict = {}

        for mkt in markets:
            title = mkt.get("question") or mkt.get("title") or ""
            if not title:
                continue
            if not is_temperature_market(title):
                continue

            city = match_city(title)
            if not city or city not in cities:
                continue

            unit = "C" if city in CITY_CELSIUS else "F"

            raw_tokens = mkt.get("tokens", [])
            raw_outcomes = mkt.get("outcomes", [])
            outcome_prices = mkt.get("outcomePrices", [])

            buckets = []

            if raw_tokens:
                for i, tok in enumerate(raw_tokens):
                    label = tok.get("outcome", "")
                    parsed = parse_bucket_range(label)
                    if parsed is None:
                        continue
                    lo, hi = parsed

                    price = None
                    tok_id = tok.get("token_id")
                    if tok_id:
                        price = await get_token_midpoint(tok_id, client)
                    if price is None and i < len(outcome_prices):
                        try:
                            price = float(outcome_prices[i])
                        except (ValueError, TypeError):
                            pass
                    if price is None or price <= 0:
                        continue

                    buckets.append({
                        "label": label,
                        "low": lo,
                        "high": hi,
                        "price": price,
                        "token_id": tok_id,
                    })

            elif raw_outcomes and outcome_prices:
                for i, label in enumerate(raw_outcomes):
                    parsed = parse_bucket_range(label)
                    if parsed is None:
                        continue
                    lo, hi = parsed
                    try:
                        price = float(outcome_prices[i])
                    except (ValueError, TypeError, IndexError):
                        continue
                    if price <= 0:
                        continue
                    buckets.append({
                        "label": label,
                        "low": lo,
                        "high": hi,
                        "price": price,
                        "token_id": None,
                    })

            if len(buckets) < 2:
                logger.debug(f"[POLY] Skipping {title[:60]} — only {len(buckets)} parseable buckets")
                continue

            volume = float(mkt.get("volume") or mkt.get("volumeNum") or 0)
            market_id = mkt.get("id", "")

            for thresh in thresholds:
                cum_prob = compute_cumulative_prob(buckets, thresh)
                if cum_prob is None:
                    continue

                key = (city, thresh)
                if key in market_map and market_map[key]["volume"] >= volume:
                    continue

                market_map[key] = {
                    "market_id": market_id,
                    "yes_price": cum_prob,
                    "volume": volume,
                    "title": title,
                    "buckets": buckets,
                    "unit": unit,
                    "price_source": "CLOB+Gamma",
                    "end_date": mkt.get("endDate", ""),
                }

            logger.info(
                f"[POLY] Parsed: {city} | {len(buckets)} buckets | "
                f"Vol=${volume:,.0f} | {title[:55]}"
            )

        logger.info(f"[POLY] Market map built: {len(market_map)} city/threshold pairs")
        if not market_map:
            logger.warning(
                "[POLY] 0 markets matched. Check:\n"
                "  1. Are daily temp markets active on Polymarket today?\n"
                "  2. City aliases match Polymarket's exact phrasing?\n"
                "  3. Bucket outcome format still parseable?"
            )
        return market_map
