# backend/data/polymarket.py
import httpx
import asyncio
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

# Known city name variations Polymarket uses in market titles
CITY_ALIASES = {
    "New York":     ["new york", "nyc", "new york city"],
    "Chicago":      ["chicago"],
    "Seattle":      ["seattle"],
    "Atlanta":      ["atlanta"],
    "Dallas":       ["dallas"],
    "Miami":        ["miami"],
    "Boston":       ["boston"],
    "Philadelphia": ["philadelphia", "philly"],
}

WEATHER_KEYWORDS = ["temperature", "high temp", "highest temp", "degrees", "fahrenheit", "°f"]


async def fetch_weather_markets(client: httpx.AsyncClient) -> list[dict]:
    """
    Pull all open weather/temperature markets from Polymarket Gamma API.
    Returns raw market list.
    """
    try:
        # Gamma API: search for temperature markets
        params = {
            "active": "true",
            "closed": "false",
            "tag_slug": "weather",
            "limit": 200,
        }
        r = await client.get(
            f"{GAMMA_API_BASE}/markets",
            params=params,
            headers=HEADERS,
            timeout=15.0,
        )
        r.raise_for_status()
        markets = r.json()

        # Also try without tag to catch any missed
        params2 = {
            "active": "true",
            "closed": "false",
            "keyword": "temperature",
            "limit": 100,
        }
        r2 = await client.get(
            f"{GAMMA_API_BASE}/markets",
            params=params2,
            headers=HEADERS,
            timeout=15.0,
        )
        if r2.status_code == 200:
            extra = r2.json()
            # Deduplicate by market id
            existing_ids = {m.get("id") for m in markets}
            markets += [m for m in extra if m.get("id") not in existing_ids]

        logger.info(f"[POLY] Fetched {len(markets)} weather markets")
        return markets if isinstance(markets, list) else markets.get("markets", [])

    except Exception as e:
        logger.error(f"[POLY] Market fetch failed: {e}")
        return []


def parse_threshold_from_title(title: str) -> Optional[float]:
    """
    Extract temperature threshold from market title.
    Examples:
      "Will the high temperature in NYC be ≥ 68°F on March 8?"  → 68.0
      "Highest temp in New York: 70°F+" → 70.0
      "NYC high temp above 65 degrees"  → 65.0
    """
    import re
    title_lower = title.lower()

    # Patterns: ≥68, >= 68, 68°f, 68 degrees, 68f
    patterns = [
        r"[≥>=]+\s*(\d{2,3})\s*°?f",
        r"(\d{2,3})\s*°f\s*\+",
        r"(\d{2,3})\s*°f",
        r"(\d{2,3})\s*degrees",
        r"(\d{2,3})f\b",
    ]
    for pat in patterns:
        m = re.search(pat, title_lower)
        if m:
            val = float(m.group(1))
            if 20 <= val <= 120:  # Sanity range for US temps
                return val
    return None


def match_city(title: str) -> Optional[str]:
    """Match market title to one of our tracked cities."""
    title_lower = title.lower()
    for city, aliases in CITY_ALIASES.items():
        if any(alias in title_lower for alias in aliases):
            return city
    return None


def extract_yes_price(market: dict) -> Optional[float]:
    """
    Extract current Yes price from Gamma API market object.
    Gamma returns outcomePrices as list of strings ["0.31", "0.69"]
    where index 0 = Yes, index 1 = No.
    """
    try:
        outcome_prices = market.get("outcomePrices", [])
        if outcome_prices and len(outcome_prices) >= 1:
            return float(outcome_prices[0])

        # Fallback: bestAsk / bestBid fields
        best_ask = market.get("bestAsk")
        if best_ask:
            return float(best_ask)

        return None
    except (ValueError, TypeError):
        return None


def extract_volume(market: dict) -> float:
    """Extract total volume from market object."""
    try:
        vol = market.get("volume") or market.get("volumeNum") or 0
        return float(vol)
    except (ValueError, TypeError):
        return 0.0


async def get_token_price(token_id: str, client: httpx.AsyncClient) -> Optional[float]:
    """
    Get real-time price from CLOB for a specific token.
    More accurate than Gamma for live orderbook mid price.
    """
    try:
        r = await client.get(
            f"{CLOB_API_BASE}/midpoint",
            params={"token_id": token_id},
            headers=HEADERS,
            timeout=10.0,
        )
        r.raise_for_status()
        data = r.json()
        mid = data.get("mid")
        return float(mid) if mid else None
    except Exception:
        return None


async def build_market_map(cities: list[str], thresholds: list[float]) -> dict:
    """
    Fetch all Polymarket weather markets and build a lookup map:
    {(city, threshold): {market_id, token_id, yes_price, volume, title}}

    This is the core function — returns real live prices for every
    city/threshold combo we want to trade.
    """
    async with httpx.AsyncClient() as client:
        markets = await fetch_weather_markets(client)

    market_map = {}
    unmatched = []

    for mkt in markets:
        title = mkt.get("question", "") or mkt.get("title", "")
        if not title:
            continue

        # Skip if not a temperature market
        if not any(kw in title.lower() for kw in WEATHER_KEYWORDS):
            continue

        city = match_city(title)
        if not city or city not in cities:
            continue

        threshold = parse_threshold_from_title(title)
        if not threshold or threshold not in thresholds:
            continue

        yes_price = extract_yes_price(mkt)
        if yes_price is None:
            continue

        volume = extract_volume(mkt)
        market_id = mkt.get("id", "")
        
        # Try to get token id for CLOB price (more precise)
        tokens = mkt.get("tokens", [])
        yes_token_id = None
        if tokens:
            for token in tokens:
                if token.get("outcome", "").lower() == "yes":
                    yes_token_id = token.get("token_id")
                    break

        key = (city, threshold)
        market_map[key] = {
            "market_id": market_id,
            "token_id": yes_token_id,
            "yes_price": yes_price,
            "volume": volume,
            "title": title,
            "end_date": mkt.get("endDate", ""),
        }
        logger.info(f"[POLY] Matched: {city} ≥{threshold}°F | Yes={yes_price:.2f} | Vol=${volume:,.0f} | {title[:60]}")

    # Try to refresh prices via CLOB for better accuracy
    async with httpx.AsyncClient() as client:
        for key, data in market_map.items():
            if data.get("token_id"):
                clob_price = await get_token_price(data["token_id"], client)
                if clob_price:
                    data["yes_price"] = clob_price
                    data["price_source"] = "CLOB"
                else:
                    data["price_source"] = "Gamma"
            else:
                data["price_source"] = "Gamma"

    logger.info(f"[POLY] Market map built: {len(market_map)} city/threshold pairs matched")
    if not market_map:
        logger.warning("[POLY] No markets matched — check if weather markets are active on Polymarket")

    return market_map
