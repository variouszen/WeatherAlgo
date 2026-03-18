"""
WeatherAlgo v2 — Polymarket Venue Adapter

Implements VenueAdapter for Polymarket's CLOB (Central Limit Order Book).

Discovery: Gamma API slug-based fetch → parse buckets → CLOB prices
Pricing: CLOB get_price(side=BUY) = truth, get_order_book() = depth
Execution: py-clob-client with neg_risk=True, tick_size="0.001"
Settlement: Polymarket resolution check (binary outcome prices)
Paper mode: DRY_RUN=true logs order to DB, skips CLOB submission

Reuses from backend/data/polymarket.py:
  - build_slug(), parse_bucket_range(), fetch_event_by_slug(), validate_bucket_set()
  - _parse_json_field(), check_event_resolution()
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

import httpx

# ── Path setup (same pattern as existing backend modules) ─────────────────────
_this_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir = os.path.dirname(_this_dir)
_backend_dir = os.path.join(_root_dir, "backend")

if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)
if _root_dir not in sys.path:
    sys.path.insert(0, _root_dir)

from config import GAMMA_API_BASE, CLOB_API_BASE, USER_AGENT, DRY_RUN
from data.polymarket import (
    build_slug,
    parse_bucket_range,
    fetch_event_by_slug,
    validate_bucket_set,
    check_event_resolution,
    _parse_json_field,
    CITY_SLUGS,
)

from venue.base import (
    VenueAdapter,
    BucketMarket,
    OrderBook,
    OrderBookLevel,
    OrderResult,
    SettlementResult,
)

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
}


class PolymarketAdapter(VenueAdapter):
    """
    Polymarket implementation of VenueAdapter.
    
    Uses Gamma API for market discovery and CLOB API for live pricing.
    All prices use get_price(side=BUY) as truth per spec decision #4.
    """
    
    def __init__(self, dry_run: bool = None):
        self.dry_run = dry_run if dry_run is not None else DRY_RUN
        self._client = None  # Lazy init for async context
    
    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create an async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=HEADERS,
                timeout=15.0,
            )
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
    
    # ── Market Discovery ──────────────────────────────────────────────────────
    
    async def discover_markets(
        self,
        city: str,
        target_date: date,
        celsius: bool = False,
    ) -> Optional[List[BucketMarket]]:
        """
        Fetch Polymarket temperature event for city+date and return
        standardized BucketMarket objects with live CLOB prices.
        
        Returns ordered list sorted by bucket_low, or None if no valid market.
        """
        client = await self._get_client()
        
        # Fetch event via existing validated slug logic
        event, rejection = await fetch_event_by_slug(city, target_date, client)
        if event is None:
            logger.info(f"[Adapter] SKIP {city}/{target_date}: {rejection}")
            return None
        
        nested_markets = event.get("markets", [])
        if not nested_markets:
            logger.warning(f"[Adapter] {city}/{target_date}: no nested markets")
            return None
        
        # Parse all buckets and fetch CLOB prices
        raw_buckets = []
        
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
                logger.debug(f"[Adapter] Unparseable bucket: '{label[:60]}'")
                continue
            
            lo, hi = parsed
            
            # Extract token IDs
            clob_ids = _parse_json_field(nm.get("clobTokenIds", "[]"))
            if len(clob_ids) < 2:
                logger.debug(f"[Adapter] Missing token IDs for '{label[:40]}'")
                continue
            
            yes_token = clob_ids[0]
            no_token = clob_ids[1]
            
            # Gamma snapshot prices (stale, for comparison logging)
            outcome_prices = _parse_json_field(nm.get("outcomePrices", "[]"))
            gamma_yes = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.0
            gamma_no = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.0
            
            # Volume
            vol = float(nm.get("volumeNum") or nm.get("volume") or 0)
            
            # Market/condition IDs
            condition_id = nm.get("conditionId", "")
            market_id = str(nm.get("id", ""))
            
            raw_buckets.append({
                "label": label,
                "low": lo,
                "high": hi,
                "yes_token": yes_token,
                "no_token": no_token,
                "gamma_yes": gamma_yes,
                "gamma_no": gamma_no,
                "volume": vol,
                "condition_id": condition_id,
                "market_id": market_id,
            })
        
        if not raw_buckets:
            logger.warning(f"[Adapter] {city}/{target_date}: no parseable buckets")
            return None
        
        # Sort by lower bound (treat None/open-lower as -inf)
        raw_buckets.sort(key=lambda b: b["low"] if b["low"] != float("-inf") and b["low"] is not None else -9999)
        
        # Fetch live CLOB prices for all buckets
        bucket_markets = []
        for idx, rb in enumerate(raw_buckets):
            # YES ask price (truth per spec)
            yes_ask = await self.get_ask_price(rb["yes_token"], side="BUY")
            if yes_ask is None:
                # Fallback to Gamma snapshot with penalty
                if rb["gamma_yes"] > 0:
                    yes_ask = rb["gamma_yes"] + max(0.01, rb["gamma_yes"] * 0.05)
                    logger.debug(
                        f"[Adapter] {rb['label']}: CLOB unavailable, "
                        f"using stale Gamma+penalty={yes_ask:.3f}"
                    )
                else:
                    yes_ask = 0.0
            
            # NO ask price
            no_ask = await self.get_ask_price(rb["no_token"], side="BUY")
            if no_ask is None:
                no_ask = rb["gamma_no"] + max(0.01, rb["gamma_no"] * 0.05) if rb["gamma_no"] > 0 else 0.0
            
            bucket_markets.append(BucketMarket(
                bucket_label=rb["label"],
                bucket_index=idx,
                bucket_low=rb["low"] if rb["low"] != float("-inf") else None,
                bucket_high=rb["high"],
                yes_token_id=rb["yes_token"],
                no_token_id=rb["no_token"],
                ask_price=yes_ask,
                bid_price=0.0,  # Unknown at discovery; resolved by order book in fill simulation
                no_ask_price=no_ask,
                volume=rb["volume"],
                venue="polymarket",
                gamma_yes_price=rb["gamma_yes"],
                gamma_no_price=rb["gamma_no"],
                condition_id=rb["condition_id"],
                market_id=rb["market_id"],
            ))
        
        if not bucket_markets:
            return None
        
        logger.info(
            f"[Adapter] {city}/{target_date}: {len(bucket_markets)} buckets discovered, "
            f"price range {bucket_markets[0].ask_price:.3f}-{bucket_markets[-1].ask_price:.3f}"
        )
        
        return bucket_markets
    
    # ── Pricing ───────────────────────────────────────────────────────────────
    
    async def get_ask_price(
        self,
        token_id: str,
        side: str = "BUY",
    ) -> Optional[float]:
        """
        Get live best ask from CLOB get_price(side=BUY).
        This is the truth price per spec decision #4.
        
        CLOB returns prices as strings (e.g., {'price': '0.011'}).
        """
        client = await self._get_client()
        try:
            resp = await client.get(
                f"{CLOB_API_BASE}/price",
                params={"token_id": token_id, "side": side},
                timeout=10.0,
            )
            if resp.status_code != 200:
                return None
            
            data = resp.json()
            price_str = data.get("price")
            if price_str is not None:
                price = float(price_str)
                if 0.0 < price < 1.0:
                    return price
            return None
            
        except Exception as e:
            logger.debug(f"[Adapter] get_ask_price failed for {token_id[:16]}...: {e}")
            return None
    
    async def get_order_book(
        self,
        token_id: str,
    ) -> Optional[OrderBook]:
        """
        Get full order book from CLOB /book endpoint.
        
        CLOB returns price/size as strings — converts to float.
        Asks sorted ascending (best ask first).
        Bids sorted descending (best bid first).
        """
        client = await self._get_client()
        try:
            resp = await client.get(
                f"{CLOB_API_BASE}/book",
                params={"token_id": token_id},
                timeout=10.0,
            )
            if resp.status_code != 200:
                return None
            
            data = resp.json()
            
            asks = []
            for a in data.get("asks", []):
                try:
                    asks.append(OrderBookLevel(
                        price=float(a["price"]),
                        size=float(a["size"]),
                    ))
                except (KeyError, ValueError, TypeError):
                    continue
            
            bids = []
            for b in data.get("bids", []):
                try:
                    bids.append(OrderBookLevel(
                        price=float(b["price"]),
                        size=float(b["size"]),
                    ))
                except (KeyError, ValueError, TypeError):
                    continue
            
            # Sort: asks ascending, bids descending
            asks.sort(key=lambda x: x.price)
            bids.sort(key=lambda x: x.price, reverse=True)
            
            return OrderBook(
                token_id=token_id,
                asks=asks,
                bids=bids,
            )
            
        except Exception as e:
            logger.debug(f"[Adapter] get_order_book failed for {token_id[:16]}...: {e}")
            return None
    
    # ── Execution ─────────────────────────────────────────────────────────────
    
    async def place_order(
        self,
        token_id: str,
        side: str,
        amount_usd: float,
        max_price: float,
    ) -> OrderResult:
        """
        Place an order on Polymarket.
        
        In DRY_RUN mode: returns simulated success without submitting.
        In live mode: uses py-clob-client with neg_risk=True, tick_size="0.001".
        """
        if self.dry_run:
            # Paper trade — simulate fill at max_price
            shares = amount_usd / max_price if max_price > 0 else 0
            logger.info(
                f"[Adapter] DRY_RUN order: {side} {token_id[:16]}... "
                f"${amount_usd:.2f} @ {max_price:.3f} ({shares:.1f} shares)"
            )
            return OrderResult(
                success=True,
                order_id=f"paper_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
                filled_size=shares,
                filled_price=max_price,
                dry_run=True,
            )
        
        # ── Live execution (future — requires POLYMARKET_PRIVATE_KEY) ─────
        # This is where py-clob-client integration goes.
        # ~50-80 lines to replace this block when transitioning to live.
        logger.error(
            "[Adapter] Live execution not yet implemented. "
            "Set DRY_RUN=true or implement py-clob-client integration."
        )
        return OrderResult(
            success=False,
            error="Live execution not implemented",
        )
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order on Polymarket."""
        if self.dry_run:
            logger.info(f"[Adapter] DRY_RUN cancel: {order_id}")
            return True
        
        # Live cancellation via py-clob-client (future)
        logger.error("[Adapter] Live cancel not yet implemented")
        return False
    
    async def get_positions(self) -> List[Dict]:
        """Get open positions from Polymarket."""
        if self.dry_run:
            return []  # Paper positions tracked in DB, not on venue
        
        # Live positions via py-clob-client (future)
        return []
    
    # ── Settlement ────────────────────────────────────────────────────────────
    
    async def check_settlement(
        self,
        city: str,
        market_date_str: str,
    ) -> Optional[SettlementResult]:
        """
        Check if a Polymarket temperature event has settled.
        
        Reuses existing check_event_resolution() from polymarket.py which
        re-fetches the event by slug and checks for binary outcome prices.
        """
        result = await check_event_resolution(city, market_date_str)
        
        if result is None:
            return None
        
        if not result.get("resolved", False):
            return SettlementResult(resolved=False)
        
        return SettlementResult(
            resolved=True,
            winning_label=result.get("winning_label", ""),
            winning_bucket_low=result.get("winning_bucket_low"),
            winning_bucket_high=result.get("winning_bucket_high"),
            estimated_high=result.get("estimated_high"),
        )
