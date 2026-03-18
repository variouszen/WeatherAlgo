"""
WeatherAlgo v2 — Kalshi Venue Adapter (Stub)

Future implementation for Kalshi KXHIGH temperature markets.
CFTC-regulated US venue, legal from NYC without VPN.

Not yet implemented — raises NotImplementedError on all methods.
Build during Phase 7 after paper validation on Polymarket.
"""

from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional

from venue.base import (
    VenueAdapter,
    BucketMarket,
    OrderBook,
    OrderResult,
    SettlementResult,
)


class KalshiAdapter(VenueAdapter):
    """
    Kalshi implementation of VenueAdapter.
    
    Stub only — all methods raise NotImplementedError.
    Build during Phase 7.
    
    Key differences from Polymarket:
      - Discovery: REST API GET /markets?series_ticker=KXHIGHNY&status=open
      - Pricing: REST API GET /markets/{ticker}/orderbook
      - Execution: kalshi-python SDK with RSA-PSS auth
      - Settlement: NWS Daily Climate Report
      - Note: Different resolution stations (Central Park vs LaGuardia for NYC)
    """
    
    def __init__(self):
        raise NotImplementedError(
            "Kalshi adapter is not yet implemented. "
            "Build during Phase 7 after Polymarket paper validation."
        )
    
    async def discover_markets(self, city: str, target_date: date, celsius: bool = False) -> Optional[List[BucketMarket]]:
        raise NotImplementedError("Kalshi adapter: Phase 7")
    
    async def get_ask_price(self, token_id: str, side: str = "BUY") -> Optional[float]:
        raise NotImplementedError("Kalshi adapter: Phase 7")
    
    async def get_order_book(self, token_id: str) -> Optional[OrderBook]:
        raise NotImplementedError("Kalshi adapter: Phase 7")
    
    async def place_order(self, token_id: str, side: str, amount_usd: float, max_price: float) -> OrderResult:
        raise NotImplementedError("Kalshi adapter: Phase 7")
    
    async def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError("Kalshi adapter: Phase 7")
    
    async def get_positions(self) -> List[Dict]:
        raise NotImplementedError("Kalshi adapter: Phase 7")
    
    async def check_settlement(self, city: str, market_date_str: str) -> Optional[SettlementResult]:
        raise NotImplementedError("Kalshi adapter: Phase 7")
