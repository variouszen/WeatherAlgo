"""
WeatherAlgo v2 — Abstract Venue Adapter Interface

Platform-agnostic interface for market discovery, pricing, execution, and settlement.
All strategy code operates on these types — never on raw venue JSON.

Implementations:
  - PolymarketAdapter (v1, built now)
  - KalshiAdapter (future)

Phase 5A: Added yes_tradable / no_tradable fields to BucketMarket.
A side is tradable only with live CLOB data (order book or get_price).
Gamma snapshot pricing does not count as tradable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional


# ── Shared Data Types ─────────────────────────────────────────────────────────

@dataclass
class BucketMarket:
    """Standardized bucket representation across all venues."""
    bucket_label: str       # Display label, e.g. "58-59°F"
    bucket_index: int       # Position in ordered bucket list (0 = lowest)
    bucket_low: Optional[float]   # Lower bound (None = open lower tail)
    bucket_high: Optional[float]  # Upper bound (None = open upper tail)
    yes_token_id: str       # Platform-specific token ID for YES
    no_token_id: str        # Platform-specific token ID for NO
    ask_price: float        # Current best ask (YES side) from live CLOB
    bid_price: float        # Current best bid (YES side), 0.0 if unknown
    no_ask_price: float     # Current best ask (NO side) from live CLOB
    volume: float           # Bucket trading volume
    venue: str              # "polymarket" or "kalshi"

    # Per-side tradability (Phase 5A)
    # True only if live CLOB data exists (order book asks or get_price).
    # Gamma snapshot alone does NOT make a side tradable.
    yes_tradable: bool = True
    no_tradable: bool = True

    # Gamma snapshot prices (discovery/logging only — never for execution)
    gamma_yes_price: float = 0.0
    gamma_no_price: float = 0.0

    # Raw market data for logging
    condition_id: str = ""
    market_id: str = ""


@dataclass
class OrderBookLevel:
    """Single price level in an order book."""
    price: float
    size: float


@dataclass
class OrderBook:
    """Order book for a single token."""
    token_id: str
    asks: List[OrderBookLevel] = field(default_factory=list)
    bids: List[OrderBookLevel] = field(default_factory=list)

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def spread(self) -> Optional[float]:
        if self.best_ask is not None and self.best_bid is not None:
            return self.best_ask - self.best_bid
        return None

    @property
    def total_ask_depth(self) -> float:
        return sum(level.size for level in self.asks)


@dataclass
class OrderResult:
    """Result of a place_order() call."""
    success: bool
    order_id: str = ""
    filled_size: float = 0.0
    filled_price: float = 0.0
    error: str = ""
    dry_run: bool = False


@dataclass
class SettlementResult:
    """Result of a check_settlement() call."""
    resolved: bool
    won: Optional[bool] = None  # None if not yet resolved
    winning_label: str = ""
    winning_bucket_low: Optional[float] = None
    winning_bucket_high: Optional[float] = None
    estimated_high: Optional[float] = None


# ── Abstract Interface ────────────────────────────────────────────────────────

class VenueAdapter(ABC):
    """
    Platform-agnostic interface for weather temperature markets.

    Every venue adapter implements these methods. Strategy evaluators
    and the scanner call only these methods — never venue-specific APIs.
    """

    @abstractmethod
    async def discover_markets(
        self,
        city: str,
        target_date: date,
        celsius: bool = False,
    ) -> Optional[List[BucketMarket]]:
        """
        Find all temperature buckets for a city-date.

        Returns ordered list of BucketMarket objects (sorted by bucket_low),
        or None if no valid market exists.
        """
        ...

    @abstractmethod
    async def get_ask_price(
        self,
        token_id: str,
        side: str = "BUY",
    ) -> Optional[float]:
        """
        Get live best ask price for a token.

        Args:
            token_id: venue-specific token identifier
            side: "BUY" for ask (what you'd pay), "SELL" for bid

        Returns float price or None on failure.
        """
        ...

    @abstractmethod
    async def get_order_book(
        self,
        token_id: str,
    ) -> Optional[OrderBook]:
        """
        Get full order book for a token.

        Returns OrderBook with sorted ask/bid levels, or None on failure.
        """
        ...

    @abstractmethod
    async def place_order(
        self,
        token_id: str,
        side: str,
        amount_usd: float,
        max_price: float,
    ) -> OrderResult:
        """
        Place an order on the venue.

        In DRY_RUN mode, logs the order but does not submit to the venue.

        Args:
            token_id: which token to buy
            side: "BUY" or "SELL"
            amount_usd: dollar amount to spend
            max_price: maximum price willing to pay per share

        Returns OrderResult with fill details.
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True on success."""
        ...

    @abstractmethod
    async def get_positions(self) -> List[Dict]:
        """Get all open positions on the venue."""
        ...

    @abstractmethod
    async def check_settlement(
        self,
        city: str,
        market_date_str: str,
    ) -> Optional[SettlementResult]:
        """
        Check if a market has settled and determine win/loss.

        Args:
            city: city name
            market_date_str: "YYYY-MM-DD" format

        Returns SettlementResult or None on failure.
        """
        ...
