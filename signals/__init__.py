"""
WeatherAlgo v2 — Strategy Signal Layer

Phase 2 modules:
- fill_simulator: Order book fill simulation with rejection gates
- spectrum: Spectrum evaluator (YES + NO, best edge per city-date)
- sniper: Sniper YES and Sniper NO evaluators
- ladder: Ladder 3 and Ladder 5 evaluators with package math

Phase 5A: Removed stale snapshot pricing path. Only live CLOB sources
(order_book, best_ask) are valid for fills. Gamma is discovery/logging only.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FillResult:
    """Result of a fill simulation against an order book."""
    filled: bool
    vwap: float = 0.0
    total_shares: float = 0.0
    total_cost: float = 0.0
    levels_swept: int = 0
    fill_quality: str = "rejected"   # "full", "shallow", "rejected"
    price_source: str = "none"       # "order_book", "best_ask"
    warnings: list[str] = field(default_factory=list)
    reject_reason: str = ""


@dataclass
class TradeSignal:
    """Everything needed to open a paper trade. Returned by evaluators."""
    strategy: str              # "spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"
    side: str                  # "YES" or "NO"
    token_id: str              # clobTokenIds[0] for YES, clobTokenIds[1] for NO
    bucket_label: str          # e.g. "58-59°F"
    bucket_index: int          # position in ordered bucket list

    # Signal data
    edge: float                # ensemble_prob - ask (YES) or (1-ensemble_prob) - no_ask (NO)
    ensemble_prob: float       # bucket probability from ensemble
    ensemble_members_in_bucket: int
    ensemble_total_members: int
    gfs_peak_index: int
    ecmwf_peak_index: int
    model_agreement: bool

    # Pricing data (live CLOB only)
    entry_price: float         # simulated VWAP or best_ask
    market_ask: float          # raw ask price at time of evaluation
    market_bid: float          # raw bid price
    spread_at_entry: float     # ask - bid
    midpoint_at_entry: float   # for comparison logging only
    book_depth_at_entry: float # ask shares within 2 ticks
    simulated_shares: float
    simulated_cost: float
    fill_quality: str          # "full", "shallow"
    price_source: str          # "order_book", "best_ask"
    levels_swept: int

    # Strategy-specific (optional)
    edge_ratio: Optional[float] = None       # Sniper YES: ensemble_prob / market_ask
    model_run_time: Optional[str] = None

    # Sizing
    target_spend: float = 0.0

    # Ladder-specific (only set for ladder trades)
    ladder_id: Optional[int] = None
    package_cost: Optional[float] = None
    package_prob: Optional[float] = None
    package_edge: Optional[float] = None
    num_legs: Optional[int] = None


@dataclass
class LadderSignal:
    """A complete ladder package signal — contains multiple TradeSignals (one per leg)."""
    strategy: str              # "ladder_3" or "ladder_5"
    width: int                 # 3 or 5
    legs: list[TradeSignal] = field(default_factory=list)
    package_cost: float = 0.0
    package_prob: float = 0.0
    package_edge: float = 0.0
    peak_index: int = 0
    model_agreement: bool = False
    gfs_peak_index: int = 0
    ecmwf_peak_index: int = 0
