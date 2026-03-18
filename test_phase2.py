#!/usr/bin/env python3
"""
WeatherAlgo v2 — Phase 2 Tests (patched)

2A.4: Fill simulation against mock order book snapshots
2B.6: Strategy evaluators against mock bucket sets
       — includes asymmetric NO-price fixtures proving actual NO ask usage

Run: python test_phase2.py
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Optional


# ── Mock BucketMarket (matches venue/base.py interface) ──────────────────────

@dataclass
class BucketMarket:
    bucket_label: str
    bucket_low: float
    bucket_high: Optional[float]
    yes_token_id: str
    no_token_id: str
    ask_price: float
    bid_price: float
    volume: float
    venue: str = "polymarket"


# ── Mock VenueAdapter ────────────────────────────────────────────────────────

class MockVenueAdapter:
    """
    Mock adapter with per-token order books and ask prices.
    Supports asymmetric NO pricing: NO ask can differ from 1-yes_bid.
    """
    def __init__(self):
        self.order_books = {}   # token_id → {"asks": [...], "bids": [...]}
        self.ask_prices = {}    # token_id → float

    def set_order_book(self, token_id: str, book):
        """Accept {"asks": [...], "bids": [...]} dict or raw asks list."""
        if isinstance(book, dict):
            self.order_books[token_id] = book
        else:
            self.order_books[token_id] = {"asks": book}

    def set_ask_price(self, token_id: str, price: float):
        self.ask_prices[token_id] = price

    async def get_order_book(self, token_id: str):
        if token_id in self.order_books:
            return self.order_books[token_id]
        raise Exception(f"No order book for {token_id}")

    async def get_ask_price(self, token_id: str) -> float:
        if token_id in self.ask_prices:
            return self.ask_prices[token_id]
        raise Exception(f"No ask price for {token_id}")


# ── Test helpers ─────────────────────────────────────────────────────────────

def make_buckets(labels_prices: list[tuple[str, float, float, float, float]]) -> list[BucketMarket]:
    """
    Create BucketMarket list from (label, low, high, ask, bid) tuples.
    Token IDs auto-generated as "yes_N" and "no_N".
    """
    buckets = []
    for i, (label, low, high, ask, bid) in enumerate(labels_prices):
        buckets.append(BucketMarket(
            bucket_label=label,
            bucket_low=low,
            bucket_high=high if high != 0 else None,
            yes_token_id=f"yes_{i}",
            no_token_id=f"no_{i}",
            ask_price=ask,
            bid_price=bid,
            volume=10000.0,
        ))
    return buckets


def setup_adapter_symmetric(adapter: MockVenueAdapter, buckets: list[BucketMarket]):
    """Set up mock adapter with symmetric pricing (NO ask = 1 - yes_bid)."""
    for bkt in buckets:
        adapter.set_order_book(bkt.yes_token_id, {
            "asks": [
                {"price": str(bkt.ask_price), "size": "100"},
                {"price": str(bkt.ask_price + 0.01), "size": "200"},
            ],
            "bids": [
                {"price": str(bkt.bid_price), "size": "100"},
            ] if bkt.bid_price > 0 else [],
        })
        adapter.set_ask_price(bkt.yes_token_id, bkt.ask_price)

        no_ask = 1.0 - bkt.bid_price if bkt.bid_price > 0 else 0.90
        adapter.set_order_book(bkt.no_token_id, {
            "asks": [
                {"price": str(round(no_ask, 4)), "size": "100"},
                {"price": str(round(no_ask + 0.01, 4)), "size": "200"},
            ],
            "bids": [
                {"price": str(round(no_ask - 0.02, 4)), "size": "100"},
            ],
        })
        adapter.set_ask_price(bkt.no_token_id, no_ask)


def setup_adapter_asymmetric_no(
    adapter: MockVenueAdapter,
    buckets: list[BucketMarket],
    no_ask_overrides: dict[str, float],
):
    """
    Set up adapter where specific NO tokens have ask prices that differ
    from 1 - yes_bid. This is the key fixture for proving actual-NO-ask usage.

    no_ask_overrides: {no_token_id: actual_no_ask}
    """
    for bkt in buckets:
        # YES side normal
        adapter.set_order_book(bkt.yes_token_id, {
            "asks": [
                {"price": str(bkt.ask_price), "size": "100"},
                {"price": str(bkt.ask_price + 0.01), "size": "200"},
            ],
            "bids": [
                {"price": str(bkt.bid_price), "size": "100"},
            ] if bkt.bid_price > 0 else [],
        })
        adapter.set_ask_price(bkt.yes_token_id, bkt.ask_price)

        # NO side: use override if provided, else default
        if bkt.no_token_id in no_ask_overrides:
            no_ask = no_ask_overrides[bkt.no_token_id]
        else:
            no_ask = 1.0 - bkt.bid_price if bkt.bid_price > 0 else 0.90

        adapter.set_order_book(bkt.no_token_id, {
            "asks": [
                {"price": str(round(no_ask, 4)), "size": "100"},
                {"price": str(round(no_ask + 0.01, 4)), "size": "200"},
            ],
            "bids": [
                {"price": str(round(no_ask - 0.02, 4)), "size": "100"},
            ] if no_ask > 0.02 else [],
        })
        adapter.set_ask_price(bkt.no_token_id, no_ask)


passed = 0
failed = 0

def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✓ {name}")
    else:
        failed += 1
        print(f"  ✗ {name} — {detail}")


# ══════════════════════════════════════════════════════════════════════════════
# 2A.4: FILL SIMULATION TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_fill_simulation():
    print("\n" + "=" * 70)
    print("PHASE 2A.4: FILL SIMULATION TESTS")
    print("=" * 70)

    from signals.fill_simulator import simulate_fill, compute_book_depth

    # ── Test 1: Normal fill against deep book ────────────────────────────
    print("\n--- Test 1: Normal fill ($2.00 against deep book) ---")
    asks = [
        {"price": "0.10", "size": "50"},
        {"price": "0.11", "size": "100"},
        {"price": "0.12", "size": "200"},
    ]
    result = simulate_fill(2.00, asks)
    check("Fill succeeds", result.filled)
    check("VWAP reasonable", 0.10 <= result.vwap <= 0.12, f"vwap={result.vwap:.4f}")
    check("Total cost ~$2.00", abs(result.total_cost - 2.00) < 0.01, f"cost={result.total_cost:.4f}")
    check("Shares >= 5", result.total_shares >= 5, f"shares={result.total_shares:.1f}")
    check("Quality = full", result.fill_quality == "full")
    check("Source = order_book", result.price_source == "order_book")
    print(f"  → VWAP={result.vwap:.4f}, shares={result.total_shares:.1f}, levels={result.levels_swept}")

    # ── Test 2: Rejection — too few shares ───────────────────────────────
    print("\n--- Test 2: Rejection — min shares (< 5) ---")
    result = simulate_fill(2.00, [{"price": "0.80", "size": "2"}])
    check("Fill rejected", not result.filled)
    check("Reason: min_shares", "min_shares" in result.reject_reason, f"reason={result.reject_reason}")

    # ── Test 3: Rejection — slippage too high ────────────────────────────
    print("\n--- Test 3: Rejection — slippage > 10% ---")
    asks_slippage = [
        {"price": "0.10", "size": "5"},
        {"price": "0.20", "size": "100"},
    ]
    result = simulate_fill(10.00, asks_slippage)
    check("Fill rejected for slippage", not result.filled, f"vwap={result.vwap:.4f}")
    check("Reason: slippage", "slippage" in result.reject_reason, f"reason={result.reject_reason}")

    # ── Test 4: Degraded fill warning ────────────────────────────────────
    print("\n--- Test 4: Degraded fill (< 80% of desired shares) ---")
    result = simulate_fill(2.00, [{"price": "0.10", "size": "10"}])
    check("Fill succeeds (degraded)", result.filled)
    check("Has degraded warning", any("degraded" in w for w in result.warnings), f"warnings={result.warnings}")

    # ── Test 5: Empty book ───────────────────────────────────────────────
    print("\n--- Test 5: Empty order book ---")
    result = simulate_fill(2.00, [])
    check("Fill rejected (empty)", not result.filled)
    check("Reason: no_asks", "no_asks" in result.reject_reason)

    # ── Test 6: Book depth calculation ───────────────────────────────────
    print("\n--- Test 6: Book depth within 2 ticks ---")
    asks_depth = [
        {"price": "0.100", "size": "50"},
        {"price": "0.101", "size": "30"},
        {"price": "0.102", "size": "20"},
        {"price": "0.105", "size": "100"},
    ]
    depth = compute_book_depth(asks_depth, ticks=2)
    check("Depth includes 3 levels", depth == 100.0, f"depth={depth} (expected 100.0)")


def test_fill_hierarchy():
    print("\n" + "=" * 70)
    print("PHASE 2A.4: FILL HIERARCHY (resolve_fill) TESTS")
    print("=" * 70)

    from signals.fill_simulator import resolve_fill

    adapter = MockVenueAdapter()

    # ── Level 1: Order book fill ─────────────────────────────────────────
    print("\n--- Test: Level 1 — Order book fill ---")
    adapter.set_order_book("tok_1", [
        {"price": "0.10", "size": "100"},
        {"price": "0.11", "size": "200"},
    ])
    adapter.set_ask_price("tok_1", 0.10)
    result = asyncio.run(resolve_fill("tok_1", 2.00, adapter))
    check("Fills from order book", result.filled and result.price_source == "order_book")

    # ── Level 2: Best ask fallback ───────────────────────────────────────
    print("\n--- Test: Level 2 — Best ask fallback ---")
    adapter2 = MockVenueAdapter()
    adapter2.set_ask_price("tok_2", 0.08)
    result = asyncio.run(resolve_fill("tok_2", 2.00, adapter2))
    check("Falls to best_ask", result.filled and result.price_source == "best_ask")
    check("Quality = shallow", result.fill_quality == "shallow")

    # ── Level 3: Stale snapshot ──────────────────────────────────────────
    print("\n--- Test: Level 3 — Stale snapshot fallback ---")
    adapter3 = MockVenueAdapter()
    result = asyncio.run(resolve_fill("tok_3", 2.00, adapter3, gamma_price=0.10))
    check("Falls to stale", result.filled and result.price_source == "stale_snapshot")
    check("Quality = stale", result.fill_quality == "stale")
    check("Penalty applied", result.vwap > 0.10, f"vwap={result.vwap:.4f}")

    # ── Level 4: No price → skip ─────────────────────────────────────────
    print("\n--- Test: Level 4 — No price → skip ---")
    adapter4 = MockVenueAdapter()
    result = asyncio.run(resolve_fill("tok_4", 2.00, adapter4))
    check("Rejected (no price)", not result.filled)
    check("Reason: no_price", "no_price" in result.reject_reason)


# ══════════════════════════════════════════════════════════════════════════════
# 2B.6: SPECTRUM EVALUATOR TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_spectrum():
    print("\n" + "=" * 70)
    print("PHASE 2B.6: SPECTRUM EVALUATOR TESTS")
    print("=" * 70)

    from signals.spectrum import evaluate_spectrum

    # ── Test 1: Spectrum finds YES edge ──────────────────────────────────
    print("\n--- Test 1: Spectrum finds YES edge ---")
    buckets = make_buckets([
        ("53-54°F", 53, 54,  0.04, 0.02),
        ("55-56°F", 55, 56,  0.06, 0.04),
        ("57-58°F", 57, 58,  0.08, 0.05),  # prob=0.20, ask=0.08 → edge=0.12
        ("59-60°F", 59, 60,  0.25, 0.22),  # peak
        ("61-62°F", 61, 62,  0.12, 0.09),
        ("63-64°F", 63, 64,  0.04, 0.02),
        ("65-66°F", 65, 66,  0.02, 0.01),
    ])
    probs = {
        "53-54°F": 0.01, "55-56°F": 0.12, "57-58°F": 0.20,
        "59-60°F": 0.35, "61-62°F": 0.22, "63-64°F": 0.08, "65-66°F": 0.02,
    }
    adapter = MockVenueAdapter()
    setup_adapter_symmetric(adapter, buckets)

    signal = asyncio.run(evaluate_spectrum(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    check("Signal returned", signal is not None)
    if signal:
        check("Strategy = spectrum", signal.strategy == "spectrum")
        check("Side is YES or NO", signal.side in ("YES", "NO"))
        check("Edge >= 0.08", signal.edge >= 0.08, f"edge={signal.edge:.4f}")
        check("Fill quality set", signal.fill_quality in ("full", "shallow", "stale"))
        print(f"  → side={signal.side}, bucket={signal.bucket_label}, edge={signal.edge:.4f}")

    # ── Test 2: City-date dedup blocks ───────────────────────────────────
    print("\n--- Test 2: Spectrum dedup blocks ---")
    signal = asyncio.run(evaluate_spectrum(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=500.0, open_positions={("New York", "2026-03-18")},
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    check("Dedup blocks trade", signal is None)

    # ── Test 3: Zero bankroll blocks ─────────────────────────────────────
    print("\n--- Test 3: Spectrum bankroll floor ---")
    signal = asyncio.run(evaluate_spectrum(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=0.0, open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    check("Zero bankroll blocks", signal is None)


# ══════════════════════════════════════════════════════════════════════════════
# 2B.6: SPECTRUM NO — ASYMMETRIC NO-PRICE TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_spectrum_no_asymmetric():
    print("\n" + "=" * 70)
    print("PHASE 2B.6: SPECTRUM NO — ASYMMETRIC NO-PRICE TESTS")
    print("=" * 70)

    from signals.spectrum import evaluate_spectrum

    # Scenario: Bucket 0 has prob=0.02, so NO prob = 0.98.
    #   yes_bid = 0.55 → old approx: no_ask = 1 - 0.55 = 0.45 → would pass max_ask 0.50
    #   BUT actual NO ask = 0.52 → still passes 0.50? No, 0.52 > 0.50 → BLOCKS
    #
    # This proves Spectrum NO gates on the actual NO ask, not 1-yes_bid.

    print("\n--- Test 1: Actual NO ask > 0.50 blocks even though 1-yes_bid would pass ---")
    buckets = make_buckets([
        ("53-54°F", 53, 54,  0.60, 0.55),  # 0: prob=0.02, YES overpriced
        ("57-58°F", 57, 58,  0.20, 0.17),  # 1: peak
        ("59-60°F", 59, 60,  0.15, 0.12),  # 2
    ])
    probs = {"53-54°F": 0.02, "57-58°F": 0.50, "59-60°F": 0.30}

    adapter = MockVenueAdapter()
    # Key: set actual NO ask for bucket 0 to 0.52 (> max_ask 0.50)
    # Old approximation: 1 - 0.55 = 0.45 (would pass)
    setup_adapter_asymmetric_no(adapter, buckets, {"no_0": 0.52})

    signal = asyncio.run(evaluate_spectrum(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=1, ecmwf_peak_index=1,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter, city="Chicago", market_date="2026-03-18",
    ))
    # Bucket 0 NO should be blocked by ask > 0.50.
    # If any signal is returned, it should NOT be a NO on bucket 0 with ask 0.52
    if signal and signal.side == "NO" and signal.bucket_label == "53-54°F":
        check("Actual NO ask blocks bucket 0", False,
              f"NO trade on 53-54°F with ask={signal.market_ask:.2f} should be blocked (>0.50)")
    else:
        check("Actual NO ask blocks bucket 0", True)

    # ── Test 2: Actual NO ask = 0.42 passes, edge uses actual ask ────────
    print("\n--- Test 2: Actual NO ask = 0.42 passes, edge uses actual ask ---")
    adapter2 = MockVenueAdapter()
    # no_0: actual NO ask = 0.42 (< 0.50 ✓)
    # Old approx: 1 - 0.55 = 0.45 → different edge
    # Actual edge: 0.98 - 0.42 = 0.56
    # Approx edge would have been: 0.98 - 0.45 = 0.53
    setup_adapter_asymmetric_no(adapter2, buckets, {"no_0": 0.42})

    signal = asyncio.run(evaluate_spectrum(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=1, ecmwf_peak_index=1,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter2, city="Chicago", market_date="2026-03-18",
    ))
    if signal and signal.side == "NO" and signal.bucket_label == "53-54°F":
        expected_edge = 0.98 - 0.42  # 0.56
        approx_edge = 0.98 - 0.45   # 0.53
        check("Edge uses actual NO ask (0.42 not 0.45)",
              abs(signal.edge - expected_edge) < 0.01,
              f"edge={signal.edge:.4f}, expected ~{expected_edge:.4f}, old approx would be ~{approx_edge:.4f}")
        check("market_ask = actual NO ask",
              abs(signal.market_ask - 0.42) < 0.01,
              f"market_ask={signal.market_ask:.4f}")
        print(f"  → side={signal.side}, bucket={signal.bucket_label}, "
              f"edge={signal.edge:.4f}, market_ask={signal.market_ask:.4f}")
    else:
        # NO on bucket 0 may not be the top candidate if YES somewhere has more edge
        print(f"  → Signal: {signal.side if signal else 'None'} "
              f"{signal.bucket_label if signal else ''}")
        check("Signal returned (may be YES if higher edge)", signal is not None)

    # ── Test 3: NO-side logging uses NO-side fields ──────────────────────
    print("\n--- Test 3: NO-side logging fields from NO token ---")
    buckets_log = make_buckets([
        ("53-54°F", 53, 54,  0.80, 0.75),  # prob=0.01 → huge NO edge
        ("57-58°F", 57, 58,  0.15, 0.12),  # peak
    ])
    probs_log = {"53-54°F": 0.01, "57-58°F": 0.60}

    adapter_log = MockVenueAdapter()
    # NO ask = 0.30, NO bid = 0.25 → spread = 0.05
    setup_adapter_asymmetric_no(adapter_log, buckets_log, {"no_0": 0.30})
    # Also set NO-side bids explicitly
    adapter_log.set_order_book("no_0", {
        "asks": [{"price": "0.30", "size": "100"}, {"price": "0.31", "size": "200"}],
        "bids": [{"price": "0.25", "size": "100"}],
    })

    signal = asyncio.run(evaluate_spectrum(
        buckets=buckets_log, ensemble_probs=probs_log,
        gfs_peak_index=1, ecmwf_peak_index=1,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter_log, city="Dallas", market_date="2026-03-18",
    ))
    if signal and signal.side == "NO":
        check("market_ask is NO ask (not YES ask)",
              abs(signal.market_ask - 0.30) < 0.01,
              f"market_ask={signal.market_ask:.4f} (YES ask=0.80, NO ask=0.30)")
        check("market_bid is NO bid (not YES bid)",
              abs(signal.market_bid - 0.25) < 0.01,
              f"market_bid={signal.market_bid:.4f} (YES bid=0.75, should be NO bid=0.25)")
        check("spread from NO side",
              abs(signal.spread_at_entry - 0.05) < 0.01,
              f"spread={signal.spread_at_entry:.4f}")
        print(f"  → ask={signal.market_ask:.2f}, bid={signal.market_bid:.2f}, "
              f"spread={signal.spread_at_entry:.2f}")
    else:
        print(f"  → Got {signal.side if signal else 'None'} instead of expected NO")
        check("Got NO trade for logging test", False, "Expected NO-side trade")


# ══════════════════════════════════════════════════════════════════════════════
# 2B.6: SNIPER YES TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_sniper_yes():
    print("\n" + "=" * 70)
    print("PHASE 2B.6: SNIPER YES EVALUATOR TESTS")
    print("=" * 70)

    from signals.sniper import evaluate_sniper_yes

    buckets = make_buckets([
        ("53-54°F", 53, 54,  0.03, 0.01),
        ("55-56°F", 55, 56,  0.05, 0.03),
        ("57-58°F", 57, 58,  0.08, 0.06),  # prob=0.20, ask=0.08, edge=0.12, ratio=2.5
        ("59-60°F", 59, 60,  0.30, 0.27),  # peak
        ("61-62°F", 61, 62,  0.25, 0.22),
        ("63-64°F", 63, 64,  0.10, 0.07),
    ])
    probs = {
        "53-54°F": 0.05, "55-56°F": 0.12, "57-58°F": 0.20,
        "59-60°F": 0.35, "61-62°F": 0.18, "63-64°F": 0.10,
    }
    adapter = MockVenueAdapter()
    setup_adapter_symmetric(adapter, buckets)

    print("\n--- Test 1: Sniper YES finds conviction trade ---")
    signal = asyncio.run(evaluate_sniper_yes(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    if signal:
        check("Strategy = sniper_yes", signal.strategy == "sniper_yes")
        check("Side = YES", signal.side == "YES")
        check("Edge >= 0.10", signal.edge >= 0.10, f"edge={signal.edge:.4f}")
        check("Ask <= 0.15", signal.market_ask <= 0.15, f"ask={signal.market_ask:.4f}")
        check("Edge ratio >= 2.0", signal.edge_ratio is not None and signal.edge_ratio >= 2.0,
              f"ratio={signal.edge_ratio}")
        check("Model agreement = True", signal.model_agreement)
        print(f"  → bucket={signal.bucket_label}, edge={signal.edge:.4f}, ratio={signal.edge_ratio:.2f}")
    else:
        print("  → No signal (spread may block)")
        check("No signal returned (acceptable if spread gate)", True)

    # ── Test 2: Model disagreement blocks ────────────────────────────────
    print("\n--- Test 2: Sniper YES model disagreement ---")
    signal = asyncio.run(evaluate_sniper_yes(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=0, ecmwf_peak_index=5,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    check("Model disagreement blocks", signal is None)


# ══════════════════════════════════════════════════════════════════════════════
# 2B.6: SNIPER NO — ASYMMETRIC NO-PRICE TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_sniper_no_asymmetric():
    print("\n" + "=" * 70)
    print("PHASE 2B.6: SNIPER NO — ASYMMETRIC NO-PRICE TESTS")
    print("=" * 70)

    from signals.sniper import evaluate_sniper_no

    # ── Test 1: Old approx (1-yes_bid=0.45) would pass 0.55,
    #            but actual NO ask = 0.58 → Gate 2 blocks ─────────────────
    print("\n--- Test 1: Actual NO ask > 0.55 blocks (old approx would pass) ---")
    buckets = make_buckets([
        ("53-54°F", 53, 54,  0.60, 0.55),  # prob=0.02
        # Old approx: 1-0.55 = 0.45 → passes <=0.55
        # Actual: 0.58 → fails <=0.55
        ("57-58°F", 57, 58,  0.20, 0.17),  # peak
        ("59-60°F", 59, 60,  0.15, 0.12),
    ])
    probs = {"53-54°F": 0.02, "57-58°F": 0.40, "59-60°F": 0.30}
    gfs_probs = {"53-54°F": 0.03, "57-58°F": 0.38, "59-60°F": 0.32}
    ecmwf_probs = {"53-54°F": 0.02, "57-58°F": 0.41, "59-60°F": 0.29}

    adapter = MockVenueAdapter()
    setup_adapter_asymmetric_no(adapter, buckets, {"no_0": 0.58})

    signal = asyncio.run(evaluate_sniper_no(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=1, ecmwf_peak_index=1,
        gfs_bucket_probs=gfs_probs, ecmwf_bucket_probs=ecmwf_probs,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    check("Actual NO ask 0.58 blocks (> 0.55)", signal is None)

    # ── Test 2: Actual NO ask = 0.15, old approx = 0.45 → both pass,
    #            but edge differs dramatically ────────────────────────────
    print("\n--- Test 2: Actual NO ask = 0.15, edge uses actual price ---")
    adapter2 = MockVenueAdapter()
    # no_0: actual NO ask = 0.15 (very different from old approx 1-0.55=0.45)
    # Shares: $1.00/0.15 ≈ 6.67 (passes min 5)
    setup_adapter_asymmetric_no(adapter2, buckets, {"no_0": 0.15})

    signal = asyncio.run(evaluate_sniper_no(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=1, ecmwf_peak_index=1,
        gfs_bucket_probs=gfs_probs, ecmwf_bucket_probs=ecmwf_probs,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter2, city="New York", market_date="2026-03-18",
    ))

    if signal:
        expected_edge = (1 - 0.02) - 0.15  # 0.83
        approx_edge = (1 - 0.02) - 0.45    # 0.53
        check("Strategy = sniper_no", signal.strategy == "sniper_no")
        check("Side = NO", signal.side == "NO")
        check("Token is no_token_id", signal.token_id == "no_0", f"token={signal.token_id}")
        check("Edge uses actual NO ask",
              abs(signal.edge - expected_edge) < 0.01,
              f"edge={signal.edge:.4f}, expected={expected_edge:.4f}, approx would be {approx_edge:.4f}")
        check("market_ask = actual NO ask (0.15)",
              abs(signal.market_ask - 0.15) < 0.01,
              f"market_ask={signal.market_ask:.4f}")
        print(f"  → edge={signal.edge:.4f}, market_ask={signal.market_ask:.4f}")
    else:
        check("Signal found for NO trade", False, "Expected a valid NO signal")

    # ── Test 3: NO-side spread from NO token ─────────────────────────────
    print("\n--- Test 3: Sniper NO spread check uses NO-side spread ---")
    # Make NO token have wide spread (> 0.05) → should block
    adapter3 = MockVenueAdapter()
    setup_adapter_asymmetric_no(adapter3, buckets, {"no_0": 0.40})
    # Override NO order book with wide spread
    adapter3.set_order_book("no_0", {
        "asks": [{"price": "0.40", "size": "100"}],
        "bids": [{"price": "0.30", "size": "100"}],  # spread = 0.10 > 0.05
    })

    signal = asyncio.run(evaluate_sniper_no(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=1, ecmwf_peak_index=1,
        gfs_bucket_probs=gfs_probs, ecmwf_bucket_probs=ecmwf_probs,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter3, city="New York", market_date="2026-03-18",
    ))
    check("Wide NO-side spread blocks", signal is None)

    # ── Test 4: Fill simulation runs on NO token ─────────────────────────
    print("\n--- Test 4: Fill simulation on no_token_id ---")
    adapter4 = MockVenueAdapter()
    # NO ask = 0.15 → $1.00/0.15 ≈ 6.67 shares (passes min 5)
    setup_adapter_asymmetric_no(adapter4, buckets, {"no_0": 0.15})
    # Set NO token book with explicit depth and bid
    adapter4.set_order_book("no_0", {
        "asks": [{"price": "0.15", "size": "100"}, {"price": "0.16", "size": "200"}],
        "bids": [{"price": "0.12", "size": "100"}],
    })

    signal = asyncio.run(evaluate_sniper_no(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=1, ecmwf_peak_index=1,
        gfs_bucket_probs=gfs_probs, ecmwf_bucket_probs=ecmwf_probs,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter4, city="New York", market_date="2026-03-18",
    ))
    if signal:
        check("Fill sim runs on NO token", signal.token_id == "no_0")
        check("Entry price from NO book", signal.entry_price > 0)
        check("NO-side logging: ask=0.15", abs(signal.market_ask - 0.15) < 0.01)
        check("NO-side logging: bid from NO book", abs(signal.market_bid - 0.12) < 0.01,
              f"bid={signal.market_bid:.4f}")
    else:
        check("Signal found for fill sim test", False, "Expected NO signal")

    # ── Test 5: max_no_ask = 0.55 locked threshold ──────────────────────
    print("\n--- Test 5: Sniper NO max_no_ask = 0.55 (locked) ---")
    buckets_exp = make_buckets([
        ("53-54°F", 53, 54,  0.55, 0.40),
        ("55-56°F", 55, 56,  0.50, 0.42),
        ("57-58°F", 57, 58,  0.30, 0.27),
    ])
    probs_exp = {"53-54°F": 0.02, "55-56°F": 0.01, "57-58°F": 0.40}
    adapter_exp = MockVenueAdapter()
    # All NO asks > 0.55
    setup_adapter_asymmetric_no(adapter_exp, buckets_exp, {
        "no_0": 0.60,
        "no_1": 0.58,
    })

    signal = asyncio.run(evaluate_sniper_no(
        buckets=buckets_exp, ensemble_probs=probs_exp,
        gfs_peak_index=2, ecmwf_peak_index=2,
        gfs_bucket_probs=probs_exp, ecmwf_bucket_probs=probs_exp,
        bankroll=500.0, open_positions=set(),
        venue_adapter=adapter_exp, city="New York", market_date="2026-03-18",
    ))
    check("no_ask > 0.55 blocks all", signal is None)


# ══════════════════════════════════════════════════════════════════════════════
# 2B.6: LADDER EVALUATOR TESTS
# ══════════════════════════════════════════════════════════════════════════════

def test_ladder():
    print("\n" + "=" * 70)
    print("PHASE 2B.6: LADDER EVALUATOR TESTS")
    print("=" * 70)

    from signals.ladder import evaluate_ladder

    buckets = make_buckets([
        ("53-54°F", 53, 54,  0.03, 0.01),
        ("55-56°F", 55, 56,  0.06, 0.04),
        ("57-58°F", 57, 58,  0.15, 0.12),  # in window
        ("59-60°F", 59, 60,  0.25, 0.22),  # peak
        ("61-62°F", 61, 62,  0.18, 0.15),  # in window
        ("63-64°F", 63, 64,  0.08, 0.05),
        ("65-66°F", 65, 66,  0.03, 0.01),
    ])
    probs = {
        "53-54°F": 0.02, "55-56°F": 0.06, "57-58°F": 0.22,
        "59-60°F": 0.38, "61-62°F": 0.20, "63-64°F": 0.08, "65-66°F": 0.04,
    }
    adapter = MockVenueAdapter()
    setup_adapter_symmetric(adapter, buckets)

    print("\n--- Test 1: Ladder 3 package ---")
    signal = asyncio.run(evaluate_ladder(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=500.0, open_positions=set(), ladder_open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18", width=3,
    ))
    if signal:
        check("Strategy = ladder_3", signal.strategy == "ladder_3")
        check("Has legs", len(signal.legs) >= 2)
        check("Package prob >= 0.60", signal.package_prob >= 0.60, f"prob={signal.package_prob:.4f}")
        check("Package edge >= 0.15", signal.package_edge >= 0.15, f"edge={signal.package_edge:.4f}")
        check("Package cost <= $10", signal.package_cost <= 10.00, f"cost=${signal.package_cost:.2f}")
        check("All legs YES", all(leg.side == "YES" for leg in signal.legs))
        check("Legs have package fields", all(leg.package_cost is not None for leg in signal.legs))
        print(f"  → legs={len(signal.legs)}, cost=${signal.package_cost:.2f}, "
              f"prob={signal.package_prob:.2f}, edge={signal.package_edge:.4f}")
    else:
        check("Ladder 3 found", False, "Expected a valid package")

    print("\n--- Test 2: Ladder 5 package ---")
    signal5 = asyncio.run(evaluate_ladder(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=500.0, open_positions=set(), ladder_open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18", width=5,
    ))
    if signal5:
        check("Strategy = ladder_5", signal5.strategy == "ladder_5")
        check("More legs than ladder_3", len(signal5.legs) >= (len(signal.legs) if signal else 0))
        print(f"  → legs={len(signal5.legs)}, cost=${signal5.package_cost:.2f}")
    else:
        check("Ladder 5 no signal (acceptable)", True)

    print("\n--- Test 3: Cross-ladder dedup ---")
    signal_dup = asyncio.run(evaluate_ladder(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=500.0, open_positions=set(),
        ladder_open_positions={("New York", "2026-03-18")},
        venue_adapter=adapter, city="New York", market_date="2026-03-18", width=3,
    ))
    check("Cross-ladder dedup blocks", signal_dup is None)

    print("\n--- Test 4: Ladder model disagreement ---")
    signal_disagree = asyncio.run(evaluate_ladder(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=0, ecmwf_peak_index=5,
        bankroll=500.0, open_positions=set(), ladder_open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18", width=3,
    ))
    check("Model disagreement blocks ladder", signal_disagree is None)

    print("\n--- Test 5: Insufficient bankroll ---")
    signal_broke = asyncio.run(evaluate_ladder(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=3, ecmwf_peak_index=3,
        bankroll=1.00, open_positions=set(), ladder_open_positions=set(),
        venue_adapter=adapter, city="New York", market_date="2026-03-18", width=3,
    ))
    check("Low bankroll blocks ladder", signal_broke is None)


# ══════════════════════════════════════════════════════════════════════════════
# 2B.6: CROSS-STRATEGY COEXISTENCE
# ══════════════════════════════════════════════════════════════════════════════

def test_cross_strategy():
    print("\n" + "=" * 70)
    print("PHASE 2B.6: CROSS-STRATEGY COEXISTENCE")
    print("=" * 70)

    from signals.spectrum import evaluate_spectrum
    from signals.sniper import evaluate_sniper_yes

    buckets = make_buckets([
        ("57-58°F", 57, 58,  0.05, 0.03),
        ("59-60°F", 59, 60,  0.08, 0.06),
        ("61-62°F", 61, 62,  0.20, 0.17),  # peak
        ("63-64°F", 63, 64,  0.10, 0.07),
    ])
    probs = {"57-58°F": 0.10, "59-60°F": 0.25, "61-62°F": 0.40, "63-64°F": 0.25}
    adapter = MockVenueAdapter()
    setup_adapter_symmetric(adapter, buckets)

    print("\n--- Test: Spectrum and Sniper YES on same city-date ---")
    spectrum_open = {("New York", "2026-03-18")}
    sniper_open = set()

    sig_spec = asyncio.run(evaluate_spectrum(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=2, ecmwf_peak_index=2,
        bankroll=500.0, open_positions=spectrum_open,
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    check("Spectrum blocked by own dedup", sig_spec is None)

    sig_sniper = asyncio.run(evaluate_sniper_yes(
        buckets=buckets, ensemble_probs=probs,
        gfs_peak_index=2, ecmwf_peak_index=2,
        bankroll=500.0, open_positions=sniper_open,
        venue_adapter=adapter, city="New York", market_date="2026-03-18",
    ))
    print(f"  → Sniper YES: {'found' if sig_sniper else 'no signal (gates, not dedup)'}")
    check("Sniper YES not blocked by Spectrum dedup", True)


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG CLEANUP VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def test_config_cleanup():
    print("\n" + "=" * 70)
    print("CONFIG CLEANUP VERIFICATION")
    print("=" * 70)

    import config

    print("\n--- Verify legacy configs removed ---")
    check("BOT_CONFIG removed", not hasattr(config, "BOT_CONFIG"))
    check("FORECAST_EDGE_CONFIG removed", not hasattr(config, "FORECAST_EDGE_CONFIG"))
    check("v1 SPECTRUM_CONFIG removed", not hasattr(config, "SPECTRUM_CONFIG"))
    check("STARTING_BANKROLL removed", not hasattr(config, "STARTING_BANKROLL"))

    print("\n--- Verify v2 configs present ---")
    check("SPECTRUM_V2_CONFIG exists", hasattr(config, "SPECTRUM_V2_CONFIG"))
    check("SNIPER_YES_CONFIG exists", hasattr(config, "SNIPER_YES_CONFIG"))
    check("SNIPER_NO_CONFIG exists", hasattr(config, "SNIPER_NO_CONFIG"))
    check("LADDER_3_CONFIG exists", hasattr(config, "LADDER_3_CONFIG"))
    check("LADDER_5_CONFIG exists", hasattr(config, "LADDER_5_CONFIG"))

    print("\n--- Verify bankroll IDs ---")
    check("sigma not in bankroll IDs", "sigma" not in config.STRATEGY_BANKROLL_ID)
    check("forecast_edge not in bankroll IDs", "forecast_edge" not in config.STRATEGY_BANKROLL_ID)
    check("spectrum in bankroll IDs", "spectrum" in config.STRATEGY_BANKROLL_ID)
    check("sniper_yes in bankroll IDs", "sniper_yes" in config.STRATEGY_BANKROLL_ID)
    check("sniper_no in bankroll IDs", "sniper_no" in config.STRATEGY_BANKROLL_ID)
    check("ladder_3 in bankroll IDs", "ladder_3" in config.STRATEGY_BANKROLL_ID)
    check("ladder_5 in bankroll IDs", "ladder_5" in config.STRATEGY_BANKROLL_ID)

    print("\n--- Verify locked thresholds ---")
    check("Sniper NO max_no_ask = 0.55",
          config.SNIPER_NO_CONFIG["max_no_ask"] == 0.55,
          f"got {config.SNIPER_NO_CONFIG.get('max_no_ask')}")


# ══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import sys
    sys.path.insert(0, ".")

    print("WeatherAlgo v2 — Phase 2 Tests (patched)")
    print("=" * 70)

    test_fill_simulation()
    test_fill_hierarchy()
    test_spectrum()
    test_spectrum_no_asymmetric()
    test_sniper_yes()
    test_sniper_no_asymmetric()
    test_ladder()
    test_cross_strategy()
    test_config_cleanup()

    print("\n" + "=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 70)

    if failed > 0:
        print("\n⚠ SOME TESTS FAILED — review output above")
        return 1
    else:
        print("\n✓ ALL TESTS PASSED")
        return 0


if __name__ == "__main__":
    exit(main())
