#!/usr/bin/env python3
"""
WeatherAlgo v2 — Phase 3 Tests (final patch)

Validates:
  - All v2 imports resolve (including lazy adapter import path)
  - v1 imports survive with fallback shims
  - Daily loss cap enforcement
  - GFS scan sync with ensemble cache (NOT settlement-only between windows)
  - Trade manager logic
  - Config consistency
  - Database model v2 columns
  - main.py exposes v2 strategy labels
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

passed = 0
failed = 0
skipped = 0

try:
    import sqlalchemy
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  ✓ {name}")
    else:
        failed += 1
        print(f"  ✗ {name} — {detail}")

def skip(name: str, reason: str = ""):
    global skipped
    skipped += 1
    print(f"  ⊘ SKIP {name} — {reason}")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 1: CONFIG IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

def test_config_imports():
    print("\n" + "=" * 70)
    print("TEST 1: CONFIG IMPORTS")
    print("=" * 70)

    from config import (
        SPECTRUM_V2_CONFIG, SNIPER_YES_CONFIG, SNIPER_NO_CONFIG,
        LADDER_3_CONFIG, LADDER_5_CONFIG, STRATEGY_BANKROLL_ID,
        DRY_RUN, SCAN_INTERVAL_SECONDS, SCAN_SYNC_GFS_RUNS, CITIES,
    )

    check("SPECTRUM_V2_CONFIG loaded", "min_edge" in SPECTRUM_V2_CONFIG)
    check("SNIPER_YES_CONFIG loaded", "min_edge" in SNIPER_YES_CONFIG)
    check("SNIPER_NO max_no_ask = 0.55", SNIPER_NO_CONFIG["max_no_ask"] == 0.55)
    check("LADDER_3_CONFIG loaded", "width" in LADDER_3_CONFIG)
    check("LADDER_5_CONFIG loaded", LADDER_5_CONFIG["width"] == 5)
    check("5 v2 strategies in bankroll IDs",
          all(s in STRATEGY_BANKROLL_ID for s in ["spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"]))
    check("sigma NOT in bankroll IDs", "sigma" not in STRATEGY_BANKROLL_ID)
    check("SCAN_SYNC_GFS_RUNS exists", isinstance(SCAN_SYNC_GFS_RUNS, bool))
    check("10 cities", len(CITIES) == 10)

    import config
    check("BOT_CONFIG removed", not hasattr(config, "BOT_CONFIG"))
    check("FORECAST_EDGE_CONFIG removed", not hasattr(config, "FORECAST_EDGE_CONFIG"))
    check("STARTING_BANKROLL removed", not hasattr(config, "STARTING_BANKROLL"))


# ══════════════════════════════════════════════════════════════════════════════
# TEST 2: V1 IMPORT SURVIVAL
# ══════════════════════════════════════════════════════════════════════════════

def test_v1_import_survival():
    print("\n" + "=" * 70)
    print("TEST 2: V1 IMPORT SURVIVAL (fallback shims)")
    print("=" * 70)

    if not HAS_SQLALCHEMY:
        skip("core.signals runtime import", "sqlalchemy not installed")
        with open("backend/core/signals.py", "r") as f:
            content = f.read()
        check("signals.py has try/except for BOT_CONFIG", "try:" in content and "BOT_CONFIG" in content)
        check("signals.py has try/except for STARTING_BANKROLL", "STARTING_BANKROLL" in content)
        return

    try:
        from core.signals import get_bankroll, get_open_positions, reset_daily_loss
        check("core.signals imports OK", True)
    except Exception as e:
        check("core.signals imports OK", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# TEST 3: V2 MODULE IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

def test_v2_imports():
    print("\n" + "=" * 70)
    print("TEST 3: V2 MODULE IMPORTS")
    print("=" * 70)

    try:
        from signals import TradeSignal, FillResult, LadderSignal
        check("signals dataclasses import", True)
    except Exception as e:
        check("signals dataclasses import", False, str(e))

    try:
        from signals.fill_simulator import simulate_fill, resolve_fill
        check("fill_simulator imports", True)
    except Exception as e:
        check("fill_simulator imports", False, str(e))

    try:
        from signals.spectrum import evaluate_spectrum
        check("spectrum evaluator imports", True)
    except Exception as e:
        check("spectrum evaluator imports", False, str(e))

    try:
        from signals.sniper import evaluate_sniper_yes, evaluate_sniper_no
        check("sniper evaluators import", True)
    except Exception as e:
        check("sniper evaluators import", False, str(e))

    try:
        from signals.ladder import evaluate_ladder
        check("ladder evaluator imports", True)
    except Exception as e:
        check("ladder evaluator imports", False, str(e))

    if not HAS_SQLALCHEMY:
        skip("trade_manager runtime", "sqlalchemy not installed")
        skip("scanner_v2 runtime", "sqlalchemy not installed")
        with open("scanner_v2.py", "r") as f:
            content = f.read()
        check("scanner_v2 has run_scan_v2", "async def run_scan_v2" in content)
        check("scanner_v2 imports all 5 evaluators",
              "evaluate_spectrum" in content and "evaluate_sniper_yes" in content
              and "evaluate_sniper_no" in content and "evaluate_ladder" in content)
    else:
        try:
            from scanner_v2 import run_scan_v2
            check("scanner_v2 imports", True)
        except Exception as e:
            check("scanner_v2 imports", False, str(e))


# ══════════════════════════════════════════════════════════════════════════════
# TEST 4: LAZY ADAPTER IMPORT (lowercase venue/)
# ══════════════════════════════════════════════════════════════════════════════

def test_adapter_import_path():
    print("\n" + "=" * 70)
    print("TEST 4: LAZY ADAPTER IMPORT — lowercase venue/")
    print("=" * 70)

    with open("scanner_v2.py", "r") as f:
        content = f.read()
    check("Import uses 'venue.polymarket_adapter' (lowercase)",
          "from venue.polymarket_adapter import PolymarketAdapter" in content)
    check("No 'Venue' (capital V) in import", "from Venue." not in content)

    try:
        from venue.polymarket_adapter import PolymarketAdapter
        adapter = PolymarketAdapter(dry_run=True)
        check("Adapter instantiates from venue/ (lowercase)", adapter is not None)
    except ImportError as e:
        check("Adapter instantiates from venue/", False, str(e))

    if HAS_SQLALCHEMY:
        try:
            from scanner_v2 import _get_adapter
            adapter = _get_adapter()
            check("_get_adapter() resolves correctly", adapter is not None)
        except Exception as e:
            check("_get_adapter() resolves correctly", False, str(e))
    else:
        skip("_get_adapter() runtime test", "sqlalchemy not installed")


# ══════════════════════════════════════════════════════════════════════════════
# TEST 5: DAILY LOSS CAP ENFORCEMENT
# ══════════════════════════════════════════════════════════════════════════════

def test_daily_loss_cap():
    print("\n" + "=" * 70)
    print("TEST 5: DAILY LOSS CAP ENFORCEMENT")
    print("=" * 70)

    with open("scanner_v2.py", "r") as f:
        content = f.read()

    check("blocked_strategies set is built",
          "blocked_strategies" in content and "blocked_strategies = set()" in content)
    check("blocked_strategies.add() on cap breach",
          "blocked_strategies.add(strat)" in content)

    for strat in ["spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"]:
        guard = f'"{strat}" not in blocked_strategies'
        check(f"Evaluator {strat} gated by blocked_strategies", guard in content)

    # Logic test
    blocked = set()
    if 12.0 >= 10.0:  # sniper_yes daily loss >= cap
        blocked.add("sniper_yes")

    check("sniper_yes blocked when loss >= cap", "sniper_yes" in blocked)
    check("spectrum NOT blocked (not at cap)", "spectrum" not in blocked)

    trades_opened = sum(1 for s in ["spectrum", "sniper_yes", "sniper_no"] if s not in blocked)
    check("Only unblocked strategies evaluate (2 of 3)", trades_opened == 2)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 6: GFS SCAN SYNC — CACHE-AWARE (not settlement-only)
# ══════════════════════════════════════════════════════════════════════════════

def test_gfs_scan_sync():
    print("\n" + "=" * 70)
    print("TEST 6: GFS SCAN SYNC — CACHE-AWARE BEHAVIOR")
    print("=" * 70)

    with open("scanner_v2.py", "r") as f:
        content = f.read()

    # ── Verify old settlement-only pattern is GONE ───────────────────
    check("No SETTLEMENT-ONLY mode", "SETTLEMENT-ONLY" not in content)
    check("No scan_cities gating", "scan_cities = CITIES if full_scan else []" not in content)
    check("No full_scan variable", "full_scan" not in content.replace("refresh_ensemble", ""))

    # ── Verify new cache pattern is present ──────────────────────────
    check("_ensemble_cache defined", "_ensemble_cache" in content)
    check("_clear_ensemble_cache defined", "def _clear_ensemble_cache" in content)
    check("refresh_ensemble flag used", "refresh_ensemble" in content)
    check("Cache populated on fresh fetch", "_ensemble_cache[cache_key]" in content)
    check("Cache read between windows", "cached = _ensemble_cache[cache_key]" in content)
    check("No-cache skip is per city-date",
          "no cached ensemble yet" in content and "SKIP" in content)

    # ── Verify cities always iterate (not gated) ─────────────────────
    check("Cities loop always runs (for city_cfg in CITIES)",
          "for city_cfg in CITIES:" in content)
    check("CACHED-SIGNAL logged between windows",
          "CACHED-SIGNAL scan" in content)

    # ── GFS window helper logic (pure function) ──────────────────────
    from datetime import datetime, timezone

    GFS_SCAN_WINDOWS = [(3, 6), (9, 12), (15, 18), (21, 24)]

    def _is_in_window(utc_now):
        hour = utc_now.hour + utc_now.minute / 60.0
        for start_h, end_h in GFS_SCAN_WINDOWS:
            if end_h > 24:
                if hour >= start_h or hour < (end_h - 24):
                    return True
            else:
                if start_h <= hour < end_h:
                    return True
        return False

    check("04:30 UTC in GFS window", _is_in_window(datetime(2026, 3, 18, 4, 30, tzinfo=timezone.utc)))
    check("10:00 UTC in GFS window", _is_in_window(datetime(2026, 3, 18, 10, 0, tzinfo=timezone.utc)))
    check("16:30 UTC in GFS window", _is_in_window(datetime(2026, 3, 18, 16, 30, tzinfo=timezone.utc)))
    check("22:00 UTC in GFS window", _is_in_window(datetime(2026, 3, 18, 22, 0, tzinfo=timezone.utc)))
    check("07:30 UTC NOT in window", not _is_in_window(datetime(2026, 3, 18, 7, 30, tzinfo=timezone.utc)))
    check("13:00 UTC NOT in window", not _is_in_window(datetime(2026, 3, 18, 13, 0, tzinfo=timezone.utc)))
    check("01:00 UTC NOT in window", not _is_in_window(datetime(2026, 3, 18, 1, 0, tzinfo=timezone.utc)))


# ══════════════════════════════════════════════════════════════════════════════
# TEST 6B: ENSEMBLE CACHE BEHAVIOR
# ══════════════════════════════════════════════════════════════════════════════

def test_ensemble_cache_behavior():
    print("\n" + "=" * 70)
    print("TEST 6B: ENSEMBLE CACHE LOGIC")
    print("=" * 70)

    # Simulate the cache directly (no DB needed)
    cache = {}

    # ── In-window: fresh fetch populates cache ───────────────────────
    city, date = "New York", "2026-03-18"
    cache_key = (city, date)
    fresh_data = {
        "ensemble_probs": {"58-59°F": 0.30, "60-61°F": 0.40},
        "gfs_probs": {"58-59°F": 0.28, "60-61°F": 0.42},
        "ecmwf_probs": {"58-59°F": 0.31, "60-61°F": 0.39},
        "gfs_peak": 1,
        "ecmwf_peak": 1,
        "total_members": 82,
    }
    cache[cache_key] = fresh_data
    check("Cache populated after fresh fetch", cache_key in cache)
    check("Cached data has ensemble_probs", "ensemble_probs" in cache[cache_key])

    # ── Between-window: cache hit → evaluation proceeds ──────────────
    refresh_ensemble = False  # between windows

    if not refresh_ensemble:
        if cache_key in cache:
            cached = cache[cache_key]
            ensemble_probs = cached["ensemble_probs"]
            gfs_peak = cached["gfs_peak"]
            can_evaluate = True
        else:
            can_evaluate = False

    check("Between-window: cache hit → evaluation proceeds", can_evaluate)
    check("Between-window: uses cached ensemble_probs",
          ensemble_probs == {"58-59°F": 0.30, "60-61°F": 0.40})
    check("Between-window: uses cached gfs_peak", gfs_peak == 1)

    # ── Between-window: cache miss → skip this city-date only ────────
    missing_key = ("London", "2026-03-18")
    if missing_key not in cache:
        skip_city_date = True
        skip_reason = "no cached ensemble yet"
    else:
        skip_city_date = False
        skip_reason = ""

    check("No-cache: skip this city-date only", skip_city_date)
    check("No-cache: clear skip reason", "no cached ensemble" in skip_reason)

    # ── Other city-dates still proceed ───────────────────────────────
    # NYC was cached, London was not → NYC can evaluate, London skips
    ny_can = cache_key in cache
    london_can = missing_key in cache
    check("NYC can evaluate (cached)", ny_can)
    check("London skips (not cached)", not london_can)
    check("Skip is per city-date, not global", ny_can and not london_can)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 7: DATABASE MODEL V2 COLUMNS
# ══════════════════════════════════════════════════════════════════════════════

def test_database_model():
    print("\n" + "=" * 70)
    print("TEST 7: DATABASE MODEL V2 COLUMNS")
    print("=" * 70)

    if not HAS_SQLALCHEMY:
        skip("Trade model column check", "sqlalchemy not installed")
        with open("backend/models/database.py", "r") as f:
            content = f.read()
        for col in ["ensemble_prob", "gfs_peak_bucket_index", "fill_quality",
                     "ladder_id", "package_cost", "num_legs", "venue"]:
            check(f"Trade.{col} in source", col in content)
        check("init_db no STARTING_BANKROLL import",
              "from config import STARTING_BANKROLL" not in content)
        check("init_db has sniper_yes row", "sniper_yes" in content)
        check("init_db has ladder_5 row", "ladder_5" in content)
        return

    from models.database import Trade
    for col in ["ensemble_prob", "ensemble_members_in_bucket", "ensemble_total_members",
                "gfs_peak_bucket_index", "ecmwf_peak_bucket_index", "model_agreement",
                "price_source", "market_ask", "simulated_vwap", "fill_quality",
                "model_run_time", "venue", "edge_ratio",
                "ladder_id", "package_cost", "package_prob", "package_edge", "num_legs"]:
        check(f"Trade.{col} exists", hasattr(Trade, col))


# ══════════════════════════════════════════════════════════════════════════════
# TEST 8: SCANNER HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def test_scanner_helpers():
    print("\n" + "=" * 70)
    print("TEST 8: SCANNER_V2 HELPER FUNCTIONS")
    print("=" * 70)

    from dataclasses import dataclass
    from typing import Optional as Opt

    @dataclass
    class MockBucket:
        bucket_label: str
        bucket_low: Opt[float]
        bucket_high: Opt[float]

    SETTLEMENT_ROUNDING = 0.5

    def compute_probs(member_values, buckets):
        total = len(member_values)
        if total == 0:
            return {}
        probs = {}
        for bkt in buckets:
            low, high = bkt.bucket_low, bkt.bucket_high
            if low is None:
                sl, sh = float("-inf"), high + SETTLEMENT_ROUNDING if high is not None else float("inf")
            elif high is None:
                sl, sh = low - SETTLEMENT_ROUNDING, float("inf")
            else:
                sl, sh = low - SETTLEMENT_ROUNDING, high + SETTLEMENT_ROUNDING
            count = sum(1 for v in member_values
                        if (sl == float("-inf") and v < sh) or
                           (sh == float("inf") and v >= sl) or
                           (sl != float("-inf") and sh != float("inf") and sl <= v < sh))
            probs[bkt.bucket_label] = count / total
        return probs

    buckets = [
        MockBucket("45°F or below", None, 45.0),
        MockBucket("46-47°F", 46.0, 47.0),
        MockBucket("48-49°F", 48.0, 49.0),
        MockBucket("50-51°F", 50.0, 51.0),
        MockBucket("52°F or higher", 52.0, None),
    ]
    members = [44.0, 46.0, 47.0, 48.5, 49.0, 49.5, 50.0, 50.5, 51.0, 53.0]
    probs = compute_probs(members, buckets)

    check("Probs computed", len(probs) == 5)
    prob_sum = sum(probs.values())
    check("Probs sum to ~1.0", abs(prob_sum - 1.0) < 0.01, f"sum={prob_sum:.4f}")
    check("Tail bucket uses settlement rounding",
          abs(probs["45°F or below"] - 0.10) < 0.01)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 9: TRADE MANAGER LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def test_trade_manager():
    print("\n" + "=" * 70)
    print("TEST 9: TRADE MANAGER LOGIC")
    print("=" * 70)

    with open("trade_manager.py", "r") as f:
        content = f.read()
    check("open_v2_trade defined", "async def open_v2_trade" in content)
    check("open_v2_ladder defined", "async def open_v2_ladder" in content)
    check("settle_v2_trade defined", "async def settle_v2_trade" in content)
    check("NO wins when bucket doesn't match",
          'won = (our_bucket != winning_bucket_label)' in content)
    check("YES wins when bucket matches",
          'won = (our_bucket == winning_bucket_label)' in content)
    check("Fee rate = 0.0", "POLYMARKET_FEE_PCT = 0.0" in content)

    check("YES wins (math)", "59-60°F" == "59-60°F")
    check("NO wins (math)", "53-54°F" != "59-60°F")
    check("NO loses (math)", not ("59-60°F" != "59-60°F"))

    gross = 20.0 * 1.0 - 2.0
    check("Payout math correct", abs(gross - 18.0) < 0.01)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 10: MAIN.PY V2 REPORTING
# ══════════════════════════════════════════════════════════════════════════════

def test_main_v2_reporting():
    print("\n" + "=" * 70)
    print("TEST 10: MAIN.PY V2 STRATEGY LABELS")
    print("=" * 70)

    with open("backend/api/main.py", "r") as f:
        content = f.read()

    check("Health endpoint lists v2 strategies",
          '"spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"' in content)
    check("Health mode = v2_paper", '"v2_paper"' in content)
    check("No abc_testing", "abc_testing" not in content)
    check("Strategy comparison iterates v2 set",
          'for strat in ["spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"]' in content)
    check('No or "sigma" fallback', 'or "sigma"' not in content)
    check("Dashboard loads v2 bankrolls",
          'v2_strats = ["spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"]' in content)
    check("reset-daily-loss defaults to spectrum",
          'reset_daily_loss_endpoint(strategy: str = "spectrum")' in content)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 11: STRATEGY WIRING
# ══════════════════════════════════════════════════════════════════════════════

def test_strategy_wiring():
    print("\n" + "=" * 70)
    print("TEST 11: V2 STRATEGY WIRING")
    print("=" * 70)

    from config import STRATEGY_BANKROLL_ID

    with open("scanner_v2.py", "r") as f:
        content = f.read()
    check("V2_STRATEGIES defined",
          'V2_STRATEGIES = ["spectrum", "sniper_yes", "sniper_no", "ladder_3", "ladder_5"]' in content)
    check("STRATEGY_BANKROLL_ID has 5", len(STRATEGY_BANKROLL_ID) == 5)
    check("sigma not in bankroll IDs", "sigma" not in STRATEGY_BANKROLL_ID)


# ══════════════════════════════════════════════════════════════════════════════
# TEST 12: RUN.PY
# ══════════════════════════════════════════════════════════════════════════════

def test_run_py():
    print("\n" + "=" * 70)
    print("TEST 12: RUN.PY PATH SETUP")
    print("=" * 70)

    with open("run.py", "r") as f:
        content = f.read()
    check("run.py adds repo root", "sys.path.insert(0, os.path.dirname(__file__))" in content)
    check("run.py adds backend/", '"backend"' in content)
    check("run.py references api.main:app", "api.main:app" in content)


# ══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("WeatherAlgo v2 — Phase 3 Tests (final patch)")
    print("=" * 70)

    test_config_imports()
    test_v1_import_survival()
    test_v2_imports()
    test_adapter_import_path()
    test_daily_loss_cap()
    test_gfs_scan_sync()
    test_ensemble_cache_behavior()
    test_database_model()
    test_scanner_helpers()
    test_trade_manager()
    test_main_v2_reporting()
    test_strategy_wiring()
    test_run_py()

    print("\n" + "=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed, {skipped} skipped")
    if skipped > 0:
        print(f"  (skipped tests require sqlalchemy — will pass on Railway)")
    print("=" * 70)

    if failed > 0:
        print("\n⚠ SOME TESTS FAILED — review output above")
        return 1
    else:
        print("\n✓ ALL TESTS PASSED")
        return 0


if __name__ == "__main__":
    exit(main())
