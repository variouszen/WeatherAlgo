# config.py — WeatherAlgo v2
from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

# ── Database ─────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://localhost/weatherarb")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ── Bot identity ──────────────────────────────────────────────────────────────
USER_AGENT = os.getenv("USER_AGENT", "WeatherArbBot/1.0 contact@example.com")

# ── Paper trading ────────────────────────────────────────────────────────────
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# ── Active strategies ─────────────────────────────────────────────────────────
# Controls which strategies evaluate NEW trades. Settlement still runs for all.
# Override in Railway env: ACTIVE_STRATEGIES=ladder_3,sniper_yes
ACTIVE_STRATEGIES = [s.strip() for s in os.getenv("ACTIVE_STRATEGIES", "ladder_3").split(",")]

# ── Ladder 3 per-city scan schedule (horizon optimization) ────────────────────
# When True, Ladder 3 only evaluates new entries when the city is inside its
# designated 4-hour entry window targeting ~30h before market close, AND the
# candidate market date's hours_to_close falls in the [24h, 38h] sweet spot.
# All other strategies are unaffected. Settlement still runs on every cycle.
# Enable in Railway env: LADDER_3_PER_CITY_SCAN_SCHEDULE=true
# Default: false — flip manually after confirming startup logs look correct.
LADDER_3_PER_CITY_SCAN_SCHEDULE = os.getenv("LADDER_3_PER_CITY_SCAN_SCHEDULE", "false").lower() == "true"

# ── Ensemble config ──────────────────────────────────────────────────────────
ENSEMBLE_MODELS = os.getenv("ENSEMBLE_MODELS", "gfs_seamless,ecmwf_ifs025")
SCAN_SYNC_GFS_RUNS = os.getenv("SCAN_SYNC_GFS_RUNS", "true").lower() == "true"

# ── Scan timing ──────────────────────────────────────────────────────────────
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "300"))
MAX_FORECAST_DAYS = 2  # day+0 and day+1 only (day+2 removed per spec)


# ══════════════════════════════════════════════════════════════════════════════
# V2 STRATEGY CONFIGS — Locked per Master Spec Sections 3A-3E
# ══════════════════════════════════════════════════════════════════════════════

# ── Spectrum — Native Bucket Benchmark ($500) ────────────────────────────────
# YES + NO combined; picks best edge per bucket; $2.00 sizing;
# one trade per city-date.
SPECTRUM_V2_CONFIG = {
    # Gate thresholds
    "min_edge": 0.08,              # Gate 1: 8% minimum edge
    "min_ensemble_prob": 0.05,     # Gate 2: 5% minimum (both sides)
    "max_ask": 0.50,               # Gate 3: 50¢ max ask
    "max_peak_distance": 4,        # Gate 4: YES only, ±4 buckets from peak
    # Sizing
    "trade_size": 2.00,            # Fixed $2.00 per trade
    # Bankroll
    "bankroll_id": 3,
    "starting_bankroll": 500.0,
    # Risk limits (Spec Section 8)
    "max_open_per_city_date": 1,
    "max_open_total": 20,
    "max_daily_loss": 20.0,
    "max_single_city_exposure": 10.0,
}

# ── Sniper YES — High-Conviction Cheap Buckets ($500) ────────────────────────
SNIPER_YES_CONFIG = {
    # Gate thresholds
    "min_edge": 0.10,              # Gate 1: 10% minimum edge
    "min_edge_ratio": 2.0,         # Gate 2: ensemble_prob >= 2x market_ask
    "max_ask": 0.15,               # Gate 3: 15¢ max ask
    "min_ensemble_prob": 0.08,     # Gate 4: 8% minimum
    "max_peak_distance": 3,        # Gate 5: ±3 buckets from peak
    "max_spread": 0.05,            # Gate 7: 5¢ max spread
    # Sizing
    "trade_size": 1.00,            # Fixed $1.00 per trade
    # Bankroll
    "bankroll_id": 4,
    "starting_bankroll": 500.0,
    # Risk limits
    "max_open_per_city_date": 1,
    "max_open_total": 20,
    "max_daily_loss": 10.0,
    "max_single_city_exposure": 5.0,
}

# ── Sniper NO — High-Conviction Overpriced Buckets ($500) ────────────────────
SNIPER_NO_CONFIG = {
    # Gate thresholds
    "max_ensemble_prob": 0.03,     # Gate 1: model says <3% chance
    "max_no_ask": 0.55,            # Gate 2: NO ask ≤ 55¢ (LOCKED — do not tighten)
    "min_edge": 0.10,              # Gate 3: 10% edge on NO side
    "max_model_prob": 0.05,        # Gate 4: both models <5%
    "max_spread": 0.05,            # Gate 5: 5¢ max spread
    # Sizing
    "trade_size": 1.00,            # Fixed $1.00 per trade
    # Bankroll
    "bankroll_id": 5,
    "starting_bankroll": 500.0,
    # Risk limits
    "max_open_per_city_date": 1,
    "max_open_total": 20,
    "max_daily_loss": 10.0,
    "max_single_city_exposure": 5.0,
}

# ── Ladder 3 — Tight Package ($500) ─────────────────────────────────────────
LADDER_3_CONFIG = {
    "width": 3,
    # Gate thresholds
    "min_package_edge": 0.15,      # Gate 1: 15% return on capital
    "min_package_prob": 0.60,      # Gate 2: 60% package probability
    "max_package_cost": 10.00,     # Gate 3: $10 max package cost
    "min_package_cost": 1.00,      # Gate 3b: $1 min package cost (blocks penny packages)
    "min_leg_ask": 0.03,           # Gate 4: $0.03 min per-leg ask (blocks penny buckets)
    "max_leg_ask": 0.95,           # Gate 4b: $0.95 max per-leg ask (blocks near-certain legs with negligible payout)
    "shares_per_bucket": 10,       # 10 shares per leg
    # Bankroll
    "bankroll_id": 6,
    "starting_bankroll": 500.0,
    # Risk limits
    "max_open_per_city_date": 1,
    "max_open_total": 50,
    "max_daily_loss": 30.0,
    "max_single_city_exposure": 20.0,
}

# ── Ladder 5 — Wide Package ($500) ──────────────────────────────────────────
LADDER_5_CONFIG = {
    "width": 5,
    # Gate thresholds (same as Ladder 3)
    "min_package_edge": 0.15,
    "min_package_prob": 0.60,
    "max_package_cost": 10.00,
    "min_package_cost": 1.00,
    "min_leg_ask": 0.03,
    "max_leg_ask": 0.95,
    "shares_per_bucket": 10,
    # Bankroll
    "bankroll_id": 7,
    "starting_bankroll": 500.0,
    # Risk limits
    "max_open_per_city_date": 1,
    "max_open_total": 10,
    "max_daily_loss": 30.0,
    "max_single_city_exposure": 20.0,
}


# ── Bankroll IDs — maps strategy name to bankroll_state row ID ───────────────
# Legacy IDs 1 (sigma) and 2 (forecast_edge) are retired and removed.
# The rows still exist in the DB but are not referenced by any v2 code.
STRATEGY_BANKROLL_ID = {
    "spectrum": 3,
    "sniper_yes": 4,
    "sniper_no": 5,
    "ladder_3": 6,
    "ladder_5": 7,
}


# ── Cities (50) ──────────────────────────────────────────────────────────────
INTL_DEFAULT_MODEL = "icon_seamless"
INTL_DEFAULT_LABEL = "ICON"

CITIES = [
    # ── US (6) — coordinates matched to Polymarket resolution stations ───
    {"name": "New York",      "lat": 40.7772,  "lon": -73.8726,  "station": "KLGA", "emoji": "🗽",  "celsius": False, "timezone": "America/New_York"},
    {"name": "Chicago",       "lat": 41.9742,  "lon": -87.9073,  "station": "KORD", "emoji": "🌬️", "celsius": False, "timezone": "America/Chicago"},
    {"name": "Seattle",       "lat": 47.4499,  "lon": -122.3118, "station": "KSEA", "emoji": "🌧️", "celsius": False, "timezone": "America/Los_Angeles"},
    {"name": "Atlanta",       "lat": 33.6367,  "lon": -84.4279,  "station": "KATL", "emoji": "🍑",  "celsius": False, "timezone": "America/New_York"},
    {"name": "Dallas",        "lat": 32.8459,  "lon": -96.8509,  "station": "KDAL", "emoji": "🤠",  "celsius": False, "timezone": "America/Chicago"},
    {"name": "Miami",         "lat": 25.7954,  "lon": -80.2901,  "station": "KMIA", "emoji": "🌴",  "celsius": False, "timezone": "America/New_York"},
    # ── Europe (7) ───────────────────────────────────────────────────────
    {"name": "London",        "lat": 51.5053,  "lon": 0.0553,    "station": "EGLC", "emoji": "🎡",  "celsius": True, "timezone": "Europe/London"},
    {"name": "Paris",         "lat": 49.0097,  "lon": 2.5479,    "station": "LFPG", "emoji": "🗼",  "celsius": True, "timezone": "Europe/Paris"},
    {"name": "Munich",        "lat": 48.3538,  "lon": 11.7861,   "station": "EDDM", "emoji": "🍺",  "celsius": True, "timezone": "Europe/Berlin"},
    {"name": "Madrid",        "lat": 40.4934,  "lon": -3.5722,   "station": "LEMD", "emoji": "🇪🇸", "celsius": True, "timezone": "Europe/Madrid"},
    {"name": "Milan",         "lat": 45.6306,  "lon": 8.7281,    "station": "LIMC", "emoji": "🇮🇹", "celsius": True, "timezone": "Europe/Rome"},
    {"name": "Warsaw",        "lat": 52.1657,  "lon": 20.9671,   "station": "EPWA", "emoji": "🇵🇱", "celsius": True, "timezone": "Europe/Warsaw"},
    {"name": "Ankara",        "lat": 40.1281,  "lon": 32.9951,   "station": "LTAC", "emoji": "🇹🇷", "celsius": True, "timezone": "Europe/Istanbul"},
    # ── East Asia (5) ────────────────────────────────────────────────────
    {"name": "Tokyo",         "lat": 35.5494,  "lon": 139.7798,  "station": "RJTT", "emoji": "🏯",  "celsius": True, "timezone": "Asia/Tokyo",     "primary_model": "jma_seamless", "primary_label": "JMA", "single_model": True},
    {"name": "Seoul",         "lat": 37.4691,  "lon": 126.4510,  "station": "RKSI", "emoji": "🇰🇷", "celsius": True, "timezone": "Asia/Seoul"},
    {"name": "Shanghai",      "lat": 31.1434,  "lon": 121.8050,  "station": "ZSPD", "emoji": "🏙️", "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Taipei",        "lat": 25.0697,  "lon": 121.5525,  "station": "RCSS", "emoji": "🇹🇼", "celsius": True, "timezone": "Asia/Taipei"},
    {"name": "Hong Kong",     "lat": 22.3019,  "lon": 114.1742,  "station": "HKO",  "emoji": "🇭🇰", "celsius": True, "timezone": "Asia/Hong_Kong"},
    # ── South / Southeast Asia (2) ───────────────────────────────────────
    {"name": "Singapore",     "lat": 1.3502,   "lon": 103.9940,  "station": "WSSS", "emoji": "🇸🇬", "celsius": True, "timezone": "Asia/Singapore"},
    {"name": "Lucknow",       "lat": 26.7606,  "lon": 80.8893,   "station": "VILK", "emoji": "🇮🇳", "celsius": True, "timezone": "Asia/Kolkata"},
    # ── Middle East (1) ──────────────────────────────────────────────────
    {"name": "Tel Aviv",      "lat": 32.0114,  "lon": 34.8867,   "station": "LLBG", "emoji": "🇮🇱", "celsius": True, "timezone": "Asia/Jerusalem"},
    # ── Canada (1) ───────────────────────────────────────────────────────
    {"name": "Toronto",       "lat": 43.6772,  "lon": -79.6306,  "station": "CYYZ", "emoji": "🇨🇦", "celsius": True, "timezone": "America/Toronto"},
    # ── South America (2) ────────────────────────────────────────────────
    {"name": "Buenos Aires",  "lat": -34.8222, "lon": -58.5358,  "station": "SAEZ", "emoji": "🇦🇷", "celsius": True, "timezone": "America/Argentina/Buenos_Aires"},
    {"name": "Sao Paulo",     "lat": -23.4313, "lon": -46.4700,  "station": "SBGR", "emoji": "🇧🇷", "celsius": True, "timezone": "America/Sao_Paulo"},
    # ── Oceania (1) ──────────────────────────────────────────────────────
    {"name": "Wellington",    "lat": -41.3272, "lon": 174.8053,  "station": "NZWN", "emoji": "🇳🇿", "celsius": True, "timezone": "Pacific/Auckland"},

    # ════════════════════════════════════════════════════════════════════════════
    # EXPANSION CITIES (25) — Session 16, Apr 2026
    # All coordinates matched to exact Polymarket resolution station.
    # ════════════════════════════════════════════════════════════════════════════

    # ── US expansion (5) — all °F ────────────────────────────────────────────
    {"name": "Houston",       "lat": 29.6454,  "lon": -95.2789,  "station": "KHOU", "emoji": "🤠",  "celsius": False, "timezone": "America/Chicago"},
    {"name": "Austin",        "lat": 30.1975,  "lon": -97.6664,  "station": "KAUS", "emoji": "🎸",  "celsius": False, "timezone": "America/Chicago"},
    {"name": "Denver",        "lat": 39.7170,  "lon": -104.7517, "station": "KBKF", "emoji": "🏔️", "celsius": False, "timezone": "America/Denver"},
    {"name": "Los Angeles",   "lat": 33.9425,  "lon": -118.4081, "station": "KLAX", "emoji": "🎬",  "celsius": False, "timezone": "America/Los_Angeles"},
    {"name": "San Francisco", "lat": 37.6213,  "lon": -122.3790, "station": "KSFO", "emoji": "🌉",  "celsius": False, "timezone": "America/Los_Angeles"},

    # ── Europe expansion (4) ─────────────────────────────────────────────────
    # Note: Istanbul and Moscow resolve via NOAA/weather.gov (same path as Ankara/Tel Aviv)
    {"name": "Istanbul",      "lat": 41.2769,  "lon": 28.7519,   "station": "LTFM", "emoji": "🕌",  "celsius": True, "timezone": "Europe/Istanbul"},
    {"name": "Moscow",        "lat": 55.5915,  "lon": 37.2615,   "station": "UUWW", "emoji": "🇷🇺", "celsius": True, "timezone": "Europe/Moscow"},
    {"name": "Helsinki",      "lat": 60.3172,  "lon": 24.9633,   "station": "EFHK", "emoji": "🇫🇮", "celsius": True, "timezone": "Europe/Helsinki"},
    {"name": "Amsterdam",     "lat": 52.3105,  "lon": 4.7683,    "station": "EHAM", "emoji": "🚲",  "celsius": True, "timezone": "Europe/Amsterdam"},

    # ── East Asia expansion (7) ──────────────────────────────────────────────
    {"name": "Beijing",       "lat": 40.0799,  "lon": 116.5843,  "station": "ZBAA", "emoji": "🏯",  "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Shenzhen",      "lat": 22.6398,  "lon": 113.8100,  "station": "ZGSZ", "emoji": "🏙️", "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Chengdu",       "lat": 30.5785,  "lon": 103.9467,  "station": "ZUUU", "emoji": "🐼",  "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Wuhan",         "lat": 30.7838,  "lon": 114.2081,  "station": "ZHHH", "emoji": "🇨🇳", "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Chongqing",     "lat": 29.7192,  "lon": 106.6421,  "station": "ZUCK", "emoji": "🌶️", "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Guangzhou",     "lat": 23.3924,  "lon": 113.2988,  "station": "ZGGG", "emoji": "🇨🇳", "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Busan",         "lat": 35.1795,  "lon": 128.9382,  "station": "RKPK", "emoji": "🇰🇷", "celsius": True, "timezone": "Asia/Seoul"},

    # ── Southeast Asia expansion (3) ─────────────────────────────────────────
    # Jakarta: Halim Perdanakusuma (WIHH), NOT Soekarno-Hatta (WIII)
    {"name": "Jakarta",       "lat": -6.2661,  "lon": 106.8907,  "station": "WIHH", "emoji": "🇮🇩", "celsius": True, "timezone": "Asia/Jakarta"},
    {"name": "Manila",        "lat": 14.5086,  "lon": 121.0200,  "station": "RPLL", "emoji": "🇵🇭", "celsius": True, "timezone": "Asia/Manila"},
    {"name": "Kuala Lumpur",  "lat": 2.7456,   "lon": 101.7072,  "station": "WMKK", "emoji": "🇲🇾", "celsius": True, "timezone": "Asia/Kuala_Lumpur"},

    # ── Middle East / South Asia expansion (2) ───────────────────────────────
    {"name": "Jeddah",        "lat": 21.6796,  "lon": 39.1565,   "station": "OEJN", "emoji": "🕌",  "celsius": True, "timezone": "Asia/Riyadh"},
    # Karachi: Jinnah International (OPKC) — confirmed via Wunderground URL
    {"name": "Karachi",       "lat": 24.9008,  "lon": 67.1681,   "station": "OPKC", "emoji": "🇵🇰", "celsius": True, "timezone": "Asia/Karachi"},

    # ── Africa expansion (2) ─────────────────────────────────────────────────
    {"name": "Lagos",         "lat": 6.5774,   "lon": 3.3215,    "station": "DNMM", "emoji": "🇳🇬", "celsius": True, "timezone": "Africa/Lagos"},
    {"name": "Cape Town",     "lat": -33.9715, "lon": 18.6022,   "station": "FACT", "emoji": "🇿🇦", "celsius": True, "timezone": "Africa/Johannesburg"},

    # ── Americas expansion (2) ───────────────────────────────────────────────
    {"name": "Mexico City",   "lat": 19.4363,  "lon": -99.0721,  "station": "MMMX", "emoji": "🇲🇽", "celsius": True, "timezone": "America/Mexico_City"},
    # Panama City: Marcos A. Gelabert (MPMG/Albrook), NOT Tocumen (MPTO)
    {"name": "Panama City",   "lat": 8.9795,   "lon": -79.5559,  "station": "MPMG", "emoji": "🇵🇦", "celsius": True, "timezone": "America/Panama"},
]

# ── Model tier mapping ────────────────────────────────────────────────────────
CITY_MODEL_TIER: dict[str, str] = {}
for _city in CITIES:
    if not _city.get("celsius", False):
        CITY_MODEL_TIER[_city["name"]] = "NOAA"
    else:
        CITY_MODEL_TIER[_city["name"]] = _city.get("primary_label", INTL_DEFAULT_LABEL)

# ── Static thresholds — DASHBOARD / ANALYTICS ONLY ───────────────────────────
TEMP_THRESHOLDS_F = [40, 45, 50, 55, 60, 62, 64, 65, 66, 67, 68, 69, 70, 72, 75, 80, 85, 90]
TEMP_THRESHOLDS_C = [-5, 0, 2, 4, 5, 6, 7, 8, 9, 10, 12, 14, 15, 16, 17, 18, 20, 22, 25, 28, 30]
TEMP_THRESHOLDS = TEMP_THRESHOLDS_F

NOAA_BASE = "https://api.weather.gov"
NOAA_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/geo+json",
}

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE  = "https://clob.polymarket.com"
