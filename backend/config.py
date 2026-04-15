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


# ── Cities (25) ──────────────────────────────────────────────────────────────
INTL_DEFAULT_MODEL = "icon_seamless"
INTL_DEFAULT_LABEL = "ICON"

CITIES = [
    # ── US (6) ───────────────────────────────────────────────────────────
    {"name": "New York",      "lat": 40.7128,  "lon": -74.0060,  "station": "KLGA", "emoji": "🗽",  "celsius": False, "timezone": "America/New_York"},
    {"name": "Chicago",       "lat": 41.8781,  "lon": -87.6298,  "station": "KORD", "emoji": "🌬️", "celsius": False, "timezone": "America/Chicago"},
    {"name": "Seattle",       "lat": 47.6062,  "lon": -122.3321, "station": "KSEA", "emoji": "🌧️", "celsius": False, "timezone": "America/Los_Angeles"},
    {"name": "Atlanta",       "lat": 33.6367,  "lon": -84.4279,  "station": "KATL", "emoji": "🍑",  "celsius": False, "timezone": "America/New_York"},
    {"name": "Dallas",        "lat": 32.8471,  "lon": -96.8518,  "station": "KDAL", "emoji": "🤠",  "celsius": False, "timezone": "America/Chicago"},
    {"name": "Miami",         "lat": 25.7617,  "lon": -80.1918,  "station": "KMIA", "emoji": "🌴",  "celsius": False, "timezone": "America/New_York"},
    # ── Europe (3) ───────────────────────────────────────────────────────
    {"name": "London",        "lat": 51.5033,  "lon": 0.0550,    "station": "EGLC", "emoji": "🎡",  "celsius": True, "timezone": "Europe/London"},
    {"name": "Paris",         "lat": 48.8566,  "lon": 2.3522,    "station": "LFPG", "emoji": "🗼",  "celsius": True, "timezone": "Europe/Paris"},
    {"name": "Munich",        "lat": 48.1351,  "lon": 11.5820,   "station": "EDDM", "emoji": "🍺",  "celsius": True, "timezone": "Europe/Berlin"},
    # ── East Asia (1 → 5) ────────────────────────────────────────────────
    {"name": "Tokyo",         "lat": 35.5494,  "lon": 139.7798,  "station": "RJTT", "emoji": "🏯",  "celsius": True, "timezone": "Asia/Tokyo",     "primary_model": "jma_seamless", "primary_label": "JMA", "single_model": True},
    {"name": "Seoul",         "lat": 37.5665,  "lon": 126.9780,  "station": "RKSS", "emoji": "🇰🇷", "celsius": True, "timezone": "Asia/Seoul"},
    {"name": "Shanghai",      "lat": 31.2304,  "lon": 121.4737,  "station": "ZSSS", "emoji": "🏙️", "celsius": True, "timezone": "Asia/Shanghai"},
    {"name": "Taipei",        "lat": 25.0330,  "lon": 121.5654,  "station": "RCSS", "emoji": "🇹🇼", "celsius": True, "timezone": "Asia/Taipei"},
    {"name": "Hong Kong",     "lat": 22.3193,  "lon": 114.1694,  "station": "VHHH", "emoji": "🇭🇰", "celsius": True, "timezone": "Asia/Hong_Kong"},
    # ── South / Southeast Asia (2) ───────────────────────────────────────
    {"name": "Singapore",     "lat": 1.3521,   "lon": 103.8198,  "station": "WSSS", "emoji": "🇸🇬", "celsius": True, "timezone": "Asia/Singapore"},
    {"name": "Lucknow",       "lat": 26.8467,  "lon": 80.9462,   "station": "VILK", "emoji": "🇮🇳", "celsius": True, "timezone": "Asia/Kolkata"},
    # ── Middle East (1) ──────────────────────────────────────────────────
    {"name": "Tel Aviv",      "lat": 32.0853,  "lon": 34.7818,   "station": "LLBG", "emoji": "🇮🇱", "celsius": True, "timezone": "Asia/Jerusalem"},
    # ── Europe (3 → 7) ───────────────────────────────────────────────────
    {"name": "Madrid",        "lat": 40.4168,  "lon": -3.7038,   "station": "LEMD", "emoji": "🇪🇸", "celsius": True, "timezone": "Europe/Madrid"},
    {"name": "Milan",         "lat": 45.4642,  "lon": 9.1900,    "station": "LIML", "emoji": "🇮🇹", "celsius": True, "timezone": "Europe/Rome"},
    {"name": "Warsaw",        "lat": 52.2297,  "lon": 21.0122,   "station": "EPWA", "emoji": "🇵🇱", "celsius": True, "timezone": "Europe/Warsaw"},
    {"name": "Ankara",        "lat": 39.9334,  "lon": 32.8597,   "station": "LTAC", "emoji": "🇹🇷", "celsius": True, "timezone": "Europe/Istanbul"},
    # ── Canada (1) ───────────────────────────────────────────────────────
    {"name": "Toronto",       "lat": 43.6532,  "lon": -79.3832,  "station": "CYYZ", "emoji": "🇨🇦", "celsius": True, "timezone": "America/Toronto"},
    # ── South America (2) ────────────────────────────────────────────────
    {"name": "Buenos Aires",  "lat": -34.6037, "lon": -58.3816,  "station": "SABE", "emoji": "🇦🇷", "celsius": True, "timezone": "America/Argentina/Buenos_Aires"},
    {"name": "Sao Paulo",     "lat": -23.5505, "lon": -46.6333,  "station": "SBSP", "emoji": "🇧🇷", "celsius": True, "timezone": "America/Sao_Paulo"},
    # ── Oceania (1) ──────────────────────────────────────────────────────
    {"name": "Wellington",    "lat": -41.2924, "lon": 174.7787,  "station": "NZWN", "emoji": "🇳🇿", "celsius": True, "timezone": "Pacific/Auckland"},
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
