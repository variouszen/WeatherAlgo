# backend/config.py
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

# ── Paper trading config ──────────────────────────────────────────────────────
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "2000.0"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# ── Strategy B: Sigma (existing bot logic) ────────────────────────────────────
BOT_CONFIG = {
    # --- Core filters (HARD — no override) ---
    "min_edge": float(os.getenv("MIN_EDGE", "0.08")),
    "min_confidence": float(os.getenv("MIN_CONFIDENCE", "0.68")),
    "min_event_volume":  float(os.getenv("MIN_EVENT_VOLUME",  "5000")),
    "min_bucket_volume": float(os.getenv("MIN_BUCKET_VOLUME", "500")),

    # --- Directional gate (HARD — no override) ---
    "require_directional_gate": True,

    # --- Multi-model consensus (FIXED: now uses directional agreement) ---
    # Both models must be on the same side of threshold as bet direction.
    # If not → hard block (trade doesn't enter).
    # If yes but spread is large → half sizing via spread gate.
    "consensus_full_size_models": 2,
    "consensus_reduced_size_models": 1,
    "consensus_reduced_factor": 0.5,
    "max_model_spread_f": 6.0,
    "max_model_spread_c": 3.0,

    # --- Crowd conviction filters ---
    "max_yes_price_for_no": 0.80,
    "max_yes_price": 0.42,
    "min_no_price": 0.58,

    # --- Early market window (timing modifier — not a gate) ---
    "early_window_hours": 6,
    "early_window_confidence_boost": 0.08,
    "early_window_kelly_boost": 1.25,

    # --- Re-entry system ---
    "reentry_enabled": True,
    "reentry_min_edge_premium": 0.04,
    "reentry_min_crowd_move": 0.08,
    "reentry_min_edge_improvement": 0.03,
    "reentry_cooldown_minutes": 45,
    "reentry_max_per_city": 2,
    "reentry_edge_hwm_cap": 0.85,
    "reentry_no_late_entry_hours": 3,

    # --- Position sizing ---
    "kelly_fraction": 0.25,
    "max_position_pct": 0.02,
    "min_position_usd": 10.0,
    "max_open_per_city": 1,
    "max_correlated_yes": 3,

    # --- Multi-day city caps ---
    "max_positions_per_city": 3,
    "max_city_exposure_pct": 0.06,

    # --- Circuit breakers ---
    # For paper A/B testing: set very high to avoid throttling data collection.
    # daily_loss_cap_pct effectively disabled at 100%.
    "daily_loss_cap_pct": 1.00,       # 100% = effectively no cap for paper testing
    "daily_loss_cap_floor_usd": 50.0, # floor for when we re-enable
    "bankroll_floor": 0.0,            # allow strategies to hit zero — that IS the data

    # --- Fees ---
    "polymarket_fee_pct": 0.02,

    # --- Forecast ---
    "max_forecast_days": 2,
    "scan_interval_seconds": 300,
}


# ── Strategy A: Forecast Edge (new) ──────────────────────────────────────────
# Simpler gate stack. No consensus, no spread gate, no directional probability
# gate. The forecast gap IS the margin of safety.
FORECAST_EDGE_CONFIG = {
    # --- Forecast gap gate: THE defining gate ---
    "forecast_gap_f": 4.0,   # US cities: primary must be ≥4°F past threshold
    "forecast_gap_c": 2.0,   # International: primary must be ≥2°C past threshold

    # --- Same core filters as Strategy B ---
    "min_edge": 0.08,
    "min_event_volume": 5000,
    "min_bucket_volume": 500,

    # --- Same crowd / price bounds ---
    "max_yes_price_for_no": 0.80,
    "max_yes_price": 0.42,
    "min_no_price": 0.58,

    # --- Same position sizing ---
    "kelly_fraction": 0.25,
    "max_position_pct": 0.02,
    "min_position_usd": 10.0,
    "max_correlated_yes": 3,

    # --- Same multi-day city caps ---
    "max_positions_per_city": 3,
    "max_city_exposure_pct": 0.06,

    # --- Circuit breakers (disabled for paper) ---
    "daily_loss_cap_pct": 1.00,
    "daily_loss_cap_floor_usd": 50.0,
    "bankroll_floor": 0.0,

    # --- Fees ---
    "polymarket_fee_pct": 0.02,
}

# Bankroll IDs — maps strategy name to bankroll_state row ID
STRATEGY_BANKROLL_ID = {
    "sigma": 1,
    "forecast_edge": 2,
}


# ── Cities ────────────────────────────────────────────────────────────────────
INTL_DEFAULT_MODEL = "icon_seamless"
INTL_DEFAULT_LABEL = "ICON"

CITIES = [
    # ── US (6) — NOAA primary + GFS validator ─────────────────────────────────
    {"name": "New York",      "lat": 40.7128,  "lon": -74.0060,  "station": "KLGA", "emoji": "🗽",  "celsius": False, "timezone": "America/New_York"},
    {"name": "Chicago",       "lat": 41.8781,  "lon": -87.6298,  "station": "KORD", "emoji": "🌬️", "celsius": False, "timezone": "America/Chicago"},
    {"name": "Seattle",       "lat": 47.6062,  "lon": -122.3321, "station": "KSEA", "emoji": "🌧️", "celsius": False, "timezone": "America/Los_Angeles"},
    {"name": "Atlanta",       "lat": 33.6367,  "lon": -84.4279,  "station": "KATL", "emoji": "🍑",  "celsius": False, "timezone": "America/New_York"},
    {"name": "Dallas",        "lat": 32.8471,  "lon": -96.8518,  "station": "KDAL", "emoji": "🤠",  "celsius": False, "timezone": "America/Chicago"},
    {"name": "Miami",         "lat": 25.7617,  "lon": -80.1918,  "station": "KMIA", "emoji": "🌴",  "celsius": False, "timezone": "America/New_York"},
    # ── Europe (3) — ICON primary + GFS validator ─────────────────────────────
    {"name": "London",        "lat": 51.5033,  "lon": 0.0550,    "station": "EGLC", "emoji": "🎡",  "celsius": True, "timezone": "Europe/London"},
    {"name": "Paris",         "lat": 48.8566,  "lon": 2.3522,    "station": "LFPG", "emoji": "🗼",  "celsius": True, "timezone": "Europe/Paris"},
    {"name": "Munich",        "lat": 48.1351,  "lon": 11.5820,   "station": "EDDM", "emoji": "🍺",  "celsius": True, "timezone": "Europe/Berlin"},
    # ── East Asia (1) — JMA single-model ──────────────────────────────────────
    {"name": "Tokyo",         "lat": 35.5494,  "lon": 139.7798,  "station": "RJTT", "emoji": "🏯",  "celsius": True, "timezone": "Asia/Tokyo",     "primary_model": "jma_seamless", "primary_label": "JMA", "single_model": True},
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
