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

# ── Bot config ─────────────────────────────────────────────────────────────────
BOT_CONFIG = {
    # --- Core filters (HARD — no override) ---
    "min_edge": float(os.getenv("MIN_EDGE", "0.08")),
    "min_confidence": float(os.getenv("MIN_CONFIDENCE", "0.68")),
    "min_event_volume":  float(os.getenv("MIN_EVENT_VOLUME",  "5000")),  # lowered — just confirm market exists
    "min_bucket_volume": float(os.getenv("MIN_BUCKET_VOLUME", "500")),   # lowered — just confirm bucket is tradeable

    # --- Directional gate (HARD — no override) ---
    # YES trade only if forecast > threshold, NO trade only if forecast < threshold
    "require_directional_gate": True,

    # --- Buffer filter REMOVED ---
    # sigma + edge + confidence already handle near-threshold uncertainty

    # --- Multi-model consensus (confidence layer — sizing modifier) ---
    # GFS called via Open-Meteo as independent validator for dual-model tiers
    # US:        NOAA primary + GFS validator (2 models)
    # Europe:    ICON primary + GFS validator (2 models)
    # Tokyo:     JMA single-model (no validator — best-in-class at 0.25°C error)
    "consensus_full_size_models": 2,    # all models agree → full kelly
    "consensus_reduced_size_models": 1, # only primary agrees → reduced kelly
    "consensus_reduced_factor": 0.5,    # multiply kelly by this when reduced
    # Max raw forecast spread between any two models before skipping entirely
    "max_model_spread_f": 6.0,
    "max_model_spread_c": 3.0,

    # --- Crowd conviction filters ---
    "max_yes_price_for_no": 0.80,   # don't fade crowd when YES > 80¢
    "max_yes_price": 0.42,           # don't buy YES above this
    "min_no_price": 0.58,            # don't buy NO when YES below this

    # --- Early market window (timing modifier — not a gate) ---
    # Market age is calculated from endDate minus 2 days (approx open time)
    "early_window_hours": 6,                # hours after market open
    "early_window_confidence_boost": 0.08,  # lower min_confidence by this amount
    "early_window_kelly_boost": 1.25,       # multiply kelly by this (capped at max_position_pct)

    # --- Re-entry system ---
    "reentry_enabled": True,
    "reentry_min_edge_premium": 0.04,       # re-entry needs min_edge + this (so 12% default)
    "reentry_min_crowd_move": 0.08,         # crowd price must have moved by this much
    "reentry_min_edge_improvement": 0.03,   # edge must beat prior high-water mark by this
    "reentry_cooldown_minutes": 45,         # minimum wait between entries on same city
    "reentry_max_per_city": 2,              # max re-entries (total 3 trades per city per day)
    "reentry_edge_hwm_cap": 0.85,           # cap edge high-water mark at 85% so blowouts don't block
    "reentry_no_late_entry_hours": 3,       # no re-entry within 3h of market close

    # --- Position sizing ---
    "kelly_fraction": 0.25,
    "max_position_pct": 0.02,
    "min_position_usd": 10.0,
    "max_open_per_city": 1,
    "max_correlated_yes": 3,

    # --- Multi-day city caps ---
    "max_positions_per_city": 3,    # hard cap: total open trades per city across all dates
    "max_city_exposure_pct": 0.06,  # max combined position_size_usd per city as % of bankroll

    # --- Circuit breakers ---
    "daily_loss_cap_pct": 0.05,
    "bankroll_floor": 200.0,

    # --- Fees ---
    "polymarket_fee_pct": 0.02,

    # --- Forecast ---
    "max_forecast_days": 2,
    "scan_interval_seconds": 300,
}

# ── Cities ────────────────────────────────────────────────────────────────────
# International primary model: defaults to ICON (DWD, Germany).
# Override per city with "primary_model" / "primary_label" for regional accuracy.
# Available models: icon_seamless, gem_seamless (Canada), jma_seamless (Japan),
#                   ukmo_seamless (UK), bom_access_global (Australia)
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
    # ── East Asia (1) — JMA single-model, no GFS validator ───────────────────
    # JMA is best-in-class for Tokyo (0.25°C avg error). GFS adds noise (+1.3°C warm bias).
    # Single-model status earned through data audit (Session 8, March 14 2026).
    # Seoul/Shanghai removed: all NWP models unreliable (2.4-5.5°C errors).
    # If re-adding Asian cities later, use ecmwf_ifs025 as primary (confirmed working).
    {"name": "Tokyo",         "lat": 35.5494,  "lon": 139.7798,  "station": "RJTT", "emoji": "🏯",  "celsius": True, "timezone": "Asia/Tokyo",     "primary_model": "jma_seamless", "primary_label": "JMA", "single_model": True},
    # To add new regions later, add "primary_model" / "primary_label" overrides:
    #   gem_seamless (Canada), bom_access_global (Australia), ukmo_seamless (UK)
]

# ── Model tier mapping (for analytics grouping) ──────────────────────────────
# Derived from CITIES config — maps city name → model tier label.
# Used by dashboard to group performance by forecast model.
CITY_MODEL_TIER: dict[str, str] = {}
for _city in CITIES:
    if not _city.get("celsius", False):
        CITY_MODEL_TIER[_city["name"]] = "NOAA"
    else:
        CITY_MODEL_TIER[_city["name"]] = _city.get("primary_label", INTL_DEFAULT_LABEL)

# ── Static thresholds — DASHBOARD / ANALYTICS ONLY ───────────────────────────
# These lists are NOT used for live trading. Live trading thresholds are
# extracted dynamically from Polymarket's actual bucket structure per city/day.
# These remain for: dashboard charts, historical analysis, noaa.py bucket_probs
# pre-computation (API endpoints), and backward compatibility.
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
