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
    # --- Core filters ---
    "min_edge": float(os.getenv("MIN_EDGE", "0.08")),
    "min_confidence": float(os.getenv("MIN_CONFIDENCE", "0.68")),
    "min_market_volume": float(os.getenv("MIN_VOLUME", "50000")),

    # --- Multi-source consensus (NEW v2) ---
    # Both NOAA and Open-Meteo must agree on direction
    "require_source_consensus": True,
    # Max spread allowed between the two forecast sources before skipping
    # If NOAA says 72°F and Open-Meteo says 65°F, spread=7 > max=5 → skip (too uncertain)
    "max_source_spread_f": 5.0,
    "max_source_spread_c": 3.0,
    # Minimum buffer between consensus forecast and threshold
    # Prevents trading razor-edge cases like NOAA=66 on a 64°F threshold
    "min_buffer_f": 4.0,
    "min_buffer_c": 1.5,
    # Skip NO trades if crowd already prices YES this high — they know it's a lock
    # Today's lesson: NYC YES was 85¢, bot shorted it and lost. This kills that.
    "max_yes_price_for_no": 0.80,
    # Skip YES trades above this price (existing)
    "max_yes_price": 0.42,
    "min_no_price": 0.58,

    # --- Position sizing ---
    "kelly_fraction": 0.25,
    "max_position_pct": 0.02,
    "min_position_usd": 10.0,
    "max_open_per_city": 1,
    "max_correlated_yes": 3,

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
CITIES = [
    {"name": "New York",      "lat": 40.7128,  "lon": -74.0060,  "station": "KLGA", "emoji": "🗽",  "celsius": False},
    {"name": "Chicago",       "lat": 41.8781,  "lon": -87.6298,  "station": "KORD", "emoji": "🌬️", "celsius": False},
    {"name": "Seattle",       "lat": 47.6062,  "lon": -122.3321, "station": "KSEA", "emoji": "🌧️", "celsius": False},
    {"name": "Atlanta",       "lat": 33.7490,  "lon": -84.3880,  "station": "KFTY", "emoji": "🍑",  "celsius": False},
    {"name": "Dallas",        "lat": 32.7767,  "lon": -96.7970,  "station": "KDFW", "emoji": "🤠",  "celsius": False},
    {"name": "Miami",         "lat": 25.7617,  "lon": -80.1918,  "station": "KMIA", "emoji": "🌴",  "celsius": False},
    {"name": "Boston",        "lat": 42.3601,  "lon": -71.0589,  "station": "KBOS", "emoji": "🦞",  "celsius": False},
    {"name": "Philadelphia",  "lat": 39.9526,  "lon": -75.1652,  "station": "KPHL", "emoji": "🔔",  "celsius": False},
    {"name": "London",        "lat": 51.5033,  "lon": 0.0550,    "station": "EGLC", "emoji": "🎡",  "celsius": True},
    {"name": "Seoul",         "lat": 37.5665,  "lon": 126.9780,  "station": "RKSS", "emoji": "🏮",  "celsius": True},
]

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
