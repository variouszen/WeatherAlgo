# backend/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# ── Database ─────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://localhost/weatherarb")
# Railway gives postgres:// — fix for SQLAlchemy async
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ── Bot identity ──────────────────────────────────────────────────────────────
USER_AGENT = os.getenv("USER_AGENT", "WeatherArbBot/1.0 contact@example.com")

# ── Paper trading config ──────────────────────────────────────────────────────
STARTING_BANKROLL = float(os.getenv("STARTING_BANKROLL", "2000.0"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"  # Always start true

# ── Edge & sizing ─────────────────────────────────────────────────────────────
BOT_CONFIG = {
    # Risk thresholds
    "min_edge": float(os.getenv("MIN_EDGE", "0.08")),           # 8% minimum edge
    "min_confidence": float(os.getenv("MIN_CONFIDENCE", "0.68")),
    "min_market_volume": float(os.getenv("MIN_VOLUME", "50000")),
    "max_yes_price": 0.42,      # Only buy Yes when market ≤ 42¢
    "min_no_price": 0.58,       # Only buy No when market ≥ 58¢ (implied)

    # Position sizing
    "kelly_fraction": 0.25,     # Quarter-Kelly
    "max_position_pct": 0.02,   # 2% bankroll per trade max
    "min_position_usd": 10.0,   # Minimum trade size
    "max_open_per_city": 1,     # One position per city at a time
    "max_correlated_yes": 3,    # If ≥3 cities have YES open, reduce sizing 50%

    # Circuit breakers
    "daily_loss_cap_pct": 0.05, # Stop trading if down 5% in a day
    "bankroll_floor": 200.0,    # Never trade below this bankroll

    # Fees (Polymarket)
    "polymarket_fee_pct": 0.02, # 2% on winnings

    # Forecast horizons
    "max_forecast_days": 2,     # Don't trade day-3+ forecasts

    # Scan interval
    "scan_interval_seconds": 300,  # Every 5 minutes

    # Signal stability (scanner.py)
    "max_threshold_distance_sigma": 2.0,   # Only evaluate thresholds within ±2σ of forecast
    "above_forecast_edge_penalty": 0.02,   # Extra edge required per degree YES is above forecast
}

# ── Cities ────────────────────────────────────────────────────────────────────
# station_id: NWS station for US cities; ICAO code for international (Open-Meteo)
# celsius=True: forecast returned in °C, will be converted for Polymarket bucket matching
CITIES = [
    {"name": "New York",      "lat": 40.7128,  "lon": -74.0060,  "station": "KLGA", "emoji": "🗽",  "celsius": False},
    {"name": "Chicago",       "lat": 41.8781,  "lon": -87.6298,  "station": "KORD", "emoji": "🌬️", "celsius": False},
    {"name": "Seattle",       "lat": 47.6062,  "lon": -122.3321, "station": "KSEA", "emoji": "🌧️", "celsius": False},
    {"name": "Atlanta",       "lat": 33.7490,  "lon": -84.3880,  "station": "KFTY", "emoji": "🍑",  "celsius": False},
    {"name": "Dallas",        "lat": 32.7767,  "lon": -96.7970,  "station": "KDFW", "emoji": "🤠",  "celsius": False},
    {"name": "Miami",         "lat": 25.7617,  "lon": -80.1918,  "station": "KMIA", "emoji": "🌴",  "celsius": False},
    {"name": "Boston",        "lat": 42.3601,  "lon": -71.0589,  "station": "KBOS", "emoji": "🦞",  "celsius": False},
    {"name": "Philadelphia",  "lat": 39.9526,  "lon": -75.1652,  "station": "KPHL", "emoji": "🔔",  "celsius": False},
    # International — forecast via Open-Meteo; Polymarket markets quote in °C buckets
    {"name": "London",        "lat": 51.5033,  "lon": 0.0550,    "station": "EGLC", "emoji": "🎡",  "celsius": True},
    {"name": "Seoul",         "lat": 37.5665,  "lon": 126.9780,  "station": "RKSS", "emoji": "🏮",  "celsius": True},
]

# ── Temperature thresholds to scan (°F for US, °C for international) ──────────
# US cities use these Fahrenheit buckets
TEMP_THRESHOLDS_F = [40, 45, 50, 55, 60, 62, 64, 65, 66, 67, 68, 69, 70, 72, 75, 80, 85, 90]
# International cities use these Celsius buckets (Polymarket quotes in °C)
TEMP_THRESHOLDS_C = [-5, 0, 2, 4, 5, 6, 7, 8, 9, 10, 12, 14, 15, 16, 17, 18, 20, 22, 25, 28, 30]
# Legacy alias used throughout codebase — points to °F list (US default)
TEMP_THRESHOLDS = TEMP_THRESHOLDS_F

# ── NOAA ──────────────────────────────────────────────────────────────────────
NOAA_BASE = "https://api.weather.gov"
NOAA_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/geo+json",
}

# ── Polymarket ────────────────────────────────────────────────────────────────
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE  = "https://clob.polymarket.com"
