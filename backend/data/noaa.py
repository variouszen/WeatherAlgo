# backend/data/noaa.py
import httpx
import asyncio
import logging
from typing import Optional
from scipy import stats
import numpy as np
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import NOAA_BASE, NOAA_HEADERS, CITIES, TEMP_THRESHOLDS_F, TEMP_THRESHOLDS_C, TEMP_THRESHOLDS

OPEN_METEO_BASE = "https://api.open-meteo.com/v1"

logger = logging.getLogger(__name__)

# Cache point lookups (lat/lon → forecast URL) — these rarely change
_point_cache: dict[str, dict] = {}


async def get_point_data(lat: float, lon: float, client: httpx.AsyncClient) -> Optional[dict]:
    """Resolve lat/lon → NWS forecast URLs. Cached per location."""
    key = f"{lat:.4f},{lon:.4f}"
    if key in _point_cache:
        return _point_cache[key]

    try:
        r = await client.get(
            f"{NOAA_BASE}/points/{key}",
            headers=NOAA_HEADERS,
            timeout=12.0,
        )
        r.raise_for_status()
        props = r.json()["properties"]
        result = {
            "forecast_url": props["forecast"],
            "forecast_hourly_url": props["forecastHourly"],
            "office": props.get("cwa", ""),
            "zone": props.get("forecastZone", ""),
        }
        _point_cache[key] = result
        return result
    except Exception as e:
        logger.warning(f"[NOAA] points lookup failed ({key}): {e}")
        return None


async def get_forecast_periods(forecast_url: str, client: httpx.AsyncClient) -> list:
    """Fetch NWS forecast periods for a city."""
    try:
        r = await client.get(forecast_url, headers=NOAA_HEADERS, timeout=12.0)
        r.raise_for_status()
        return r.json()["properties"]["periods"]
    except Exception as e:
        logger.warning(f"[NOAA] forecast fetch failed: {e}")
        return []


async def get_latest_observation(station_id: str, client: httpx.AsyncClient) -> Optional[float]:
    """
    Fetch the latest observed temperature (°F) from NWS station.
    Used for resolution checking and calibration logging.
    """
    try:
        r = await client.get(
            f"{NOAA_BASE}/stations/{station_id}/observations/latest",
            headers=NOAA_HEADERS,
            timeout=12.0,
        )
        r.raise_for_status()
        obs = r.json()["properties"]
        temp_c = obs.get("temperature", {}).get("value")
        if temp_c is not None:
            return round(temp_c * 9 / 5 + 32, 1)
        return None
    except Exception as e:
        logger.warning(f"[NOAA] observation failed ({station_id}): {e}")
        return None


def parse_high_low(periods: list, day_offset: int = 0) -> dict:
    """
    Parse NWS periods into high/low for target day.
    Periods alternate: [Daytime, Nighttime, Daytime, Nighttime, ...]
    day_offset=0 → today, 1 → tomorrow
    """
    day_periods   = [p for p in periods if p.get("isDaytime", False)]
    night_periods = [p for p in periods if not p.get("isDaytime", False)]

    result = {
        "high": None, "low": None,
        "high_label": "", "low_label": "",
        "detailed_forecast": "",
    }

    if len(day_periods) > day_offset:
        dp = day_periods[day_offset]
        result["high"] = float(dp["temperature"])
        result["high_label"] = dp.get("name", "")
        result["detailed_forecast"] = dp.get("detailedForecast", "")

    if len(night_periods) > day_offset:
        np_ = night_periods[day_offset]
        result["low"] = float(np_["temperature"])
        result["low_label"] = np_.get("name", "")

    return result


def compute_sigma(day_offset: int, season_factor: float = 1.0) -> float:
    """
    Forecast uncertainty (°F) by horizon.
    season_factor > 1.0 for winter (more variability), < 1.0 for summer.
    """
    base = {0: 3.5, 1: 4.5, 2: 5.5}.get(day_offset, 6.0)
    return round(base * season_factor, 2)


def prob_above(threshold: float, forecast: float, sigma: float) -> float:
    """P(actual high >= threshold) using Normal(forecast, sigma)."""
    dist = stats.norm(loc=forecast, scale=sigma)
    return float(np.clip(1 - dist.cdf(threshold), 0.01, 0.99))


def prob_range(low: float, high: float, forecast: float, sigma: float) -> float:
    """P(low <= actual < high)."""
    dist = stats.norm(loc=forecast, scale=sigma)
    return float(np.clip(dist.cdf(high) - dist.cdf(low), 0.01, 0.99))


def compute_confidence(sigma: float) -> float:
    """Map sigma → 0–1 confidence. Lower uncertainty = higher confidence."""
    return round(float(np.clip(1.0 - (sigma - 3.0) / 10.0, 0.50, 0.95)), 3)


async def fetch_openmeteo_forecast(lat: float, lon: float, day_offset: int, client: httpx.AsyncClient) -> Optional[dict]:
    """
    Fetch daily high temperature from Open-Meteo for international cities.
    Returns temp in °C.
    """
    try:
        r = await client.get(
            f"{OPEN_METEO_BASE}/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "auto",
                "forecast_days": 3,
            },
            timeout=12.0,
        )
        r.raise_for_status()
        data = r.json()
        daily = data.get("daily", {})
        highs = daily.get("temperature_2m_max", [])
        lows  = daily.get("temperature_2m_min", [])
        if len(highs) > day_offset:
            return {
                "high_c": float(highs[day_offset]),
                "low_c":  float(lows[day_offset]) if len(lows) > day_offset else None,
            }
        return None
    except Exception as e:
        logger.warning(f"[OpenMeteo] fetch failed ({lat},{lon}): {e}")
        return None


async def get_openmeteo_observation(lat: float, lon: float, client: httpx.AsyncClient) -> Optional[float]:
    """Get current temperature in °C from Open-Meteo (hourly, latest)."""
    try:
        r = await client.get(
            f"{OPEN_METEO_BASE}/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m",
                "timezone": "auto",
                "forecast_days": 1,
            },
            timeout=12.0,
        )
        r.raise_for_status()
        data = r.json()
        temps = data.get("hourly", {}).get("temperature_2m", [])
        if temps:
            return float(temps[-1])  # latest hour
        return None
    except Exception:
        return None


async def fetch_city_forecast(city: dict, day_offset: int, client: httpx.AsyncClient) -> Optional[dict]:
    """Full pipeline for one city: uses NOAA for US, Open-Meteo for international."""
    lat, lon = city["lat"], city["lon"]
    is_celsius = city.get("celsius", False)
    thresholds = TEMP_THRESHOLDS_C if is_celsius else TEMP_THRESHOLDS_F

    if is_celsius:
        # ── International: Open-Meteo ─────────────────────────────────────────
        om = await fetch_openmeteo_forecast(lat, lon, day_offset, client)
        if not om:
            return None

        forecast_high = om["high_c"]  # °C
        forecast_low  = om.get("low_c")
        sigma = compute_sigma(day_offset)
        confidence = compute_confidence(sigma)
        current_obs = await get_openmeteo_observation(lat, lon, client)

        bucket_probs = {t: round(prob_above(t, forecast_high, sigma), 4) for t in thresholds}

        return {
            "city": city["name"],
            "station": city["station"],
            "lat": lat,
            "lon": lon,
            "forecast_high_f": forecast_high,   # °C stored in this field for intl cities
            "forecast_high_c": forecast_high,
            "forecast_low_f": forecast_low,
            "condition": f"Day +{day_offset}",
            "detailed_forecast": "",
            "day_offset": day_offset,
            "sigma": sigma,
            "confidence": confidence,
            "bucket_probs": bucket_probs,
            "current_obs_f": current_obs,        # °C for intl
            "unit": "C",
            "source": "Open-Meteo",
        }

    else:
        # ── US: NOAA/NWS ──────────────────────────────────────────────────────
        point = await get_point_data(lat, lon, client)
        if not point:
            return None

        periods = await get_forecast_periods(point["forecast_url"], client)
        if not periods:
            return None

        temps = parse_high_low(periods, day_offset)
        if temps["high"] is None:
            return None

        sigma = compute_sigma(day_offset)
        confidence = compute_confidence(sigma)

        bucket_probs = {
            t: round(prob_above(t, temps["high"], sigma), 4)
            for t in TEMP_THRESHOLDS_F
        }

        current_obs = await get_latest_observation(city["station"], client)

        return {
            "city": city["name"],
            "station": city["station"],
            "lat": lat,
            "lon": lon,
            "forecast_high_f": temps["high"],
            "forecast_low_f": temps["low"],
            "condition": temps["high_label"],
            "detailed_forecast": temps["detailed_forecast"],
            "day_offset": day_offset,
            "sigma": sigma,
            "confidence": confidence,
            "bucket_probs": bucket_probs,
            "current_obs_f": current_obs,
            "unit": "F",
            "source": "NOAA/NWS",
        }


async def fetch_all_cities(day_offset: int = 0) -> list[dict]:
    """Fetch forecasts for all configured cities concurrently."""
    results = []
    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_city_forecast(city, day_offset, client)
            for city in CITIES
        ]
        raw = await asyncio.gather(*tasks, return_exceptions=True)
        for city, result in zip(CITIES, raw):
            if isinstance(result, dict):
                results.append(result)
                unit = result.get("unit", "F")
                logger.info(f"[Forecast] {city['name']}: {result['forecast_high_f']}°{unit} (σ={result['sigma']})")
            else:
                logger.warning(f"[Forecast] {city['name']}: failed — {result}")
    return results
