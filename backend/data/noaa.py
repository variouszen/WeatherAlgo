# backend/data/noaa.py
import httpx
import asyncio
import logging
from typing import Optional
from datetime import datetime, timedelta, timezone
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


async def fetch_openmeteo_forecast(lat: float, lon: float, day_offset: int, client: httpx.AsyncClient, target_date: str = None) -> Optional[dict]:
    """
    Fetch daily high temperature from Open-Meteo for international cities.
    Returns temp in °C. Retries up to 3 times with backoff on 429/504.

    If target_date (YYYY-MM-DD) is provided, matches by date string rather than
    array index — fixes the day offset bug when scanning after local midnight.
    """
    for attempt in range(3):
        try:
            r = await client.get(
                f"{OPEN_METEO_BASE}/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_max,temperature_2m_min",
                    "timezone": "UTC",
                    "forecast_days": 4,
                },
                timeout=15.0,
            )
            if r.status_code in (429, 504):
                wait = (attempt + 1) * 5
                logger.warning(f"[OpenMeteo] {r.status_code} for ({lat},{lon}), retrying in {wait}s...")
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            highs = daily.get("temperature_2m_max", [])
            lows  = daily.get("temperature_2m_min", [])

            if target_date and target_date in dates:
                idx = dates.index(target_date)
            elif len(highs) > day_offset:
                idx = day_offset
            else:
                return None

            return {
                "high_c": float(highs[idx]),
                "low_c":  float(lows[idx]) if len(lows) > idx else None,
            }
        except Exception as e:
            logger.warning(f"[OpenMeteo] fetch failed ({lat},{lon}): {e}")
            if attempt < 2:
                await asyncio.sleep((attempt + 1) * 5)
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
        # ── International: ECMWF primary, Open-Meteo GFS as fallback ──────────
        # ECMWF IFS is the most accurate global model for 1-5 day forecasts.
        utc_now = datetime.now(timezone.utc)
        # Use straight UTC date + day_offset — consistent with ECMWF/GFS validator logic.
        # day_offset is already market-date-derived by the scanner, so no noon adjustment needed.
        target_date = (utc_now.date() + timedelta(days=day_offset)).strftime("%Y-%m-%d")

        # Primary: ECMWF
        forecast_low = None  # ECMWF returns high only; set from om only if fallback runs
        forecast_high = await fetch_ecmwf_forecast_high(lat, lon, day_offset=day_offset, celsius=True)

        # Fallback: generic Open-Meteo (GFS-based) if ECMWF unavailable
        if forecast_high is None:
            om = await fetch_openmeteo_forecast(lat, lon, day_offset, client, target_date=target_date)
            if not om:
                return None
            forecast_high = om["high_c"]
            forecast_low = om.get("low_c")
            logger.warning(f"[{city['name']}] ECMWF unavailable, fell back to Open-Meteo GFS")
        sigma = compute_sigma(day_offset)
        confidence = compute_confidence(sigma)
        current_obs = await get_openmeteo_observation(lat, lon, client)

        bucket_probs = {t: round(prob_above(t, forecast_high, sigma), 4) for t in thresholds}

        return {
            "city": city["name"],
            "station": city["station"],
            "lat": lat,
            "lon": lon,
            "forecast_high": forecast_high,     # unit-agnostic: °C for intl, °F for US
            "forecast_high_c": forecast_high,   # kept for backwards compat
            "forecast_low": forecast_low,
            "condition": f"Day +{day_offset}",
            "detailed_forecast": "",
            "day_offset": day_offset,
            "sigma": sigma,
            "confidence": confidence,
            "bucket_probs": bucket_probs,
            "current_obs": current_obs,
            "unit": "C",
            "source": "ECMWF" if forecast_low is None else "Open-Meteo-GFS",
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
            "forecast_high": temps["high"],
            "forecast_low": temps["low"],
            "condition": temps["high_label"],
            "detailed_forecast": temps["detailed_forecast"],
            "day_offset": day_offset,
            "sigma": sigma,
            "confidence": confidence,
            "bucket_probs": bucket_probs,
            "current_obs": current_obs,
            "unit": "F",
            "source": "NOAA/NWS",
        }


async def get_nws_daily_high(station_id: str, target_date) -> Optional[float]:
    """
    Fetch the confirmed daily high (°F) for a US NWS station on a given date.
    Uses the observations history endpoint — takes max temp across all readings.
    target_date: date object or ISO string (YYYY-MM-DD)
    """
    date_str = str(target_date)
    start = f"{date_str}T00:00:00Z"
    end   = f"{date_str}T23:59:59Z"
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                f"{NOAA_BASE}/stations/{station_id}/observations",
                params={"start": start, "end": end, "limit": 500},
                headers=NOAA_HEADERS,
                timeout=20.0,
            )
            r.raise_for_status()
            features = r.json().get("features", [])
            temps_f = []
            for feat in features:
                temp_c = feat.get("properties", {}).get("temperature", {}).get("value")
                if temp_c is not None:
                    temps_f.append(round(temp_c * 9 / 5 + 32, 1))
            if temps_f:
                daily_high = max(temps_f)
                logger.info(f"[NWS Daily] {station_id} on {date_str}: high={daily_high}°F ({len(temps_f)} obs)")
                return daily_high
            logger.warning(f"[NWS Daily] {station_id} on {date_str}: no observations found")
            return None
    except Exception as e:
        logger.warning(f"[NWS Daily] {station_id} on {date_str} failed: {e}")
        return None


async def get_openmeteo_daily_high(lat: float, lon: float, target_date) -> Optional[float]:
    """
    Fetch confirmed daily high (°C) from Open-Meteo for a past date.
    Uses the historical API endpoint for completed days.
    target_date: date object or ISO string (YYYY-MM-DD)
    """
    date_str = str(target_date)
    for attempt in range(3):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{OPEN_METEO_BASE}/archive",  # historical endpoint
                    params={
                        "latitude": lat,
                        "longitude": lon,
                        "start_date": date_str,
                        "end_date": date_str,
                        "daily": "temperature_2m_max",
                        "timezone": "auto",
                    },
                    timeout=15.0,
                )
                if r.status_code in (429, 504):
                    await asyncio.sleep((attempt + 1) * 5)
                    continue
                r.raise_for_status()
                data = r.json()
                highs = data.get("daily", {}).get("temperature_2m_max", [])
                if highs and highs[0] is not None:
                    daily_high = float(highs[0])
                    logger.info(f"[OpenMeteo Daily] ({lat},{lon}) on {date_str}: high={daily_high}°C")
                    return daily_high
                logger.warning(f"[OpenMeteo Daily] ({lat},{lon}) on {date_str}: no data")
                return None
        except Exception as e:
            logger.warning(f"[OpenMeteo Daily] ({lat},{lon}) on {date_str} failed: {e}")
            if attempt < 2:
                await asyncio.sleep((attempt + 1) * 5)
    return None


async def get_openmeteo_forecast_high(
    lat: float,
    lon: float,
    day_offset: int = 0,
    celsius: bool = False,
    city_timezone: str = "UTC",
) -> Optional[float]:
    """
    Fetch today's (or day_offset) forecast high from Open-Meteo forecast API.
    Returns °F for US cities (celsius=False) or °C for international (celsius=True).
    Uses UTC date with day_offset. city_timezone kept for API compatibility but unused.
    Used as the second source for multi-source consensus filtering.
    """
    from datetime import timedelta, date, datetime as _datetime, timezone as _tz
    # Use UTC explicitly — no pytz required, avoids drift warnings on Railway
    utc_now = _datetime.now(_tz.utc)
    target_date = (utc_now.date() + timedelta(days=day_offset)).isoformat()
    # Note: uses UTC date, not city local time. city_timezone param is unused but kept for API compat.

    temperature_unit = "celsius"  # Open-Meteo always returns °C; we convert if needed

    for attempt in range(3):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{OPEN_METEO_BASE}/forecast",
                    params={
                        "latitude": lat,
                        "longitude": lon,
                        "daily": "temperature_2m_max",
                        "temperature_unit": temperature_unit,
                        "timezone": "auto",
                        "forecast_days": max(2, day_offset + 1),
                    },
                    timeout=15.0,
                )
                if r.status_code in (429, 504):
                    await asyncio.sleep((attempt + 1) * 3)
                    continue
                r.raise_for_status()
                data = r.json()
                dates = data.get("daily", {}).get("time", [])
                highs = data.get("daily", {}).get("temperature_2m_max", [])

                for d, h in zip(dates, highs):
                    if d == target_date and h is not None:
                        high_c = float(h)
                        if celsius:
                            result = round(high_c, 1)
                        else:
                            result = round(high_c * 9 / 5 + 32, 1)
                        logger.info(
                            f"[OM Forecast] ({lat},{lon}) {target_date}: "
                            f"{high_c:.1f}°C → {result:.1f}{'°C' if celsius else '°F'}"
                        )
                        return result

                logger.warning(f"[OM Forecast] ({lat},{lon}) {target_date}: date not found in response")
                return None

        except Exception as e:
            logger.warning(f"[OM Forecast] ({lat},{lon}) attempt {attempt+1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep((attempt + 1) * 3)

    return None


async def fetch_gfs_forecast_high(
    lat: float,
    lon: float,
    day_offset: int = 0,
    celsius: bool = False,
    city_timezone: str = "UTC",
) -> Optional[float]:
    """
    Fetch forecast high from GFS model via Open-Meteo.
    Returns °F for US cities (celsius=False) or °C for international (celsius=True).
    GFS is independent from NOAA point forecasts — genuine second signal for US cities.
    """
    from datetime import timedelta, date, datetime as _datetime, timezone as _tz
    # Use UTC explicitly — no pytz required, avoids drift warnings on Railway
    utc_now = _datetime.now(_tz.utc)
    target_date = (utc_now.date() + timedelta(days=day_offset)).isoformat()

    for attempt in range(3):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{OPEN_METEO_BASE}/forecast",
                    params={
                        "latitude": lat,
                        "longitude": lon,
                        "daily": "temperature_2m_max",
                        "temperature_unit": "celsius",
                        "timezone": "auto",
                        "forecast_days": max(2, day_offset + 1),
                        "models": "gfs_seamless",
                    },
                    timeout=15.0,
                )
                if r.status_code in (429, 504):
                    await asyncio.sleep((attempt + 1) * 3)
                    continue
                r.raise_for_status()
                data = r.json()
                dates = data.get("daily", {}).get("time", [])
                highs = data.get("daily", {}).get("temperature_2m_max", [])
                for d, h in zip(dates, highs):
                    if d == target_date and h is not None:
                        high_c = float(h)
                        result = round(high_c, 1) if celsius else round(high_c * 9 / 5 + 32, 1)
                        logger.info(f"[GFS] ({lat},{lon}) {target_date}: {high_c:.1f}°C → {result:.1f}{'°C' if celsius else '°F'}")
                        return result
                logger.warning(f"[GFS] ({lat},{lon}) {target_date}: date not found")
                return None
        except Exception as e:
            logger.warning(f"[GFS] ({lat},{lon}) attempt {attempt+1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep((attempt + 1) * 3)
    return None


async def fetch_ecmwf_forecast_high(
    lat: float,
    lon: float,
    day_offset: int = 0,
    celsius: bool = False,
    city_timezone: str = "UTC",
) -> Optional[float]:
    """
    Fetch forecast high from ECMWF IFS model via Open-Meteo.
    ECMWF is generally the most accurate global model for 1-5 day forecasts.
    Returns °F for US cities (celsius=False) or °C for international (celsius=True).
    """
    from datetime import timedelta, date, datetime as _datetime, timezone as _tz
    # Use UTC explicitly — no pytz required, avoids drift warnings on Railway
    utc_now = _datetime.now(_tz.utc)
    target_date = (utc_now.date() + timedelta(days=day_offset)).isoformat()

    for attempt in range(3):
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    f"{OPEN_METEO_BASE}/forecast",
                    params={
                        "latitude": lat,
                        "longitude": lon,
                        "daily": "temperature_2m_max",
                        "temperature_unit": "celsius",
                        "timezone": "auto",
                        "forecast_days": max(2, day_offset + 1),
                        "models": "ecmwf_ifs04",
                    },
                    timeout=15.0,
                )
                if r.status_code in (429, 504):
                    await asyncio.sleep((attempt + 1) * 3)
                    continue
                r.raise_for_status()
                data = r.json()
                dates = data.get("daily", {}).get("time", [])
                highs = data.get("daily", {}).get("temperature_2m_max", [])
                for d, h in zip(dates, highs):
                    if d == target_date and h is not None:
                        high_c = float(h)
                        result = round(high_c, 1) if celsius else round(high_c * 9 / 5 + 32, 1)
                        logger.info(f"[ECMWF] ({lat},{lon}) {target_date}: {high_c:.1f}°C → {result:.1f}{'°C' if celsius else '°F'}")
                        return result
                logger.warning(f"[ECMWF] ({lat},{lon}) {target_date}: date not found")
                return None
        except Exception as e:
            logger.warning(f"[ECMWF] ({lat},{lon}) attempt {attempt+1} failed: {e}")
            if attempt < 2:
                await asyncio.sleep((attempt + 1) * 3)
    return None



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
                logger.info(f"[Forecast] {city['name']}: {result['forecast_high']}°{unit} (σ={result['sigma']})")
            else:
                logger.warning(f"[Forecast] {city['name']}: failed — {result}")
    return results
