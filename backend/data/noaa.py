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
from config import NOAA_BASE, NOAA_HEADERS, CITIES, TEMP_THRESHOLDS_F, TEMP_THRESHOLDS_C, TEMP_THRESHOLDS, INTL_DEFAULT_MODEL, INTL_DEFAULT_LABEL

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


def parse_high_low(periods: list, day_offset: int = 0, target_date: str = None) -> dict:
    """
    Parse NWS periods into high/low for target day.
    If target_date (YYYY-MM-DD) is provided, match by startTime date string
    instead of positional indexing — prevents off-by-one when NWS period
    boundaries shift between scans.

    When target_date is provided and no match is found, returns high=None
    so the caller skips this city-date. Does NOT fall back to positional
    indexing — that was the root cause of Dallas #87 and Atlanta #89.

    Positional indexing is only used when target_date is not provided
    (backward compat for direct callers outside the scanner).
    """
    day_periods   = [p for p in periods if p.get("isDaytime", False)]
    night_periods = [p for p in periods if not p.get("isDaytime", False)]

    result = {
        "high": None, "low": None,
        "high_label": "", "low_label": "",
        "detailed_forecast": "",
    }

    if target_date:
        # Date-matched (reliable) — match startTime against target_date
        for dp in day_periods:
            start = dp.get("startTime", "")
            if start[:10] == target_date:
                result["high"] = float(dp["temperature"])
                result["high_label"] = dp.get("name", "")
                result["detailed_forecast"] = dp.get("detailedForecast", "")
                break
        for np_ in night_periods:
            start = np_.get("startTime", "")
            if start[:10] == target_date:
                result["low"] = float(np_["temperature"])
                result["low_label"] = np_.get("name", "")
                break

        if result["high"] is None:
            available_dates = sorted(set(
                dp.get("startTime", "")[:10] for dp in day_periods if dp.get("startTime")
            ))
            logger.warning(
                f"[NOAA] No period matched target_date={target_date} — "
                f"returning None (safe skip). Available dates: {available_dates}"
            )
        return result

    # Positional fallback — ONLY when target_date was not provided.
    # The scanner always provides target_date, so this path only fires
    # from direct callers (e.g. fetch_all_cities backward compat).
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


def compute_sigma(day_offset: int, is_celsius: bool = False, season_factor: float = 1.0) -> float:
    """
    Forecast uncertainty by horizon.
    Base values are calibrated in F (NWS empirical norms):
      day 0 -> 3.5F, day 1 -> 4.5F, day 2 -> 5.5F, day 3+ -> 6.0F

    For Celsius cities, convert to C via x(5/9).
    This gives day+1 sigma ~2.5C, matching typical next-day empirical error.
    season_factor > 1.0 for winter (more variability), < 1.0 for summer.
    """
    base_f = {0: 3.5, 1: 4.5, 2: 5.5}.get(day_offset, 6.0)
    base = base_f * (5 / 9) if is_celsius else base_f
    return round(base * season_factor, 2)


def prob_above(threshold: float, forecast: float, sigma: float) -> float:
    """P(actual high >= threshold) using Normal(forecast, sigma)."""
    dist = stats.norm(loc=forecast, scale=sigma)
    return float(np.clip(1 - dist.cdf(threshold), 0.01, 0.99))


def prob_range(low: float, high: float, forecast: float, sigma: float) -> float:
    """P(low <= actual < high)."""
    dist = stats.norm(loc=forecast, scale=sigma)
    return float(np.clip(dist.cdf(high) - dist.cdf(low), 0.01, 0.99))


# ── Level 1: Bucket-native probability engine ────────────────────────────────
# Maps forecast distribution onto Polymarket's actual bucket structure with
# settlement-aware rounding correction (±0.5 degree boundaries).
# Polymarket resolves on whole-degree values from Wunderground.
# Any true temp >= X.5 rounds to X+1 on Wunderground display.
# Therefore bucket "82-83°F" wins when true temp is in [81.5, 83.5).

SETTLEMENT_ROUNDING = 0.5  # whole-degree rounding for both F and C


def compute_bucket_probabilities(buckets: list, forecast: float, sigma: float) -> list:
    """
    Map forecast Normal(forecast, sigma) onto native Polymarket buckets
    using settlement-corrected boundaries.

    Each bucket gets P(Wunderground displays a value in this bucket's range).

    Bucket types:
      Interior "82-83°F": P(true in [81.5, 83.5))
      Interior "13°C":    P(true in [12.5, 13.5))
      Lower tail "31°F or below": P(true < 31.5)
      Upper tail "46°F or higher": P(true >= 45.5)

    Args:
        buckets: list of dicts with 'low', 'high', 'price', 'label' etc
                 (from Polymarket market_data["buckets"])
        forecast: primary forecast value in native units (°F or °C)
        sigma: forecast uncertainty in same units

    Returns: list of dicts, each original bucket + 'forecast_prob' field
    """
    if not buckets or sigma <= 0:
        return []

    dist = stats.norm(loc=forecast, scale=sigma)
    result = []

    for b in buckets:
        low = b.get("low", float("-inf"))
        high = b.get("high")

        # Compute settlement-corrected boundaries
        if low == float("-inf"):
            # Lower tail: "31°F or below" → true < 31.5
            settle_low = float("-inf")
            settle_high = high + SETTLEMENT_ROUNDING if high is not None else float("inf")
        elif high is None:
            # Upper tail: "46°F or higher" → true >= 45.5
            settle_low = low - SETTLEMENT_ROUNDING
            settle_high = float("inf")
        else:
            # Interior bucket: "82-83°F" → true in [81.5, 83.5)
            settle_low = low - SETTLEMENT_ROUNDING
            settle_high = high + SETTLEMENT_ROUNDING

        # Compute probability — NO CLIPPING here.
        # Raw CDF values flow through to preserve total probability mass.
        # Clipping happens only at final consumption points:
        #   - cumulative_from_buckets() clips its return to [0.01, 0.99]
        #   - Spectrum evaluator works with raw forecast_prob values
        if settle_low == float("-inf"):
            p = dist.cdf(settle_high)
        elif settle_high == float("inf"):
            p = 1.0 - dist.cdf(settle_low)
        else:
            p = dist.cdf(settle_high) - dist.cdf(settle_low)

        p = max(0.0, float(p))  # floor at 0, no ceiling — preserves mass

        result.append({
            **b,
            "forecast_prob": round(p, 6),
            "settle_low": settle_low,
            "settle_high": settle_high,
        })

    return result


def cumulative_from_buckets(bucket_probs: list, threshold: float) -> float:
    """
    Derive P(>= threshold) by summing forecast probabilities of all buckets
    at or above the threshold.

    This replaces prob_above() for trade decisions. The rounding correction
    is already embedded in each bucket's forecast_prob from
    compute_bucket_probabilities().

    Args:
        bucket_probs: output of compute_bucket_probabilities()
        threshold: the cumulative threshold (e.g. 82 for P(>=82))

    Returns: float probability in [0.01, 0.99]
    """
    if not bucket_probs:
        return 0.50  # no data → neutral

    cum = 0.0
    for b in bucket_probs:
        low = b.get("low", float("-inf"))
        high = b.get("high")
        # Bucket counts toward cumulative if its lower bound >= threshold
        # Upper tail (high=None) counts if low >= threshold
        # Lower tail (low=-inf) never counts
        if low != float("-inf") and low >= threshold:
            cum += b["forecast_prob"]

    return float(np.clip(cum, 0.01, 0.99))


def compute_confidence(sigma: float, is_celsius: bool = False) -> float:
    """
    Map sigma -> 0-1 confidence. Lower uncertainty = higher confidence.
    Formula calibrated in F. For Celsius sigma, convert to F-equivalent
    first so the confidence curve degrades correctly across day offsets.
    Without this, Celsius sigma (~2.5) always clips to 0.95 regardless
    of forecast horizon, making the confidence signal useless for intl cities.
    """
    sigma_f = sigma * (9 / 5) if is_celsius else sigma
    return round(float(np.clip(1.0 - (sigma_f - 3.0) / 10.0, 0.50, 0.95)), 3)


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


async def fetch_city_forecast(city: dict, day_offset: int, client: httpx.AsyncClient, target_date: str = None) -> Optional[dict]:
    """
    Full pipeline for one city: uses NOAA for US, Open-Meteo for international.
    target_date: optional YYYY-MM-DD string. When provided, US cities use date-matched
    period selection instead of positional indexing — prevents off-by-one bugs when
    NWS period boundaries shift between scans.
    """
    lat, lon = city["lat"], city["lon"]
    is_celsius = city.get("celsius", False)
    thresholds = TEMP_THRESHOLDS_C if is_celsius else TEMP_THRESHOLDS_F

    if is_celsius:
        # ── International: ICON primary (or per-city override), GFS as fallback ──
        utc_now = datetime.now(timezone.utc)
        target_date = (utc_now.date() + timedelta(days=day_offset)).strftime("%Y-%m-%d")

        # Read per-city model override, or use global default (ICON)
        model_name = city.get("primary_model", INTL_DEFAULT_MODEL)
        model_label = city.get("primary_label", INTL_DEFAULT_LABEL)

        # Primary: city's configured model (default ICON)
        forecast_low = None  # model returns high only; set from om only if fallback runs
        forecast_high = await fetch_model_forecast_high(lat, lon, day_offset=day_offset, celsius=True, model=model_name)

        source = model_label
        # Fallback: generic Open-Meteo (GFS-based) if primary model unavailable
        if forecast_high is None:
            om = await fetch_openmeteo_forecast(lat, lon, day_offset, client, target_date=target_date)
            if not om:
                return None
            forecast_high = om["high_c"]
            forecast_low = om.get("low_c")
            source = "Open-Meteo-GFS"
            logger.warning(f"[{city['name']}] {model_label} unavailable, fell back to Open-Meteo GFS")
        sigma = compute_sigma(day_offset, is_celsius=True)
        confidence = compute_confidence(sigma, is_celsius=True)
        current_obs = await get_openmeteo_observation(lat, lon, client)

        # bucket_probs: pre-computed at static thresholds for dashboard/API use only.
        # Live trading computes prob_above() on the fly at dynamic Polymarket thresholds.
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
            "source": source,
        }

    else:
        # ── US: NOAA/NWS ──────────────────────────────────────────────────────
        point = await get_point_data(lat, lon, client)
        if not point:
            return None

        periods = await get_forecast_periods(point["forecast_url"], client)
        if not periods:
            return None

        # Compute target_date if not passed (backward compat for direct callers)
        us_target_date = target_date
        if us_target_date is None:
            utc_now = datetime.now(timezone.utc)
            us_target_date = (utc_now.date() + timedelta(days=day_offset)).strftime("%Y-%m-%d")

        temps = parse_high_low(periods, day_offset, target_date=us_target_date)
        if temps["high"] is None:
            return None

        sigma = compute_sigma(day_offset, is_celsius=False)
        confidence = compute_confidence(sigma, is_celsius=False)

        # bucket_probs: pre-computed at static thresholds for dashboard/API use only.
        # Live trading computes prob_above() on the fly at dynamic Polymarket thresholds.
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
                        "forecast_days": max(3, day_offset + 2),
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


async def fetch_model_forecast_high(
    lat: float,
    lon: float,
    day_offset: int = 0,
    celsius: bool = False,
    model: str = "icon_seamless",
    city_timezone: str = "UTC",
) -> Optional[float]:
    """
    Fetch forecast high from any Open-Meteo model.
    Used for international primary (ICON default, per-city overrideable)
    and as US validator (ICON replacing ECMWF).

    Available models: icon_seamless, gem_seamless, jma_seamless,
                      ukmo_seamless, bom_access_global, ecmwf_ifs04
    Returns °F for US cities (celsius=False) or °C for international (celsius=True).
    """
    from datetime import timedelta, date, datetime as _datetime, timezone as _tz
    utc_now = _datetime.now(_tz.utc)
    target_date = (utc_now.date() + timedelta(days=day_offset)).isoformat()
    label = model.split("_")[0].upper()  # icon_seamless → ICON

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
                        "forecast_days": max(3, day_offset + 2),
                        "models": model,
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
                        logger.info(f"[{label}] ({lat},{lon}) {target_date}: {high_c:.1f}°C → {result:.1f}{'°C' if celsius else '°F'}")
                        return result
                # Diagnostic on failure
                null_dates = [d for d, h in zip(dates, highs) if d == target_date and h is None]
                if null_dates:
                    logger.warning(
                        f"[{label}] ({lat},{lon}) {target_date}: date found but high is NULL | "
                        f"available dates={dates} | highs={highs}"
                    )
                else:
                    logger.warning(
                        f"[{label}] ({lat},{lon}) {target_date}: date not in response | "
                        f"available dates={dates} | forecast_days={max(3, day_offset + 2)}"
                    )
                return None
        except Exception as e:
            logger.warning(f"[{label}] ({lat},{lon}) attempt {attempt+1} failed: {e}")
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
