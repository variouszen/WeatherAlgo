"""
WeatherAlgo v2 — Ensemble Signal Engine

Fetches GFS (31 members) + ECMWF (51 members) from Open-Meteo Ensemble API
and computes per-bucket probabilities with settlement rounding correction.

Replaces the v1 Normal(forecast, sigma) approach with empirical ensemble counting.

Key format (discovered Phase 0):
  Request models:  gfs_seamless, ecmwf_ifs025
  Response keys:   temperature_2m_max_member{NN}_{suffix}
  GFS suffix:      ncep_gefs_seamless   (control + 30 numbered = 31)
  ECMWF suffix:    ecmwf_ifs025_ensemble (control + 50 numbered = 51)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# ── API Configuration ─────────────────────────────────────────────────────────

ENSEMBLE_API_BASE = "https://ensemble-api.open-meteo.com/v1/ensemble"
REQUEST_MODELS = "gfs_seamless,ecmwf_ifs025"

# Response key suffixes (discovered from live API in Phase 0)
MODEL_SUFFIXES = {
    "gfs": "ncep_gefs_seamless",
    "ecmwf": "ecmwf_ifs025_ensemble",
}

EXPECTED_MEMBERS = {"gfs": 31, "ecmwf": 51}

# Settlement rounding: Polymarket resolves on whole-degree values from Wunderground.
# True temp >= X.5 rounds to X+1. So bucket "58-59°F" wins when true temp in [57.5, 59.5).
SETTLEMENT_ROUNDING = 0.5


# ── Data Types ────────────────────────────────────────────────────────────────

@dataclass
class EnsembleResult:
    """Complete ensemble signal for one city-date."""
    city: str
    target_date: str  # YYYY-MM-DD
    
    # Per-bucket probabilities (bucket_label -> prob)
    bucket_probs: Dict[str, float] = field(default_factory=dict)
    
    # Peak detection
    combined_peak_index: int = -1
    combined_peak_label: str = ""
    combined_peak_prob: float = 0.0
    
    gfs_peak_index: int = -1
    gfs_peak_label: str = ""
    ecmwf_peak_index: int = -1
    ecmwf_peak_label: str = ""
    
    # Model agreement
    model_agreement: bool = False
    peak_diff: int = 0
    
    # Per-model probabilities (for diagnostics / logging)
    gfs_probs: Dict[str, float] = field(default_factory=dict)
    ecmwf_probs: Dict[str, float] = field(default_factory=dict)
    
    # Member counts
    gfs_members: int = 0
    ecmwf_members: int = 0
    total_members: int = 0
    
    # Metadata
    model_run_time: str = ""  # Which GFS run the data came from
    fetch_timestamp: str = ""
    
    # Raw member values (for diagnostics)
    gfs_values: List[float] = field(default_factory=list)
    ecmwf_values: List[float] = field(default_factory=list)


@dataclass
class EnsembleFetchResult:
    """Raw fetch result including date array for day-index matching."""
    members_by_model: Dict[str, List[List[float]]]
    dates: List[str]  # API-returned date strings, e.g. ["2026-03-16", "2026-03-17"]


# ── Core Functions ────────────────────────────────────────────────────────────

async def fetch_ensemble_members(
    lat: float,
    lon: float,
    forecast_days: int = 2,
    celsius: bool = False,
) -> Optional[EnsembleFetchResult]:
    """
    Fetch ensemble member forecasts from Open-Meteo Ensemble API.
    
    Returns:
        EnsembleFetchResult containing:
          - members_by_model: {model_key: [member_values_array, ...]}
            where each member_values_array is [day0_val, day1_val, ...]
          - dates: API-returned date strings in local timezone
            
    Returns None on failure.
    """
    temp_unit = "celsius" if celsius else "fahrenheit"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max",
        "models": REQUEST_MODELS,
        "forecast_days": forecast_days,
        "timezone": "auto",
        "temperature_unit": temp_unit,
    }
    
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(ENSEMBLE_API_BASE, params=params)
                
                if resp.status_code in (429, 504):
                    wait = (attempt + 1) * 3
                    logger.warning(
                        f"[Ensemble] {resp.status_code} for ({lat},{lon}), "
                        f"retrying in {wait}s..."
                    )
                    await asyncio.sleep(wait)
                    continue
                    
                resp.raise_for_status()
                data = resp.json()
                
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            members_by_model = {}
            
            for model_key, suffix in MODEL_SUFFIXES.items():
                model_members = []
                
                # 1. Control run (no member number)
                control_key = f"temperature_2m_max_{suffix}"
                control_vals = daily.get(control_key)
                if control_vals is not None:
                    model_members.append(control_vals)
                
                # 2. Numbered members starting at 01
                member_idx = 1
                while True:
                    key = f"temperature_2m_max_member{member_idx:02d}_{suffix}"
                    values = daily.get(key)
                    if values is None:
                        break
                    model_members.append(values)
                    member_idx += 1
                
                members_by_model[model_key] = model_members
                
                expected = EXPECTED_MEMBERS[model_key]
                actual = len(model_members)
                if actual != expected:
                    logger.warning(
                        f"[Ensemble] {model_key} member count mismatch: "
                        f"expected {expected}, got {actual}"
                    )
                else:
                    logger.debug(
                        f"[Ensemble] {model_key}: {actual} members OK"
                    )
            
            await asyncio.sleep(0.5)  # Throttle between city fetches — prevents 429s with 50 cities
            return EnsembleFetchResult(
                members_by_model=members_by_model,
                dates=dates,
            )

        except Exception as e:
            logger.warning(
                f"[Ensemble] fetch attempt {attempt + 1} failed "
                f"for ({lat},{lon}): {e}"
            )
            if attempt < 2:
                await asyncio.sleep((attempt + 1) * 3)
    
    logger.error(f"[Ensemble] all attempts failed for ({lat},{lon})")
    return None


def compute_ensemble_bucket_probs(
    member_values: List[float],
    buckets: List[Dict],
) -> Dict[str, float]:
    """
    Count ensemble members per bucket with settlement rounding correction.
    
    Args:
        member_values: flat list of forecast values from ensemble members
        buckets: list of dicts with keys:
            - "label": bucket display name (e.g., "58-59°F")
            - "low": lower bound (None for lower tail)
            - "high": upper bound (None for upper tail)
            
    Returns:
        {bucket_label: probability} where probability = count / total_members
        
    Settlement rounding rules:
        Interior "58-59°F": member in [57.5, 59.5)
        Lower tail "45°F or below": member < 45.5
        Upper tail "64°F or higher": member >= 63.5
    """
    total = len(member_values)
    if total == 0:
        return {b["label"]: 0.0 for b in buckets}
    
    probs = {}
    
    for bucket in buckets:
        label = bucket["label"]
        low = bucket.get("low")    # None for lower tail
        high = bucket.get("high")  # None for upper tail
        
        # Apply settlement rounding to boundaries
        if low is None:
            # Lower tail: "45°F or below" → member < (high + 0.5)
            settle_low = None
            settle_high = high + SETTLEMENT_ROUNDING
        elif high is None:
            # Upper tail: "64°F or higher" → member >= (low - 0.5)
            settle_low = low - SETTLEMENT_ROUNDING
            settle_high = None
        else:
            # Interior: "58-59°F" → member in [(low - 0.5), (high + 0.5))
            settle_low = low - SETTLEMENT_ROUNDING
            settle_high = high + SETTLEMENT_ROUNDING
        
        count = 0
        for val in member_values:
            if settle_low is None:
                in_bucket = val < settle_high
            elif settle_high is None:
                in_bucket = val >= settle_low
            else:
                in_bucket = settle_low <= val < settle_high
            if in_bucket:
                count += 1
        
        probs[label] = count / total
    
    return probs


def _find_peak_bucket(
    probs: Dict[str, float],
    bucket_labels: List[str],
) -> Tuple[int, str, float]:
    """Find bucket with highest probability. Returns (index, label, prob)."""
    best_idx, best_label, best_prob = 0, "", 0.0
    for i, label in enumerate(bucket_labels):
        p = probs.get(label, 0.0)
        if p > best_prob:
            best_idx, best_label, best_prob = i, label, p
    return best_idx, best_label, best_prob


def _detect_model_run_time() -> str:
    """
    Estimate which GFS model run the current data came from.
    
    GFS runs initialize at 00, 06, 12, 18 UTC.
    Data typically available ~3.5-4.5 hours after initialization.
    
    Returns string like "2026-03-16T12Z" indicating the likely run.
    """
    now = datetime.now(timezone.utc)
    hour = now.hour
    
    # Work backwards to find most recent available run
    # Data available ~4h after init, so at 16:30 UTC the 12Z run is available
    run_hours = [18, 12, 6, 0]
    for run_hour in run_hours:
        available_hour = run_hour + 4  # ~4h delay
        if available_hour > 24:
            available_hour -= 24
        if hour >= available_hour or (run_hour == 0 and hour >= 4):
            run_date = now.date()
            if run_hour > hour + 4:
                # Previous day's late run
                run_date = run_date - timedelta(days=1)
            return f"{run_date.isoformat()}T{run_hour:02d}Z"
    
    # Fallback: previous day's 18Z
    yesterday = (now.date() - timedelta(days=1)).isoformat()
    return f"{yesterday}T18Z"


async def get_ensemble_signal(
    city: Dict,
    target_date: str,
    buckets: List[Dict],
    forecast_days: int = 2,
) -> Optional[EnsembleResult]:
    """
    Full ensemble signal pipeline for one city-date.
    
    1. Fetch ensemble members from Open-Meteo
    2. Match target_date against API-returned date array (not UTC math)
    3. Compute per-bucket probabilities (combined + per-model)
    4. Detect peaks and model agreement
    
    Args:
        city: city config dict from CITIES (needs lat, lon, celsius, name)
        target_date: "YYYY-MM-DD" string
        buckets: list of bucket dicts with "label", "low", "high" keys
                 (from venue adapter's discover_markets())
        forecast_days: number of days to request (default 2 for day+0 and day+1)
        
    Returns:
        EnsembleResult with all signal data, or None on failure.
    """
    lat = city["lat"]
    lon = city["lon"]
    celsius = city.get("celsius", False)
    city_name = city["name"]
    
    # ── Step 1: Fetch ensemble members ────────────────────────────────────────
    fetch_result = await fetch_ensemble_members(
        lat, lon,
        forecast_days=forecast_days,
        celsius=celsius,
    )
    
    if fetch_result is None:
        logger.error(f"[Ensemble] {city_name}: fetch failed for {target_date}")
        return None
    
    members_by_model = fetch_result.members_by_model
    api_dates = fetch_result.dates
    
    # ── Step 2: Match target_date against API-returned date array ─────────────
    # The API returns dates in the location's local timezone (timezone=auto).
    # We match target_date against this array directly, avoiding UTC-based
    # day_index math that can cause off-by-one for non-US cities near
    # UTC boundaries.
    if target_date not in api_dates:
        logger.warning(
            f"[Ensemble] {city_name}: target_date {target_date} not in "
            f"API dates {api_dates} — skipping"
        )
        return None
    
    day_index = api_dates.index(target_date)
    
    # Collect member values for the target day
    gfs_values = []
    ecmwf_values = []
    
    for model_key, member_arrays in members_by_model.items():
        for member_arr in member_arrays:
            if len(member_arr) > day_index:
                val = member_arr[day_index]
                if val is not None:
                    if model_key == "gfs":
                        gfs_values.append(val)
                    elif model_key == "ecmwf":
                        ecmwf_values.append(val)
    
    all_values = gfs_values + ecmwf_values
    
    if not all_values:
        logger.warning(
            f"[Ensemble] {city_name}: no valid member values "
            f"for {target_date} (day_index={day_index})"
        )
        return None
    
    # ── Step 3: Compute bucket probabilities ──────────────────────────────────
    bucket_labels = [b["label"] for b in buckets]
    
    combined_probs = compute_ensemble_bucket_probs(all_values, buckets)
    gfs_probs = compute_ensemble_bucket_probs(gfs_values, buckets) if gfs_values else {}
    ecmwf_probs = compute_ensemble_bucket_probs(ecmwf_values, buckets) if ecmwf_values else {}
    
    # ── Step 4: Peak detection and model agreement ────────────────────────────
    combined_peak_idx, combined_peak_label, combined_peak_prob = \
        _find_peak_bucket(combined_probs, bucket_labels)
    
    gfs_peak_idx, gfs_peak_label, _ = _find_peak_bucket(gfs_probs, bucket_labels)
    ecmwf_peak_idx, ecmwf_peak_label, _ = _find_peak_bucket(ecmwf_probs, bucket_labels)
    
    peak_diff = abs(gfs_peak_idx - ecmwf_peak_idx)
    model_agreement = peak_diff <= 2  # Per spec: within 2 bucket indexes
    
    # ── Step 5: Model run time detection ──────────────────────────────────────
    model_run_time = _detect_model_run_time()
    
    # ── Build result ──────────────────────────────────────────────────────────
    unit = "C" if celsius else "F"
    logger.info(
        f"[Ensemble] {city_name} {target_date}: "
        f"{len(all_values)} members ({len(gfs_values)} GFS + {len(ecmwf_values)} ECMWF), "
        f"range={min(all_values):.1f}-{max(all_values):.1f}{unit}, "
        f"peak=[{combined_peak_idx}] {combined_peak_label} ({combined_peak_prob:.1%}), "
        f"agreement={'YES' if model_agreement else 'NO'} (diff={peak_diff}), "
        f"run={model_run_time}"
    )
    
    return EnsembleResult(
        city=city_name,
        target_date=target_date,
        bucket_probs=combined_probs,
        combined_peak_index=combined_peak_idx,
        combined_peak_label=combined_peak_label,
        combined_peak_prob=combined_peak_prob,
        gfs_peak_index=gfs_peak_idx,
        gfs_peak_label=gfs_peak_label,
        ecmwf_peak_index=ecmwf_peak_idx,
        ecmwf_peak_label=ecmwf_peak_label,
        model_agreement=model_agreement,
        peak_diff=peak_diff,
        gfs_probs=gfs_probs,
        ecmwf_probs=ecmwf_probs,
        gfs_members=len(gfs_values),
        ecmwf_members=len(ecmwf_values),
        total_members=len(all_values),
        model_run_time=model_run_time,
        fetch_timestamp=datetime.now(timezone.utc).isoformat(),
        gfs_values=gfs_values,
        ecmwf_values=ecmwf_values,
    )
