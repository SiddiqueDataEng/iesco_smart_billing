"""Reusable ingestion, standardization, and KPI helpers for city energy data."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def load_source_data(data_path: str | Path = ROOT) -> dict[str, pd.DataFrame]:
    """Load the latest checked-in source extracts when they are available."""
    source_path = Path(data_path)
    frames: dict[str, pd.DataFrame] = {}
    for name in ("meters", "readings", "bills"):
        matches = sorted(source_path.glob(f"{name}_*.csv"))
        if matches:
            frames[name] = pd.read_csv(matches[-1], low_memory=False)
    return frames


def standardize_meters(meters: pd.DataFrame) -> pd.DataFrame:
    """Return a portable meter asset table with consistent field names."""
    result = meters.copy()
    result.columns = [str(column).strip().lower() for column in result.columns]
    if "city" not in result:
        result["city"] = "Reference dataset"
    for column in ("latitude", "longitude", "connected_load_kw", "sanctioned_load_kw"):
        if column in result:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    if "status" in result:
        result["status"] = result["status"].astype("string").str.strip().str.title()
    return result.drop_duplicates(subset=["meter_number"])


def standardize_readings(readings: pd.DataFrame) -> pd.DataFrame:
    """Normalize time series fields and derive a monthly grain for analytics."""
    result = readings.copy()
    result.columns = [str(column).strip().lower() for column in result.columns]
    result["timestamp"] = pd.to_datetime(result["timestamp"], errors="coerce", utc=True)
    for column in ("reading_kwh", "energy_consumed_kwh", "voltage_v", "current_a", "power_factor", "frequency_hz", "temperature_c"):
        if column in result:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    result["year_month"] = result["timestamp"].dt.to_period("M").astype("string")
    quality = result.get("data_quality_flag", pd.Series("Unknown", index=result.index))
    result["quality_status"] = quality.astype("string").fillna("Unknown").str.strip().str.title()
    return result.dropna(subset=["timestamp", "meter_number"])


def build_kpis(meters: pd.DataFrame, readings: pd.DataFrame) -> dict[str, Any]:
    """Calculate dashboard KPIs without requiring a warehouse connection."""
    consumption = pd.to_numeric(readings["energy_consumed_kwh"], errors="coerce").fillna(0)
    valid_quality = readings["quality_status"].isin(["Normal", "Good", "Valid"])
    return {
        "meter_count": int(meters["meter_number"].nunique()),
        "reading_count": int(len(readings)),
        "consumption_kwh": float(consumption.sum()),
        "quality_rate": float(valid_quality.mean() * 100) if len(readings) else 0.0,
        "active_meter_count": int((meters.get("status", pd.Series(dtype=str)) == "Active").sum()),
    }


def monthly_consumption(readings: pd.DataFrame) -> pd.DataFrame:
    """Aggregate standardized readings for a city-ready time-series chart."""
    return (
        readings.groupby("year_month", as_index=False)["energy_consumed_kwh"]
        .sum()
        .rename(columns={"energy_consumed_kwh": "consumption_kwh"})
    )


def quality_summary(readings: pd.DataFrame) -> pd.DataFrame:
    """Summarize source quality flags for monitoring and pipeline triage."""
    return readings["quality_status"].value_counts(dropna=False).rename_axis("quality_status").reset_index(name="reading_count")