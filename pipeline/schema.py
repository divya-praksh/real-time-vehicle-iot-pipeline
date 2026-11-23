"""Telemetry event contract and timestamp helpers."""
from datetime import datetime, timezone

NUMBER = (int, float)

# Field -> expected Python type. Every field is required.
REQUIRED_FIELDS = {
    "event_id": str,
    "vehicle_id": str,
    "trip_id": str,
    "route_id": str,
    "event_ts": str,          # ISO-8601 with timezone, e.g. 2026-11-23T10:15:00.250Z
    "latitude": NUMBER,
    "longitude": NUMBER,
    "speed_kmh": NUMBER,
    "heading_deg": NUMBER,
    "engine_temp_c": NUMBER,
    "fuel_level_pct": NUMBER,
    "ambient_temp_c": NUMBER,
    "humidity_pct": NUMBER,
    "pm25_ugm3": NUMBER,
    "road_condition": str,
    "odometer_km": NUMBER,
}

# Physically plausible ranges (inclusive).
RANGES = {
    "latitude": (-90.0, 90.0),
    "longitude": (-180.0, 180.0),
    "speed_kmh": (0.0, 250.0),
    "heading_deg": (0.0, 360.0),
    "engine_temp_c": (-40.0, 150.0),
    "fuel_level_pct": (0.0, 100.0),
    "ambient_temp_c": (-50.0, 60.0),
    "humidity_pct": (0.0, 100.0),
    "pm25_ugm3": (0.0, 1000.0),
    "odometer_km": (0.0, 2_000_000.0),
}

ROAD_CONDITIONS = frozenset({"dry", "wet", "waterlogged"})


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    """Canonical wire format: UTC, millisecond precision, trailing Z."""
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def parse_ts(value) -> datetime:
    """Parse an ISO-8601 timestamp. Naive timestamps are rejected (ambiguous zone)."""
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    ts = datetime.fromisoformat(text)
    if ts.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return ts.astimezone(timezone.utc)
