from datetime import datetime, timedelta, timezone

import pytest

from pipeline.schema import iso
from pipeline.validation import StreamValidator

NOW = datetime(2026, 11, 23, 10, 0, 0, tzinfo=timezone.utc)


def make_event(**overrides):
    event = {
        "event_id": "e-1",
        "vehicle_id": "VH-0001",
        "trip_id": "t-1",
        "route_id": "R1-ORR",
        "event_ts": iso(NOW),
        "latitude": 12.95,
        "longitude": 77.65,
        "speed_kmh": 52.0,
        "heading_deg": 90.0,
        "engine_temp_c": 90.0,
        "fuel_level_pct": 60.0,
        "ambient_temp_c": 27.0,
        "humidity_pct": 55.0,
        "pm25_ugm3": 40.0,
        "road_condition": "dry",
        "odometer_km": 12000.0,
    }
    event.update(overrides)
    return event


@pytest.fixture
def validator():
    return StreamValidator(clock=lambda: NOW)


def test_valid_event_passes(validator):
    result = validator.validate(make_event())
    assert result.valid
    assert result.warnings == []
    assert result.event_time == NOW


def test_non_object_rejected(validator):
    assert validator.validate([1, 2]).errors == ["not_an_object"]


@pytest.mark.parametrize("field", ["speed_kmh", "latitude", "event_ts", "road_condition"])
def test_missing_field(validator, field):
    event = make_event()
    del event[field]
    assert f"missing_field:{field}" in validator.validate(event).errors


def test_null_counts_as_missing(validator):
    assert "missing_field:latitude" in validator.validate(make_event(latitude=None)).errors


@pytest.mark.parametrize("value", ["N/A", True, float("nan"), float("inf")])
def test_numeric_type_check(validator, value):
    assert "invalid_type:speed_kmh" in validator.validate(make_event(speed_kmh=value)).errors


def test_empty_string_rejected(validator):
    assert "empty_field:vehicle_id" in validator.validate(make_event(vehicle_id="  ")).errors


@pytest.mark.parametrize("field,value", [
    ("latitude", 123.4), ("longitude", -200.0), ("speed_kmh", -5.0),
    ("fuel_level_pct", 140.0), ("humidity_pct", -1.0), ("engine_temp_c", 480.0),
])
def test_range_checks(validator, field, value):
    assert f"out_of_range:{field}" in validator.validate(make_event(**{field: value})).errors


def test_enum_and_reference_checks(validator):
    result = validator.validate(make_event(road_condition="snowy", route_id="R9-UNMAPPED"))
    assert "invalid_enum:road_condition" in result.errors
    assert "unknown_reference:route_id" in result.errors


@pytest.mark.parametrize("ts", ["yesterday", "2026-11-23T10:00:00"])  # garbage, and naive (no zone)
def test_bad_timestamps(validator, ts):
    assert "invalid_timestamp:event_ts" in validator.validate(make_event(event_ts=ts)).errors


def test_future_timestamp_rejected_but_small_skew_allowed(validator):
    assert "future_timestamp" in validator.validate(make_event(event_ts=iso(NOW + timedelta(hours=1)))).errors
    assert validator.validate(make_event(event_id="e-2", event_ts=iso(NOW + timedelta(seconds=60)))).valid


def test_stale_rejected_late_flagged(validator):
    assert "stale_event" in validator.validate(make_event(event_ts=iso(NOW - timedelta(days=3)))).errors
    late = validator.validate(make_event(event_id="e-2", event_ts=iso(NOW - timedelta(minutes=20))))
    assert late.valid and "late_arrival" in late.warnings


def test_offset_timestamps_normalised(validator):
    result = validator.validate(make_event(event_ts="2026-11-23T15:30:00+05:30"))
    assert result.valid and result.event_time == NOW


def test_duplicate_event_id_rejected(validator):
    assert validator.validate(make_event()).valid
    assert validator.validate(make_event()).errors == ["duplicate_event_id"]


def test_rejected_event_does_not_poison_dedup_cache(validator):
    assert not validator.validate(make_event(speed_kmh=-1.0)).valid
    assert validator.validate(make_event()).valid  # same id, now correct


def test_dedup_cache_is_bounded():
    v = StreamValidator(clock=lambda: NOW, dedup_cache_size=2)
    for i in range(3):
        assert v.validate(make_event(event_id=f"e-{i}", vehicle_id=f"VH-{i}")).valid
    assert v.validate(make_event(event_id="e-0", vehicle_id="VH-9")).valid  # evicted -> accepted


def test_gps_jump_flagged_and_anchor_kept(validator):
    t1 = NOW - timedelta(seconds=10)
    assert validator.validate(make_event(event_id="a", event_ts=iso(t1))).valid
    jump = validator.validate(make_event(event_id="b", latitude=13.5))  # ~60 km in 10 s
    assert jump.valid and "gps_jump" in jump.warnings
    # Next normal fix is compared with the last GOOD fix, so it is not flagged.
    normal = validator.validate(make_event(event_id="c", event_ts=iso(NOW + timedelta(seconds=5)), latitude=12.9502))
    assert normal.warnings == []


def test_out_of_order_flagged(validator):
    assert validator.validate(make_event(event_id="a")).valid
    older = validator.validate(make_event(event_id="b", event_ts=iso(NOW - timedelta(seconds=30))))
    assert older.valid and "out_of_order" in older.warnings


def test_unexpected_field_is_warning_not_error(validator):
    result = validator.validate(make_event(firmware="1.2.3"))
    assert result.valid and "unexpected_field:firmware" in result.warnings
