"""Streaming data-quality checks.

Checks run in order from cheap/stateless to stateful:

  1. structure   - payload is a JSON object
  2. schema      - every required field present, correct type, non-empty
  3. range       - values physically plausible (lat/lon, speed, fuel %...)
  4. domain      - enum values (road_condition) and reference data (route_id)
  5. timeliness  - timestamp parseable, not in the future, not too old;
                   moderately old events are accepted but flagged late
  6. uniqueness  - event_id not seen before (idempotency under at-least-once)
  7. continuity  - GPS fix consistent with the vehicle's previous fix
                   (implied speed) and ordering; flagged, not rejected

Errors reject the event to the DLQ. Warnings keep the event but tag it, so
analysts can filter it out or investigate it in Athena.

Stateful checks (6, 7) only see events that passed 1-5, so a bad record can
never poison the dedup cache or the last-known position.
"""
import math
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional

from . import config
from .routes import ROUTES, haversine_km
from .schema import NUMBER, RANGES, REQUIRED_FIELDS, ROAD_CONDITIONS, parse_ts, utcnow


@dataclass
class ValidationResult:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    event_time: Optional[datetime] = None

    @property
    def valid(self) -> bool:
        return not self.errors


def _is_number(value) -> bool:
    return isinstance(value, NUMBER) and not isinstance(value, bool) and math.isfinite(value)


class StreamValidator:
    def __init__(
        self,
        clock: Callable[[], datetime] = utcnow,
        dedup_cache_size: int = config.DEDUP_CACHE_SIZE,
        max_future_skew_s: float = config.MAX_FUTURE_SKEW_S,
        late_arrival_s: float = config.LATE_ARRIVAL_S,
        max_event_age_s: float = config.MAX_EVENT_AGE_S,
        max_implied_speed_kmh: float = config.MAX_IMPLIED_SPEED_KMH,
    ):
        self.clock = clock
        self.dedup_cache_size = dedup_cache_size
        self.max_future_skew_s = max_future_skew_s
        self.late_arrival_s = late_arrival_s
        self.max_event_age_s = max_event_age_s
        self.max_implied_speed_kmh = max_implied_speed_kmh
        self._seen_ids: "OrderedDict[str, None]" = OrderedDict()   # bounded LRU
        self._last_fix = {}  # vehicle_id -> (event_time, lat, lon)

    def validate(self, event) -> ValidationResult:
        result = ValidationResult()
        if not isinstance(event, dict):
            result.errors.append("not_an_object")
            return result

        self._check_schema(event, result)
        if result.errors:
            return result  # later checks assume well-typed fields
        self._check_ranges(event, result)
        self._check_domain(event, result)
        self._check_timeliness(event, result)
        if result.errors:
            return result
        self._check_uniqueness(event, result)
        if result.errors:
            return result
        self._check_continuity(event, result)
        self._remember(event, result)
        return result

    # ------------------------------------------------------------- checks
    def _check_schema(self, event: dict, result: ValidationResult) -> None:
        for name, expected in REQUIRED_FIELDS.items():
            if name not in event or event[name] is None:
                result.errors.append(f"missing_field:{name}")
                continue
            value = event[name]
            if expected is NUMBER:
                if not _is_number(value):
                    result.errors.append(f"invalid_type:{name}")
            elif not isinstance(value, expected):
                result.errors.append(f"invalid_type:{name}")
            elif isinstance(value, str) and not value.strip():
                result.errors.append(f"empty_field:{name}")
        for name in event.keys() - REQUIRED_FIELDS.keys():
            result.warnings.append(f"unexpected_field:{name}")

    def _check_ranges(self, event: dict, result: ValidationResult) -> None:
        for name, (lo, hi) in RANGES.items():
            if not lo <= event[name] <= hi:
                result.errors.append(f"out_of_range:{name}")

    def _check_domain(self, event: dict, result: ValidationResult) -> None:
        if event["road_condition"] not in ROAD_CONDITIONS:
            result.errors.append("invalid_enum:road_condition")
        if event["route_id"] not in ROUTES:
            result.errors.append("unknown_reference:route_id")

    def _check_timeliness(self, event: dict, result: ValidationResult) -> None:
        try:
            ts = parse_ts(event["event_ts"])
        except ValueError:
            result.errors.append("invalid_timestamp:event_ts")
            return
        result.event_time = ts
        age_s = (self.clock() - ts).total_seconds()
        if age_s < -self.max_future_skew_s:
            result.errors.append("future_timestamp")
        elif age_s > self.max_event_age_s:
            result.errors.append("stale_event")
        elif age_s > self.late_arrival_s:
            result.warnings.append("late_arrival")

    def _check_uniqueness(self, event: dict, result: ValidationResult) -> None:
        if event["event_id"] in self._seen_ids:
            self._seen_ids.move_to_end(event["event_id"])
            result.errors.append("duplicate_event_id")

    def _check_continuity(self, event: dict, result: ValidationResult) -> None:
        prev = self._last_fix.get(event["vehicle_id"])
        if prev is None:
            return
        prev_ts, prev_lat, prev_lon = prev
        dt_s = (result.event_time - prev_ts).total_seconds()
        if dt_s < 0:
            result.warnings.append("out_of_order")
            return
        dist_km = haversine_km(prev_lat, prev_lon, event["latitude"], event["longitude"])
        if dt_s == 0:
            if dist_km > 0.05:
                result.warnings.append("gps_jump")
            return
        if dist_km / (dt_s / 3600.0) > self.max_implied_speed_kmh:
            result.warnings.append("gps_jump")

    def _remember(self, event: dict, result: ValidationResult) -> None:
        self._seen_ids[event["event_id"]] = None
        if len(self._seen_ids) > self.dedup_cache_size:
            self._seen_ids.popitem(last=False)
        # Don't move the anchor to a suspect fix or backwards in time.
        if "gps_jump" in result.warnings or "out_of_order" in result.warnings:
            return
        self._last_fix[event["vehicle_id"]] = (result.event_time, event["latitude"], event["longitude"])
