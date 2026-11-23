"""Stream processor: parse -> validate -> enrich -> route.

  valid events    -> `telemetry` dataset (data lake)
  speeding events -> `speed_alerts` dataset + alert topic
  invalid events  -> `dlq_events` dataset + dead-letter topic, with reasons
"""
import json
from collections import Counter
from datetime import datetime
from typing import Callable, Optional

from . import config
from .routes import ROUTES
from .schema import iso, utcnow
from .validation import StreamValidator, ValidationResult

Publisher = Callable[[str, str, str], None]  # (topic, key, value)


def speed_band(speed_kmh: float) -> str:
    if speed_kmh < 2:
        return "stopped"
    if speed_kmh < 20:
        return "crawl"
    if speed_kmh < 50:
        return "urban"
    if speed_kmh < 80:
        return "arterial"
    return "highway"


def alert_severity(over_limit_kmh: float) -> str:
    if over_limit_kmh <= 15:
        return "minor"
    if over_limit_kmh <= 30:
        return "major"
    return "critical"


class StreamProcessor:
    def __init__(
        self,
        sink,
        validator: Optional[StreamValidator] = None,
        publish: Optional[Publisher] = None,
        clock: Callable[[], datetime] = utcnow,
        alert_tolerance_kmh: float = config.ALERT_TOLERANCE_KMH,
    ):
        self.sink = sink
        self.clock = clock
        self.validator = validator or StreamValidator(clock=clock)
        self.publish = publish
        self.alert_tolerance_kmh = alert_tolerance_kmh
        self.stats: Counter = Counter()

    def process(self, payload, source: Optional[dict] = None) -> str:
        """Handle one message. Returns 'valid' or 'invalid'."""
        ingested_at = self.clock()
        self.stats["received"] += 1

        if isinstance(payload, (bytes, bytearray)):
            try:
                payload = payload.decode("utf-8")
            except UnicodeDecodeError:
                return self._reject(repr(payload[:500]), ["invalid_encoding"], ingested_at, source)
        try:
            event = json.loads(payload) if isinstance(payload, str) else payload
        except json.JSONDecodeError:
            return self._reject(payload, ["malformed_json"], ingested_at, source)

        result = self.validator.validate(event)
        if not result.valid:
            return self._reject(payload, result.errors, ingested_at, source, event)

        record = self._enrich(event, result, ingested_at)
        self.sink.write("telemetry", record, result.event_time)
        self.stats["valid"] += 1
        for w in result.warnings:
            self.stats[f"warning:{w.split(':')[0]}"] += 1

        if record["is_speeding"]:
            self._raise_alert(record, result.event_time)
        return "valid"

    # ------------------------------------------------------------------
    def _enrich(self, event: dict, result: ValidationResult, ingested_at: datetime) -> dict:
        route = ROUTES[event["route_id"]]
        limit = float(route["speed_limit_kmh"])
        over = round(event["speed_kmh"] - limit, 1)
        record = dict(event)
        record.update(
            event_ts=iso(result.event_time),  # normalise every timestamp to UTC Z
            route_name=route["route_name"],
            speed_limit_kmh=limit,
            over_limit_kmh=over,
            is_speeding=over > self.alert_tolerance_kmh,
            speed_band=speed_band(event["speed_kmh"]),
            ingested_at=iso(ingested_at),
            ingest_lag_s=round((ingested_at - result.event_time).total_seconds(), 3),
            late_arrival="late_arrival" in result.warnings,
            quality_warnings=list(result.warnings),
        )
        return record

    def _raise_alert(self, record: dict, event_time: datetime) -> None:
        alert = {
            "alert_id": f"{record['event_id']}-speed",
            "alert_type": "overspeed",
            "severity": alert_severity(record["over_limit_kmh"]),
            "event_id": record["event_id"],
            "vehicle_id": record["vehicle_id"],
            "trip_id": record["trip_id"],
            "route_id": record["route_id"],
            "route_name": record["route_name"],
            "event_ts": record["event_ts"],
            "latitude": record["latitude"],
            "longitude": record["longitude"],
            "speed_kmh": record["speed_kmh"],
            "speed_limit_kmh": record["speed_limit_kmh"],
            "over_limit_kmh": record["over_limit_kmh"],
            "road_condition": record["road_condition"],
            "ambient_temp_c": record["ambient_temp_c"],
        }
        self.sink.write("speed_alerts", alert, event_time)
        self.stats["alerts"] += 1
        self.stats[f"alert:{alert['severity']}"] += 1
        if self.publish:
            self.publish(config.ALERT_TOPIC, record["vehicle_id"], json.dumps(alert))

    def _reject(self, raw, errors, ingested_at: datetime, source: Optional[dict], event=None) -> str:
        source = source or {}
        record = {
            "ingested_at": iso(ingested_at),
            "error_codes": list(errors),
            "vehicle_id": event.get("vehicle_id") if isinstance(event, dict) else None,
            "raw_payload": raw if isinstance(raw, str) else json.dumps(raw, default=str),
            "source_topic": source.get("topic"),
            "source_partition": source.get("partition"),
            "source_offset": source.get("offset"),
        }
        self.sink.write("dlq_events", record, ingested_at)
        self.stats["invalid"] += 1
        for e in errors:
            self.stats[f"error:{e.split(':')[0]}"] += 1
        if self.publish:
            self.publish(config.DLQ_TOPIC, record["vehicle_id"] or "unknown", json.dumps(record))
        return "invalid"

    def summary(self) -> str:
        s = self.stats
        received = s["received"] or 1
        lines = [
            f"received={s['received']}  valid={s['valid']}  invalid={s['invalid']} "
            f"({100 * s['invalid'] / received:.1f}% rejected)  speed_alerts={s['alerts']}",
        ]
        groups = {"error": "rejections", "warning": "warnings", "alert": "alerts by severity"}
        for prefix, label in groups.items():
            items = sorted((k.split(":", 1)[1], v) for k, v in s.items() if k.startswith(prefix + ":"))
            if items:
                lines.append(f"  {label}: " + ", ".join(f"{k}={v}" for k, v in items))
        return "\n".join(lines)
