"""Fleet telemetry simulator.

Vehicles drive back and forth along the reference routes. Speed reacts to road
conditions and driver behaviour, engine temperature follows ambient temperature
and load, and PM2.5 rises in congestion, so the environmental-correlation
queries have real signal to find.

With ``anomaly_rate > 0`` a fraction of events is corrupted the way real
telematics data goes wrong (missing fields, bad types, clock skew, duplicates,
GPS jumps...), which exercises the validation layer.
"""
import json
import math
import random
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from .routes import ROUTES, bearing_deg, haversine_km
from .schema import iso, utcnow

# Anomalies the validator must REJECT (sent to the DLQ).
REJECTED_ANOMALIES = (
    "missing_field", "out_of_range", "negative_speed", "bad_type", "future_ts",
    "stale_ts", "duplicate", "unknown_route", "malformed_json",
)
# Anomalies that are suspicious but still valid data (accepted with a warning).
FLAGGED_ANOMALIES = ("gps_jump", "late_delivery")
ANOMALIES = REJECTED_ANOMALIES + FLAGGED_ANOMALIES

ROAD_SPEED_FACTOR = {"dry": 0.95, "wet": 0.80, "waterlogged": 0.55}
# Per 5-second tick transition probabilities between road conditions.
WEATHER_TRANSITIONS = {
    "dry": [("wet", 0.004)],
    "wet": [("dry", 0.010), ("waterlogged", 0.006)],
    "waterlogged": [("wet", 0.020)],
}
LOCAL_UTC_OFFSET_H = 5.5  # routes are in IST; drives the daily temperature cycle

Payload = Tuple[str, str]  # (kafka key, json value)


@dataclass
class Vehicle:
    vehicle_id: str
    route_id: str
    trip_id: str
    aggressiveness: float
    odometer: float
    fuel: float
    seg: int = 0
    pos: float = 0.0          # fraction travelled along the current segment
    direction: int = 1        # 1 = outbound, -1 = return trip
    speed: float = 0.0
    engine_temp: float = 75.0
    stop_ticks: int = 0
    burst_ticks: int = 0


class VehicleSimulator:
    def __init__(
        self,
        num_vehicles: int = 10,
        anomaly_rate: float = 0.0,
        tick_seconds: float = 5.0,
        start_time: Optional[datetime] = None,
        seed: Optional[int] = None,
        realtime: bool = False,
    ):
        self.rng = random.Random(seed)
        self.tick_seconds = tick_seconds
        self.anomaly_rate = anomaly_rate
        self.realtime = realtime
        self.now = start_time or utcnow()
        self.weather = {rid: "dry" for rid in ROUTES}
        self.env = {}
        self.anomaly_counts: Counter = Counter()
        route_ids = sorted(ROUTES)
        self.vehicles = [self._new_vehicle(i, route_ids[i % len(route_ids)]) for i in range(num_vehicles)]

    # ------------------------------------------------------------------ setup
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _new_vehicle(self, index: int, route_id: str) -> Vehicle:
        rng = self.rng
        # ~20% of drivers habitually speed; the rest drive close to the limit.
        aggressiveness = rng.gauss(1.17, 0.04) if rng.random() < 0.2 else rng.gauss(0.95, 0.05)
        v = Vehicle(
            vehicle_id=f"VH-{index + 1:04d}",
            route_id=route_id,
            trip_id=self._uuid(),
            aggressiveness=aggressiveness,
            odometer=rng.uniform(5_000, 90_000),
            fuel=rng.uniform(30, 95),
        )
        # Spread vehicles along the route so they don't drive in a convoy.
        v.seg = rng.randrange(len(ROUTES[route_id]["waypoints"]) - 1)
        v.pos = rng.random()
        v.direction = rng.choice([1, -1])
        v.speed = ROUTES[route_id]["speed_limit_kmh"] * 0.6
        return v

    # ------------------------------------------------------------- main loop
    def tick(self) -> List[Payload]:
        """Advance the simulation one step and return one payload per vehicle (plus anomalies)."""
        self.now = utcnow() if self.realtime else self.now + timedelta(seconds=self.tick_seconds)
        self._update_environment()
        out: List[Payload] = []
        for v in self.vehicles:
            event = self._step(v)
            if self.anomaly_rate and self.rng.random() < self.anomaly_rate:
                out.extend(self._inject_anomaly(event))
            else:
                out.append((v.vehicle_id, json.dumps(event)))
        return out

    def _update_environment(self) -> None:
        scale = self.tick_seconds / 5.0
        local_hour = (self.now.hour + self.now.minute / 60 + LOCAL_UTC_OFFSET_H) % 24
        for rid in ROUTES:
            cond = self.weather[rid]
            for nxt, p in WEATHER_TRANSITIONS[cond]:
                if self.rng.random() < p * scale:
                    self.weather[rid] = nxt
                    break
            cond = self.weather[rid]
            temp = 26 + 5 * math.sin(2 * math.pi * (local_hour - 9) / 24) - (3 if cond != "dry" else 0)
            humidity = 55 - 1.5 * (temp - 26) + (25 if cond != "dry" else 0)
            self.env[rid] = (temp, humidity)

    def _waypoints(self, v: Vehicle):
        wps = ROUTES[v.route_id]["waypoints"]
        return wps if v.direction == 1 else wps[::-1]

    def _step(self, v: Vehicle) -> dict:
        rng = self.rng
        route = ROUTES[v.route_id]
        cond = self.weather[v.route_id]

        # Speed: traffic stops, then ease towards a target driven by limit, driver and road.
        if v.stop_ticks > 0:
            v.stop_ticks -= 1
            target = 0.0
        elif rng.random() < 0.02:
            v.stop_ticks = rng.randint(2, 12)  # signal / congestion
            target = 0.0
        else:
            target = route["speed_limit_kmh"] * v.aggressiveness * ROAD_SPEED_FACTOR[cond]
            if v.burst_ticks > 0:
                v.burst_ticks -= 1
                target *= 1.45  # reckless overtaking / racing an empty stretch
            elif rng.random() < 0.002 * v.aggressiveness:
                v.burst_ticks = rng.randint(4, 15)
        v.speed = max(0.0, min(160.0, v.speed + (target - v.speed) * 0.35 + rng.gauss(0, 2.5)))
        if target == 0.0 and v.speed < 3:
            v.speed = 0.0

        km = v.speed * self.tick_seconds / 3600
        self._advance(v, km)
        lat, lon, heading = self._position(v)

        base_temp, base_humidity = self.env[v.route_id]
        ambient = base_temp + rng.gauss(0, 0.4)
        humidity = min(100.0, max(10.0, base_humidity + rng.gauss(0, 2)))
        v.engine_temp += ((82 + 0.15 * ambient + 0.06 * v.speed) - v.engine_temp) * 0.2 + rng.gauss(0, 0.3)
        v.fuel -= km * 0.08
        if v.fuel < 8:
            v.fuel = rng.uniform(85, 100)  # refuelled
        v.odometer += km
        pm25 = route["pm25_base"] + max(0.0, 45 - v.speed) * 0.7 - (12 if cond != "dry" else 0) + rng.gauss(0, 4)

        return {
            "event_id": self._uuid(),
            "vehicle_id": v.vehicle_id,
            "trip_id": v.trip_id,
            "route_id": v.route_id,
            "event_ts": iso(self.now),
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "speed_kmh": round(v.speed, 1),
            "heading_deg": round(heading, 1),
            "engine_temp_c": round(v.engine_temp, 1),
            "fuel_level_pct": round(max(0.0, v.fuel), 1),
            "ambient_temp_c": round(ambient, 1),
            "humidity_pct": round(humidity, 1),
            "pm25_ugm3": round(max(3.0, pm25), 1),
            "road_condition": cond,
            "odometer_km": round(v.odometer, 3),
        }

    def _advance(self, v: Vehicle, km: float) -> None:
        wps = self._waypoints(v)
        while km > 0:
            (lat1, lon1), (lat2, lon2) = wps[v.seg], wps[v.seg + 1]
            seg_len = max(haversine_km(lat1, lon1, lat2, lon2), 1e-6)
            left = seg_len * (1 - v.pos)
            if km < left:
                v.pos += km / seg_len
                return
            km -= left
            v.seg += 1
            v.pos = 0.0
            if v.seg >= len(wps) - 1:
                # End of trip: turn around after a layover and start a new trip.
                v.direction *= -1
                v.seg = 0
                v.trip_id = self._uuid()
                v.stop_ticks = self.rng.randint(6, 24)
                return

    def _position(self, v: Vehicle):
        wps = self._waypoints(v)
        (lat1, lon1), (lat2, lon2) = wps[v.seg], wps[v.seg + 1]
        lat = lat1 + (lat2 - lat1) * v.pos + self.rng.gauss(0, 0.00005)  # ~5 m GPS noise
        lon = lon1 + (lon2 - lon1) * v.pos + self.rng.gauss(0, 0.00005)
        return lat, lon, bearing_deg(lat1, lon1, lat2, lon2)

    # ------------------------------------------------------------- anomalies
    def _inject_anomaly(self, event: dict) -> List[Payload]:
        rng = self.rng
        kind = rng.choice(ANOMALIES)
        self.anomaly_counts[kind] += 1
        key = event["vehicle_id"]
        e = dict(event)

        if kind == "missing_field":
            del e[rng.choice(["speed_kmh", "latitude", "event_ts", "road_condition", "fuel_level_pct"])]
        elif kind == "out_of_range":
            field, bad = rng.choice([
                ("latitude", 123.4), ("fuel_level_pct", 140.0),
                ("humidity_pct", -5.0), ("engine_temp_c", 480.0),
            ])
            e[field] = bad
        elif kind == "negative_speed":
            e["speed_kmh"] = -round(rng.uniform(5, 40), 1)
        elif kind == "bad_type":
            e["speed_kmh"] = "N/A"
        elif kind == "future_ts":
            e["event_ts"] = iso(self.now + timedelta(hours=rng.uniform(1, 48)))
        elif kind == "stale_ts":
            e["event_ts"] = iso(self.now - timedelta(days=rng.uniform(2, 30)))
        elif kind == "unknown_route":
            e["route_id"] = "R9-UNMAPPED"
        elif kind == "gps_jump":
            e["latitude"] = round(e["latitude"] + rng.choice([-1, 1]) * rng.uniform(0.3, 0.8), 6)
        elif kind == "late_delivery":
            # Buffered on the device during a coverage gap, delivered 11-45 minutes late.
            e["event_ts"] = iso(self.now - timedelta(minutes=rng.uniform(11, 45)))
        elif kind == "malformed_json":
            return [(key, json.dumps(e)[:-9])]
        elif kind == "duplicate":
            payload = json.dumps(e)
            return [(key, payload), (key, payload)]  # at-least-once retry from the device
        return [(key, json.dumps(e))]
