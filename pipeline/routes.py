"""Route reference data (the 'master data' the stream is validated and enriched against)."""
from math import asin, atan2, cos, degrees, radians, sin, sqrt

# Waypoints are (lat, lon). Speed limits are the posted limits used for alerting.
ROUTES = {
    "R1-ORR": {
        "route_name": "Outer Ring Road loop",
        "speed_limit_kmh": 60,
        "pm25_base": 45,
        "waypoints": [
            (12.9352, 77.6245), (12.9569, 77.7011), (12.9784, 77.7270),
            (13.0358, 77.6970), (13.0450, 77.6200),
        ],
    },
    "R2-AIRPORT": {
        "route_name": "City centre to airport expressway",
        "speed_limit_kmh": 80,
        "pm25_base": 25,
        "waypoints": [
            (12.9716, 77.5946), (13.0350, 77.5970), (13.1007, 77.5963), (13.1986, 77.7066),
        ],
    },
    "R3-ECITY": {
        "route_name": "Electronic City elevated corridor",
        "speed_limit_kmh": 70,
        "pm25_base": 35,
        "waypoints": [
            (12.9165, 77.6101), (12.8800, 77.6450), (12.8452, 77.6602),
        ],
    },
}

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = radians(lat1), radians(lat2)
    dlat, dlon = p2 - p1, radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(p1) * cos(p2) * sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * asin(sqrt(a))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = radians(lat1), radians(lat2)
    dlon = radians(lon2 - lon1)
    x = sin(dlon) * cos(p2)
    y = cos(p1) * sin(p2) - sin(p1) * cos(p2) * cos(dlon)
    return (degrees(atan2(x, y)) + 360.0) % 360.0


def route_wkt(route_id: str) -> str:
    """WKT LINESTRING (lon lat order) for Athena geospatial functions."""
    points = ", ".join(f"{lon} {lat}" for lat, lon in ROUTES[route_id]["waypoints"])
    return f"LINESTRING ({points})"


def reference_records() -> list:
    return [
        {
            "route_id": route_id,
            "route_name": r["route_name"],
            "speed_limit_kmh": r["speed_limit_kmh"],
            "route_wkt": route_wkt(route_id),
            "waypoint_count": len(r["waypoints"]),
        }
        for route_id, r in sorted(ROUTES.items())
    ]
