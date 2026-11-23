-- USE CASE 1: ROUTE MONITORING
-- Every query filters on the dt partition so Athena scans only recent data.
-- Run all: python -m pipeline athena-query athena/queries/route_monitoring.sql --bucket <bucket>

-- name: fleet_live_position
-- Latest known position and status of every vehicle (the "where is my fleet" map feed).
WITH ranked AS (
  SELECT
    vehicle_id, route_name, trip_id, event_time, latitude, longitude,
    speed_kmh, speed_band, fuel_level_pct, road_condition,
    row_number() OVER (PARTITION BY vehicle_id ORDER BY event_time DESC) AS rn
  FROM vehicle_iot.v_telemetry
  WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
)
SELECT vehicle_id, route_name, event_time AS last_seen, latitude, longitude,
       speed_kmh, speed_band, fuel_level_pct, road_condition
FROM ranked
WHERE rn = 1
ORDER BY vehicle_id;

-- name: route_kpis_hourly
-- Throughput and speed profile per route per hour.
SELECT
  route_name,
  date_trunc('hour', event_time)                       AS hour_utc,
  count(DISTINCT vehicle_id)                           AS active_vehicles,
  count(*)                                             AS pings,
  round(avg(speed_kmh), 1)                             AS avg_speed_kmh,
  round(approx_percentile(speed_kmh, 0.85), 1)         AS p85_speed_kmh,
  round(100.0 * count_if(speed_band = 'stopped') / count(*), 1) AS pct_time_stopped,
  round(100.0 * count_if(is_speeding) / count(*), 2)   AS pct_speeding
FROM vehicle_iot.v_telemetry
WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
GROUP BY 1, 2
ORDER BY 2 DESC, 1;

-- name: trip_summary
-- One row per trip: duration, distance (odometer delta), speed, speeding events.
SELECT
  trip_id,
  vehicle_id,
  route_name,
  min(event_time)                                      AS trip_start,
  max(event_time)                                      AS trip_end,
  date_diff('minute', min(event_time), max(event_time)) AS duration_min,
  round(max(odometer_km) - min(odometer_km), 2)        AS distance_km,
  round(avg(speed_kmh), 1)                             AS avg_speed_kmh,
  round(max(speed_kmh), 1)                             AS max_speed_kmh,
  count_if(is_speeding)                                AS speeding_pings,
  count(*)                                             AS pings
FROM vehicle_iot.v_telemetry
WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
GROUP BY 1, 2, 3
HAVING count(*) > 10
ORDER BY trip_start DESC
LIMIT 50;

-- name: long_stoppages
-- Gaps-and-islands: consecutive stopped pings grouped into stoppage episodes >= 2 minutes
-- (congestion hotspots, unscheduled stops, possible breakdowns).
WITH pings AS (
  SELECT vehicle_id, trip_id, route_name, event_time, latitude, longitude,
         speed_kmh < 2 AS is_stopped
  FROM vehicle_iot.v_telemetry
  WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
),
changes AS (
  SELECT *,
         CASE WHEN is_stopped = coalesce(
                lag(is_stopped) OVER (PARTITION BY vehicle_id, trip_id ORDER BY event_time), NOT is_stopped)
              THEN 0 ELSE 1 END AS state_change
  FROM pings
),
islands AS (
  SELECT *,
         sum(state_change) OVER (PARTITION BY vehicle_id, trip_id ORDER BY event_time
                                 ROWS UNBOUNDED PRECEDING) AS island_id
  FROM changes
)
SELECT
  vehicle_id, route_name, trip_id,
  min(event_time)                                       AS stop_start,
  date_diff('second', min(event_time), max(event_time)) AS stop_seconds,
  round(avg(latitude), 5)                               AS latitude,
  round(avg(longitude), 5)                              AS longitude
FROM islands
WHERE is_stopped
GROUP BY vehicle_id, route_name, trip_id, island_id
HAVING date_diff('second', min(event_time), max(event_time)) >= 120
ORDER BY stop_seconds DESC
LIMIT 50;

-- name: route_deviation
-- Pings more than 0.5 km from the planned route line (geofence breach / GPS fault).
-- Planar distance in degrees x 111 km is a good approximation at these latitudes.
SELECT
  t.vehicle_id,
  t.route_name,
  t.event_time,
  t.latitude,
  t.longitude,
  round(ST_Distance(ST_Point(t.longitude, t.latitude), ST_GeometryFromText(r.route_wkt)) * 111.0, 2)
    AS km_off_route,
  t.quality_warnings
FROM vehicle_iot.v_telemetry t
JOIN vehicle_iot.route_reference r ON r.route_id = t.route_id
WHERE t.dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
  AND ST_Distance(ST_Point(t.longitude, t.latitude), ST_GeometryFromText(r.route_wkt)) * 111.0 > 0.5
ORDER BY km_off_route DESC
LIMIT 50;
