-- USE CASE 2: SPEED ALERTS
-- Alerts are raised in-stream (and published to the alert topic for real-time
-- consumers); these queries are the analytical/historical view of the same alerts.

-- name: alerts_by_route_and_severity
SELECT
  route_name,
  severity,
  count(*)                       AS alerts,
  count(DISTINCT vehicle_id)     AS vehicles,
  round(avg(over_limit_kmh), 1)  AS avg_over_limit_kmh,
  round(max(speed_kmh), 1)       AS max_speed_kmh
FROM vehicle_iot.v_speed_alerts
WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
GROUP BY 1, 2
ORDER BY 1, CASE severity WHEN 'critical' THEN 1 WHEN 'major' THEN 2 ELSE 3 END;

-- name: top_offenders
-- Normalised by exposure: alerts per 1,000 pings, so busy vehicles aren't unfairly ranked.
WITH exposure AS (
  SELECT vehicle_id, count(*) AS pings
  FROM vehicle_iot.v_telemetry
  WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
  GROUP BY 1
),
alerts AS (
  SELECT vehicle_id,
         count(*)                      AS alerts,
         count_if(severity = 'critical') AS critical_alerts,
         round(max(over_limit_kmh), 1)  AS worst_over_limit_kmh
  FROM vehicle_iot.v_speed_alerts
  WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
  GROUP BY 1
)
SELECT a.vehicle_id, a.alerts, a.critical_alerts, a.worst_over_limit_kmh, e.pings,
       round(1000.0 * a.alerts / e.pings, 1) AS alerts_per_1k_pings
FROM alerts a
JOIN exposure e ON e.vehicle_id = a.vehicle_id
ORDER BY alerts_per_1k_pings DESC
LIMIT 20;

-- name: sustained_speeding_episodes
-- Sustained speeding (3+ consecutive alert pings in a trip) is far riskier than a
-- single spike, and is what a fleet manager actually wants to be paged about.
WITH t AS (
  SELECT vehicle_id, trip_id, route_name, event_time, speed_kmh, over_limit_kmh, is_speeding,
         row_number() OVER (PARTITION BY vehicle_id, trip_id ORDER BY event_time)
         - row_number() OVER (PARTITION BY vehicle_id, trip_id, is_speeding ORDER BY event_time) AS grp
  FROM vehicle_iot.v_telemetry
  WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
)
SELECT
  vehicle_id, route_name, trip_id,
  min(event_time)               AS episode_start,
  date_diff('second', min(event_time), max(event_time)) AS duration_s,
  count(*)                      AS pings,
  round(max(speed_kmh), 1)      AS peak_speed_kmh,
  round(avg(over_limit_kmh), 1) AS avg_over_limit_kmh
FROM t
WHERE is_speeding
GROUP BY vehicle_id, route_name, trip_id, grp
HAVING count(*) >= 3
ORDER BY duration_s DESC
LIMIT 25;

-- name: alert_hotspots
-- ~100 m grid cells with the most alerts: candidates for speed cameras or signage.
SELECT
  route_name,
  round(latitude, 3)            AS grid_lat,
  round(longitude, 3)           AS grid_lon,
  count(*)                      AS alerts,
  count(DISTINCT vehicle_id)    AS vehicles,
  round(avg(over_limit_kmh), 1) AS avg_over_limit_kmh
FROM vehicle_iot.v_speed_alerts
WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
GROUP BY 1, 2, 3
HAVING count(*) >= 3
ORDER BY alerts DESC
LIMIT 20;

-- name: alerts_by_hour_of_day
SELECT
  hour(event_time)  AS hour_utc,
  count(*)          AS alerts,
  count_if(severity = 'critical') AS critical
FROM vehicle_iot.v_speed_alerts
WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
GROUP BY 1
ORDER BY 1;
