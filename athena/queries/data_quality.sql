-- DATA QUALITY MONITORING
-- Observability for the validation layer: what is being rejected, how much is
-- flagged, how fresh the data is, and which vehicles have gone silent.

-- name: rejection_reasons
-- Error codes look like "missing_field:speed_kmh"; split into type + field.
SELECT
  split_part(err, ':', 1)            AS error_type,
  coalesce(split_part(err, ':', 2), '') AS field,
  count(*)                           AS events,
  count(DISTINCT vehicle_id)         AS vehicles
FROM vehicle_iot.dlq_events
CROSS JOIN UNNEST(error_codes) AS u(err)
WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
GROUP BY 1, 2
ORDER BY events DESC;

-- name: hourly_rejection_rate
-- Telemetry is partitioned by event time and DLQ by ingest time, so this is
-- a close (not exact) hourly comparison.
WITH ok AS (
  SELECT dt, hr, count(*) AS valid FROM vehicle_iot.telemetry
  WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar) GROUP BY 1, 2
),
bad AS (
  SELECT dt, hr, count(*) AS rejected FROM vehicle_iot.dlq_events
  WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar) GROUP BY 1, 2
)
SELECT
  coalesce(ok.dt, bad.dt) AS dt,
  coalesce(ok.hr, bad.hr) AS hr,
  coalesce(valid, 0)      AS valid,
  coalesce(rejected, 0)   AS rejected,
  round(100.0 * coalesce(rejected, 0) / nullif(coalesce(valid, 0) + coalesce(rejected, 0), 0), 2)
                          AS rejection_rate_pct
FROM ok
FULL OUTER JOIN bad ON ok.dt = bad.dt AND ok.hr = bad.hr
ORDER BY 1 DESC, 2 DESC;

-- name: quality_warnings
-- Accepted-but-suspicious events (late arrival, GPS jump, out-of-order).
SELECT w AS warning, count(*) AS events, count(DISTINCT vehicle_id) AS vehicles
FROM vehicle_iot.telemetry
CROSS JOIN UNNEST(quality_warnings) AS u(w)
WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
GROUP BY 1
ORDER BY 2 DESC;

-- name: ingestion_latency
-- End-to-end lag from device timestamp to pipeline ingest.
SELECT
  count(*)                                         AS events,
  round(approx_percentile(ingest_lag_s, 0.50), 2)  AS p50_lag_s,
  round(approx_percentile(ingest_lag_s, 0.95), 2)  AS p95_lag_s,
  round(approx_percentile(ingest_lag_s, 0.99), 2)  AS p99_lag_s,
  round(max(ingest_lag_s), 2)                      AS max_lag_s,
  count_if(late_arrival)                           AS late_events
FROM vehicle_iot.telemetry
WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar);

-- name: duplicate_check
-- Should return zero rows: the stream dedupes on event_id. Non-zero means a
-- replay after a restart (at-least-once) - dedupe downstream with this pattern.
SELECT event_id, count(*) AS copies
FROM vehicle_iot.telemetry
WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
GROUP BY 1
HAVING count(*) > 1
LIMIT 20;

-- name: silent_vehicles
-- Vehicles whose last ping is more than 10 minutes old (device offline / no coverage).
SELECT
  vehicle_id,
  max(event_time) AS last_seen,
  date_diff('minute', max(event_time), CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)) AS minutes_silent
FROM vehicle_iot.v_telemetry
WHERE dt >= CAST(current_date - INTERVAL '1' DAY AS varchar)
GROUP BY 1
HAVING date_diff('minute', max(event_time), CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp)) > 10
ORDER BY minutes_silent DESC;
