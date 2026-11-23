-- USE CASE 3: ENVIRONMENTAL CORRELATION
-- How do weather, road surface and air quality relate to driving behaviour and
-- vehicle health? Stopped pings are excluded where they would distort speed stats.

-- name: correlation_matrix_by_route
-- Pearson correlation (-1..1). Expect: speed vs wet roads negative,
-- engine temp vs ambient temp positive, PM2.5 vs speed negative (congestion).
SELECT
  route_name,
  count(*)                                          AS moving_pings,
  round(corr(speed_kmh, ambient_temp_c), 3)         AS speed_vs_ambient_temp,
  round(corr(speed_kmh, humidity_pct), 3)           AS speed_vs_humidity,
  round(corr(engine_temp_c, ambient_temp_c), 3)     AS engine_temp_vs_ambient,
  round(corr(engine_temp_c, speed_kmh), 3)          AS engine_temp_vs_speed,
  round(corr(pm25_ugm3, speed_kmh), 3)              AS pm25_vs_speed
FROM vehicle_iot.v_telemetry
WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
  AND speed_kmh >= 2
GROUP BY 1
ORDER BY 1;

-- name: road_condition_impact
-- Speed and risk by road surface. Speeding on a wet or waterlogged road is the
-- highest-risk combination, so alerts per 1k pings is broken out here.
SELECT
  road_condition,
  count(*)                                              AS pings,
  round(avg(speed_kmh) FILTER (WHERE speed_kmh >= 2), 1) AS avg_moving_speed_kmh,
  round(100.0 * count_if(speed_kmh < 2) / count(*), 1)  AS pct_time_stopped,
  round(1000.0 * count_if(is_speeding) / count(*), 1)   AS speeding_per_1k_pings,
  round(avg(humidity_pct), 1)                           AS avg_humidity_pct
FROM vehicle_iot.v_telemetry
WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
GROUP BY 1
ORDER BY CASE road_condition WHEN 'dry' THEN 1 WHEN 'wet' THEN 2 ELSE 3 END;

-- name: engine_temp_by_ambient_band
-- Engine thermal load in 2-degree ambient bands: early warning for cooling issues.
SELECT
  CAST(floor(ambient_temp_c / 2) * 2 AS integer)  AS ambient_band_c,
  count(*)                                        AS pings,
  round(avg(engine_temp_c), 1)                    AS avg_engine_temp_c,
  round(approx_percentile(engine_temp_c, 0.95), 1) AS p95_engine_temp_c
FROM vehicle_iot.v_telemetry
WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
GROUP BY 1
ORDER BY 1;

-- name: air_quality_vs_congestion
-- PM2.5 exposure by speed band: stop-and-go traffic concentrates pollution.
SELECT
  speed_band,
  count(*)                                     AS pings,
  round(avg(pm25_ugm3), 1)                     AS avg_pm25,
  round(approx_percentile(pm25_ugm3, 0.9), 1)  AS p90_pm25,
  round(100.0 * count_if(pm25_ugm3 > 60) / count(*), 1) AS pct_above_india_24h_limit
FROM vehicle_iot.v_telemetry
WHERE dt >= CAST(current_date - INTERVAL '7' DAY AS varchar)
GROUP BY 1
ORDER BY CASE speed_band WHEN 'stopped' THEN 1 WHEN 'crawl' THEN 2 WHEN 'urban' THEN 3
                         WHEN 'arterial' THEN 4 ELSE 5 END;
