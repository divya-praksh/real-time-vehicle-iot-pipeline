"""Run the three business use cases against the LOCAL data lake with DuckDB.

This is the zero-cost way to demo the analytics without AWS. The SQL mirrors
the Athena queries in athena/queries/ (DuckDB and Athena/Trino dialects differ
slightly, so these are kept separately).
"""
from pathlib import Path

REPORT = [
    ("ROUTE MONITORING - route KPIs", """
        SELECT route_name,
               count(DISTINCT vehicle_id)                              AS vehicles,
               count(DISTINCT trip_id)                                 AS trips,
               count(*)                                                AS pings,
               round(avg(speed_kmh), 1)                                AS avg_speed_kmh,
               round(quantile_cont(speed_kmh, 0.85), 1)                AS p85_speed_kmh,
               round(100.0 * count(*) FILTER (WHERE speed_band = 'stopped') / count(*), 1) AS pct_stopped,
               round(100.0 * count(*) FILTER (WHERE is_speeding) / count(*), 2)          AS pct_speeding
        FROM telemetry GROUP BY 1 ORDER BY 1"""),
    ("ROUTE MONITORING - longest completed trips", """
        SELECT left(CAST(trip_id AS VARCHAR), 8) AS trip, vehicle_id, route_name,
               min(event_time) AS trip_start,
               date_diff('minute', min(event_time), max(event_time)) AS duration_min,
               round(max(odometer_km) - min(odometer_km), 2) AS distance_km,
               round(avg(speed_kmh), 1) AS avg_speed_kmh,
               count(*) FILTER (WHERE is_speeding) AS speeding_pings
        FROM telemetry GROUP BY trip_id, vehicle_id, route_name
        HAVING count(*) > 10 ORDER BY distance_km DESC LIMIT 8"""),
    ("SPEED ALERTS - by route and severity", """
        SELECT route_name, severity, count(*) AS alerts, count(DISTINCT vehicle_id) AS vehicles,
               round(avg(over_limit_kmh), 1) AS avg_over_limit_kmh, max(speed_kmh) AS max_speed_kmh
        FROM speed_alerts GROUP BY 1, 2
        ORDER BY 1, CASE severity WHEN 'critical' THEN 1 WHEN 'major' THEN 2 ELSE 3 END"""),
    ("SPEED ALERTS - top offenders (per 1k pings)", """
        WITH e AS (SELECT vehicle_id, count(*) AS pings FROM telemetry GROUP BY 1),
             a AS (SELECT vehicle_id, count(*) AS alerts,
                          count(*) FILTER (WHERE severity = 'critical') AS critical,
                          max(over_limit_kmh) AS worst_over_kmh
                   FROM speed_alerts GROUP BY 1)
        SELECT a.vehicle_id, a.alerts, a.critical, a.worst_over_kmh, e.pings,
               round(1000.0 * a.alerts / e.pings, 1) AS alerts_per_1k
        FROM a JOIN e USING (vehicle_id) ORDER BY alerts_per_1k DESC LIMIT 8"""),
    ("ENVIRONMENT - correlations by route (moving vehicles)", """
        SELECT route_name,
               round(corr(speed_kmh, humidity_pct), 3)        AS speed_vs_humidity,
               round(corr(engine_temp_c, ambient_temp_c), 3)  AS engine_vs_ambient,
               round(corr(engine_temp_c, speed_kmh), 3)       AS engine_vs_speed,
               round(corr(pm25_ugm3, speed_kmh), 3)           AS pm25_vs_speed
        FROM telemetry WHERE speed_kmh >= 2 GROUP BY 1 ORDER BY 1"""),
    ("ENVIRONMENT - road condition impact", """
        SELECT road_condition, count(*) AS pings,
               round(avg(speed_kmh) FILTER (WHERE speed_kmh >= 2), 1) AS avg_moving_speed,
               round(1000.0 * count(*) FILTER (WHERE is_speeding) / count(*), 1) AS speeding_per_1k,
               round(avg(humidity_pct), 1) AS avg_humidity
        FROM telemetry GROUP BY 1
        ORDER BY CASE road_condition WHEN 'dry' THEN 1 WHEN 'wet' THEN 2 ELSE 3 END"""),
    ("ENVIRONMENT - air quality vs congestion", """
        SELECT speed_band, count(*) AS pings, round(avg(pm25_ugm3), 1) AS avg_pm25,
               round(100.0 * count(*) FILTER (WHERE pm25_ugm3 > 60) / count(*), 1) AS pct_above_60
        FROM telemetry GROUP BY 1
        ORDER BY CASE speed_band WHEN 'stopped' THEN 1 WHEN 'crawl' THEN 2 WHEN 'urban' THEN 3
                                 WHEN 'arterial' THEN 4 ELSE 5 END"""),
    ("DATA QUALITY - rejection reasons (DLQ)", """
        SELECT split_part(err, ':', 1) AS error_type, split_part(err, ':', 2) AS field, count(*) AS events
        FROM (SELECT unnest(error_codes) AS err FROM dlq_events) GROUP BY 1, 2 ORDER BY 3 DESC"""),
    ("DATA QUALITY - accepted with warnings", """
        SELECT w AS warning, count(*) AS events
        FROM (SELECT unnest(quality_warnings) AS w FROM telemetry) GROUP BY 1 ORDER BY 2 DESC"""),
    ("DATA QUALITY - ingestion lag and duplicates", """
        SELECT count(*) AS valid_events,
               round(quantile_cont(ingest_lag_s, 0.5), 2) AS p50_lag_s,
               round(quantile_cont(ingest_lag_s, 0.99), 2) AS p99_lag_s,
               count(*) FILTER (WHERE late_arrival) AS late_events,
               count(*) - count(DISTINCT event_id) AS duplicate_events
        FROM telemetry"""),
]


def _register(con, name: str, lake: Path) -> bool:
    folder = lake / name
    if not any(folder.rglob("*.json.gz")):
        return False
    pattern = (folder / "**" / "*.json.gz").as_posix()
    # Timestamps are read as strings and parsed explicitly, exactly like the Athena views.
    con.execute(f"""
        CREATE OR REPLACE VIEW {name}_raw AS
        SELECT * FROM read_json('{pattern}', format='newline_delimited',
                                hive_partitioning=true, union_by_name=true,
                                timestampformat='NOT_A_TIMESTAMP')""")
    columns = {row[0] for row in con.execute(f"DESCRIBE {name}_raw").fetchall()}
    extra = ", CAST(event_ts AS TIMESTAMPTZ) AS event_time" if "event_ts" in columns else ""
    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT *{extra} FROM {name}_raw")
    return True


def run_report(lake_dir: str) -> None:
    import duckdb

    lake = Path(lake_dir)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    available = {name: _register(con, name, lake) for name in ("telemetry", "speed_alerts", "dlq_events")}
    if not available["telemetry"]:
        raise SystemExit(f"No telemetry found under {lake.resolve()} - run `python -m pipeline demo` first.")

    for title, sql in REPORT:
        needs = [t for t in ("speed_alerts", "dlq_events") if t in sql]
        print(f"\n=== {title} ===")
        if not all(available[t] for t in needs):
            print("(no data yet)")
            continue
        con.sql(sql).show(max_width=200)
