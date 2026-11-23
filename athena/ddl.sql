-- Athena schema for the vehicle IoT data lake.
-- `python -m pipeline athena-setup` replaces ${LAKE} with s3://<bucket>/<prefix>
-- and runs each statement. Partition projection means new dt/hr folders are
-- queryable immediately: no Glue crawler and no MSCK REPAIR TABLE.

-- name: create_database
CREATE DATABASE IF NOT EXISTS vehicle_iot;

-- name: create_telemetry
CREATE EXTERNAL TABLE IF NOT EXISTS vehicle_iot.telemetry (
  event_id          string,
  vehicle_id        string,
  trip_id           string,
  route_id          string,
  route_name        string,
  event_ts          string,
  latitude          double,
  longitude         double,
  speed_kmh         double,
  heading_deg       double,
  engine_temp_c     double,
  fuel_level_pct    double,
  ambient_temp_c    double,
  humidity_pct      double,
  pm25_ugm3         double,
  road_condition    string,
  odometer_km       double,
  speed_limit_kmh   double,
  over_limit_kmh    double,
  is_speeding       boolean,
  speed_band        string,
  ingested_at       string,
  ingest_lag_s      double,
  late_arrival      boolean,
  quality_warnings  array<string>
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION '${LAKE}/telemetry/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.dt.range' = '2024-01-01,NOW',
  'projection.dt.interval' = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'projection.hr.type' = 'integer',
  'projection.hr.range' = '0,23',
  'projection.hr.digits' = '2',
  'storage.location.template' = '${LAKE}/telemetry/dt=${dt}/hr=${hr}/'
);

-- name: create_speed_alerts
CREATE EXTERNAL TABLE IF NOT EXISTS vehicle_iot.speed_alerts (
  alert_id          string,
  alert_type        string,
  severity          string,
  event_id          string,
  vehicle_id        string,
  trip_id           string,
  route_id          string,
  route_name        string,
  event_ts          string,
  latitude          double,
  longitude         double,
  speed_kmh         double,
  speed_limit_kmh   double,
  over_limit_kmh    double,
  road_condition    string,
  ambient_temp_c    double
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION '${LAKE}/speed_alerts/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.dt.range' = '2024-01-01,NOW',
  'projection.dt.interval' = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'projection.hr.type' = 'integer',
  'projection.hr.range' = '0,23',
  'projection.hr.digits' = '2',
  'storage.location.template' = '${LAKE}/speed_alerts/dt=${dt}/hr=${hr}/'
);

-- name: create_dlq_events
CREATE EXTERNAL TABLE IF NOT EXISTS vehicle_iot.dlq_events (
  ingested_at       string,
  error_codes       array<string>,
  vehicle_id        string,
  raw_payload       string,
  source_topic      string,
  source_partition  int,
  source_offset     bigint
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION '${LAKE}/dlq_events/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.dt.type' = 'date',
  'projection.dt.format' = 'yyyy-MM-dd',
  'projection.dt.range' = '2024-01-01,NOW',
  'projection.dt.interval' = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'projection.hr.type' = 'integer',
  'projection.hr.range' = '0,23',
  'projection.hr.digits' = '2',
  'storage.location.template' = '${LAKE}/dlq_events/dt=${dt}/hr=${hr}/'
);

-- name: create_route_reference
CREATE EXTERNAL TABLE IF NOT EXISTS vehicle_iot.route_reference (
  route_id          string,
  route_name        string,
  speed_limit_kmh   double,
  route_wkt         string,
  waypoint_count    int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '${LAKE}/reference/routes/';

-- Views parse the ISO-8601 strings once so queries can use real timestamps.
-- (Athena views cannot expose "timestamp with time zone", so cast to UTC timestamp.)

-- name: create_v_telemetry
CREATE OR REPLACE VIEW vehicle_iot.v_telemetry AS
SELECT
  t.*,
  CAST(from_iso8601_timestamp(t.event_ts) AS timestamp)    AS event_time,
  CAST(from_iso8601_timestamp(t.ingested_at) AS timestamp) AS ingested_time
FROM vehicle_iot.telemetry t;

-- name: create_v_speed_alerts
CREATE OR REPLACE VIEW vehicle_iot.v_speed_alerts AS
SELECT
  a.*,
  CAST(from_iso8601_timestamp(a.event_ts) AS timestamp) AS event_time
FROM vehicle_iot.speed_alerts a;
