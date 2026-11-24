# Real-Time Vehicle IoT Streaming Pipeline

**Stack:** Python · Apache Kafka · AWS S3 · AWS Athena (SQL) · DuckDB (local analytics)

A fleet of vehicles streams GPS, speed, engine and environmental telemetry. The
pipeline validates every event in-stream, raises real-time speed alerts, quarantines
bad data in a dead-letter queue, and lands clean data in a partitioned S3 data lake
that Athena queries with plain SQL.

```
 ┌─────────────────┐   vehicle.telemetry.raw   ┌──────────────────────────────┐
 │ Vehicle         │ ────────────────────────▶ │ Stream processor             │
 │ simulator       │   (key = vehicle_id,      │  1. parse JSON               │
 │ (12 vehicles,   │    3 partitions)          │  2. validate (7 check types) │
 │  3 routes,      │                           │  3. enrich (route, limits)   │
 │  fault injection)│                          │  4. detect overspeed         │
 └─────────────────┘                           └──────┬─────────┬─────────┬───┘
                                                      │valid    │alert    │invalid
                                vehicle.alerts.speed ◀┘         │         └▶ vehicle.telemetry.dlq
                                                      ▼         ▼         ▼
                         S3 data lake (gzip JSON, Hive partitions dt=YYYY-MM-DD/hr=HH)
                         telemetry/          speed_alerts/          dlq_events/
                                                      │
                                                      ▼
                         Athena (partition projection, no crawler) → SQL use cases
```

## Business use cases

| Use case | Question it answers | Where |
|---|---|---|
| **Route monitoring** | Where is every vehicle now? Hourly route KPIs, trip summaries, long stoppages (gaps-and-islands), route deviation via geospatial distance to the planned route | [route_monitoring.sql](athena/queries/route_monitoring.sql) |
| **Speed alerts** | Real-time overspeed alerts (minor / major / critical) published to Kafka; historical analysis of top offenders normalised by exposure, sustained speeding episodes, alert hotspots on a ~100 m grid | [processor.py](pipeline/processor.py), [speed_alerts.sql](athena/queries/speed_alerts.sql) |
| **Environmental correlation** | How do road surface, humidity and temperature affect speed and risk? Engine thermal load by ambient band; PM2.5 exposure against congestion | [environmental_correlation.sql](athena/queries/environmental_correlation.sql) |
| **Data quality** | What is rejected and why, warning rates, ingestion lag percentiles, duplicate check, silent vehicles | [data_quality.sql](athena/queries/data_quality.sql) |

## Validation checks ([validation.py](pipeline/validation.py))

| # | Check | Example failure | Outcome |
|---|---|---|---|
| 1 | Structure | truncated JSON, binary payload | reject → DLQ |
| 2 | Schema: required fields, types, non-empty | `speed_kmh: "N/A"`, missing `event_ts`, NaN | reject → DLQ |
| 3 | Physical range | latitude 123.4, fuel 140 %, negative speed | reject → DLQ |
| 4 | Domain + reference data | `road_condition: "snowy"`, unknown `route_id` | reject → DLQ |
| 5 | Timeliness | timestamp >5 min in the future, >24 h old, no timezone | reject → DLQ |
|   |  | 10 min to 24 h old (device buffered offline) | accept + `late_arrival` flag |
| 6 | Uniqueness (bounded LRU on `event_id`) | device retry duplicates | reject → DLQ |
| 7 | GPS continuity | implied speed >300 km/h (teleport), out-of-order fix | accept + `gps_jump` / `out_of_order` flag |

Design points:
- **Errors vs warnings.** Bad data never reaches the lake. Suspicious but plausible data does, tagged in `quality_warnings` so analysts can decide.
- **Stateful checks only see clean events.** A rejected record can't poison the dedup cache or the last-known GPS anchor.
- **The DLQ keeps the raw payload and the reason codes,** so bad data can be debugged and replayed.
- **At-least-once delivery.** Kafka offsets are committed only after the S3 write succeeds. File names are unique, so a replay never overwrites data, and the dedup check plus the `duplicate_check` query cover replays.
- **Event-time partitioning.** Late events land in the hour they *happened*, so hourly analytics stay correct.

The simulator injects 11 kinds of real-world faults (`--anomaly-rate`). The test suite proves that every injected rejection ends up in the DLQ.

---

## Quick start: local demo, no Kafka or AWS needed

Requires Python 3.10+.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m pipeline demo          # simulate 3 h of driving: about 26k events in about 5 s
python -m pipeline analyze       # run the use-case report with DuckDB
pytest -q                        # 39 tests
```

`demo` runs producer → broker → processor → data lake in one process on a simulated
clock, with an in-memory queue standing in for Kafka. Sample output:

```
received=26000  valid=25349  invalid=651 (2.5% rejected)  speed_alerts=2670
  rejections: duplicate_event_id=80, future_timestamp=68, invalid_type=81, malformed_json=69, ...
  warnings: gps_jump=73, late_arrival=70, out_of_order=70
  alerts by severity: critical=99, major=238, minor=2333
```

> **Windows:** if you see `FileNotFoundError` or `WinError 206`, the project path is too long
> for Windows' 260-character limit. Move the project to a shorter folder, e.g. `C:\dev\`.

## Running with real Kafka

Requires Docker Desktop.

```powershell
docker compose up -d                       # Kafka on localhost:9092, UI on http://localhost:8088
python -m pipeline create-topics
python -m pipeline process                 # terminal 1: consumer -> validation -> lake
python -m pipeline produce --vehicles 12   # terminal 2: live telemetry, 1 ping/s per vehicle
```

Watch messages flow through `vehicle.telemetry.raw`, `vehicle.alerts.speed` and
`vehicle.telemetry.dlq` in Kafka UI. Stop either side with Ctrl+C; the processor flushes and commits before it exits.

## Running on AWS: S3 + Athena

Requires an AWS account and the AWS CLI configured (`aws configure`). Cost for this
project is close to zero: S3 storage is a few MB, and Athena charges $5 per TB scanned while these
queries scan KBs.

```powershell
# 1. Create the bucket, upload route reference data, create the database, tables and views
python -m pipeline athena-setup --bucket <your-unique-bucket> --region ap-south-1

# 2. Load data: either the fast simulated backfill ...
python -m pipeline demo --sink s3 --bucket <your-unique-bucket>
#    ... or the live Kafka processor writing straight to S3
python -m pipeline process --sink s3 --bucket <your-unique-bucket>

# 3. Run the use-case SQL (prints result tables and bytes scanned)
python -m pipeline athena-query athena/queries/route_monitoring.sql --bucket <your-unique-bucket>
python -m pipeline athena-query athena/queries/speed_alerts.sql --bucket <your-unique-bucket>
python -m pipeline athena-query athena/queries/environmental_correlation.sql --bucket <your-unique-bucket>
python -m pipeline athena-query athena/queries/data_quality.sql --bucket <your-unique-bucket> --only rejection_reasons
```

You can also paste any query from `athena/queries/` into the Athena console.

**Athena design:** the tables use **partition projection**, so new `dt=/hr=` folders are
queryable immediately with no Glue crawler and no `MSCK REPAIR TABLE`. Every query filters on `dt` to prune
partitions. The `v_telemetry` and `v_speed_alerts` views parse ISO-8601 strings into timestamps once.

**Production path:** swap local Kafka for **Amazon MSK** by setting `KAFKA_BOOTSTRAP_SERVERS`, and run the
processor on ECS/Fargate. At larger scale, convert to Parquet (Glue job or Athena CTAS) to cut scan
cost by about 10x.

## Project layout

```
pipeline/
  simulator.py        fleet simulator: physics, weather and fault injection
  validation.py       7 classes of streaming data-quality checks
  processor.py        parse -> validate -> enrich -> alert / DLQ routing
  sinks.py            buffered, partitioned, gzip JSON writer (local or S3)
  routes.py           route reference data + geo helpers
  schema.py           event contract, timestamp handling
  athena.py           S3/Athena provisioning + query runner
  local_analytics.py  DuckDB report over the local lake
  cli.py              python -m pipeline <command>
athena/
  ddl.sql             database, 4 tables (partition projection), 2 views
  queries/*.sql       20 named analytics queries across the 4 use cases
tests/                39 pytest tests
docker-compose.yml    Kafka (KRaft) + Kafka UI
```

## Configuration

Everything in [config.py](pipeline/config.py) can be overridden with environment variables, e.g.
`KAFKA_BOOTSTRAP_SERVERS`, `ALERT_TOLERANCE_KMH` (default 5), `LATE_ARRIVAL_S` (600),
`MAX_EVENT_AGE_S` (86400), `FLUSH_MAX_RECORDS` (2000), `FLUSH_MAX_SECONDS` (60), `S3_PREFIX`.
