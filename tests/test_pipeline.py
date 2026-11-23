import gzip
import json
from datetime import datetime, timedelta, timezone

from pipeline import config
from pipeline.athena import ATHENA_DIR, split_sql
from pipeline.processor import StreamProcessor, alert_severity, speed_band
from pipeline.simulator import ANOMALIES, REJECTED_ANOMALIES, VehicleSimulator
from pipeline.sinks import LocalSink, S3Sink
from pipeline.validation import StreamValidator

START = datetime(2026, 11, 23, 8, 0, 0, tzinfo=timezone.utc)


def run_simulation(sink, ticks=300, anomaly_rate=0.0, seed=7):
    sim = VehicleSimulator(num_vehicles=6, anomaly_rate=anomaly_rate, start_time=START, seed=seed)
    published = []
    processor = StreamProcessor(
        sink, clock=lambda: sim.now + timedelta(seconds=1),
        publish=lambda topic, key, value: published.append((topic, key, value)),
    )
    for _ in range(ticks):
        for _key, value in sim.tick():
            processor.process(value)
    sink.flush()
    return sim, processor, published


def read_dataset(root, dataset):
    rows = []
    for path in sorted((root / dataset).rglob("*.json.gz")):
        rows += [json.loads(line) for line in gzip.decompress(path.read_bytes()).decode().splitlines()]
    return rows


def test_clean_simulation_is_fully_valid(tmp_path):
    sink = LocalSink(tmp_path)
    _, processor, _ = run_simulation(sink)
    assert processor.stats["received"] == 6 * 300
    assert processor.stats["invalid"] == 0
    rows = read_dataset(tmp_path, "telemetry")
    assert len(rows) == 6 * 300
    assert {r["route_id"] for r in rows} == {"R1-ORR", "R2-AIRPORT", "R3-ECITY"}


def test_every_injected_rejection_reaches_the_dlq(tmp_path):
    sink = LocalSink(tmp_path)
    sim, processor, published = run_simulation(sink, ticks=600, anomaly_rate=0.2)
    expected_rejections = sum(sim.anomaly_counts[k] for k in REJECTED_ANOMALIES)
    assert expected_rejections > 0
    assert processor.stats["invalid"] == expected_rejections
    assert len(read_dataset(tmp_path, "dlq_events")) == expected_rejections
    assert sum(1 for t, _, _ in published if t == config.DLQ_TOPIC) == expected_rejections
    assert set(sim.anomaly_counts) <= set(ANOMALIES)


def test_enrichment_and_alerts_are_consistent(tmp_path):
    sink = LocalSink(tmp_path)
    _, processor, published = run_simulation(sink, ticks=600)
    telemetry = read_dataset(tmp_path, "telemetry")
    alerts = read_dataset(tmp_path, "speed_alerts")
    speeding = [r for r in telemetry if r["is_speeding"]]
    assert alerts and len(alerts) == len(speeding) == processor.stats["alerts"]
    assert sum(1 for t, _, _ in published if t == config.ALERT_TOPIC) == len(alerts)
    for a in alerts:
        assert a["over_limit_kmh"] > config.ALERT_TOLERANCE_KMH
        assert a["severity"] == alert_severity(a["over_limit_kmh"])
    for r in telemetry[:50]:
        assert r["speed_band"] == speed_band(r["speed_kmh"])
        assert r["event_ts"].endswith("Z") and r["ingest_lag_s"] >= 0


def test_partitions_follow_event_time(tmp_path):
    sink = LocalSink(tmp_path)
    run_simulation(sink, ticks=900)  # 75 minutes from 08:00 -> spans hr=08 and hr=09
    hours = {p.name for p in (tmp_path / "telemetry" / "dt=2026-11-23").iterdir()}
    assert hours == {"hr=08", "hr=09"}
    for path in (tmp_path / "telemetry").rglob("*.json.gz"):
        hr = path.parent.name.split("=")[1]
        for line in gzip.decompress(path.read_bytes()).decode().splitlines():
            assert json.loads(line)["event_ts"][11:13] == hr


def test_malformed_and_binary_payloads_go_to_dlq(tmp_path):
    sink = LocalSink(tmp_path)
    processor = StreamProcessor(sink, validator=StreamValidator())
    assert processor.process('{"event_id": "x"') == "invalid"
    assert processor.process(b"\xff\xfe") == "invalid"
    sink.flush()
    codes = [r["error_codes"] for r in read_dataset(tmp_path, "dlq_events")]
    assert ["malformed_json"] in codes and ["invalid_encoding"] in codes


def test_flush_policy():
    sink = LocalSink("unused", max_records=3, max_seconds=3600)
    for _ in range(2):
        sink.write("telemetry", {"a": 1}, START)
    assert not sink.should_flush()
    sink.write("telemetry", {"a": 1}, START)
    assert sink.should_flush()


def test_s3_sink_keys():
    class FakeS3:
        def __init__(self):
            self.objects = {}

        def put_object(self, Bucket, Key, Body):
            self.objects[(Bucket, Key)] = Body

    client = FakeS3()
    sink = S3Sink("my-bucket", prefix="/vehicle-iot/", client=client)
    sink.write("speed_alerts", {"x": 1}, START)
    sink.flush()
    (bucket, key), body = next(iter(client.objects.items()))
    assert bucket == "my-bucket"
    assert key.startswith("vehicle-iot/speed_alerts/dt=2026-11-23/hr=08/part-") and key.endswith(".json.gz")
    assert json.loads(gzip.decompress(body)) == {"x": 1}


def test_simulator_is_deterministic_with_seed():
    a = VehicleSimulator(num_vehicles=3, anomaly_rate=0.1, start_time=START, seed=1)
    b = VehicleSimulator(num_vehicles=3, anomaly_rate=0.1, start_time=START, seed=1)
    assert [a.tick() for _ in range(20)] == [b.tick() for _ in range(20)]


def test_sql_files_split_into_named_statements():
    ddl = split_sql((ATHENA_DIR / "ddl.sql").read_text(encoding="utf-8"))
    assert [n for n, _ in ddl] == [
        "create_database", "create_telemetry", "create_speed_alerts", "create_dlq_events",
        "create_route_reference", "create_v_telemetry", "create_v_speed_alerts",
    ]
    for name, sql in ddl:
        assert not sql.lstrip().startswith("--"), name
    for path in (ATHENA_DIR / "queries").glob("*.sql"):
        statements = split_sql(path.read_text(encoding="utf-8"))
        assert len(statements) >= 4, path.name
        assert all(not n.startswith("statement_") for n, _ in statements), path.name
