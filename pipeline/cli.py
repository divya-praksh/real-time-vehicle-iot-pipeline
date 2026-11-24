"""Command line entry point:  python -m pipeline <command> [options]

  demo            end-to-end run with an in-memory broker (no Kafka needed)
  create-topics   create the Kafka topics
  produce         stream simulated telemetry into Kafka
  process         consume from Kafka, validate, alert, write to the lake
  analyze         run the use-case report on the local lake with DuckDB
  athena-setup    create the S3 bucket + Athena database/tables/views
  athena-query    run a .sql file from athena/queries against Athena
"""
import argparse
import json
import random
import signal
import time
from collections import defaultdict, deque
from datetime import timedelta

from . import config
from .processor import StreamProcessor
from .routes import reference_records
from .schema import utcnow
from .simulator import VehicleSimulator
from .sinks import LocalSink, S3Sink

REFERENCE_PATH = "reference/routes/routes.json"


# ----------------------------------------------------------------- helpers
def _make_sink(args):
    if args.sink == "s3":
        if not args.bucket:
            raise SystemExit("--bucket is required with --sink s3")
        return S3Sink(args.bucket, args.prefix)
    return LocalSink(args.output)


def _add_sink_args(p):
    p.add_argument("--sink", choices=["local", "s3"], default="local")
    p.add_argument("--output", default=config.LOCAL_LAKE_DIR, help="local lake directory")
    p.add_argument("--bucket", help="S3 bucket (with --sink s3)")
    p.add_argument("--prefix", default=config.S3_PREFIX, help="S3 key prefix")


class _Stop:
    """Turns Ctrl+C into a clean shutdown flag so buffers get flushed."""
    def __init__(self):
        self.stopped = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_):
        self.stopped = True


# ---------------------------------------------------------------- commands
def cmd_demo(args):
    """Producer -> broker -> processor -> lake, all in one process, on a simulated clock."""
    broker = defaultdict(deque)  # topic -> queue; stands in for Kafka
    start = utcnow() - timedelta(minutes=args.minutes)
    sim = VehicleSimulator(args.vehicles, args.anomaly_rate, args.tick_seconds, start_time=start, seed=args.seed)
    lag_rng = random.Random(args.seed)
    # The processor's clock = simulated time + realistic network lag (0.2-3 s).
    clock = lambda: sim.now + timedelta(seconds=lag_rng.uniform(0.2, 3.0))
    sink = _make_sink(args)
    processor = StreamProcessor(sink, clock=clock, publish=lambda t, k, v: broker[t].append((k, v)))

    ticks = int(args.minutes * 60 / args.tick_seconds)
    print(f"Simulating {args.vehicles} vehicles x {ticks} ticks ({args.minutes} min of driving, "
          f"anomaly rate {args.anomaly_rate:.0%}) ...")
    t0 = time.perf_counter()
    for i in range(ticks):
        for key, value in sim.tick():
            broker[config.RAW_TOPIC].append((key, value))
        raw = broker[config.RAW_TOPIC]
        offset = i * 10_000
        while raw:
            _, value = raw.popleft()
            processor.process(value, source={"topic": config.RAW_TOPIC, "partition": 0, "offset": offset})
            offset += 1
        if sink.should_flush():
            sink.flush()
    sink.flush()
    sink.write_reference(REFERENCE_PATH, reference_records())
    elapsed = time.perf_counter() - t0

    print(f"\nDone in {elapsed:.1f}s -> {sink.describe()}")
    print(f"  files written: {sink.files_written}, records: {sink.records_written}")
    print(f"  topic sizes: alerts={len(broker[config.ALERT_TOPIC])}, dlq={len(broker[config.DLQ_TOPIC])}")
    print(f"  anomalies injected: {dict(sorted(sim.anomaly_counts.items()))}")
    print(processor.summary())


def cmd_create_topics(args):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    week_ms = str(7 * 24 * 3600 * 1000)
    topics = [
        NewTopic(config.RAW_TOPIC, num_partitions=config.TOPIC_PARTITIONS, replication_factor=1),
        NewTopic(config.ALERT_TOPIC, num_partitions=config.TOPIC_PARTITIONS, replication_factor=1),
        NewTopic(config.DLQ_TOPIC, num_partitions=1, replication_factor=1, config={"retention.ms": week_ms}),
    ]
    for name, future in admin.create_topics(topics).items():
        try:
            future.result()
            print(f"created {name}")
        except Exception as exc:  # noqa: BLE001 - surface Kafka's message as-is
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"exists  {name}")
            else:
                raise


def cmd_produce(args):
    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,   # no broker-side duplicates on retry
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    failures = []
    on_delivery = lambda err, _msg: err and failures.append(err)
    sim = VehicleSimulator(args.vehicles, args.anomaly_rate, args.tick_seconds, realtime=True)
    stop = _Stop()
    sent = 0
    print(f"Producing to {config.RAW_TOPIC} every {args.tick_seconds}s for {args.vehicles} vehicles. Ctrl+C to stop.")
    deadline = time.time() + args.duration if args.duration else None
    while not stop.stopped and (deadline is None or time.time() < deadline):
        started = time.time()
        for key, value in sim.tick():
            while True:
                try:
                    producer.produce(config.RAW_TOPIC, key=key, value=value, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)  # local queue full: wait for deliveries
            sent += 1
        producer.poll(0)
        if sent and sent % (args.vehicles * 12) < args.vehicles:
            print(f"  sent={sent} delivery_failures={len(failures)}")
        time.sleep(max(0.0, args.tick_seconds - (time.time() - started)))
    producer.flush(10)
    print(f"Stopped. sent={sent} delivery_failures={len(failures)}")


def cmd_process(args):
    from confluent_kafka import Consumer, KafkaError, Producer

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit only after the lake write succeeds
    })
    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all"})
    sink = _make_sink(args)
    sink.write_reference(REFERENCE_PATH, reference_records())
    processor = StreamProcessor(sink, publish=lambda t, k, v: producer.produce(t, key=k, value=v))
    consumer.subscribe([config.RAW_TOPIC])
    stop = _Stop()
    last_report = time.time()
    print(f"Consuming {config.RAW_TOPIC} as group '{config.CONSUMER_GROUP}' -> {sink.describe()}. Ctrl+C to stop.")

    def checkpoint():
        if sink.flush():
            producer.flush(10)
            consumer.commit(asynchronous=False)

    deadline = time.time() + args.duration if args.duration else None
    try:
        while not stop.stopped and (deadline is None or time.time() < deadline):
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"consumer error: {msg.error()}")
                else:
                    processor.process(msg.value(), source={
                        "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()})
            producer.poll(0)
            if sink.should_flush():
                checkpoint()
            if time.time() - last_report > 30:
                print(processor.summary())
                last_report = time.time()
    finally:
        checkpoint()
        consumer.close()
        print("Stopped.\n" + processor.summary())


def cmd_analyze(args):
    from .local_analytics import run_report
    run_report(args.lake)


def cmd_athena_setup(args):
    from .athena import setup
    setup(args.bucket, args.region, args.prefix)


def cmd_athena_query(args):
    from .athena import run_file
    run_file(args.file, args.bucket, args.region, args.only)


# -------------------------------------------------------------------- main
def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m pipeline", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def sim_args(p, tick_default):
        p.add_argument("--vehicles", type=int, default=12)
        p.add_argument("--anomaly-rate", type=float, default=0.03, help="fraction of events corrupted (0-1)")
        p.add_argument("--tick-seconds", type=float, default=tick_default, help="seconds between pings per vehicle")

    p = sub.add_parser("demo", help="end-to-end run without Kafka")
    sim_args(p, 5.0)
    p.add_argument("--minutes", type=int, default=180, help="minutes of driving to simulate")
    p.add_argument("--seed", type=int, default=42)
    _add_sink_args(p)
    p.set_defaults(func=cmd_demo)

    p = sub.add_parser("create-topics")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP_SERVERS)
    p.set_defaults(func=cmd_create_topics)

    p = sub.add_parser("produce")
    sim_args(p, 1.0)
    p.add_argument("--duration", type=int, default=0, help="seconds to run (0 = until Ctrl+C)")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP_SERVERS)
    p.set_defaults(func=cmd_produce)

    p = sub.add_parser("process")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP_SERVERS)
    p.add_argument("--duration", type=int, default=0, help="seconds to run (0 = until Ctrl+C)")
    _add_sink_args(p)
    p.set_defaults(func=cmd_process)

    p = sub.add_parser("analyze")
    p.add_argument("--lake", default=config.LOCAL_LAKE_DIR)
    p.set_defaults(func=cmd_analyze)

    p = sub.add_parser("athena-setup")
    p.add_argument("--bucket", required=True)
    p.add_argument("--region")
    p.add_argument("--prefix", default=config.S3_PREFIX)
    p.set_defaults(func=cmd_athena_setup)

    p = sub.add_parser("athena-query")
    p.add_argument("file", help="e.g. athena/queries/speed_alerts.sql")
    p.add_argument("--bucket", required=True, help="bucket used for Athena results")
    p.add_argument("--region")
    p.add_argument("--only", help="run just the statement with this -- name:")
    p.set_defaults(func=cmd_athena_query)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
