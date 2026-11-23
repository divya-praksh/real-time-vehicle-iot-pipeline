"""Central configuration. Every value can be overridden with an environment variable."""
import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


# --- Kafka -------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "vehicle.telemetry.raw")
ALERT_TOPIC = os.getenv("ALERT_TOPIC", "vehicle.alerts.speed")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "vehicle.telemetry.dlq")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "vehicle-iot-processor")
TOPIC_PARTITIONS = _int("TOPIC_PARTITIONS", 3)

# --- Validation thresholds ---------------------------------------------------
MAX_FUTURE_SKEW_S = _int("MAX_FUTURE_SKEW_S", 300)        # device clock ahead of server
LATE_ARRIVAL_S = _int("LATE_ARRIVAL_S", 600)              # older than this -> flagged late
MAX_EVENT_AGE_S = _int("MAX_EVENT_AGE_S", 24 * 3600)      # older than this -> rejected
MAX_IMPLIED_SPEED_KMH = _float("MAX_IMPLIED_SPEED_KMH", 300.0)  # GPS teleport detection
DEDUP_CACHE_SIZE = _int("DEDUP_CACHE_SIZE", 200_000)

# --- Business rules ----------------------------------------------------------
ALERT_TOLERANCE_KMH = _float("ALERT_TOLERANCE_KMH", 5.0)  # grace above the posted limit

# --- Sink / data lake --------------------------------------------------------
FLUSH_MAX_RECORDS = _int("FLUSH_MAX_RECORDS", 2000)
FLUSH_MAX_SECONDS = _int("FLUSH_MAX_SECONDS", 60)
S3_PREFIX = os.getenv("S3_PREFIX", "vehicle-iot")
LOCAL_LAKE_DIR = os.getenv("LOCAL_LAKE_DIR", "data/lake")

# --- Athena ------------------------------------------------------------------
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "vehicle_iot")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
