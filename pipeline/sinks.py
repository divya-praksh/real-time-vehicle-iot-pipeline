"""Data lake sinks.

Records are buffered per (dataset, dt, hr) and written as gzipped JSON Lines
using Hive-style partitions:

    <root>/<dataset>/dt=YYYY-MM-DD/hr=HH/part-<ts>-<id>.json.gz

Telemetry and alerts are partitioned by EVENT time (so late events land in the
hour they happened); DLQ records by ingestion time. Athena reads this layout
directly via partition projection - no crawler or MSCK REPAIR needed.

Delivery guarantee: the Kafka consumer commits offsets only after flush()
succeeds, so the sink is at-least-once. Every file name is unique, so a replay
never overwrites data, and downstream queries can dedupe on event_id.
"""
import gzip
import json
import os
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from . import config


class PartitionedJsonSink(ABC):
    def __init__(self, max_records: int = config.FLUSH_MAX_RECORDS, max_seconds: float = config.FLUSH_MAX_SECONDS):
        self.max_records = max_records
        self.max_seconds = max_seconds
        self._buffers: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
        self._buffered = 0
        self._last_flush = time.monotonic()
        self.files_written = 0
        self.records_written = 0

    def write(self, dataset: str, record: dict, ts: datetime) -> None:
        ts = ts.astimezone(timezone.utc)
        key = (dataset, ts.strftime("%Y-%m-%d"), ts.strftime("%H"))
        self._buffers[key].append(json.dumps(record, separators=(",", ":"), default=str))
        self._buffered += 1

    def should_flush(self) -> bool:
        if self._buffered >= self.max_records:
            return True
        return self._buffered > 0 and time.monotonic() - self._last_flush >= self.max_seconds

    def flush(self) -> List[str]:
        written = []
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        for (dataset, dt, hr), lines in self._buffers.items():
            path = f"{dataset}/dt={dt}/hr={hr}/part-{stamp}-{uuid.uuid4().hex[:12]}.json.gz"
            self._put(path, gzip.compress(("\n".join(lines) + "\n").encode("utf-8")))
            written.append(path)
            self.records_written += len(lines)
        self.files_written += len(written)
        self._buffers.clear()
        self._buffered = 0
        self._last_flush = time.monotonic()
        return written

    def write_reference(self, path: str, records: List[dict]) -> None:
        """Small, uncompressed reference tables (overwritten in place)."""
        body = "\n".join(json.dumps(r, separators=(",", ":")) for r in records) + "\n"
        self._put(path, body.encode("utf-8"))

    @abstractmethod
    def _put(self, path: str, body: bytes) -> None: ...

    @abstractmethod
    def describe(self) -> str: ...


class LocalSink(PartitionedJsonSink):
    def __init__(self, root: str = config.LOCAL_LAKE_DIR, **kwargs):
        super().__init__(**kwargs)
        self.root = Path(root)

    def _put(self, path: str, body: bytes) -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".tmp")
        tmp.write_bytes(body)
        os.replace(tmp, target)  # atomic: readers never see half a file

    def describe(self) -> str:
        return str(self.root.resolve())


class S3Sink(PartitionedJsonSink):
    def __init__(self, bucket: str, prefix: str = config.S3_PREFIX, client=None, **kwargs):
        super().__init__(**kwargs)
        if client is None:
            import boto3
            client = boto3.client("s3")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _put(self, path: str, body: bytes) -> None:
        key = f"{self.prefix}/{path}" if self.prefix else path
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body)

    def describe(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"
