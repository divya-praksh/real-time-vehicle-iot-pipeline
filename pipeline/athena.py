"""Provision the S3 data lake + Athena schema, and run .sql files against Athena."""
import json
import re
import time
from pathlib import Path
from typing import List, Optional, Tuple

from . import config
from .routes import reference_records

ATHENA_DIR = Path(__file__).resolve().parent.parent / "athena"
_NAME = re.compile(r"--\s*name:\s*([\w\-]+)")


def split_sql(text: str) -> List[Tuple[str, str]]:
    """Split a .sql file into (name, statement) pairs.

    Statements are separated by ';'. A '-- name: foo' comment labels the
    statement. Full-line comments are stripped (Athena DDL rejects leading comments).
    """
    statements, name, lines = [], None, []

    def emit():
        body = "\n".join(lines).strip()
        if body:
            statements.append((name or f"statement_{len(statements) + 1}", body))

    for line in text.splitlines():
        if line.strip().startswith("--"):
            m = _NAME.search(line)
            if m:
                name = m.group(1)
            continue  # comments (which may contain ';') never reach the splitter
        parts = line.split(";")
        lines.append(parts[0])
        for part in parts[1:]:
            emit()
            name, lines = None, [part]
    emit()
    return statements


def resolve_region(region: Optional[str]) -> str:
    if region:
        return region
    import boto3
    found = boto3.session.Session().region_name
    if not found:
        raise SystemExit("No AWS region configured. Pass --region or run `aws configure`.")
    return found


class AthenaRunner:
    def __init__(self, region: str, output_location: str, workgroup: str = config.ATHENA_WORKGROUP):
        import boto3
        self.client = boto3.client("athena", region_name=region)
        self.output_location = output_location
        self.workgroup = workgroup

    def run(self, sql: str, database: Optional[str] = config.ATHENA_DATABASE, timeout_s: int = 300):
        kwargs = {
            "QueryString": sql,
            "WorkGroup": self.workgroup,
            "ResultConfiguration": {"OutputLocation": self.output_location},
        }
        if database:
            kwargs["QueryExecutionContext"] = {"Database": database}
        qid = self.client.start_query_execution(**kwargs)["QueryExecutionId"]

        deadline = time.time() + timeout_s
        delay = 0.5
        while True:
            execution = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = execution["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                reason = execution["Status"].get("StateChangeReason", "unknown error")
                raise RuntimeError(f"Athena query {state}: {reason}")
            if time.time() > deadline:
                self.client.stop_query_execution(QueryExecutionId=qid)
                raise TimeoutError(f"Athena query {qid} exceeded {timeout_s}s")
            time.sleep(delay)
            delay = min(delay * 1.5, 5)

        scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
        columns, rows = [], []
        skip_header = execution.get("StatementType") == "DML"
        for page in self.client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
            result_set = page["ResultSet"]
            if not columns:
                columns = [c["Name"] for c in result_set["ResultSetMetadata"]["ColumnInfo"]]
            for row in result_set["Rows"]:
                values = [d.get("VarCharValue", "") for d in row["Data"]]
                if skip_header:  # first row of a SELECT is the header
                    skip_header = False
                    continue
                rows.append(values)
        return columns, rows, scanned


def format_table(columns: List[str], rows: List[List[str]], max_width: int = 40) -> str:
    if not columns:
        return "(no result set)"
    cells = [[str(v)[:max_width] for v in r] for r in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells if i < len(r)]) for i, c in enumerate(columns)]
    line = lambda vals: " | ".join(v.ljust(w) for v, w in zip(vals, widths))
    out = [line(columns), "-+-".join("-" * w for w in widths)] + [line(r) for r in cells]
    if not rows:
        out.append("(0 rows)")
    return "\n".join(out)


def ensure_bucket(s3, bucket: str, region: str) -> None:
    from botocore.exceptions import ClientError
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"bucket s3://{bucket} exists")
        return
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("403", "AccessDenied"):
            raise SystemExit(f"Bucket {bucket} exists but belongs to another account - choose another name.")
        if code not in ("404", "NoSuchBucket", "NotFound"):
            raise
    params = {"Bucket": bucket}
    if region != "us-east-1":
        params["CreateBucketConfiguration"] = {"LocationConstraint": region}
    s3.create_bucket(**params)
    print(f"created bucket s3://{bucket} in {region} (public access blocked by default)")


def setup(bucket: str, region: Optional[str], prefix: str = config.S3_PREFIX) -> None:
    import boto3
    region = resolve_region(region)
    prefix = prefix.strip("/")
    s3 = boto3.client("s3", region_name=region)
    ensure_bucket(s3, bucket, region)

    body = "\n".join(json.dumps(r) for r in reference_records()) + "\n"
    s3.put_object(Bucket=bucket, Key=f"{prefix}/reference/routes/routes.json", Body=body.encode())
    print(f"uploaded route reference data to s3://{bucket}/{prefix}/reference/routes/")

    lake = f"s3://{bucket}/{prefix}"
    runner = AthenaRunner(region, f"s3://{bucket}/athena-results/")
    ddl = (ATHENA_DIR / "ddl.sql").read_text(encoding="utf-8").replace("${LAKE}", lake)
    for name, sql in split_sql(ddl):
        database = None if name == "create_database" else config.ATHENA_DATABASE
        runner.run(sql, database=database)
        print(f"  ok  {name}")
    print(f"\nAthena database '{config.ATHENA_DATABASE}' is ready over {lake}")


def run_file(path: str, bucket: str, region: Optional[str], only: Optional[str] = None) -> None:
    region = resolve_region(region)
    runner = AthenaRunner(region, f"s3://{bucket}/athena-results/")
    statements = split_sql(Path(path).read_text(encoding="utf-8"))
    if only:
        statements = [s for s in statements if s[0] == only]
        if not statements:
            raise SystemExit(f"No statement named '{only}' in {path}")
    for name, sql in statements:
        columns, rows, scanned = runner.run(sql)
        print(f"\n=== {name}  ({len(rows)} rows, {scanned / 1024:.1f} KiB scanned) ===")
        print(format_table(columns, rows))
