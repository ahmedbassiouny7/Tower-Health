"""
Generate historical RAN telemetry files and optionally upload them to S3.

The output keeps the same S3 partition shape used by Kafka Connect:
  <S3_PREFIX>/<topic>/year=YYYY/month=MM/day=DD/hour=00/<file>.json

Each file contains newline-delimited JSON snapshots for one day chunk.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import boto3

import ran_data_generator as generator


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_date(name: str, default: date) -> date:
    raw = os.getenv(name)
    if not raw:
        return default
    return date.fromisoformat(raw)


def each_day(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def timestamp_range(start: datetime, stop: datetime, step_minutes: int) -> Iterable[datetime]:
    current = start
    step = timedelta(minutes=step_minutes)
    while current < stop:
        yield current
        current += step


def parse_clock(value: str) -> time:
    hour_text, minute_text = value.split(":", 1)
    return time(hour=int(hour_text), minute=int(minute_text), tzinfo=timezone.utc)


def env_windows() -> list[tuple[time, time]]:
    raw = os.getenv("RAN_BACKFILL_WINDOWS", "00:00-12:00,12:15-00:00")
    windows: list[tuple[time, time]] = []
    for part in raw.split(","):
        start_text, stop_text = part.strip().split("-", 1)
        windows.append((parse_clock(start_text), parse_clock(stop_text)))
    if not windows:
        raise ValueError("RAN_BACKFILL_WINDOWS must contain at least one window")
    return windows


def day_chunks(day: date, windows: list[tuple[time, time]]) -> Iterable[tuple[datetime, datetime]]:
    for start_time, stop_time in windows:
        chunk_start = datetime.combine(day, start_time)
        chunk_stop = datetime.combine(day, stop_time)
        if chunk_stop <= chunk_start:
            chunk_stop += timedelta(days=1)
        yield chunk_start, chunk_stop


def chunk_suffix(chunk_start: datetime) -> str:
    return f"_{chunk_start:%H}" if chunk_start.minute == 0 else f"_{chunk_start:%H%M}"


def s3_key_for_chunk(prefix: str, topic: str, chunk_start: datetime, files_per_day: int) -> str:
    clean_prefix = prefix.strip("/")
    clean_topic = topic.strip("/")
    suffix = "" if files_per_day == 1 else chunk_suffix(chunk_start)
    return (
        f"{clean_prefix}/{clean_topic}/"
        f"year={chunk_start:%Y}/month={chunk_start:%m}/day={chunk_start:%d}/hour={chunk_start:%H}/"
        f"{clean_topic}_{chunk_start:%Y%m%d}{suffix}.json"
    )


def write_chunk_file(
    path: Path,
    sites: list[generator.SiteState],
    chunk_start: datetime,
    chunk_stop: datetime,
    step_minutes: int,
) -> int:
    rows = 0
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as handle:
        for ts in timestamp_range(chunk_start, chunk_stop, step_minutes):
            for site in sites:
                snapshot = generator.build_snapshot(site, ts)
                handle.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
                rows += 1

    return rows


def s3_object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            return False
        raise


def upload_file(client, path: Path, bucket: str, key: str) -> None:
    client.upload_file(str(path), bucket, key)


def main() -> None:
    today = datetime.now(timezone.utc).date()
    start = env_date("RAN_BACKFILL_START", date(2025, 11, 23))
    end = env_date("RAN_BACKFILL_END", today)
    if end < start:
        raise ValueError(f"RAN_BACKFILL_END {end} is before RAN_BACKFILL_START {start}")

    step_minutes = int(os.getenv("RAN_BACKFILL_STEP_MINUTES", "60"))
    if step_minutes <= 0 or (24 * 60) % step_minutes != 0:
        raise ValueError("RAN_BACKFILL_STEP_MINUTES must divide one day evenly")
    windows = env_windows()
    files_per_day = len(windows)

    output_dir = Path(os.getenv("OUTPUT_DIR", "/data/historical_ran"))
    bucket = os.getenv("S3_BUCKET_NAME", "tower-iti-project")
    region = os.getenv("AWS_REGION", "us-east-1")
    prefix = os.getenv("S3_PREFIX", "raw-data")
    topic = (
        os.getenv("S3_TOPIC_DIR")
        or os.getenv("KAFKA_TOPIC")
        or os.getenv("RAN_KAFKA_TOPIC")
        or "ran_telemetry"
    )
    upload_enabled = env_bool("RAN_BACKFILL_UPLOAD", True)
    overwrite = env_bool("RAN_BACKFILL_OVERWRITE", False)
    keep_local = env_bool("RAN_BACKFILL_KEEP_LOCAL", False)
    s3_client = boto3.client("s3", region_name=region) if upload_enabled else None

    generator.INTERVAL_SECONDS = step_minutes * 60

    total_days = (end - start).days + 1
    total_files = total_days * files_per_day
    sites = [generator.SiteState(meta) for meta in generator.SITE_TOPOLOGY]
    print(
        f"Historical RAN backfill: {start} -> {end} "
        f"({total_files} files, windows={os.getenv('RAN_BACKFILL_WINDOWS', '00:00-12:00,12:15-00:00')}, "
        f"step={step_minutes}m, upload={upload_enabled})",
        flush=True,
    )
    print(f"S3 target: s3://{bucket}/{prefix.strip('/')}/{topic.strip('/')}/", flush=True)

    file_index = 0
    for day in each_day(start, end):
        for chunk_start, chunk_stop in day_chunks(day, windows):
            file_index += 1
            suffix = "" if files_per_day == 1 else chunk_suffix(chunk_start)
            local_file = output_dir / f"{topic}_{chunk_start:%Y%m%d}{suffix}.json"
            rows = write_chunk_file(local_file, sites, chunk_start, chunk_stop, step_minutes)
            key = s3_key_for_chunk(prefix, topic, chunk_start, files_per_day)

            if upload_enabled:
                if not overwrite and s3_object_exists(s3_client, bucket, key):
                    action = f"skipped existing s3://{bucket}/{key}"
                else:
                    upload_file(s3_client, local_file, bucket, key)
                    action = f"uploaded s3://{bucket}/{key}"
            else:
                action = f"wrote {local_file}"

            if upload_enabled and not keep_local:
                local_file.unlink()

            print(
                f"[{file_index}/{total_files}] {chunk_start:%Y-%m-%d %H:%M}"
                f"-{chunk_stop:%H:%M} rows={rows} {action}",
                flush=True,
            )


if __name__ == "__main__":
    main()
