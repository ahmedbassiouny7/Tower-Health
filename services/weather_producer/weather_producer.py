"""
Weather producer for NetPulse.

OUTPUT_MODE env var:
  "stdout"  – fetch and print JSON to console (no Kafka needed)
  "kafka"   – publish events to a Kafka topic

Environment variables:
  WEATHERAPI_KEY              required
  OUTPUT_MODE                 default: stdout
  WEATHER_LOCATION            default: Cairo
  WEATHER_LAT / WEATHER_LON   optional; overrides WEATHER_LOCATION
  KAFKA_BOOTSTRAP_SERVERS     default: kafka-broker-1:9092
  KAFKA_TOPIC                 default: weather_events
  WEATHER_POLL_SECONDS        default: 300
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from weatherapi_client import (
    WeatherApiError,
    fetch_current_weather,
    load_env_file,
    map_to_tower_weather_fields,
    project_weather_fields,
)

OUTPUT_MODE                 = os.getenv("OUTPUT_MODE", "stdout")
DEFAULT_KAFKA_BOOTSTRAP     = "kafka-broker-1:9092"
DEFAULT_KAFKA_TOPIC         = "weather_events"
DEFAULT_LOCATION            = "Cairo"
DEFAULT_POLL_SECONDS        = 300
KAFKA_CONNECT_RETRY_SECONDS = 5

running = True


def stop(_signum: int, _frame: Any) -> None:
    global running
    running = False


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def get_weather_location() -> str:
    lat = os.getenv("WEATHER_LAT")
    lon = os.getenv("WEATHER_LON")
    if lat and lon:
        return f"{lat},{lon}"
    return os.getenv("WEATHER_LOCATION", DEFAULT_LOCATION)


def build_weather_event(location_query: str, mapped_weather: dict[str, Any]) -> dict[str, Any]:
    event: dict[str, Any] = {
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_system":   "weather_producer",
        "location_query":  location_query,
    }
    event.update(project_weather_fields(mapped_weather))
    event.update({
        "weather_observed_at":    mapped_weather.get("weather_observed_at"),
        "weather_fetched_at":     mapped_weather.get("weather_fetched_at"),
        "weather_location_name":  mapped_weather.get("weather_location_name"),
        "weather_region":         mapped_weather.get("weather_region"),
        "weather_country":        mapped_weather.get("weather_country"),
        "weather_latitude":       mapped_weather.get("weather_latitude"),
        "weather_longitude":      mapped_weather.get("weather_longitude"),
    })
    return event


def emit_stdout(event: dict[str, Any]) -> None:
    print("\n" + "=" * 60)
    print(f"  Weather Event  |  {event['event_timestamp']}")
    print("=" * 60)
    print(json.dumps(event, indent=2, ensure_ascii=False), flush=True)


def create_kafka_producer(bootstrap_servers: str):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    while running:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                retries=5,
                linger_ms=100,
            )
        except NoBrokersAvailable:
            print(
                f"Kafka not ready at {bootstrap_servers}; "
                f"retrying in {KAFKA_CONNECT_RETRY_SECONDS}s...",
                flush=True,
            )
            time.sleep(KAFKA_CONNECT_RETRY_SECONDS)

    raise RuntimeError("Producer stopped before Kafka became available")


def emit_kafka(producer, topic: str, event: dict[str, Any]) -> None:
    key = str(event.get("weather_location_name") or event.get("location_query", "unknown"))
    metadata = producer.send(topic, key=key, value=event).get(timeout=30)
    producer.flush()
    print(
        f"Published weather topic={metadata.topic} "
        f"partition={metadata.partition} offset={metadata.offset} location={key}",
        flush=True,
    )


def main() -> int:
    load_env_file()

    api_key = os.getenv("WEATHERAPI_KEY")
    if not api_key:
        print("Missing WEATHERAPI_KEY", file=sys.stderr)
        return 2

    location_query = get_weather_location()
    poll_seconds   = env_int("WEATHER_POLL_SECONDS", DEFAULT_POLL_SECONDS)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = None
    topic    = None
    if OUTPUT_MODE == "kafka":
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP)
        topic     = os.getenv("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
        producer  = create_kafka_producer(bootstrap)

    print(
        f"Weather producer started  mode={OUTPUT_MODE}  "
        f"location={location_query}  poll={poll_seconds}s",
        flush=True,
    )

    try:
        while running:
            try:
                raw_weather    = fetch_current_weather(api_key, location_query)
                mapped_weather = map_to_tower_weather_fields(raw_weather)
                event          = build_weather_event(location_query, mapped_weather)

                if OUTPUT_MODE == "kafka" and producer:
                    emit_kafka(producer, topic, event)
                else:
                    emit_stdout(event)

            except WeatherApiError as exc:
                print(f"Weather fetch failed: {exc}", file=sys.stderr, flush=True)

            for _ in range(poll_seconds):
                if not running:
                    break
                time.sleep(1)
    finally:
        if producer:
            producer.close(timeout=10)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
