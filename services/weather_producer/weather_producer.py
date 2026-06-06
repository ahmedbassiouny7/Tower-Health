"""
Weather producer for TowerHealth.

OUTPUT_MODE env var:
  "stdout"  – fetch and print JSON to console (no Kafka needed)
  "kafka"   – publish events to a Kafka topic

Environment variables:
  WEATHERAPI_KEY              required
  OUTPUT_MODE                 default: stdout
  WEATHER_LOCATIONS           optional semicolon-separated label|lat,lon entries
  WEATHER_LOCATION            default: Cairo when WEATHER_LOCATIONS is not set
  WEATHER_LAT / WEATHER_LON   optional; overrides WEATHER_LOCATION
  KAFKA_BOOTSTRAP_SERVERS     default: kafka-broker-1:9092
  KAFKA_TOPIC                 default: weather_events
  WEATHER_POLL_SECONDS        default: 900
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
DEFAULT_POLL_SECONDS        = 900
KAFKA_CONNECT_RETRY_SECONDS = 5

DEFAULT_TOWER_LOCATIONS: list[dict[str, str]] = [
    {
        "site_id": "SITE_001",
        "site_name": "AL_TOWER_01",
        "region": "Alexandria",
        "query": "31.2001,29.9187",
    },
    {
        "site_id": "SITE_002",
        "site_name": "CA_TOWER_02",
        "region": "Cairo",
        "query": "30.0444,31.2357",
    },
    {
        "site_id": "SITE_003",
        "site_name": "GZ_TOWER_03",
        "region": "Giza",
        "query": "30.0131,31.2089",
    },
    {
        "site_id": "SITE_004",
        "site_name": "KS_TOWER_04",
        "region": "North Sinai",
        "query": "31.1107,33.7961",
    },
]

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


def get_weather_location() -> dict[str, str]:
    """Prefer exact coordinates, then fall back to a named WeatherAPI location."""
    lat = os.getenv("WEATHER_LAT")
    lon = os.getenv("WEATHER_LON")
    if lat and lon:
        query = f"{lat},{lon}"
        return {"region": os.getenv("WEATHER_LOCATION", query), "query": query}

    location = os.getenv("WEATHER_LOCATION", DEFAULT_LOCATION)
    return {"region": location, "query": location}


def get_weather_locations() -> list[dict[str, str]]:
    """Return all WeatherAPI locations to poll this cycle."""
    configured_locations = os.getenv("WEATHER_LOCATIONS")
    if not configured_locations:
        if os.getenv("WEATHER_LOCATION") or os.getenv("WEATHER_LAT") or os.getenv("WEATHER_LON"):
            return [get_weather_location()]
        return DEFAULT_TOWER_LOCATIONS

    locations: list[dict[str, str]] = []
    for raw_location in configured_locations.split(";"):
        raw_location = raw_location.strip()
        if not raw_location:
            continue

        label, sep, query = raw_location.partition("|")
        if sep:
            locations.append({"region": label.strip(), "query": query.strip()})
        else:
            locations.append({"region": raw_location, "query": raw_location})

    return locations or DEFAULT_TOWER_LOCATIONS


def resolve_tower_site(location: dict[str, str]) -> dict[str, str]:
    """Map polled coordinates to the matching RAN site_id when possible."""
    if location.get("site_id"):
        return location
    query = location.get("query", "")
    for tower in DEFAULT_TOWER_LOCATIONS:
        if tower["query"] == query:
            return {**location, "site_id": tower["site_id"], "site_name": tower["site_name"]}
    return location


def build_weather_event(location: dict[str, str], mapped_weather: dict[str, Any]) -> dict[str, Any]:
    """Wrap mapped WeatherAPI fields with metadata useful for Kafka consumers."""
    event: dict[str, Any] = {
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_system":   "weather_producer",
        "location_query":  location["query"],
        "ran_site_id":     location.get("site_id"),
        "ran_site_name":   location.get("site_name"),
        "ran_region":      location.get("region"),
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
            # Kafka can start slower than this container. Keep retrying so
            # docker compose up works without manual ordering.
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
    """Publish one weather event using location as the Kafka message key."""
    key = str(event.get("ran_site_id") or event.get("weather_location_name") or event.get("location_query", "unknown"))
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

    locations      = get_weather_locations()
    poll_seconds   = env_int("WEATHER_POLL_SECONDS", DEFAULT_POLL_SECONDS)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    producer = None
    topic    = None
    if OUTPUT_MODE == "kafka":
        # In stdout mode this service is useful for local API testing.
        # In kafka mode it becomes part of the streaming pipeline.
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP)
        topic     = os.getenv("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
        producer  = create_kafka_producer(bootstrap)

    print(
        f"Weather producer started  mode={OUTPUT_MODE}  "
        f"locations={len(locations)}  poll={poll_seconds}s",
        flush=True,
    )

    try:
        while running:
            for location in locations:
                try:
                    raw_weather    = fetch_current_weather(api_key, location["query"])
                    mapped_weather = map_to_tower_weather_fields(raw_weather)
                    event          = build_weather_event(resolve_tower_site(location), mapped_weather)

                    if OUTPUT_MODE == "kafka" and producer:
                        emit_kafka(producer, topic, event)
                    else:
                        emit_stdout(event)

                except WeatherApiError as exc:
                    region = location.get("region", location["query"])
                    print(f"Weather fetch failed for {region}: {exc}", file=sys.stderr, flush=True)

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
