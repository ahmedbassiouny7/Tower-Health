"""
Fetch current weather from WeatherAPI.com for Tower Health data pipelines.

Usage:
  $env:WEATHERAPI_KEY = "your_api_key_here"
  python weatherapi_client.py --location "Cairo"
  python weatherapi_client.py --lat 30.0663 --lon 31.3409 --output cairo_weather.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


WEATHERAPI_CURRENT_URL = "https://api.weatherapi.com/v1/current.json"
DEFAULT_TIMEOUT_SECONDS = 20
ENV_FILE = Path(".env")


class WeatherApiError(RuntimeError):
    """Raised when WeatherAPI cannot return usable weather data."""


def load_env_file(env_path: Path = ENV_FILE) -> None:
    """Load simple KEY=VALUE lines from a local .env file."""
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def fetch_current_weather(
    api_key: str,
    location: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Fetch current weather data from WeatherAPI.com."""
    query = urlencode(
        {
            "key": api_key,
            "q": location,
            "aqi": "no",
        }
    )
    url = f"{WEATHERAPI_CURRENT_URL}?{query}"

    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise WeatherApiError(f"WeatherAPI HTTP {exc.code}: {details}") from exc
    except URLError as exc:
        raise WeatherApiError(f"Could not connect to WeatherAPI: {exc.reason}") from exc

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise WeatherApiError("WeatherAPI returned invalid JSON") from exc

    if "error" in data:
        message = data["error"].get("message", "unknown WeatherAPI error")
        raise WeatherApiError(message)

    return data


def map_to_tower_weather_fields(data: dict[str, Any]) -> dict[str, Any]:
    """Map WeatherAPI current response to the Tower Health weather schema."""
    current = data.get("current") or {}
    location = data.get("location") or {}
    condition = current.get("condition") or {}

    return {
        "weather_temperature_c": current.get("temp_c"),
        "weather_humidity_pct": current.get("humidity"),
        "weather_rainfall_mm": current.get("precip_mm"),
        "weather_wind_speed_kmh": current.get("wind_kph"),
        "weather_condition": condition.get("text"),
        "weather_observed_at": current.get("last_updated"),
        "weather_fetched_at": datetime.now(timezone.utc).isoformat(),
        "weather_location_name": location.get("name"),
        "weather_region": location.get("region"),
        "weather_country": location.get("country"),
        "weather_latitude": location.get("lat"),
        "weather_longitude": location.get("lon"),
    }


def project_weather_fields(weather: dict[str, Any]) -> dict[str, Any]:
    """Keep only the five weather fields defined in the NetPulse source schema."""
    return {
        "weather_temperature_c": weather.get("weather_temperature_c"),
        "weather_humidity_pct": weather.get("weather_humidity_pct"),
        "weather_rainfall_mm": weather.get("weather_rainfall_mm"),
        "weather_wind_speed_kmh": weather.get("weather_wind_speed_kmh"),
        "weather_condition": weather.get("weather_condition"),
    }


def load_json_file(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WeatherApiError(f"Input message not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise WeatherApiError(f"Input message is not valid JSON: {path}") from exc

    if not isinstance(data, dict):
        raise WeatherApiError("Input message must be a JSON object")

    return data


def enrich_tower_message(message: dict[str, Any], weather: dict[str, Any]) -> dict[str, Any]:
    """Return a tower KPI message with WeatherAPI values inserted."""
    enriched = dict(message)
    enriched.update(project_weather_fields(weather))
    return enriched


def build_location(args: argparse.Namespace) -> str:
    if args.location:
        return args.location

    if args.input_message:
        message = load_json_file(args.input_message)
        lat = message.get("latitude")
        lon = message.get("longitude")
        if lat is None or lon is None:
            raise WeatherApiError(
                "Input message must contain latitude and longitude, or pass --location"
            )
        return f"{lat},{lon}"

    if args.lat is None or args.lon is None:
        raise WeatherApiError("Provide --location, --input-message, or both --lat and --lon")

    return f"{args.lat},{args.lon}"


def write_json(data: dict[str, Any], output_path: Path | None) -> None:
    text = json.dumps(data, indent=2, ensure_ascii=False)

    if output_path is None:
        print(text)
        return

    output_path.write_text(text + "\n", encoding="utf-8")
    print(f"Wrote weather data to {output_path}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    load_env_file()

    parser = argparse.ArgumentParser(
        description="Fetch WeatherAPI.com current weather for Tower Health."
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("WEATHERAPI_KEY"),
        help="WeatherAPI key. Defaults to WEATHERAPI_KEY environment variable.",
    )
    parser.add_argument(
        "--location",
        help='City, area, or WeatherAPI query string, for example "Cairo".',
    )
    parser.add_argument("--lat", type=float, help="Tower latitude.")
    parser.add_argument("--lon", type=float, help="Tower longitude.")
    parser.add_argument(
        "--input-message",
        type=Path,
        help="Existing tower KPI JSON message to enrich using its latitude/longitude.",
    )
    parser.add_argument(
        "--project-fields-only",
        action="store_true",
        help="Output only the five NetPulse weather source fields.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print the full WeatherAPI response instead of mapped project fields.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON file path to write the result.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT_SECONDS}.",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)

    if not args.api_key:
        print(
            "Missing API key. Set WEATHERAPI_KEY or pass --api-key.",
            file=sys.stderr,
        )
        return 2

    try:
        location = build_location(args)
        raw_weather = fetch_current_weather(args.api_key, location, args.timeout)
        mapped_weather = map_to_tower_weather_fields(raw_weather)

        if args.raw:
            output = raw_weather
        elif args.input_message:
            output = enrich_tower_message(load_json_file(args.input_message), mapped_weather)
        elif args.project_fields_only:
            output = project_weather_fields(mapped_weather)
        else:
            output = mapped_weather

        write_json(output, args.output)
    except WeatherApiError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
