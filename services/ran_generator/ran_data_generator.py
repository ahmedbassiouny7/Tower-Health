"""
NetPulse – RAN Telemetry Data Generator
========================================
Uses Faker to emit a fresh JSON snapshot every N seconds.
Static tower identity is generated once at startup; all KPI
metrics are re-generated each tick from realistic value ranges.

Output modes (OUTPUT_MODE env var):
  "stdout"  – pretty-print to console
  "file"    – append each snapshot to OUTPUT_DIR/ran_data_<date>.jsonl
  "both"    – stdout + file
  "kafka"   – publish to Kafka topic
"""

import json
import os
import time
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

# ─────────────────────────── configuration ───────────────────────────────────
INTERVAL_SECONDS        = int(os.getenv("RAN_INTERVAL_SECONDS", "30"))
OUTPUT_MODE             = os.getenv("OUTPUT_MODE", "stdout")
OUTPUT_DIR              = os.getenv("OUTPUT_DIR", "/data")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "ran_telemetry")
KAFKA_RETRY_SECONDS     = 5
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────── value helpers ───────────────────────────────────

def flt(lo, hi, digits=2):
    # Shorthand: generate a random float in [lo, hi] rounded to `digits` decimal places.
    return round(fake.random.uniform(lo, hi), digits)

def up(chance=96):
    """Return 'UP' or 'DOWN' weighted by chance_of_up percent."""
    return "UP" if fake.boolean(chance_of_getting_true=chance) else "DOWN"

def lerp(lo, hi, t, jitter=0.0, digits=2):
    """Interpolate from lo (t=0) to hi (t=1), add optional jitter, clamp, round.

    Supports inverse ranges (lo > hi) — used for metrics that get WORSE as health
    improves (e.g. lerp(8, 0.2, health) for BLER: health=0 → 8%, health=1 → 0.2%).
    The lo_/hi_ swap ensures clamping works correctly regardless of direction.
    """
    val = lo + (hi - lo) * t
    if jitter:
        val += fake.random.uniform(-jitter, jitter)
    # Normalise bounds so clamp always uses (smaller, larger) regardless of direction.
    lo_, hi_ = (lo, hi) if lo <= hi else (hi, lo)
    return round(max(lo_, min(hi_, val)), digits)

def generate_sector_state():
    """Return (health_score, ru_status, cell_status) for one sector.

    Encodes the RU → cell cascade in a single call:
      - RU DOWN  → health forced to 0.0, cell forced to DOWN (no signal, no service)
      - RU UP    → health drawn from [0.1, 1.0]; cell independently has a 5% DOWN chance
                   (e.g. baseband failure while the radio is still on)

    health_score is the master RF quality knob passed to make_antenna and make_cell
    so every RF metric in the sector degrades together rather than varying independently.
    health_score minimum is 0.1 (not 0.0) when UP — even a badly degraded live sector
    has some residual signal.
    """
    ru_status = "UP" if fake.boolean(chance_of_getting_true=96) else "DOWN"
    if ru_status == "DOWN":
        # Radio unit is off — no power, no signal, cell cannot serve users.
        return (0.0, "DOWN", "DOWN")
    health = round(fake.pyfloat(min_value=0.1, max_value=1.0), 3)
    cell_status = "UP" if fake.boolean(chance_of_getting_true=95) else "DOWN"
    return (health, ru_status, cell_status)


# ─────────────────────────── static tower identity ───────────────────────────
# Generated once at process startup so every snapshot shares the same site.

VENDORS = ["Ericsson", "Nokia", "Huawei", "ZTE"]

# Every field in STATIC is evaluated once when the module loads.
# This guarantees that site_id, site_name, lat/lon, vendor, azimuths, and all
# sector/antenna/cell mappings never change between snapshots — only KPI metrics vary.
STATIC = {
    "site_id":   fake.bothify(text="SITE_###"),     # e.g. "SITE_042"
    "site_name": fake.bothify(text="??_TOWER_##").upper(),  # e.g. "KQ_TOWER_17"
    "location":  {
        "latitude":  float(fake.latitude()),
        "longitude": float(fake.longitude()),
    },
    "region":     fake.state(),
    "vendor":     fake.random_element(VENDORS),
    "technology": ["4G", "5G"],

    # Sector ↔ RU mapping: RU_1 serves S1, RU_2 serves S2, RU_3 serves S3.
    # This 1-to-1 binding is static so the cascade (RU DOWN → cell DOWN) is deterministic.
    "ru_sectors": [
        {"ru_id": f"RU_{i}", "sector_id": f"S{i}"} for i in range(1, 4)
    ],

    # Tri-sector antenna layout: 0°, 120°, 240° — evenly divides the 360° cell.
    # Tilt and MIMO layers are physical properties that don't change between ticks.
    "antenna_statics": [
        {
            "antenna_id":     f"ANT_{i}",
            "sector_id":      f"S{i}",
            "tilt_degree":    flt(2.0, 6.0),
            "azimuth_degree": (i - 1) * 120,    # 0° → 120° → 240°
            "mimo_layers":    fake.random_element([2, 4, 8]),
        }
        for i in range(1, 4)
    ],

    # Only two cells are modelled (S1=4G, S2=5G); S3 has a radio unit but no cell.
    # Bandwidth is a licensed spectrum property — fixed per technology.
    "cell_statics": [
        {"cell_id": "CELL_1", "sector_id": "S1", "technology": "4G",
         "carrier_frequency_mhz": 1800, "bandwidth_mhz": 20},
        {"cell_id": "CELL_2", "sector_id": "S2", "technology": "5G",
         "carrier_frequency_mhz": 3500, "bandwidth_mhz": 100},
    ],

    # Two backhaul links with different media — both connect the site to the core.
    "link_statics": [
        {"link_id": "BH_1", "type": "FIBER"},
        {"link_id": "BH_2", "type": "MICROWAVE"},
    ],
}


# ─────────────────────────── per-tick builders ───────────────────────────────

def make_ru(ru_id, sector_id, ru_status):
    """Build one Radio Unit record.

    ru_status comes from generate_sector_state(), not rolled here, so the
    RU → cell cascade stays consistent across the whole snapshot.
    All metrics are None when DOWN — a powered-off unit reports nothing.
    """
    s = ru_status
    return {
        "ru_id":                    ru_id,
        "sector_id":                sector_id,
        "status":                   s,
        "temperature_c":            flt(30, 70)    if s == "UP" else None,
        "tx_power_watts":           flt(20, 200)   if s == "UP" else None,
        "rx_signal_strength_dbm":   flt(-100, -50) if s == "UP" else None,
        "vswr":                     flt(1.0, 2.0)  if s == "UP" else None,  # ideal VSWR is 1.0; >2.0 indicates antenna mismatch
        "current_ampere":           flt(5, 30)     if s == "UP" else None,
        "voltage_volt":             flt(46, 50)    if s == "UP" else None,
        # flt(0,5)/100 scales the range to 0.000–0.050 (0%–5% packet error rate)
        "packet_error_rate":        round(flt(0, 5) / 100, 5) if s == "UP" else None,
        "throughput_mbps":          flt(0, 600)    if s == "UP" else None,
    }


def make_bbu(bbu_id, cell_active_users):
    """Build one Baseband Unit record.

    BBU_1 is paired with CELL_1 (S1) and BBU_2 with CELL_2 (S2).
    active_users is derived from the paired cell's user count so the two
    stay in sync — the BBU processes exactly the traffic its cell carries.
    A small noise term (+/-15) accounts for control-plane sessions and
    users mid-handover that the cell hasn't counted yet.
    """
    s = up(97)
    # `cell_active_users or 0` guards against None: a DOWN cell returns
    # active_users=0, but we use `or 0` defensively in case it is None.
    bbu_users = max(0, (cell_active_users or 0) + fake.pyint(min_value=-5, max_value=15)) if s == "UP" else None
    return {
        "bbu_id":                       bbu_id,
        "status":                       s,
        "cpu_utilization_percent":      flt(10, 100) if s == "UP" else None,
        "memory_utilization_percent":   flt(10, 100) if s == "UP" else None,
        "disk_usage_percent":           flt(10, 95)  if s == "UP" else None,
        "process_latency_ms":           flt(5, 50)   if s == "UP" else None,
        "active_users":                 bbu_users,
        "control_plane_latency_ms":     flt(5, 30)   if s == "UP" else None,
        "user_plane_latency_ms":        flt(5, 40)   if s == "UP" else None,
    }


def make_antenna(meta, health):
    """Build one antenna record.

    Antennas are passive elements — tilt, azimuth, and MIMO config never
    change (they come from STATIC). Only the live RF readings vary per tick.
    Both rssi and snr are driven by the same sector health score so they
    move together: a weak sector shows low rssi AND low snr, not one without
    the other.
      health=0.0 → rssi=-100 dBm, snr=0 dB   (very poor signal)
      health=1.0 → rssi=-50  dBm, snr=35 dB  (excellent signal)
    """
    return {
        "antenna_id":     meta["antenna_id"],
        "sector_id":      meta["sector_id"],
        "tilt_degree":    meta["tilt_degree"],
        "azimuth_degree": meta["azimuth_degree"],
        "mimo_layers":    meta["mimo_layers"],
        "rssi_dbm":       lerp(-100, -50, health, jitter=2),    # stronger signal as health rises
        "snr_db":         lerp(0, 35, health, jitter=1.5),      # better noise margin as health rises
    }


def make_cell(meta, health, cell_status):
    """Build one cell record.

    cell_status and health both come from generate_sector_state() so the
    RU → cell cascade is already applied before this function is called.

    DOWN branch:
      active_users and throughput are 0 (not None) — their value is known
      (zero traffic), whereas signal metrics like rsrp are genuinely
      unmeasurable on a dead cell and are therefore None.
      Count-like fields (handover_attempts, rrc_connection_attempts) are
      also 0 so handover_failures ≤ handover_attempts holds trivially.

    UP branch — all RF metrics derived from the single health score:
      Good signal chain: high health → strong rsrp → better rsrq → higher
      sinr → higher cqi → lower BLER → fewer HARQ retransmissions → higher
      throughput.  Using lerp() for each keeps them correlated.
      Inverse metrics (BLER, HARQ) use lo > hi in lerp() so they increase
      as health falls — see lerp() docstring for how clamping handles this.

    Latency bounds are technology-specific (3GPP targets):
      5G NR : 5–20 ms    4G LTE: 10–40 ms
    """
    is_5g  = meta["technology"] == "5G"
    dl_max = 600 if is_5g else 300   # max downlink throughput (Mbps)
    lat_lo = 5   if is_5g else 10    # latency lower bound (ms)
    lat_hi = 20  if is_5g else 40    # latency upper bound (ms)

    base = {
        "cell_id":               meta["cell_id"],
        "sector_id":             meta["sector_id"],
        "technology":            meta["technology"],
        "carrier_frequency_mhz": meta["carrier_frequency_mhz"],
        "bandwidth_mhz":         meta["bandwidth_mhz"],
        "status":                cell_status,
    }

    if cell_status == "DOWN":
        return {
            **base,
            # Known-zero: no users, no traffic when the cell is out of service.
            "active_users":                     0,
            "connected_users":                  0,   # always ≥ active_users (both 0)
            "prb_utilization_percent":          None,
            "throughput_downlink_mbps":         0,
            "throughput_uplink_mbps":           0,
            # Signal metrics are None — the cell is not transmitting so they cannot be measured.
            "spectral_efficiency_bps_per_hz":   None,
            "rsrp_dbm":                         None,
            "rsrq_db":                          None,
            "sinr_db":                          None,
            "cqi_avg":                          None,
            "bler_downlink_percent":            None,
            "bler_uplink_percent":              None,
            "harq_retransmission_rate_percent": None,
            "latency_downlink_ms":              None,
            "latency_uplink_ms":                None,
            # Zero attempts means zero failures — satisfies handover_failures ≤ attempts.
            "handover_attempts":                0,
            "handover_success_rate_percent":    None,
            "handover_failures":                0,
            "rrc_connection_attempts":          0,
            "rrc_success_rate_percent":         None,
            "erab_setup_success_rate_percent":  None,
            "call_drop_rate_percent":           None,
            "abnormal_release_rate_percent":    None,
        }

    # ── UP: generate correlated metrics from the sector health score ──────────

    # User count scales with health: a weak sector serves fewer users.
    # jitter=50 adds realistic tick-to-tick variation around the health-derived mean.
    active = int(lerp(0, 600, health, jitter=50))

    # Handover volume roughly tracks user count (more users → more mobility events).
    attempts = fake.pyint(min_value=max(0, active - 20), max_value=active + 100)

    # Handover success rate improves with health.
    ho_rate = lerp(85, 99.5, health, jitter=0.5)

    return {
        **base,
        "active_users":   active,
        # connected_users adds 0–30 on top of active — accounts for sessions in
        # setup/teardown (TCP handshakes, RRC idle-connected transitions).
        # This guarantees connected_users ≥ active_users by construction.
        "connected_users":                  active + fake.pyint(min_value=0, max_value=30),
        "prb_utilization_percent":          flt(10, 100),   # load is independent of signal quality
        "throughput_downlink_mbps":         lerp(0, dl_max, health, jitter=dl_max * 0.05),
        "throughput_uplink_mbps":           lerp(0, 150, health, jitter=10),
        "spectral_efficiency_bps_per_hz":   lerp(0.5, 7.5, health, jitter=0.2),
        # Signal quality chain — all degrade together as health falls.
        "rsrp_dbm":                         lerp(-120, -70, health, jitter=3),    # reference signal received power
        "rsrq_db":                          lerp(-15, -5, health, jitter=0.5),    # reference signal quality
        "sinr_db":                          lerp(-5, 25, health, jitter=2),       # signal-to-interference ratio
        "cqi_avg":                          lerp(2, 14, health, jitter=0.5, digits=1),  # channel quality index (1–15 scale)
        # Inverse metrics: lo > hi so low health produces HIGH error rates.
        "bler_downlink_percent":            lerp(8, 0.2, health, jitter=0.3),    # block error rate
        "bler_uplink_percent":              lerp(8, 0.2, health, jitter=0.3),
        "harq_retransmission_rate_percent": lerp(18, 0.5, health, jitter=0.8),   # retransmissions needed due to errors
        # Latency is bounded by technology spec, not health (scheduler/propagation driven).
        "latency_downlink_ms":              flt(lat_lo, lat_hi),
        "latency_uplink_ms":                flt(lat_lo, lat_hi),
        "handover_attempts":                attempts,
        "handover_success_rate_percent":    ho_rate,
        # Arithmetic guarantee: failures = attempts × failure_rate ≤ attempts always.
        "handover_failures":                max(0, int(attempts * (1 - ho_rate / 100))),
        "rrc_connection_attempts":          fake.pyint(min_value=0, max_value=5000),
        "rrc_success_rate_percent":         lerp(85, 99, health, jitter=0.5),
        "erab_setup_success_rate_percent":  lerp(85, 99, health, jitter=0.5),
        "call_drop_rate_percent":           lerp(5, 0.1, health, jitter=0.2),    # inverse: fewer drops when healthy
        "abnormal_release_rate_percent":    lerp(5, 0.1, health, jitter=0.2),    # inverse
    }


def make_link(meta):
    s = up(99)
    return {
        "link_id":             meta["link_id"],
        "type":                meta["type"],
        "status":              s,
        "latency_ms":          flt(1, 20)      if s == "UP" else None,
        "jitter_ms":           flt(0, 5)       if s == "UP" else None,
        "packet_loss_percent": round(flt(0, 1, digits=4), 4) if s == "UP" else None,
        "throughput_mbps":     flt(50, 10000)  if s == "UP" else None,
        "utilization_percent": flt(5, 100)     if s == "UP" else None,
    }


def make_rectifier(rec_id):
    s = up(99)
    return {
        "rectifier_id":        rec_id,
        "status":              s,
        "output_voltage_volt": flt(46, 50) if s == "UP" else None,
        "current_ampere":      flt(5, 50)  if s == "UP" else None,
    }


def make_battery(bat_id):
    s = up(99)
    return {
        "battery_id":     bat_id,
        "status":         s,
        "charge_percent": flt(0, 100, digits=1) if s == "UP" else None,
        "temperature_c":  flt(15, 45)           if s == "UP" else None,
    }


# ─────────────────────────── snapshot ────────────────────────────────────────

def build_snapshot():
    """Assemble one complete tower snapshot.

    The build order matters — later steps depend on earlier results:
      1. Sector states first  → RU and cell statuses are decided once and shared.
      2. Cells before BBUs    → BBU user count is derived from cell output.
      3. Batteries before generator → generator ON/OFF depends on charge level.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── 1. Sector states ──────────────────────────────────────────────────────
    # One call per sector; the tuple encodes the full cascade result:
    #   index 0 → health score (float 0.0–1.0)
    #   index 1 → ru_status   ("UP" | "DOWN")
    #   index 2 → cell_status ("UP" | "DOWN")  forced DOWN if ru is DOWN
    sector_states = {
        m["sector_id"]: generate_sector_state()
        for m in STATIC["ru_sectors"]
    }

    # ── 2. Radio units ────────────────────────────────────────────────────────
    # Status is read from sector_states[1], not rolled independently, so the
    # RU and its downstream cell always agree on whether the sector is up.
    radio_units = [
        make_ru(m["ru_id"], m["sector_id"], sector_states[m["sector_id"]][1])
        for m in STATIC["ru_sectors"]
    ]

    # ── 3. Antennas ───────────────────────────────────────────────────────────
    # Health [0] drives rssi/snr so antenna RF readings track sector quality.
    antennas = [
        make_antenna(m, sector_states[m["sector_id"]][0])
        for m in STATIC["antenna_statics"]
    ]

    # ── 4. Cells ──────────────────────────────────────────────────────────────
    # Health [0] drives all RF metrics; cell_status [2] enforces the cascade.
    cells = [
        make_cell(
            m,
            health=sector_states[m["sector_id"]][0],
            cell_status=sector_states[m["sector_id"]][2],
        )
        for m in STATIC["cell_statics"]
    ]

    # ── 5. BBUs ───────────────────────────────────────────────────────────────
    # Cells must be built first so we can read their active_users here.
    # BBU_1 serves S1 (CELL_1 / 4G), BBU_2 serves S2 (CELL_2 / 5G).
    cell_users = {c["sector_id"]: c["active_users"] for c in cells}
    baseband_units = [
        make_bbu("BBU_1", cell_users.get("S1")),
        make_bbu("BBU_2", cell_users.get("S2")),
    ]

    # ── 6. Power system ───────────────────────────────────────────────────────
    rectifiers = [make_rectifier(f"REC_{i}") for i in range(1, 3)]
    batteries  = [make_battery(f"BAT_{i}")   for i in range(1, 3)]

    # Site has mains power if at least one rectifier is operational.
    power_up = any(r["status"] == "UP" for r in rectifiers)

    # Check whether any live battery is critically low (< 20 % charge).
    # `or 100` guards against charge_percent being None on a DOWN battery —
    # defaulting to 100 % means a DOWN battery never triggers the alarm.
    low_bat = any(
        (b["charge_percent"] or 100) < 20
        for b in batteries
        if b["status"] == "UP"
    )

    # Generator turns ON automatically on power failure or low battery.
    # The 3 % random chance simulates routine maintenance test runs.
    gen_on = (not power_up) or low_bat or fake.boolean(chance_of_getting_true=3)

    return {
        "timestamp": ts,
        "ran_metadata": {
            "site_id":    STATIC["site_id"],
            "site_name":  STATIC["site_name"],
            "location":   STATIC["location"],
            "region":     STATIC["region"],
            "vendor":     STATIC["vendor"],
            "technology": STATIC["technology"],
        },
        "radio_units":     radio_units,
        "baseband_units":  baseband_units,
        "antennas":        antennas,
        "cells":           cells,
        "transport_links": [make_link(m) for m in STATIC["link_statics"]],
        "power_system": {
            "status":     "UP" if power_up else "DOWN",
            "rectifiers": rectifiers,
            "batteries":  batteries,
            "generator": {
                "status":             "ON" if gen_on else "OFF",
                "fuel_level_percent": flt(10, 100, digits=1),
                "runtime_hours":      flt(0, 500),
            },
        },
        "environment": {
            "status": "UP",
            "temperature_sensors": [
                {"sensor_id": f"TEMP_{i}", "value_c": flt(20, 50, digits=1)}
                for i in range(1, 3)
            ],
            "humidity_sensors": [
                {"sensor_id": "HUM_1", "value_percent": flt(30, 90, digits=1)}
            ],
            "door_status":    "OPEN" if fake.boolean(chance_of_getting_true=1) else "CLOSED",
            "smoke_detected": fake.boolean(chance_of_getting_true=1),
        },
    }


# ─────────────────────────── output helpers ──────────────────────────────────

def to_stdout(snapshot):
    print("\n" + "=" * 70)
    print(f"  NetPulse Snapshot  |  {snapshot['timestamp']}")
    print("=" * 70)
    print(json.dumps(snapshot, indent=2, ensure_ascii=False), flush=True)


def to_file(snapshot):
    from datetime import datetime as _dt
    date_str = _dt.now().strftime("%Y-%m-%d")
    filename = os.path.join(OUTPUT_DIR, f"ran_data_{date_str}.jsonl")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(filename, "a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
    print(f"[{snapshot['timestamp']}] Written -> {filename}", flush=True)


def create_kafka_producer():
    """Create and return a KafkaProducer, retrying until the broker is reachable.

    Kafka may not be ready when this container starts (Docker startup ordering).
    Retrying in a loop is safer than crashing — the generator will begin
    emitting data as soon as the broker comes up, without needing a restart.

    linger_ms=100 tells the producer to wait up to 100 ms before sending a
    batch, which reduces network round-trips when messages arrive quickly.
    retries=5 handles transient broker leadership changes automatically.

    Kafka imports are deferred to here so the module loads cleanly when
    OUTPUT_MODE is not "kafka" and kafka-python is irrelevant.
    """
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                retries=5,
                linger_ms=100,
            )
        except NoBrokersAvailable:
            print(
                f"Kafka not ready at {KAFKA_BOOTSTRAP_SERVERS}; "
                f"retrying in {KAFKA_RETRY_SECONDS}s...",
                flush=True,
            )
            time.sleep(KAFKA_RETRY_SECONDS)


def to_kafka(producer, snapshot):
    """Publish one snapshot to Kafka and block until the broker acknowledges it.

    site_id is used as the message key so all records from the same tower
    land on the same partition — preserving per-site ordering for consumers.

    .get(timeout=30) waits for the broker ack before returning, turning the
    async send into a synchronous call and ensuring no message is silently
    dropped.  flush() drains any remaining buffered messages afterward.
    """
    key      = snapshot["ran_metadata"]["site_id"]
    metadata = producer.send(KAFKA_TOPIC, key=key, value=snapshot).get(timeout=30)
    producer.flush()
    print(
        f"[{snapshot['timestamp']}] Kafka "
        f"topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}",
        flush=True,
    )


# ─────────────────────────── main ────────────────────────────────────────────

def main():
    print("=" * 66)
    print("  NetPulse  -  RAN Telemetry Generator  (Faker)")
    print(f"  Site     : {STATIC['site_name']}  ({STATIC['site_id']})")
    print(f"  Vendor   : {STATIC['vendor']}  |  Region: {STATIC['region']}")
    print(f"  Interval : {INTERVAL_SECONDS}s  |  Output : {OUTPUT_MODE}")
    if OUTPUT_MODE == "kafka":
        print(f"  Kafka    : {KAFKA_BOOTSTRAP_SERVERS}  topic={KAFKA_TOPIC}")
    print("=" * 66)
    print("Press Ctrl+C to stop.\n")

    producer = None
    if OUTPUT_MODE == "kafka":
        producer = create_kafka_producer()

    while True:
        snapshot = build_snapshot()

        if OUTPUT_MODE in ("stdout", "both"):
            to_stdout(snapshot)
        if OUTPUT_MODE in ("file", "both"):
            to_file(snapshot)
        if OUTPUT_MODE == "kafka" and producer:
            to_kafka(producer, snapshot)

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nGenerator stopped.")
