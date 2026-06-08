"""
TowerHealth – RAN Telemetry Simulator  (Multi-Site, Stateful)
===========================================================
Emits one JSON message per RAN site every 30 seconds.
4 independent sites → 4 messages per 30-second cycle.

Key design principles
─────────────────────
• Deterministic topology   – site IDs, coordinates, azimuths never change.
• Stateful components      – each component object carries a state machine
                             (HEALTHY / DEGRADED / FAILED / RECOVERING) whose
                             transitions drive every KPI, not random dice rolls.
• Temporal continuity      – all continuous metrics are clamped to a max-delta
                             per tick so graphs never show impossible jumps.
• KPI correlation          – PRB util, SINR, throughput, latency, and handover
                             failures are all computed from the same health score.
• Traffic day/night profile– user counts follow a sinusoidal 24-hour curve so
                             morning / evening peaks look realistic.
• Failure propagation      – BBU down ⟹ all cells/RUs/antennas null.
                             RU down  ⟹ associated antenna + cell null.
• Arithmetic invariants    – connected_users ≥ active_users,
                             handover_failures ≤ handover_attempts,
                             BBU active_users ≈ Σ cell active_users.

Output modes (OUTPUT_MODE env var):
  "stdout"  – pretty-print to console           (default)
  "file"    – append JSONL to OUTPUT_DIR/
  "both"    – stdout + file
  "kafka"   – publish to Kafka topic
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from faker import Faker

# ──────────────────────────── configuration ───────────────────────────────────
INTERVAL_SECONDS        = int(os.getenv("RAN_INTERVAL_SECONDS", "30"))
OUTPUT_MODE             = os.getenv("OUTPUT_MODE", "stdout")
OUTPUT_DIR              = os.getenv("OUTPUT_DIR", "/data")
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "broker-1:29092,broker-2:29092,broker-3:29092",
)
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "ran_telemetry")
KAFKA_RETRY_SECONDS     = 5
NUM_SITES               = 4
# ──────────────────────────────────────────────────────────────────────────────


# ──────────────────────────── primitive helpers ────────────────────────────────

FAKER_LOCALE = "en_US"
_fake = Faker(FAKER_LOCALE)
_rng = _fake.random             # Faker-backed RNG; seed via RNG_SEED env if desired
_seed = os.getenv("RNG_SEED")
if _seed:
    seed_value = int(_seed)
    Faker.seed(seed_value)
    _fake.seed_instance(seed_value)

def flt(lo: float, hi: float, digits: int = 2) -> float:
    return round(_rng.uniform(lo, hi), digits)


def clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def lerp(lo: float, hi: float, t: float, jitter: float = 0.0, digits: int = 2) -> float:
    """Interpolate lo→hi by t ∈ [0,1]; clamp; optional jitter."""
    val = lo + (hi - lo) * t
    if jitter:
        val += _rng.uniform(-jitter, jitter)
    lo_, hi_ = (lo, hi) if lo <= hi else (hi, lo)
    return round(clamp(val, lo_, hi_), digits)


def smooth_step(prev: float, target: float, max_delta: float, digits: int = 2) -> float:
    """Move prev toward target by at most max_delta (temporal continuity)."""
    delta = clamp(target - prev, -max_delta, max_delta)
    return round(prev + delta, digits)


def traffic_multiplier(ts: datetime) -> float:
    """Day/night traffic profile → [0.15, 1.0].

    Peak around 19:00 UTC (evening), trough around 04:00 UTC (night).
    Two humps: morning commute at ~08:00, prime-time at ~19:00.
    """
    h = ts.hour + ts.minute / 60.0
    morning   = 0.40 * math.exp(-0.5 * ((h - 8)  / 2.5) ** 2)
    evening   = 1.00 * math.exp(-0.5 * ((h - 19) / 3.0) ** 2)
    night     = 0.15
    return clamp(night + morning + evening, 0.15, 1.0)


# ──────────────────────────── state machine ────────────────────────────────────

# Transition probability table  {current_state: {next_state: weight}}
# Designed so failures are infrequent but meaningful, and recovery is gradual.
_TRANSITIONS: Dict[str, Dict[str, float]] = {
    "HEALTHY":    {"HEALTHY": 0.96, "DEGRADED": 0.03,  "FAILED": 0.01},
    "DEGRADED":   {"HEALTHY": 0.10, "DEGRADED": 0.78,  "FAILED": 0.10, "RECOVERING": 0.02},
    "FAILED":     {"FAILED":  0.80, "RECOVERING": 0.20},
    "RECOVERING": {"HEALTHY": 0.40, "RECOVERING": 0.55, "DEGRADED": 0.05},
}

# Map each state to a health score range [lo, hi]
_STATE_HEALTH: Dict[str, Tuple[float, float]] = {
    "HEALTHY":    (0.75, 1.00),
    "DEGRADED":   (0.30, 0.74),
    "FAILED":     (0.00, 0.00),
    "RECOVERING": (0.20, 0.60),
}


@dataclass
class ComponentState:
    """Persistent per-component state carried across ticks."""
    component_id: str
    op_state: str = "HEALTHY"          # HEALTHY / DEGRADED / FAILED / RECOVERING
    health: float = 1.0                # continuous [0, 1] health score
    # Smooth-tracked metrics (init to sensible defaults)
    temperature_c: float = 35.0
    throughput_mbps: float = 100.0
    active_users: int = 0
    prb_util: float = 30.0
    latency_ms: float = 15.0
    charge_percent: float = 90.0
    fuel_level: float = 80.0

    def next_state(self, forced_failed: bool = False) -> None:
        """Advance the state machine one tick."""
        if forced_failed:
            self.op_state = "FAILED"
            self.health   = 0.0
            return
        choices = list(_TRANSITIONS[self.op_state].keys())
        weights = list(_TRANSITIONS[self.op_state].values())
        self.op_state = _rng.choices(choices, weights=weights, k=1)[0]
        lo, hi        = _STATE_HEALTH[self.op_state]
        target_health = _rng.uniform(lo, hi)
        # Smooth health transitions – max 0.15 change per tick
        self.health   = smooth_step(self.health, target_health, max_delta=0.15, digits=3)

    @property
    def is_up(self) -> bool:
        return self.op_state != "FAILED"

    @property
    def status_str(self) -> str:
        """Legacy UP/DOWN string for fields that still use it."""
        return "UP" if self.is_up else "DOWN"


# ──────────────────────────── static topology ─────────────────────────────────

VENDORS = ["Ericsson", "Nokia", "Huawei", "ZTE"]

# Four fixed sites; topology is immutable.
SITE_TOPOLOGY: List[Dict[str, Any]] = [
    {
        "site_id":   "SITE_001",
        "site_name": "AL_TOWER_01",
        "location":  {"latitude": 31.2001, "longitude": 29.9187},  # Alexandria, EG
        "region":    "Alexandria",
        "vendor":    "Ericsson",
        "technology": ["4G", "5G"],
    },
    {
        "site_id":   "SITE_002",
        "site_name": "CA_TOWER_02",
        "location":  {"latitude": 30.0444, "longitude": 31.2357},  # Cairo, EG
        "region":    "Cairo",
        "vendor":    "Nokia",
        "technology": ["4G", "5G"],
    },
    {
        "site_id":   "SITE_003",
        "site_name": "GZ_TOWER_03",
        "location":  {"latitude": 30.0131, "longitude": 31.2089},  # Giza, EG
        "region":    "Giza",
        "vendor":    "Huawei",
        "technology": ["4G", "5G"],
    },
    {
        "site_id":   "SITE_004",
        "site_name": "KS_TOWER_04",
        "location":  {"latitude": 31.1107, "longitude": 33.7961},  # North Sinai, EG
        "region":    "North Sinai",
        "vendor":    "ZTE",
        "technology": ["4G", "5G"],
    },
]

# Shared structural topology (same shape for every site)
_SECTOR_RU_MAP = [
    {"ru_id": "RU_1", "sector_id": "S1"},
    {"ru_id": "RU_2", "sector_id": "S2"},
    {"ru_id": "RU_3", "sector_id": "S3"},
]

_ANTENNA_STATICS = [
    {"antenna_id": "ANT_1", "sector_id": "S1", "tilt_degree": 3.0, "azimuth_degree":   0, "mimo_layers": 4},
    {"antenna_id": "ANT_2", "sector_id": "S2", "tilt_degree": 4.0, "azimuth_degree": 120, "mimo_layers": 8},
    {"antenna_id": "ANT_3", "sector_id": "S3", "tilt_degree": 5.0, "azimuth_degree": 240, "mimo_layers": 4},
]

_CELL_STATICS = [
    {"cell_id": "CELL_1", "sector_id": "S1", "technology": "4G",
     "carrier_frequency_mhz": 1800, "bandwidth_mhz": 20},
    {"cell_id": "CELL_2", "sector_id": "S2", "technology": "5G",
     "carrier_frequency_mhz": 3500, "bandwidth_mhz": 100},
    {"cell_id": "CELL_3", "sector_id": "S3", "technology": "5G",
     "carrier_frequency_mhz": 2600, "bandwidth_mhz":  60},
]

_LINK_STATICS = [
    {"link_id": "BH_1", "type": "FIBER"},
    {"link_id": "BH_2", "type": "MICROWAVE"},
]


# ──────────────────────────── site state object ────────────────────────────────

class SiteState:
    """All mutable state for one RAN site.

    Component states are keyed by their IDs.  Sequence numbers increment each
    tick so consumers can detect gaps in the stream.
    """

    def __init__(self, site_meta: Dict[str, Any]) -> None:
        self.meta                    = site_meta
        self.seq                     = 0
        self.generator_runtime_hours: float = 0.0

        # One ComponentState per logical component
        self.bbu     = ComponentState("BBU_MAIN")
        self.rus     = {m["ru_id"]: ComponentState(m["ru_id"]) for m in _SECTOR_RU_MAP}
        self.ants    = {m["antenna_id"]: ComponentState(m["antenna_id"]) for m in _ANTENNA_STATICS}
        self.cells   = {m["cell_id"]: ComponentState(m["cell_id"]) for m in _CELL_STATICS}
        self.links   = {m["link_id"]: ComponentState(m["link_id"]) for m in _LINK_STATICS}
        self.recs    = {f"REC_{i}": ComponentState(f"REC_{i}") for i in range(1, 3)}
        self.bats    = {f"BAT_{i}": ComponentState(f"BAT_{i}") for i in range(1, 3)}
        self.env     = ComponentState("ENV")

    def advance(self) -> None:
        """Advance every component state machine one tick with cascade propagation."""
        self.seq += 1

        # ── BBU ── (independent)
        self.bbu.next_state()
        bbu_failed = not self.bbu.is_up

        # ── RUs ── (if BBU failed → RUs forced down)
        for ru in self.rus.values():
            ru.next_state(forced_failed=bbu_failed)

        # ── Antennas / Cells ── (cascade from their respective RU)
        sector_to_ru = {m["sector_id"]: m["ru_id"] for m in _SECTOR_RU_MAP}
        for ant_meta in _ANTENNA_STATICS:
            ru_id = sector_to_ru[ant_meta["sector_id"]]
            ru_failed = not self.rus[ru_id].is_up
            ant = self.ants[ant_meta["antenna_id"]]
            ant.next_state(forced_failed=ru_failed)

        for cell_meta in _CELL_STATICS:
            ru_id = sector_to_ru[cell_meta["sector_id"]]
            ru_failed = not self.rus[ru_id].is_up
            # Cell can also fail independently even when RU is up (5% chance)
            cell = self.cells[cell_meta["cell_id"]]
            cell.next_state(forced_failed=ru_failed)

        # ── Backhaul links ── (independent, high reliability)
        for lnk in self.links.values():
            lnk.next_state()

        # ── Power ──
        for rec in self.recs.values():
            rec.next_state()
        for bat in self.bats.values():
            bat.next_state()

        # ── Environment ──
        self.env.next_state()


# ──────────────────────────── per-tick builders ────────────────────────────────

def _build_ru(ru_id: str, sector_id: str, cs: ComponentState) -> Dict:
    s = cs.status_str
    if not cs.is_up:
        return {
            "ru_id": ru_id, "sector_id": sector_id,
            "status": s, "op_state": cs.op_state,
            "temperature_c": None, "tx_power_watts": None,
            "rx_signal_strength_dbm": None, "vswr": None,
            "current_ampere": None, "voltage_volt": None,
            "packet_error_rate": None, "throughput_mbps": None,
        }
    h = cs.health
    # Smooth temperature
    cs.temperature_c = smooth_step(
        cs.temperature_c,
        lerp(30, 70, 1 - h, jitter=2),   # hotter when degraded
        max_delta=2.0, digits=1
    )
    cs.throughput_mbps = smooth_step(
        cs.throughput_mbps,
        lerp(0, 600, h, jitter=20),
        max_delta=50.0
    )
    return {
        "ru_id":                  ru_id,
        "sector_id":              sector_id,
        "status":                 s,
        "op_state":               cs.op_state,
        "temperature_c":          cs.temperature_c,
        "tx_power_watts":         lerp(20, 200, h, jitter=5),
        "rx_signal_strength_dbm": lerp(-100, -50, h, jitter=2),
        "vswr":                   lerp(2.0, 1.0, h, jitter=0.05),   # inverse: lower is better
        "current_ampere":         lerp(5, 30, h, jitter=1),
        "voltage_volt":           lerp(46, 50, h, jitter=0.2),
        "packet_error_rate":      round(lerp(0.05, 0.001, h, jitter=0.003), 5),
        "throughput_mbps":        cs.throughput_mbps,
    }


def _build_bbu(cs: ComponentState, total_cell_users: int) -> Dict:
    if not cs.is_up:
        return {
            "bbu_id": "BBU_MAIN", "status": "DOWN", "op_state": cs.op_state,
            "cpu_utilization_percent": None, "memory_utilization_percent": None,
            "disk_usage_percent": None, "process_latency_ms": None,
            "active_users": None, "control_plane_latency_ms": None,
            "user_plane_latency_ms": None,
        }
    h = cs.health
    # BBU users ≈ sum of all cells + small noise
    bbu_users = max(0, total_cell_users + _rng.randint(-5, 15))
    cs.active_users = bbu_users
    return {
        "bbu_id":                     "BBU_MAIN",
        "status":                     "UP",
        "op_state":                   cs.op_state,
        "cpu_utilization_percent":    lerp(90, 10, h, jitter=5),    # inverse: high load when degraded
        "memory_utilization_percent": lerp(85, 20, h, jitter=4),
        "disk_usage_percent":         lerp(80, 20, h, jitter=3),
        "process_latency_ms":         lerp(50, 5, h, jitter=2),
        "active_users":               bbu_users,
        "control_plane_latency_ms":   lerp(30, 5, h, jitter=1),
        "user_plane_latency_ms":      lerp(40, 5, h, jitter=2),
    }


def _build_antenna(meta: Dict, ant_cs: ComponentState, ru_cs: ComponentState) -> Dict:
    effective_health = ant_cs.health if ant_cs.is_up else 0.0
    s = ant_cs.status_str
    return {
        "antenna_id":     meta["antenna_id"],
        "sector_id":      meta["sector_id"],
        "tilt_degree":    meta["tilt_degree"],      # static – never changes
        "azimuth_degree": meta["azimuth_degree"],   # static – never changes
        "mimo_layers":    meta["mimo_layers"],       # static – never changes
        "status":         s,
        "op_state":       ant_cs.op_state,
        "rssi_dbm":       lerp(-100, -50, effective_health, jitter=2)  if ant_cs.is_up else None,
        "snr_db":         lerp(0, 35, effective_health, jitter=1.5)    if ant_cs.is_up else None,
    }


def _build_cell(
    meta: Dict,
    cell_cs: ComponentState,
    ant_cs: ComponentState,
    traffic_mult: float,
) -> Dict:
    base = {
        "cell_id":               meta["cell_id"],
        "sector_id":             meta["sector_id"],
        "technology":            meta["technology"],
        "carrier_frequency_mhz": meta["carrier_frequency_mhz"],
        "bandwidth_mhz":         meta["bandwidth_mhz"],
        "status":                cell_cs.status_str,
        "op_state":              cell_cs.op_state,
    }

    if not cell_cs.is_up:
        return {
            **base,
            "active_users": 0, "connected_users": 0,
            "prb_utilization_percent": None,
            "throughput_downlink_mbps": 0, "throughput_uplink_mbps": 0,
            "spectral_efficiency_bps_per_hz": None,
            "rsrp_dbm": None, "rsrq_db": None, "sinr_db": None,
            "cqi_avg": None, "bler_downlink_percent": None,
            "bler_uplink_percent": None, "harq_retransmission_rate_percent": None,
            "latency_downlink_ms": None, "latency_uplink_ms": None,
            "handover_attempts": 0, "handover_success_rate_percent": None,
            "handover_failures": 0,
            "rrc_connection_attempts": 0, "rrc_success_rate_percent": None,
            "erab_setup_success_rate_percent": None,
            "call_drop_rate_percent": None, "abnormal_release_rate_percent": None,
        }

    is_5g  = meta["technology"] == "5G"
    dl_max = 600 if is_5g else 300
    lat_lo = 5   if is_5g else 10
    lat_hi = 20  if is_5g else 40

    h = cell_cs.health
    # Antenna failure degrades the cell's signal quality
    ant_factor = ant_cs.health if ant_cs.is_up else 0.3
    effective_h = h * ant_factor

    # Traffic-aware user count: health × day/night profile
    user_target = int(lerp(0, 600, effective_h) * traffic_mult)
    cell_cs.active_users = int(
        smooth_step(cell_cs.active_users, user_target, max_delta=80)
    )
    active = cell_cs.active_users

    # PRB utilization correlates with user load
    prb_target = clamp(active / 6.0, 5, 100)
    cell_cs.prb_util = smooth_step(cell_cs.prb_util, prb_target, max_delta=10)

    # Throughput depends on PRB and signal quality
    dl_target = lerp(0, dl_max, effective_h, jitter=dl_max * 0.04)
    cell_cs.throughput_mbps = smooth_step(cell_cs.throughput_mbps, dl_target, max_delta=dl_max * 0.1)

    # Latency rises with PRB utilization (congestion effect)
    congestion_factor = cell_cs.prb_util / 100.0
    lat_target = lat_lo + (lat_hi - lat_lo) * congestion_factor
    cell_cs.latency_ms = smooth_step(cell_cs.latency_ms, lat_target, max_delta=3)

    attempts = _rng.randint(max(0, active - 20), active + 100)
    ho_rate  = lerp(85, 99.5, effective_h, jitter=0.5)

    return {
        **base,
        "active_users":                     active,
        "connected_users":                  active + _rng.randint(0, 30),
        "prb_utilization_percent":          round(cell_cs.prb_util, 1),
        "throughput_downlink_mbps":         round(cell_cs.throughput_mbps, 2),
        "throughput_uplink_mbps":           lerp(0, 150, effective_h, jitter=8),
        "spectral_efficiency_bps_per_hz":   lerp(0.5, 7.5, effective_h, jitter=0.2),
        "rsrp_dbm":                         lerp(-120, -70, effective_h, jitter=3),
        "rsrq_db":                          lerp(-15, -5, effective_h, jitter=0.5),
        "sinr_db":                          lerp(-5, 25, effective_h, jitter=2),
        "cqi_avg":                          lerp(2, 14, effective_h, jitter=0.5, digits=1),
        "bler_downlink_percent":            lerp(8, 0.2, effective_h, jitter=0.3),
        "bler_uplink_percent":              lerp(8, 0.2, effective_h, jitter=0.3),
        "harq_retransmission_rate_percent": lerp(18, 0.5, effective_h, jitter=0.8),
        "latency_downlink_ms":              round(cell_cs.latency_ms, 1),
        "latency_uplink_ms":                lerp(lat_lo, lat_hi, congestion_factor, jitter=1),
        "handover_attempts":                attempts,
        "handover_success_rate_percent":    ho_rate,
        "handover_failures":                max(0, int(attempts * (1 - ho_rate / 100))),
        "rrc_connection_attempts":          _rng.randint(0, 5000),
        "rrc_success_rate_percent":         lerp(85, 99, effective_h, jitter=0.5),
        "erab_setup_success_rate_percent":  lerp(85, 99, effective_h, jitter=0.5),
        "call_drop_rate_percent":           lerp(5, 0.1, effective_h, jitter=0.2),
        "abnormal_release_rate_percent":    lerp(5, 0.1, effective_h, jitter=0.2),
    }


def _build_link(meta: Dict, cs: ComponentState) -> Dict:
    s = cs.status_str
    if not cs.is_up:
        return {
            "link_id": meta["link_id"], "type": meta["type"],
            "status": s, "op_state": cs.op_state,
            "latency_ms": None, "jitter_ms": None,
            "packet_loss_percent": None, "throughput_mbps": None,
            "utilization_percent": None,
        }
    h = cs.health
    return {
        "link_id":             meta["link_id"],
        "type":                meta["type"],
        "status":              s,
        "op_state":            cs.op_state,
        "latency_ms":          lerp(20, 1, h, jitter=1),
        "jitter_ms":           lerp(5, 0, h, jitter=0.3),
        "packet_loss_percent": round(lerp(0.1, 0.001, h, jitter=0.005), 5),
        "throughput_mbps":     flt(50, 10000),
        "utilization_percent": lerp(90, 5, h, jitter=5),
    }


def _build_rectifier(rec_id: str, cs: ComponentState) -> Dict:
    s = cs.status_str
    return {
        "rectifier_id":        rec_id,
        "status":              s,
        "op_state":            cs.op_state,
        "output_voltage_volt": lerp(46, 50, cs.health, jitter=0.1) if cs.is_up else None,
        "current_ampere":      lerp(5, 50, cs.health, jitter=1)   if cs.is_up else None,
    }


def _build_battery(bat_id: str, cs: ComponentState, power_up: bool) -> Dict:
    s = cs.status_str
    if not cs.is_up:
        return {"battery_id": bat_id, "status": s, "op_state": cs.op_state,
                "charge_percent": None, "temperature_c": None}
    # Charge drains slowly when power is out, recharges when power is on
    delta = 0.5 if power_up else -1.5
    cs.charge_percent = clamp(cs.charge_percent + _rng.uniform(delta - 0.2, delta + 0.2), 0, 100)
    return {
        "battery_id":     bat_id,
        "status":         s,
        "op_state":       cs.op_state,
        "charge_percent": round(cs.charge_percent, 1),
        "temperature_c":  lerp(15, 45, 1 - cs.health, jitter=1),
    }


def _build_environment(cs: ComponentState) -> Dict:
    """Shelter / cabin environment; temperature drifts slowly."""
    cs.temperature_c = smooth_step(
        cs.temperature_c,
        lerp(20, 50, 1 - cs.health, jitter=1),
        max_delta=0.5, digits=1
    )
    return {
        "status":   cs.status_str,
        "op_state": cs.op_state,
        "temperature_sensors": [
            {"sensor_id": "TEMP_1", "value_c": cs.temperature_c},
            {"sensor_id": "TEMP_2", "value_c": round(cs.temperature_c + _rng.uniform(-2, 2), 1)},
        ],
        "humidity_sensors": [
            {"sensor_id": "HUM_1", "value_percent": flt(30, 90, digits=1)}
        ],
        "door_status":    "OPEN" if _rng.random() < 0.01 else "CLOSED",
        "smoke_detected": _rng.random() < 0.005,
    }


# ──────────────────────────── snapshot builder ────────────────────────────────

def _alert(
    alerts: List[Dict[str, Any]],
    site_id: str,
    sequence_number: int,
    severity: str,
    category: str,
    component_type: str,
    component_id: str,
    code: str,
    message: str,
    value: Any = None,
) -> None:
    alerts.append({
        "alert_id": f"{site_id}-{sequence_number}-{code}-{component_id}",
        "severity": severity,
        "category": category,
        "component_type": component_type,
        "component_id": component_id,
        "code": code,
        "message": message,
        "value": value,
    })


def _component_state_alerts(
    alerts: List[Dict[str, Any]],
    site_id: str,
    sequence_number: int,
    category: str,
    component_type: str,
    component_id_field: str,
    components: List[Dict[str, Any]],
) -> None:
    for component in components:
        component_id = str(component.get(component_id_field, "unknown"))
        op_state = component.get("op_state")
        status = component.get("status")

        if op_state == "FAILED" or status == "DOWN":
            _alert(
                alerts, site_id, sequence_number,
                "CRITICAL", category, component_type, component_id,
                "COMPONENT_DOWN",
                f"{component_type} {component_id} is down",
                op_state,
            )
        elif op_state == "DEGRADED":
            _alert(
                alerts, site_id, sequence_number,
                "WARNING", category, component_type, component_id,
                "COMPONENT_DEGRADED",
                f"{component_type} {component_id} is degraded",
                op_state,
            )
        elif op_state == "RECOVERING":
            _alert(
                alerts, site_id, sequence_number,
                "INFO", category, component_type, component_id,
                "COMPONENT_RECOVERING",
                f"{component_type} {component_id} is recovering",
                op_state,
            )


def build_alerts(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Derive operational alerts from component state and KPI thresholds."""
    site_id = snapshot["ran_metadata"]["site_id"]
    seq = snapshot["sequence_number"]
    alerts: List[Dict[str, Any]] = []

    _component_state_alerts(alerts, site_id, seq, "radio", "radio_unit", "ru_id", snapshot["radio_units"])
    _component_state_alerts(alerts, site_id, seq, "radio", "antenna", "antenna_id", snapshot["antennas"])
    _component_state_alerts(alerts, site_id, seq, "radio", "cell", "cell_id", snapshot["cells"])
    _component_state_alerts(alerts, site_id, seq, "core", "baseband_unit", "bbu_id", snapshot["baseband_units"])
    _component_state_alerts(alerts, site_id, seq, "transport", "transport_link", "link_id", snapshot["transport_links"])
    _component_state_alerts(alerts, site_id, seq, "power", "rectifier", "rectifier_id", snapshot["power_system"]["rectifiers"])
    _component_state_alerts(alerts, site_id, seq, "power", "battery", "battery_id", snapshot["power_system"]["batteries"])

    environment = snapshot["environment"]
    if environment["status"] == "DOWN":
        _alert(
            alerts, site_id, seq, "WARNING", "environment", "environment", "ENV",
            "ENVIRONMENT_SENSOR_DOWN", "Environment sensors are down", environment["op_state"],
        )
    if environment["door_status"] == "OPEN":
        _alert(
            alerts, site_id, seq, "WARNING", "security", "door", "SHELTER_DOOR",
            "SHELTER_DOOR_OPEN", "Shelter door is open", environment["door_status"],
        )
    if environment["smoke_detected"]:
        _alert(
            alerts, site_id, seq, "CRITICAL", "environment", "smoke_sensor", "SMOKE",
            "SMOKE_DETECTED", "Smoke detected in tower shelter", True,
        )

    power_system = snapshot["power_system"]
    if power_system["status"] == "DOWN":
        _alert(
            alerts, site_id, seq, "CRITICAL", "power", "power_system", "POWER",
            "SITE_POWER_DOWN", "Site power system is down", power_system["status"],
        )
    if power_system["generator"]["status"] == "ON":
        _alert(
            alerts, site_id, seq, "INFO", "power", "generator", "GENERATOR",
            "GENERATOR_RUNNING", "Generator is running", power_system["generator"]["fuel_level_percent"],
        )
    if power_system["generator"]["fuel_level_percent"] < 20:
        _alert(
            alerts, site_id, seq, "WARNING", "power", "generator", "GENERATOR",
            "LOW_GENERATOR_FUEL", "Generator fuel is below 20 percent",
            power_system["generator"]["fuel_level_percent"],
        )

    for battery in power_system["batteries"]:
        charge = battery.get("charge_percent")
        if charge is not None and charge < 20:
            _alert(
                alerts, site_id, seq, "WARNING", "power", "battery", battery["battery_id"],
                "LOW_BATTERY_CHARGE", "Battery charge is below 20 percent", charge,
            )

    for ru in snapshot["radio_units"]:
        temp = ru.get("temperature_c")
        if temp is not None and temp >= 60:
            _alert(
                alerts, site_id, seq, "WARNING", "radio", "radio_unit", ru["ru_id"],
                "RU_HIGH_TEMPERATURE", "Radio unit temperature is high", temp,
            )

    for cell in snapshot["cells"]:
        prb = cell.get("prb_utilization_percent")
        sinr = cell.get("sinr_db")
        handover_rate = cell.get("handover_success_rate_percent")
        if prb is not None and prb >= 90:
            _alert(
                alerts, site_id, seq, "WARNING", "radio", "cell", cell["cell_id"],
                "CELL_CONGESTION", "Cell PRB utilization is above 90 percent", prb,
            )
        if sinr is not None and sinr < 0:
            _alert(
                alerts, site_id, seq, "WARNING", "radio", "cell", cell["cell_id"],
                "LOW_SINR", "Cell SINR is below 0 dB", sinr,
            )
        if handover_rate is not None and handover_rate < 90:
            _alert(
                alerts, site_id, seq, "WARNING", "mobility", "cell", cell["cell_id"],
                "LOW_HANDOVER_SUCCESS", "Cell handover success rate is below 90 percent",
                handover_rate,
            )

    severity_rank = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
    return sorted(alerts, key=lambda a: (severity_rank[a["severity"]], a["category"], a["component_id"], a["code"]))


def summarize_alerts(alerts: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "total": len(alerts),
        "critical": sum(1 for alert in alerts if alert["severity"] == "CRITICAL"),
        "warning": sum(1 for alert in alerts if alert["severity"] == "WARNING"),
        "info": sum(1 for alert in alerts if alert["severity"] == "INFO"),
        "highest_severity": (
            "CRITICAL" if any(alert["severity"] == "CRITICAL" for alert in alerts)
            else "WARNING" if any(alert["severity"] == "WARNING" for alert in alerts)
            else "INFO" if alerts
            else "NONE"
        ),
    }


def build_snapshot(site: SiteState, ts: datetime = None) -> Dict:
    """Build one complete site snapshot from current state.

    The build order is critical:
      1. Advance state machines (cascade propagation happens inside advance()).
      2. Build RUs + antennas (sector-level RF components).
      3. Build cells (need antenna health for effective_h).
      4. Build BBU (needs total cell user count).
      5. Build power, links, environment (mostly independent).
    """
    site.advance()

    if ts is None:
        ts = datetime.now(timezone.utc)
    ts_str = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    tmult  = traffic_multiplier(ts)

    sector_to_ru = {m["sector_id"]: m["ru_id"] for m in _SECTOR_RU_MAP}

    # ── Radio Units ────────────────────────────────────────────────────────────
    radio_units = [
        _build_ru(m["ru_id"], m["sector_id"], site.rus[m["ru_id"]])
        for m in _SECTOR_RU_MAP
    ]

    # ── Antennas ───────────────────────────────────────────────────────────────
    antennas = [
        _build_antenna(
            m,
            ant_cs=site.ants[m["antenna_id"]],
            ru_cs=site.rus[sector_to_ru[m["sector_id"]]],
        )
        for m in _ANTENNA_STATICS
    ]

    # Build antenna lookup for cell builder
    ant_by_sector = {m["sector_id"]: site.ants[m["antenna_id"]] for m in _ANTENNA_STATICS}

    # ── Cells ──────────────────────────────────────────────────────────────────
    cells = [
        _build_cell(
            m,
            cell_cs=site.cells[m["cell_id"]],
            ant_cs=ant_by_sector[m["sector_id"]],
            traffic_mult=tmult,
        )
        for m in _CELL_STATICS
    ]

    # ── BBU ────────────────────────────────────────────────────────────────────
    total_users = sum(c["active_users"] for c in cells)
    bbu_record  = _build_bbu(site.bbu, total_users)

    # ── Power ──────────────────────────────────────────────────────────────────
    rectifiers = [_build_rectifier(rid, site.recs[rid]) for rid in site.recs]
    power_up   = any(r["status"] == "UP" for r in rectifiers)
    batteries  = [_build_battery(bid, site.bats[bid], power_up) for bid in site.bats]

    low_bat = any(
        (b["charge_percent"] or 100) < 20
        for b in batteries
        if b["status"] == "UP"
    )
    gen_on = (not power_up) or low_bat or _rng.random() < 0.03

    # Fuel drains while running, refills otherwise; runtime accumulates while on
    if gen_on:
        site.bbu.fuel_level = clamp(site.bbu.fuel_level - _rng.uniform(0.1, 0.3), 0, 100)
        site.generator_runtime_hours = round(
            site.generator_runtime_hours + INTERVAL_SECONDS / 3600, 2
        )
    else:
        site.bbu.fuel_level = clamp(site.bbu.fuel_level + _rng.uniform(0.0, 0.05), 0, 100)

    transport_links = [
        _build_link(m, site.links[m["link_id"]]) for m in _LINK_STATICS
    ]
    power_system = {
        "status":     "UP" if power_up else "DOWN",
        "rectifiers": rectifiers,
        "batteries":  batteries,
        "generator": {
            "status":             "ON" if gen_on else "OFF",
            "fuel_level_percent": round(site.bbu.fuel_level, 1),
            "runtime_hours":      site.generator_runtime_hours,
        },
    }
    environment = _build_environment(site.env)

    snapshot = {
        "message_id":      _fake.uuid4(),
        "timestamp":       ts_str,
        "sequence_number": site.seq,
        "ran_metadata": {
            "site_id":    site.meta["site_id"],
            "site_name":  site.meta["site_name"],
            "location":   site.meta["location"],
            "region":     site.meta["region"],
            "vendor":     site.meta["vendor"],
            "technology": site.meta["technology"],
        },
        "radio_units":     radio_units,
        "baseband_units":  [bbu_record],
        "antennas":        antennas,
        "cells":           cells,
        "transport_links": transport_links,
        "power_system": power_system,
        "environment": environment,
    }
    snapshot["alerts"] = build_alerts(snapshot)
    snapshot["alert_summary"] = summarize_alerts(snapshot["alerts"])
    return snapshot


# ──────────────────────────── output helpers ──────────────────────────────────

def to_stdout(snapshot: Dict) -> None:
    print("\n" + "=" * 72)
    site_id   = snapshot["ran_metadata"]["site_id"]
    site_name = snapshot["ran_metadata"]["site_name"]
    print(f"  TowerHealth  |  {snapshot['timestamp']}  |  {site_name} ({site_id})  |  seq={snapshot['sequence_number']}")
    print("=" * 72)
    print(json.dumps(snapshot, indent=2, ensure_ascii=False), flush=True)


def to_file(snapshot: Dict) -> None:
    date_str = datetime.now().strftime("%Y-%m-%d")
    site_id  = snapshot["ran_metadata"]["site_id"].lower()
    filename = os.path.join(OUTPUT_DIR, f"ran_data_{site_id}_{date_str}.jsonl")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(filename, "a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
    print(f"[{snapshot['timestamp']}] {site_id}  seq={snapshot['sequence_number']} → {filename}", flush=True)


def create_kafka_producer():
    """Create a KafkaProducer, retrying until the broker is available."""
    from kafka import KafkaProducer          # type: ignore
    from kafka.errors import NoBrokersAvailable  # type: ignore
    bootstrap_servers = [
        server.strip()
        for server in KAFKA_BOOTSTRAP_SERVERS.split(",")
        if server.strip()
    ]

    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                acks="all",
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


def to_kafka(producer, snapshot: Dict) -> None:
    key      = snapshot["ran_metadata"]["site_id"]
    metadata = producer.send(KAFKA_TOPIC, key=key, value=snapshot).get(timeout=30)
    producer.flush()
    print(
        f"[{snapshot['timestamp']}] Kafka  site={key}  "
        f"partition={metadata.partition}  offset={metadata.offset}",
        flush=True,
    )


# ──────────────────────────── main ────────────────────────────────────────────

def main() -> None:
    # Initialise persistent state for all sites
    sites = [SiteState(meta) for meta in SITE_TOPOLOGY]

    print("=" * 72)
    print("  TowerHealth  –  RAN Telemetry Simulator  (Multi-Site, Stateful)")
    print(f"  Sites    : {NUM_SITES}  |  Interval : {INTERVAL_SECONDS}s  |  Output : {OUTPUT_MODE}")
    for s in sites:
        loc = s.meta["location"]
        print(f"    {s.meta['site_id']}  {s.meta['site_name']}  "
              f"({loc['latitude']:.4f}, {loc['longitude']:.4f})  "
              f"vendor={s.meta['vendor']}")
    if OUTPUT_MODE == "kafka":
        print(f"  Kafka    : {KAFKA_BOOTSTRAP_SERVERS}  topic={KAFKA_TOPIC}")
    print("=" * 72)
    print("Press Ctrl+C to stop.\n")

    producer = create_kafka_producer() if OUTPUT_MODE == "kafka" else None

    while True:
        cycle_start = time.monotonic()
        cycle_ts = datetime.now(timezone.utc)

        for site in sites:
            snapshot = build_snapshot(site, cycle_ts)

            if OUTPUT_MODE in ("stdout", "both"):
                to_stdout(snapshot)
            if OUTPUT_MODE in ("file", "both"):
                to_file(snapshot)
            if OUTPUT_MODE == "kafka" and producer:
                to_kafka(producer, snapshot)

        elapsed  = time.monotonic() - cycle_start
        sleep_for = max(0.0, INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
