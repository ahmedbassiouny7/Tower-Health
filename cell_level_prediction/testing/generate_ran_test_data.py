"""
generate_ran_test_data.py
==========================
Generates a small, balanced RAN cell test dataset that exactly matches
the schema of ran_training_data.parquet.

OUTPUT
------
    ran_test_data.parquet   (~500 rows, ~15% failure rate)

USAGE
-----
    python generate_ran_test_data.py
    python generate_ran_test_data.py --rows 300 --sites 3 --output my_test.parquet

DESIGN CHOICES
--------------
  * Small : 500 rows by default (vs typical thousands in training)
  * Balanced: ~85% NORMAL, ~15% failure/degraded scenarios
  * Realistic: physics-aware ranges (RSRP, VSWR, temperatures, etc.)
  * Reproducible: fixed random seed
"""

import argparse
import uuid
import numpy as np
import pandas as pd
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
SEED        = 42
N_ROWS      = 500          # total site-hour rows
N_SITES     = 5
N_CELLS     = 3
OUTPUT      = "ran_test_data.parquet"
START_TS    = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

# Failure rate: ~15% of rows will have degraded/failure scenario
FAILURE_RATE = 0.15

# ── Categorical value pools (from actual training data footer) ──────────────
STATUS_NORMAL    = "UP"
STATUS_DEGRADED  = "DEGRADED"
STATUS_DOWN      = "DOWN"

OP_ACTIVE   = "ACTIVE"
OP_INACTIVE = "INACTIVE"

VENDORS  = ["Nokia", "Ericsson", "Huawei", "ZTE"]
REGIONS  = ["Giza", "Alexandria", "Cairo", "Aswan"]
BH_TYPES = ["FIBER", "MICROWAVE"]
TECHS    = ["4G", "5G", "3G"]
BANDWIDTHS = [10, 15, 20, 25, 50, 100]   # MHz

# Scenario labels that appear in training data
SCENARIO_LABELS_NORMAL = ["NORMAL"]
SCENARIO_LABELS_FAULT  = [
    "BBU_OVERLOAD", "RU_FAILURE", "ANT_TILT_DRIFT",
    "BACKHAUL_DEGRADED", "POWER_ISSUE", "THERMAL_ALARM",
    "CELL_OUTAGE", "HIGH_INTERFERENCE",
]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _rng(seed=SEED):
    return np.random.default_rng(seed)

def clip(v, lo, hi):
    return float(np.clip(v, lo, hi))

def _status(rng, is_fault, fault_prob=0.6):
    """Return (status, op_state) pair."""
    if is_fault and rng.random() < fault_prob:
        choice = rng.choice([STATUS_DOWN, STATUS_DEGRADED], p=[0.4, 0.6])
        op = OP_INACTIVE if choice == STATUS_DOWN else OP_ACTIVE
        return choice, op
    return STATUS_NORMAL, OP_ACTIVE


# ══════════════════════════════════════════════════════════════════════════════
# SITE-LEVEL STATIC DATA
# ══════════════════════════════════════════════════════════════════════════════

def build_sites(rng, n_sites):
    sites = []
    for i in range(1, n_sites + 1):
        sid   = f"SITE_{i:03d}"
        name  = f"{rng.choice(['GZA','CAI','ALEX','ASW','LXR'])}_TOWER_{i:02d}"
        lat   = float(rng.uniform(22.0, 31.5))
        lon   = float(rng.uniform(25.0, 34.5))
        region = rng.choice(REGIONS)
        vendor = rng.choice(VENDORS)
        bh1t   = rng.choice(BH_TYPES)
        bh2t   = rng.choice(BH_TYPES)
        sites.append(dict(
            site_id=sid, site_name=name,
            latitude=lat, longitude=lon,
            region=region, vendor=vendor,
            _bh1_type=bh1t, _bh2_type=bh2t,
        ))
    return sites


# ══════════════════════════════════════════════════════════════════════════════
# ROW GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def make_row(rng, site, ts, seq, is_fault):
    f = is_fault  # shorthand

    # ── BBU ───────────────────────────────────────────────────────────────────
    bbu_s, bbu_op = _status(rng, f, 0.5)
    bbu_cpu  = clip(rng.normal(85 if f else 45, 15), 0, 100)
    bbu_mem  = clip(rng.normal(80 if f else 50, 12), 0, 100)
    bbu_disk = clip(rng.normal(60, 10), 10, 95)
    bbu_proc_lat = clip(rng.normal(120 if f else 30, 30), 5, 500)
    bbu_users = int(rng.integers(0, 40 if f else 200))
    bbu_cp_lat = clip(rng.normal(20 if f else 5, 5), 1, 200)
    bbu_up_lat = clip(rng.normal(30 if f else 8, 8), 1, 300)

    row = dict(
        message_id      = str(uuid.uuid4()),
        timestamp       = ts,
        sequence_number = int(seq),
        scenario_label  = rng.choice(SCENARIO_LABELS_FAULT if f else SCENARIO_LABELS_NORMAL),
        site_id         = site["site_id"],
        site_name       = site["site_name"],
        latitude        = site["latitude"],
        longitude       = site["longitude"],
        region          = site["region"],
        vendor          = site["vendor"],
        # BBU
        bbu_status                    = bbu_s,
        bbu_op_state                  = bbu_op,
        bbu_cpu_utilization_percent   = bbu_cpu,
        bbu_memory_utilization_percent= bbu_mem,
        bbu_disk_usage_percent        = bbu_disk,
        bbu_process_latency_ms        = bbu_proc_lat,
        bbu_active_users              = bbu_users,
        bbu_control_plane_latency_ms  = bbu_cp_lat,
        bbu_user_plane_latency_ms     = bbu_up_lat,
    )

    # ── RU 1-3 ────────────────────────────────────────────────────────────────
    for ru in range(1, 4):
        ru_s, ru_op = _status(rng, f, 0.45)
        row.update({
            f"ru_{ru}_status"              : ru_s,
            f"ru_{ru}_op_state"            : ru_op,
            f"ru_{ru}_temperature_c"       : clip(rng.normal(55 if f else 35, 10), 10, 90),
            f"ru_{ru}_tx_power_watts"      : clip(rng.normal(15 if f else 40, 8), 1, 80),
            f"ru_{ru}_rx_signal_strength_dbm": clip(rng.normal(-80 if f else -60, 10), -120, -30),
            f"ru_{ru}_vswr"                : clip(rng.normal(2.5 if f else 1.3, 0.5), 1.0, 5.0),
            f"ru_{ru}_current_ampere"      : clip(rng.normal(5, 1.5), 0.5, 15),
            f"ru_{ru}_voltage_volt"        : clip(rng.normal(48, 2), 40, 56),
            f"ru_{ru}_packet_error_rate"   : clip(rng.beta(2, 20 if not f else 5), 0, 0.5),
            f"ru_{ru}_throughput_mbps"     : clip(rng.normal(20 if f else 80, 20), 0, 200),
        })

    # ── Antenna 1-3 ──────────────────────────────────────────────────────────
    for ant in range(1, 4):
        ant_s, ant_op = _status(rng, f, 0.35)
        row.update({
            f"ant_{ant}_tilt_degree"   : clip(rng.normal(20 if f else 6, 5), -10, 45),
            f"ant_{ant}_azimuth_degree": float(rng.choice([0, 60, 120, 180, 240, 300]) +
                                               rng.uniform(-5, 5)),
            f"ant_{ant}_mimo_layers"   : int(rng.choice([2, 4, 8])),
            f"ant_{ant}_status"        : ant_s,
            f"ant_{ant}_op_state"      : ant_op,
            f"ant_{ant}_rssi_dbm"      : clip(rng.normal(-85 if f else -65, 10), -120, -30),
            f"ant_{ant}_snr_db"        : clip(rng.normal(5 if f else 20, 5), -5, 40),
        })

    # ── Backhaul 1-2 ──────────────────────────────────────────────────────────
    for bh in range(1, 3):
        bh_s, bh_op = _status(rng, f, 0.4)
        bh_type = site[f"_bh{bh}_type"]
        row.update({
            f"bh_{bh}_status"              : bh_s,
            f"bh_{bh}_op_state"            : bh_op,
            f"bh_{bh}_latency_ms"          : clip(rng.normal(80 if f else 5, 20), 1, 500),
            f"bh_{bh}_jitter_ms"           : clip(rng.normal(20 if f else 2, 5), 0, 100),
            f"bh_{bh}_packet_loss_percent" : clip(rng.normal(5 if f else 0.1, 2), 0, 30),
            f"bh_{bh}_throughput_mbps"     : clip(rng.normal(50 if f else 500, 100), 1, 1000),
            f"bh_{bh}_utilization_percent" : clip(rng.normal(80 if f else 40, 15), 0, 100),
            f"bh_{bh}_type"                : bh_type,
        })

    # ── Power / Generator ─────────────────────────────────────────────────────
    pwr_s = STATUS_DOWN if (f and rng.random() < 0.2) else STATUS_NORMAL
    gen_s, _ = _status(rng, f, 0.3)
    row.update({
        "power_status" : pwr_s,
        "gen_status"   : gen_s,
        "gen_fuel_pct" : clip(rng.normal(20 if f else 75, 15), 0, 100),
    })

    # ── Rectifiers 1-2 ────────────────────────────────────────────────────────
    for rec in range(1, 3):
        rec_s, _ = _status(rng, f, 0.3)
        row.update({
            f"rec_{rec}_status"    : rec_s,
            f"rec_{rec}_voltage_v" : clip(rng.normal(44 if f else 48, 2), 38, 56),
            f"rec_{rec}_current_a" : clip(rng.normal(20, 5), 5, 60),
        })

    # ── Batteries 1-2 ────────────────────────────────────────────────────────
    for bat in range(1, 3):
        bat_s, _ = _status(rng, f, 0.25)
        row.update({
            f"bat_{bat}_status"    : bat_s,
            f"bat_{bat}_charge_pct": clip(rng.normal(30 if f else 80, 20), 0, 100),
            f"bat_{bat}_temp_c"    : clip(rng.normal(40 if f else 25, 5), 10, 65),
        })

    # ── Environment ──────────────────────────────────────────────────────────
    env_s, _ = _status(rng, f, 0.2)
    row.update({
        "env_status"   : env_s,
        "env_temp_1_c" : clip(rng.normal(45 if f else 22, 8), 5, 70),
        "env_temp_2_c" : clip(rng.normal(47 if f else 23, 8), 5, 70),
        "env_humidity" : clip(rng.normal(80 if f else 45, 15), 10, 100),
        "door_open"    : int(rng.random() < (0.15 if f else 0.02)),
        "smoke"        : int(rng.random() < (0.05 if f else 0.001)),
    })

    # ── Cells 1-3 ─────────────────────────────────────────────────────────────
    for c in range(1, 4):
        cell_s, cell_op = _status(rng, f, 0.55)
        cell_up = cell_s == STATUS_NORMAL

        active_u   = int(rng.integers(0, 20  if f else 120))
        conn_u     = int(active_u + rng.integers(0, 10))
        prb_util   = clip(rng.normal(80 if f else 40, 15), 0, 100)
        dl_tput    = clip(rng.normal(10 if f else 150, 30), 0, 500)
        ul_tput    = clip(rng.normal(3  if f else 30,  10), 0, 150)
        spec_eff   = clip(rng.normal(1  if f else 4,   1),  0, 8)
        rsrp       = clip(rng.normal(-110 if f else -85, 10), -140, -44)
        rsrq       = clip(rng.normal(-15  if f else -8,   4), -20,   0)
        sinr       = clip(rng.normal(-3   if f else 15,   6), -10,  40)
        cqi        = clip(rng.normal(4    if f else 10,   2),   1,  15)
        bler_dl    = clip(rng.beta(3, 10  if not f else 3),     0, 0.5)
        bler_ul    = clip(rng.beta(3, 10  if not f else 3),     0, 0.5)
        harq_rr    = clip(rng.beta(2, 15  if not f else 4),     0, 0.5)
        lat_dl     = clip(rng.normal(50   if f else 10,   15),  1, 500)
        lat_ul     = clip(rng.normal(70   if f else 12,   15),  1, 500)
        ho_att     = int(rng.integers(0, 5 if f else 30))
        ho_sr      = clip(rng.normal(50 if f else 96, 10), 0, 100)
        ho_fail    = int(ho_att * (1 - ho_sr / 100))
        rrc_att    = int(rng.integers(0, 20 if f else 200))
        rrc_sr     = clip(rng.normal(60 if f else 97, 10), 0, 100)
        erab_sr    = clip(rng.normal(60 if f else 97, 10), 0, 100)
        cdr        = clip(rng.normal(5 if f else 0.5,  2), 0, 30)
        abr        = clip(rng.normal(5 if f else 0.5,  2), 0, 30)
        tech       = str(rng.choice(TECHS))
        bw         = int(rng.choice(BANDWIDTHS))

        row.update({
            f"cell_{c}_status"                      : cell_s,
            f"cell_{c}_op_state"                    : cell_op,
            f"cell_{c}_active_users"                : active_u,
            f"cell_{c}_connected_users"             : conn_u,
            f"cell_{c}_prb_utilization_percent"     : prb_util,
            f"cell_{c}_throughput_downlink_mbps"    : dl_tput,
            f"cell_{c}_throughput_uplink_mbps"      : ul_tput,
            f"cell_{c}_spectral_efficiency_bps_per_hz": spec_eff,
            f"cell_{c}_rsrp_dbm"                    : rsrp,
            f"cell_{c}_rsrq_db"                     : rsrq,
            f"cell_{c}_sinr_db"                     : sinr,
            f"cell_{c}_cqi_avg"                     : cqi,
            f"cell_{c}_bler_downlink_percent"       : bler_dl,
            f"cell_{c}_bler_uplink_percent"         : bler_ul,
            f"cell_{c}_harq_retransmission_rate_percent": harq_rr,
            f"cell_{c}_latency_downlink_ms"         : lat_dl,
            f"cell_{c}_latency_uplink_ms"           : lat_ul,
            f"cell_{c}_handover_attempts"           : ho_att,
            f"cell_{c}_handover_success_rate_percent": ho_sr,
            f"cell_{c}_handover_failures"           : ho_fail,
            f"cell_{c}_rrc_connection_attempts"     : rrc_att,
            f"cell_{c}_rrc_success_rate_percent"    : rrc_sr,
            f"cell_{c}_erab_setup_success_rate_percent": erab_sr,
            f"cell_{c}_call_drop_rate_percent"      : cdr,
            f"cell_{c}_abnormal_release_rate_percent": abr,
            f"cell_{c}_technology"                  : tech,
            f"cell_{c}_bandwidth_mhz"               : bw,
        })

    return row


# ══════════════════════════════════════════════════════════════════════════════
# DTYPE ENFORCEMENT  (must match training parquet exactly)
# ══════════════════════════════════════════════════════════════════════════════

INT64_COLS = [
    "sequence_number", "bbu_active_users", "door_open", "smoke",
    "ant_1_mimo_layers", "ant_2_mimo_layers", "ant_3_mimo_layers",
] + [f"cell_{c}_{m}" for c in range(1, 4)
     for m in ["active_users", "connected_users", "handover_attempts",
               "handover_failures", "rrc_connection_attempts", "bandwidth_mhz"]]


def enforce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in INT64_COLS:
        if col in df.columns:
            df[col] = df[col].astype("int64")
    # timestamp must be UTC-tz-aware datetime64[ns]
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).astype("datetime64[ns, UTC]")
    # everything else that's numeric stays float64 (pandas default)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def generate(n_rows=N_ROWS, n_sites=N_SITES, output=OUTPUT, seed=SEED):
    rng   = _rng(seed)
    sites = build_sites(rng, n_sites)

    rows      = []
    seq       = 0
    n_fault   = int(n_rows * FAILURE_RATE)
    fault_idx = set(rng.choice(n_rows, size=n_fault, replace=False).tolist())

    for i in range(n_rows):
        site   = sites[i % n_sites]
        ts     = pd.Timestamp(START_TS) + pd.Timedelta(hours=i)
        is_fault = i in fault_idx
        row    = make_row(rng, site, ts, seq, is_fault)
        rows.append(row)
        seq   += 1

    df = pd.DataFrame(rows)
    df = enforce_dtypes(df)

    df.to_parquet(output, index=False, engine="pyarrow")

    # ── Summary ───────────────────────────────────────────────────────────────
    n_norm  = (df["scenario_label"] == "NORMAL").sum()
    n_fault_out = (df["scenario_label"] != "NORMAL").sum()
    print(f"[DONE]  {len(df):,} rows  ×  {len(df.columns)} columns  →  {output}")
    print(f"        NORMAL: {n_norm}  |  FAULT: {n_fault_out}  "
          f"({100*n_fault_out/len(df):.1f}%)")
    print(f"        Sites : {df['site_id'].nunique()}  |  "
          f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")
    return df


def _parse():
    p = argparse.ArgumentParser(description="RAN test data generator")
    p.add_argument("--rows",   type=int, default=N_ROWS,   help="Total rows")
    p.add_argument("--sites",  type=int, default=N_SITES,  help="Number of sites")
    p.add_argument("--output", type=str, default=OUTPUT,   help="Output parquet path")
    p.add_argument("--seed",   type=int, default=SEED,     help="Random seed")
    args, _ = p.parse_known_args()
    return args


if __name__ == "__main__":
    args = _parse()
    generate(n_rows=args.rows, n_sites=args.sites,
             output=args.output, seed=args.seed)
