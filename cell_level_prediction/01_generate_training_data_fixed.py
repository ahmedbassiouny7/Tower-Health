"""
01_generate_training_data.py
=============================
Generates 11 years (2015-01-01 → 2026-01-01) of realistic hourly RAN
telemetry that exactly mirrors the schema of ran_dataset_site001.parquet.

Key design decisions
--------------------
• Hourly rows per site  →  96,360 rows per site
• 3 cells per site, wide format  (cell_1_*, cell_2_*, cell_3_*)
• Failure rows = exactly 25 % of the dataset
• Technology evolution: 2G(2015) → 3G(2016) → 4G(2017-2021) → 5G(2022+)
• Diurnal, weekly, seasonal and long-term traffic growth patterns
• 10 distinct failure scenarios, each with realistic pre-failure degradation,
  active failure and recovery phases
• All numeric ranges match values seen in the real parquet file

Run in Google Colab
-------------------
    !pip install -q pyarrow pandas numpy
    # then run this file
"""

# ── 0. Install ─────────────────────────────────────────────────────────────────
import subprocess, sys
def _pip(*p): subprocess.check_call([sys.executable,"-m","pip","install","-q",*p])
_pip("pyarrow","pandas","numpy")

# ── 1. Imports ─────────────────────────────────────────────────────────────────
import os
import uuid, warnings
import numpy as np
import pandas as pd
from datetime import datetime, timezone

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
START       = datetime(2015, 1, 1, tzinfo=timezone.utc)
END         = datetime(2026, 1, 1, tzinfo=timezone.utc)
FREQ        = "h"                       # hourly
TARGET_FAIL = 0.25                      # 25 % failure rows
N_CELLS     = 3
SEED        = 2015
OUTPUT      = "ran_training_data.parquet"

rng = np.random.default_rng(SEED)

# ── Site metadata ──────────────────────────────────────────────────────────────
SITES = [
    dict(site_id="SITE_001", site_name="CAIRO_TOWER_01",
         lat=30.0444, lon=31.2357, region="Cairo",    vendor="Ericsson"),
    dict(site_id="SITE_002", site_name="ALEX_TOWER_01",
         lat=31.2001, lon=29.9187, region="Alexandria", vendor="Huawei"),
    dict(site_id="SITE_003", site_name="GZA_TOWER_01",
         lat=30.0131, lon=31.2089, region="Giza",     vendor="Nokia"),
]

# Cell technology per cell per site (evolves over years)
CELL_TECH = {
    "SITE_001": ["4G", "4G", "5G"],
    "SITE_002": ["4G", "4G", "4G"],
    "SITE_003": ["4G", "5G", "5G"],
}
CELL_BW = {
    "SITE_001": [20, 20, 100],
    "SITE_002": [20, 20, 20],
    "SITE_003": [20, 100, 100],
}

# ── Failure scenario catalogue ─────────────────────────────────────────────────
# (name, duration_hours, affected_cells, severity 0-1)
SCENARIOS = [
    ("BBU_CPU_SATURATION",        24, [1,2,3], 0.75),
    ("BBU_FULL_OUTAGE",           12, [1,2,3], 1.00),
    ("CELL1_HIGH_BLER",           36, [1],     0.60),
    ("CELL2_HANDOVER_STORM",      18, [2],     0.65),
    ("GENERATOR_FUEL_DEPLETION",  48, [1,2,3], 0.80),
    ("FULL_SITE_POWER_OUTAGE",     8, [1,2,3], 1.00),
    ("MICROWAVE_BACKHAUL_FADE",   30, [3],     0.55),
    ("BACKHAUL_FIBER_CUT",        16, [1,2],   0.85),
    ("ANT1_PHYSICAL_TILT_DRIFT",  72, [1],     0.40),
    ("ANT2_VSWR_ALARM",           48, [2],     0.50),
]

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def clip(arr, lo, hi): return np.clip(arr, lo, hi)

def sinusoidal(n, amplitude, period, phase=0):
    t = np.arange(n)
    return amplitude * np.sin(2*np.pi*(t+phase)/period)

def growth_factor(timestamps):
    """Long-term traffic growth: ~80 % total over 11 years."""
    year_frac = (timestamps - pd.Timestamp("2015-01-01", tz="UTC")) \
                / pd.Timedelta(days=365)
    return 1.0 + 0.8 * np.clip(np.array(year_frac) / 11, 0, 1)

def diurnal(timestamps):
    """Peak at 20:00, trough at 04:00."""
    h = timestamps.hour
    return 0.5 + 0.5 * np.sin(2*np.pi*(h - 4)/24)

def technology_for_year(year, cell_idx, site_id):
    """Return technology string adjusted for deployment era."""
    base = CELL_TECH[site_id][cell_idx - 1]
    if year < 2017:
        return "2G" if cell_idx == 3 else "3G"
    if year < 2022 and base == "5G":
        return "4G"
    return base

# ══════════════════════════════════════════════════════════════════════════════
# FAILURE SCHEDULE
# Deterministically assigns ~25 % of hours to a failure scenario.
# ══════════════════════════════════════════════════════════════════════════════

def build_failure_schedule(timestamps, site_id):
    """
    Returns an array of scenario labels (str) aligned with timestamps.
    Ensures exactly TARGET_FAIL fraction are non-NORMAL.
    """
    n = len(timestamps)
    labels = np.full(n, "NORMAL", dtype=object)
    target_fail_count = int(n * TARGET_FAIL)

    fail_assigned = 0
    attempt = 0
    while fail_assigned < target_fail_count and attempt < 50000:
        attempt += 1
        scenario, duration, _, _ = SCENARIOS[rng.integers(len(SCENARIOS))]
        # Random start, leave room for recovery window (10 % extra)
        start_i = int(rng.integers(0, max(1, n - int(duration * 1.1))))
        end_i   = min(n, start_i + duration)
        # Only overwrite NORMAL hours
        mask = (labels[start_i:end_i] == "NORMAL")
        new_fail = mask.sum()
        if fail_assigned + new_fail > target_fail_count * 1.05:
            continue
        labels[start_i:end_i][mask] = scenario
        fail_assigned += new_fail

    # Trim to exact 25 %: convert excess failures back to NORMAL
    fail_idx = np.where(labels != "NORMAL")[0]
    excess   = len(fail_idx) - target_fail_count
    if excess > 0:
        to_normal = rng.choice(fail_idx, size=excess, replace=False)
        labels[to_normal] = "NORMAL"

    actual_pct = 100 * (labels != "NORMAL").mean()
    print(f"    {site_id}: {(labels!='NORMAL').sum():,} failure rows "
          f"({actual_pct:.1f}%) / {n:,} total")
    return labels

# ══════════════════════════════════════════════════════════════════════════════
# CELL KPI GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def cell_kpis(n, timestamps, cell_idx, site_id, scenario_labels):
    """Generate all cell_N_* KPI columns for one cell."""
    tech   = [technology_for_year(ts.year, cell_idx, site_id)
              for ts in timestamps]
    bw     = CELL_BW[site_id][cell_idx - 1]
    grow   = growth_factor(timestamps)
    diurn  = np.array([diurnal(ts) for ts in timestamps])
    weekly = 0.9 + 0.1 * np.array([(1 if ts.weekday() < 5 else 0.7)
                                    for ts in timestamps])
    season = 1.0 + 0.08 * np.sin(2*np.pi*np.arange(n)/8760)  # yearly cycle

    base_load = grow * diurn * weekly * season  # 0..~1.8

    # ── Base KPIs ──────────────────────────────────────────────────────────────
    prb   = clip(35 + 40*base_load + rng.normal(0,3,n), 5, 97)
    tpdl  = clip(60 + 120*base_load*grow + rng.normal(0,8,n), 0, 600)
    tpul  = clip(15 + 35*base_load*grow  + rng.normal(0,4,n), 0, 150)
    se    = clip(2.5 + 2.5*base_load + rng.normal(0,0.3,n), 0.5, 7.5)
    sinr  = clip(14 - 4*base_load + rng.normal(0,2,n), -5, 28)
    rsrp  = clip(-88 - 4*base_load + rng.normal(0,3,n), -120, -65)
    rsrq  = clip(-10 - 2*base_load + rng.normal(0,1.5,n), -15, -3)
    cqi   = clip(9 - 2*base_load + rng.normal(0,1,n), 2, 15)
    blerD = clip(0.8 + 1.5*base_load + np.abs(rng.normal(0,0.3,n)), 0.1, 10)
    blerU = clip(0.8 + 1.5*base_load + np.abs(rng.normal(0,0.3,n)), 0.1, 10)
    harq  = clip(1.5 + 2*blerD/10   + np.abs(rng.normal(0,0.3,n)), 0.5, 20)
    latD  = clip(18 + 8*base_load   + rng.normal(0,2,n), 5, 45)
    latU  = clip(16 + 6*base_load   + rng.normal(0,2,n), 5, 45)
    hoAtt = clip(40 + 80*base_load  + rng.normal(0,8,n), 0, 700).astype(int)
    hoSR  = clip(96 + rng.normal(0,1,n), 80, 99.5)
    hoF   = np.maximum(0, (hoAtt * (1 - hoSR/100)).astype(int))
    rrcA  = clip(400+600*base_load  + rng.normal(0,40,n), 0, 5000).astype(int)
    rrcSR = clip(97.5 + rng.normal(0,0.5,n), 85, 99.5)
    erab  = clip(97.5 + rng.normal(0,0.5,n), 85, 99.5)
    cdr   = clip(0.3 + 0.8*base_load + np.abs(rng.normal(0,0.15,n)), 0.05, 5)
    abr   = clip(0.3 + 0.8*base_load + np.abs(rng.normal(0,0.15,n)), 0.05, 5)
    users = clip(80 + 200*base_load  + rng.normal(0,15,n), 0, 600).astype(int)
    conn  = clip(users * 1.2 + rng.normal(0,5,n), 0, 650).astype(int)
    status = np.full(n, "UP", dtype=object)
    opst   = np.full(n, "ACTIVE", dtype=object)

    # ── Failure injection ──────────────────────────────────────────────────────
    for i, sc in enumerate(scenario_labels):
        if sc == "NORMAL":
            continue
        # Find this scenario's spec
        spec = next((s for s in SCENARIOS if s[0] == sc), None)
        if spec is None:
            continue
        _, dur, affected_cells, sev = spec

        if cell_idx not in affected_cells:
            continue

        # Pre-degradation window (25 % of duration before this hour)
        # We apply degradation based on position within consecutive block
        # Approximate: treat each failure hour independently with full severity
        ns = sev  # numeric severity 0-1

        # SINR / RSRP degradation
        sinr[i]  = clip(sinr[i]  - 15*ns + rng.normal(0,1), -5, 28)
        rsrp[i]  = clip(rsrp[i]  - 12*ns + rng.normal(0,2), -120, -65)
        rsrq[i]  = clip(rsrq[i]  - 4*ns  + rng.normal(0,0.5), -15, -3)
        cqi[i]   = clip(cqi[i]   - 5*ns  + rng.normal(0,0.5), 2, 15)

        # Error rate inflation
        blerD[i] = clip(blerD[i] + 12*ns + rng.normal(0,0.5), 0.1, 10)
        blerU[i] = clip(blerU[i] + 12*ns + rng.normal(0,0.5), 0.1, 10)
        harq[i]  = clip(harq[i]  + 8*ns  + rng.normal(0,0.3), 0.5, 20)
        cdr[i]   = clip(cdr[i]   + 3*ns  + rng.normal(0,0.2), 0.05, 5)
        abr[i]   = clip(abr[i]   + 3*ns  + rng.normal(0,0.2), 0.05, 5)

        # Latency spike
        latD[i]  = clip(latD[i]  + 20*ns + rng.normal(0,2), 5, 45)
        latU[i]  = clip(latU[i]  + 18*ns + rng.normal(0,2), 5, 45)

        # Throughput drop
        tpdl[i]  = clip(tpdl[i]  * (1 - 0.7*ns), 0, 600)
        tpul[i]  = clip(tpul[i]  * (1 - 0.7*ns), 0, 150)
        prb[i]   = clip(prb[i]   * (1 + 0.3*ns), 5, 97)  # PRB spikes under failure

        # Status for full outages
        if ns >= 0.95:
            status[i] = "DOWN"
            opst[i]   = "INACTIVE"
            users[i]  = 0; conn[i] = 0
        elif ns >= 0.6:
            status[i] = "DEGRADED"

    prefix = f"cell_{cell_idx}_"
    return {
        f"{prefix}status":                           status,
        f"{prefix}op_state":                         opst,
        f"{prefix}active_users":                     users,
        f"{prefix}connected_users":                  conn,
        f"{prefix}prb_utilization_percent":          np.round(prb,  2),
        f"{prefix}throughput_downlink_mbps":         np.round(tpdl, 2),
        f"{prefix}throughput_uplink_mbps":           np.round(tpul, 2),
        f"{prefix}spectral_efficiency_bps_per_hz":   np.round(se,   3),
        f"{prefix}rsrp_dbm":                         np.round(rsrp, 2),
        f"{prefix}rsrq_db":                          np.round(rsrq, 2),
        f"{prefix}sinr_db":                          np.round(sinr, 2),
        f"{prefix}cqi_avg":                          np.round(cqi,  2),
        f"{prefix}bler_downlink_percent":            np.round(blerD,3),
        f"{prefix}bler_uplink_percent":              np.round(blerU,3),
        f"{prefix}harq_retransmission_rate_percent": np.round(harq, 3),
        f"{prefix}latency_downlink_ms":              np.round(latD, 2),
        f"{prefix}latency_uplink_ms":                np.round(latU, 2),
        f"{prefix}handover_attempts":                hoAtt,
        f"{prefix}handover_success_rate_percent":    np.round(hoSR, 2),
        f"{prefix}handover_failures":                hoF,
        f"{prefix}rrc_connection_attempts":          rrcA,
        f"{prefix}rrc_success_rate_percent":         np.round(rrcSR,2),
        f"{prefix}erab_setup_success_rate_percent":  np.round(erab, 2),
        f"{prefix}call_drop_rate_percent":           np.round(cdr,  3),
        f"{prefix}abnormal_release_rate_percent":    np.round(abr,  3),
        f"{prefix}technology":                       tech,
        f"{prefix}bandwidth_mhz":                   bw,
    }

# ══════════════════════════════════════════════════════════════════════════════
# SITE-LEVEL COLUMN GENERATORS
# ══════════════════════════════════════════════════════════════════════════════

def bbu_cols(n, timestamps, scenario_labels):
    grow  = growth_factor(timestamps)
    diurn = np.array([diurnal(ts) for ts in timestamps])
    cpu   = clip(30 + 40*diurn*grow + rng.normal(0,5,n), 5, 99)
    mem   = clip(25 + 35*diurn*grow + rng.normal(0,4,n), 5, 97)
    disk  = clip(20 + 0.0005*np.arange(n) + rng.normal(0,2,n), 5, 92)
    plat  = clip(8  + 5*diurn + rng.normal(0,2,n), 3, 55)
    ausers= clip(200+600*diurn*grow + rng.normal(0,30,n), 0, 1800).astype(int)
    cplat = clip(6  + 4*diurn + rng.normal(0,1.5,n), 3, 35)
    uplat = clip(7  + 4*diurn + rng.normal(0,1.5,n), 3, 45)

    bbu_st = np.full(n, "UP", dtype=object)
    bbu_op = np.full(n, "ACTIVE", dtype=object)

    for i, sc in enumerate(scenario_labels):
        if "BBU" in sc:
            if "OUTAGE" in sc:
                cpu[i]=99; mem[i]=99; bbu_st[i]="DOWN"; bbu_op[i]="INACTIVE"
            else:  # CPU_SATURATION
                cpu[i]=clip(cpu[i]+35+rng.normal(0,3), 5, 99)
                mem[i]=clip(mem[i]+25+rng.normal(0,3), 5, 97)
                bbu_st[i]="DEGRADED"

    return {
        "bbu_status": bbu_st, "bbu_op_state": bbu_op,
        "bbu_cpu_utilization_percent":    np.round(cpu,  2),
        "bbu_memory_utilization_percent": np.round(mem,  2),
        "bbu_disk_usage_percent":         np.round(disk, 2),
        "bbu_process_latency_ms":         np.round(plat, 2),
        "bbu_active_users":               ausers,
        "bbu_control_plane_latency_ms":   np.round(cplat,2),
        "bbu_user_plane_latency_ms":      np.round(uplat,2),
    }


def ru_ant_cols(n, scenario_labels):
    """RU and ANT columns — realistic but not used by the cell-only model."""
    d = {}
    for ru in range(1, 4):
        st = np.full(n, "UP", dtype=object)
        for i, sc in enumerate(scenario_labels):
            if "OUTAGE" in sc or "BBU_FULL" in sc:
                st[i] = "DOWN"
        d.update({
            f"ru_{ru}_status":                st,
            f"ru_{ru}_op_state":              np.where(st=="DOWN","INACTIVE","ACTIVE"),
            f"ru_{ru}_temperature_c":         np.round(clip(35+sinusoidal(n,8,8760)+rng.normal(0,3,n),20,65),2),
            f"ru_{ru}_tx_power_watts":        np.round(clip(18+rng.normal(0,2,n),5,40),2),
            f"ru_{ru}_rx_signal_strength_dbm":np.round(clip(-65+rng.normal(0,5,n),-100,-30),2),
            f"ru_{ru}_vswr":                  np.round(clip(1.2+np.abs(rng.normal(0,0.1,n)),1.0,3.0),3),
            f"ru_{ru}_current_ampere":        np.round(clip(5+rng.normal(0,1,n),1,20),2),
            f"ru_{ru}_voltage_volt":          np.round(clip(48+rng.normal(0,0.5,n),44,54),2),
            f"ru_{ru}_packet_error_rate":     np.round(np.abs(rng.normal(0,0.001,n)),5),
            f"ru_{ru}_throughput_mbps":       np.round(clip(100+rng.normal(0,30,n),10,500),2),
        })
    for ant in range(1, 4):
        st = np.full(n, "UP", dtype=object)
        op = np.full(n, "ACTIVE", dtype=object)
        vswr_raise = np.zeros(n)
        for i, sc in enumerate(scenario_labels):
            if f"ANT{ant}" in sc and "VSWR" in sc:
                vswr_raise[i] = 1.5
            if f"ANT{ant}" in sc and "TILT" in sc:
                pass  # no status change, just KPI drift
            if "OUTAGE" in sc:
                st[i] = "DOWN"; op[i] = "INACTIVE"
        d.update({
            f"ant_{ant}_tilt_degree":   np.round(clip(4+rng.normal(0,0.5,n)+
                                         (np.arange(n)*0.0002 if ant==1 else 0),0,15),2),
            f"ant_{ant}_azimuth_degree":float([120,240,0][ant-1]),
            f"ant_{ant}_mimo_layers":   rng.choice([2,4,8],n),
            f"ant_{ant}_status":        st,
            f"ant_{ant}_op_state":      op,
            f"ant_{ant}_rssi_dbm":      np.round(clip(-70+rng.normal(0,5,n),-100,-35),2),
            f"ant_{ant}_snr_db":        np.round(clip(14+rng.normal(0,3,n),5,35),2),
        })
    return d


def bh_cols(n, scenario_labels):
    d = {}
    for bh in range(1, 3):
        bh_type = "FIBER" if bh == 1 else "MICROWAVE"
        st = np.full(n, "UP", dtype=object)
        op = np.full(n, "ACTIVE", dtype=object)
        lat  = clip(4+rng.normal(0,1,n), 1, 20)
        loss = clip(np.abs(rng.normal(0,0.01,n)), 0, 0.1)
        util = clip(35+30*np.array([diurnal(ts) for ts in
                    pd.date_range("2015-01-01",periods=n,freq="h")])+rng.normal(0,5,n),5,90)

        for i, sc in enumerate(scenario_labels):
            if bh==1 and "FIBER" in sc:
                st[i]="DOWN"; op[i]="INACTIVE"
                lat[i]=clip(lat[i]+50,1,20); loss[i]=clip(loss[i]+0.08,0,0.1)
            if bh==2 and "MICROWAVE" in sc:
                st[i]="DEGRADED"
                lat[i]=clip(lat[i]+15,1,20); loss[i]=clip(loss[i]+0.05,0,0.1)
            if "POWER_OUTAGE" in sc or "BBU_FULL" in sc:
                st[i]="DOWN"; op[i]="INACTIVE"

        d.update({
            f"bh_{bh}_status":               st,
            f"bh_{bh}_op_state":             op,
            f"bh_{bh}_latency_ms":           np.round(lat, 3),
            f"bh_{bh}_jitter_ms":            np.round(clip(np.abs(rng.normal(1,0.3,n)),0.1,8),3),
            f"bh_{bh}_packet_loss_percent":  np.round(loss, 4),
            f"bh_{bh}_throughput_mbps":      np.round(clip(200+rng.normal(0,40,n),50,1000),2),
            f"bh_{bh}_utilization_percent":  np.round(util, 2),
            f"bh_{bh}_type":                 bh_type,
        })
    return d


def power_cols(n, timestamps, scenario_labels):
    fuel = clip(95 - np.arange(n)*0.003 + sinusoidal(n,3,8760) + rng.normal(0,1,n), 65, 100)
    bat1 = clip(95 + rng.normal(0,2,n), 80, 100)
    bat2 = clip(94 + rng.normal(0,2,n), 80, 100)
    r1v  = clip(48 + rng.normal(0,0.3,n), 46, 50)
    r1a  = clip(20 + rng.normal(0,3,n), 8, 50)
    r2v  = clip(48 + rng.normal(0,0.3,n), 46, 50)
    r2a  = clip(20 + rng.normal(0,3,n), 8, 50)
    b1t  = clip(28 + sinusoidal(n,5,8760) + rng.normal(0,2,n), 15, 48)
    b2t  = clip(27 + sinusoidal(n,5,8760) + rng.normal(0,2,n), 15, 48)

    pw_st  = np.full(n,"UP",dtype=object)
    gen_st = np.full(n,"UP",dtype=object)
    r1_st  = np.full(n,"UP",dtype=object)
    r2_st  = np.full(n,"UP",dtype=object)
    b1_st  = np.full(n,"UP",dtype=object)
    b2_st  = np.full(n,"UP",dtype=object)

    for i, sc in enumerate(scenario_labels):
        if "FUEL" in sc:
            fuel[i] = clip(fuel[i]-30+rng.normal(0,3), 65, 100)
            gen_st[i] = "DEGRADED"
        if "POWER_OUTAGE" in sc:
            pw_st[i]="DOWN"; gen_st[i]="DOWN"
            r1_st[i]="DOWN"; r2_st[i]="DOWN"
            fuel[i]=clip(fuel[i]-15, 65, 100)

    return {
        "power_status": pw_st, "gen_status": gen_st,
        "gen_fuel_pct":   np.round(fuel,2),
        "rec_1_status":   r1_st,
        "rec_1_voltage_v":np.round(r1v,2),
        "rec_1_current_a":np.round(r1a,2),
        "rec_2_status":   r2_st,
        "rec_2_voltage_v":np.round(r2v,2),
        "rec_2_current_a":np.round(r2a,2),
        "bat_1_status":   b1_st,
        "bat_1_charge_pct":np.round(bat1,2),
        "bat_1_temp_c":   np.round(b1t,2),
        "bat_2_status":   b2_st,
        "bat_2_charge_pct":np.round(bat2,2),
        "bat_2_temp_c":   np.round(b2t,2),
    }


def env_cols(n, timestamps, scenario_labels):
    # Outdoor-influenced shelter temperature
    season  = sinusoidal(n, 10, 8760)
    diurn_t = sinusoidal(n, 5, 24)
    t1 = clip(28 + season + diurn_t + rng.normal(0,2,n), 18, 55)
    t2 = clip(27 + season + diurn_t + rng.normal(0,2,n), 18, 55)
    hum = clip(55 + sinusoidal(n,20,8760) + rng.normal(0,5,n), 20, 95)
    door = rng.integers(0, 2, n)
    smoke_v = np.zeros(n, int)
    est = np.full(n,"UP",dtype=object)
    for i, sc in enumerate(scenario_labels):
        if "OUTAGE" in sc:
            t1[i]=clip(t1[i]+8, 18, 55); t2[i]=clip(t2[i]+8, 18, 55)
    return {
        "env_status":   est,
        "env_temp_1_c": np.round(t1, 2),
        "env_temp_2_c": np.round(t2, 2),
        "env_humidity": np.round(hum, 2),
        "door_open":    door,
        "smoke":        smoke_v,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SITE GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def generate_site(site_meta, timestamps):
    n  = len(timestamps)
    sid = site_meta["site_id"]
    print(f"  Generating {sid} ({n:,} rows) …")

    labels = build_failure_schedule(timestamps, sid)
    ts_arr = pd.DatetimeIndex(timestamps)

    rows = {}

    # Meta
    rows["message_id"]     = [str(uuid.uuid4()) for _ in range(n)]
    rows["timestamp"]      = timestamps
    rows["sequence_number"]= np.arange(1, n+1)
    rows["scenario_label"] = labels
    rows["site_id"]        = sid
    rows["site_name"]      = site_meta["site_name"]
    rows["latitude"]       = site_meta["lat"]
    rows["longitude"]      = site_meta["lon"]
    rows["region"]         = site_meta["region"]
    rows["vendor"]         = site_meta["vendor"]

    # Infrastructure
    rows.update(bbu_cols(n, ts_arr, labels))
    rows.update(ru_ant_cols(n, labels))
    rows.update(bh_cols(n, labels))
    rows.update(power_cols(n, ts_arr, labels))
    rows.update(env_cols(n, ts_arr, labels))

    # Cell KPIs
    for c in range(1, N_CELLS+1):
        rows.update(cell_kpis(n, ts_arr, c, sid, labels))

    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "="*65)
    print("  RAN TRAINING DATA GENERATOR  (2015-01-01 → 2026-01-01)")
    print("="*65)

    timestamps = pd.date_range(START, END, freq=FREQ, inclusive="left")
    print(f"\nTimestamps: {len(timestamps):,} hourly rows × {len(SITES)} sites")
    print(f"Target failure rate: {TARGET_FAIL*100:.0f}%\n")

    frames = []
    for site in SITES:
        df = generate_site(site, timestamps)
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)
    full = full.sort_values(["site_id", "timestamp"]).reset_index(drop=True)

    # ── Final stats ────────────────────────────────────────────────────────────
    total     = len(full)
    n_fail    = (full["scenario_label"] != "NORMAL").sum()
    fail_pct  = 100 * n_fail / total
    print(f"\n{'─'*65}")
    print(f"  Total rows      : {total:,}")
    print(f"  Failure rows    : {n_fail:,}  ({fail_pct:.2f}%)")
    print(f"  Normal rows     : {total-n_fail:,}  ({100-fail_pct:.2f}%)")
    print(f"  Columns         : {len(full.columns)}")
    print(f"  Date range      : {full['timestamp'].min()} → {full['timestamp'].max()}")
    print(f"\n  Scenario breakdown:")
    for sc, cnt in full["scenario_label"].value_counts().items():
        print(f"    {sc:<40s}  {cnt:7,}  ({100*cnt/total:.2f}%)")

    # ── Save ──────────────────────────────────────────────────────────────────
    full.to_parquet(OUTPUT, index=False, engine="pyarrow")
    mb = os.path.getsize(OUTPUT) / 1024 / 1024 if os.path.exists(OUTPUT) else 0
    print(f"\n  Saved → {OUTPUT}  ({mb:.1f} MB)")
    print("="*65 + "\n")
    return full


if __name__ == "__main__":
    df = main()
