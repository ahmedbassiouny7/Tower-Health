"""
02_train_cell_model.py
=======================
RAN Cell Failure Prediction — Training on Cell Metrics Only

FEATURE SCOPE
-------------
Only the 27 per-cell RF/KPI metrics are used as raw inputs:
    status, op_state, active_users, connected_users,
    prb_utilization_percent, throughput_downlink/uplink_mbps,
    spectral_efficiency_bps_per_hz, rsrp_dbm, rsrq_db, sinr_db, cqi_avg,
    bler_downlink/uplink_percent, harq_retransmission_rate_percent,
    latency_downlink/uplink_ms, handover_attempts/success_rate/failures,
    rrc_connection_attempts/success_rate, erab_setup_success_rate,
    call_drop_rate_percent, abnormal_release_rate_percent,
    technology, bandwidth_mhz

Engineered from those:
    • Temporal flags from timestamp  (NOT the raw date)
    • Rolling mean/std/max/min at 3h, 6h, 12h, 24h
    • Lag values at 1h, 3h, 6h, 12h back
    • Rate-of-change (delta) at steps 1h and 3h

Label
-----
    binary:  1 = failure  (scenario_label != 'NORMAL')
             0 = normal

Output artefacts
----------------
    ran_cell_model.txt          LightGBM native (production, portable)
    ran_cell_model.pkl          joblib pickle   (sklearn-pipeline use)
    ran_cell_model_features.json  ordered feature list for inference
    ran_cell_training_report.png  evaluation charts

Designed for Google Colab — all installs included.
"""

# ── 0. Install ─────────────────────────────────────────────────────────────────
import subprocess, sys
def _pip(*p): subprocess.check_call([sys.executable,"-m","pip","install","-q",*p])
_pip("lightgbm","pyarrow","joblib","pandas","numpy","scikit-learn",
     "matplotlib","seaborn")

# ── 1. Imports ─────────────────────────────────────────────────────────────────
import os, json, warnings, time
from pathlib import Path
import numpy as np
import pandas as pd
import lightgbm as lgb
import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    classification_report, confusion_matrix,
    roc_curve, precision_recall_curve,
    f1_score,
)

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION A — CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

DATA_FILE      = "ran_training_data.parquet"
MODEL_TXT      = "ran_cell_model.txt"
MODEL_PKL      = "ran_cell_model.pkl"
FEATURES_JSON  = "ran_cell_model_features.json"
REPORT_PNG     = "ran_cell_training_report.png"

LABEL_COL      = "scenario_label"
NORMAL_VALUE   = "NORMAL"
N_CELLS        = 3
THRESHOLD      = 0.5
CV_SPLITS      = 5
EARLY_STOP     = 50

# ── LightGBM params ────────────────────────────────────────────────────────────
LGB_PARAMS = dict(
    objective        = "binary",
    boosting_type    = "gbdt",
    n_estimators     = 1000,      # upper bound; early stopping kicks in
    learning_rate    = 0.04,
    num_leaves       = 63,
    max_depth        = -1,
    min_child_samples= 40,
    subsample        = 0.8,
    subsample_freq   = 1,
    colsample_bytree = 0.8,
    reg_alpha        = 0.1,
    reg_lambda       = 1.0,
    scale_pos_weight = 3,         # balances 25 % failure vs 75 % normal
    random_state     = 42,
    n_jobs           = -1,
    verbose          = -1,
)

# ── Cell metric columns (suffixes — will be resolved with cell_N_ prefix) ─────
CELL_METRIC_SUFFIXES = [
    "status", "op_state",
    "active_users", "connected_users",
    "prb_utilization_percent",
    "throughput_downlink_mbps", "throughput_uplink_mbps",
    "spectral_efficiency_bps_per_hz",
    "rsrp_dbm", "rsrq_db", "sinr_db", "cqi_avg",
    "bler_downlink_percent", "bler_uplink_percent",
    "harq_retransmission_rate_percent",
    "latency_downlink_ms", "latency_uplink_ms",
    "handover_attempts", "handover_success_rate_percent", "handover_failures",
    "rrc_connection_attempts", "rrc_success_rate_percent",
    "erab_setup_success_rate_percent",
    "call_drop_rate_percent", "abnormal_release_rate_percent",
    "technology", "bandwidth_mhz",
]

# KPI subsets used for time-series feature engineering
ROLL_KPIS = [
    "prb_utilization_percent",
    "throughput_downlink_mbps", "throughput_uplink_mbps",
    "sinr_db", "rsrp_dbm", "rsrq_db", "cqi_avg",
    "bler_downlink_percent", "bler_uplink_percent",
    "harq_retransmission_rate_percent",
    "latency_downlink_ms", "latency_uplink_ms",
    "call_drop_rate_percent", "handover_success_rate_percent",
]
LAG_KPIS = [
    "prb_utilization_percent", "sinr_db", "rsrp_dbm",
    "bler_downlink_percent", "call_drop_rate_percent",
]
DELTA_KPIS = [
    "prb_utilization_percent", "sinr_db", "bler_downlink_percent",
]

ROLL_WINDOWS = [3, 6, 12, 24]
LAG_STEPS    = [1, 3, 6, 12]
DELTA_STEPS  = [1, 3]

# Columns to drop before building X (meta / date / label)
DROP_COLS = {
    "timestamp", "message_id", "sequence_number",
    "site_id", "site_name", "scenario_label",
    "latitude", "longitude", "region", "vendor",
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION B — DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_data(path: str) -> pd.DataFrame:
    print(f"  Loading {path} …")
    df = pd.read_parquet(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["site_id", "timestamp"]).reset_index(drop=True)
    print(f"  Rows: {len(df):,}   Columns: {len(df.columns)}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SECTION C — WIDE → LONG  (one site-row → N_CELLS cell-rows)
# ══════════════════════════════════════════════════════════════════════════════

def wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    shared = ["timestamp", "site_id", LABEL_COL]
    shared = [c for c in shared if c in df.columns]

    frames = []
    for idx in range(1, N_CELLS + 1):
        prefix = f"cell_{idx}_"
        rename = {f"{prefix}{s}": s
                  for s in CELL_METRIC_SUFFIXES
                  if f"{prefix}{s}" in df.columns}
        tmp = df[shared + list(rename)].copy().rename(columns=rename)
        tmp["cell_id"] = np.int8(idx)
        frames.append(tmp)

    long = (pd.concat(frames, ignore_index=True)
              .sort_values(["site_id", "cell_id", "timestamp"])
              .reset_index(drop=True))
    print(f"  Wide→Long: {len(long):,} cell-rows")
    return long


# ══════════════════════════════════════════════════════════════════════════════
# SECTION D — LABEL
# ══════════════════════════════════════════════════════════════════════════════

def build_label(df: pd.DataFrame) -> pd.Series:
    y = (df[LABEL_COL] != NORMAL_VALUE).astype(np.int8)
    n1, n0 = y.sum(), (y == 0).sum()
    print(f"  Label: {n1:,} failures ({100*n1/len(y):.1f}%)  "
          f"| {n0:,} normal ({100*n0/len(y):.1f}%)")
    return y


# ══════════════════════════════════════════════════════════════════════════════
# SECTION E — FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════

# ── Status encoding ────────────────────────────────────────────────────────────
_STATUS_MAP = {
    "UP":1,"ACTIVE":1,"ON":1,"OK":1,"OPERATIONAL":1,"NORMAL":1,
    "DOWN":0,"INACTIVE":0,"OFF":0,"FAILED":0,"FAULT":0,
    "DEGRADED":2,"WARNING":2,"PARTIAL":2,
    "STANDBY":3,"IDLE":3,
}
_TECH_MAP = {"2G":0,"3G":1,"4G":2,"5G":3,"NR":3}


def encode_cats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes("object").columns:
        if col in DROP_COLS or col == LABEL_COL:
            continue
        if col == "technology":
            df[col] = df[col].map(_TECH_MAP).fillna(2).astype(np.int8)
        else:
            df[col] = df[col].map(_STATUS_MAP).fillna(0).astype(np.int8)
    return df


def add_temporal(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["timestamp"]
    df = df.copy()
    df["hour_of_day"]  = ts.dt.hour.astype(np.int8)
    df["day_of_week"]  = ts.dt.dayofweek.astype(np.int8)
    df["day_of_month"] = ts.dt.day.astype(np.int8)
    df["month"]        = ts.dt.month.astype(np.int8)
    df["is_weekend"]   = (df["day_of_week"] >= 5).astype(np.int8)
    df["is_night"]     = ((df["hour_of_day"] >= 22) |
                          (df["hour_of_day"] < 6)).astype(np.int8)
    # ⚠  Raw timestamp intentionally NOT added as a feature
    return df


def add_rolling(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    g = df.groupby(["site_id", "cell_id"])
    for w in ROLL_WINDOWS:
        for kpi in ROLL_KPIS:
            if kpi not in df.columns:
                continue
            r = g[kpi].transform
            df[f"{kpi}_roll{w}_mean"] = r(lambda x,w=w: x.rolling(w,min_periods=1).mean())
            df[f"{kpi}_roll{w}_std"]  = r(lambda x,w=w: x.rolling(w,min_periods=1).std().fillna(0))
            df[f"{kpi}_roll{w}_max"]  = r(lambda x,w=w: x.rolling(w,min_periods=1).max())
            df[f"{kpi}_roll{w}_min"]  = r(lambda x,w=w: x.rolling(w,min_periods=1).min())
    return df


def add_lags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    g = df.groupby(["site_id", "cell_id"])
    for kpi in LAG_KPIS:
        if kpi not in df.columns:
            continue
        for lag in LAG_STEPS:
            df[f"{kpi}_lag{lag}"] = g[kpi].transform(
                lambda x, l=lag: x.shift(l).bfill().fillna(x.mean()))
    return df


def add_deltas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    g = df.groupby(["site_id", "cell_id"])
    for kpi in DELTA_KPIS:
        if kpi not in df.columns:
            continue
        for step in DELTA_STEPS:
            df[f"{kpi}_delta{step}"] = g[kpi].transform(
                lambda x, s=step: x.diff(s).fillna(0))
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    print("  Encoding categoricals …")
    df = encode_cats(df)
    print("  Adding temporal features (NO raw date in model) …")
    df = add_temporal(df)
    print("  Adding rolling statistics …")
    df = add_rolling(df)
    print("  Adding lag features …")
    df = add_lags(df)
    print("  Adding delta features …")
    df = add_deltas(df)
    return df


def select_features(df: pd.DataFrame) -> list[str]:
    """Return final ordered feature column list."""
    exclude = DROP_COLS | {LABEL_COL}
    return [c for c in df.columns
            if c not in exclude and df[c].dtype != object]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION F — TRAINING
# ══════════════════════════════════════════════════════════════════════════════

def train(X: pd.DataFrame, y: pd.Series):
    print(f"\n  Feature matrix : {X.shape[0]:,} rows × {X.shape[1]} features")

    tscv = TimeSeriesSplit(n_splits=CV_SPLITS)
    cv_metrics = []

    print(f"  Time-series CV ({CV_SPLITS} folds) …")
    best_iters = []
    for fold, (tr_idx, va_idx) in enumerate(tscv.split(X), 1):
        Xtr, Xva = X.iloc[tr_idx], X.iloc[va_idx]
        ytr, yva = y.iloc[tr_idx], y.iloc[va_idx]

        m = lgb.LGBMClassifier(**LGB_PARAMS)
        m.fit(Xtr, ytr,
              eval_set=[(Xva, yva)],
              callbacks=[lgb.early_stopping(EARLY_STOP, verbose=False),
                         lgb.log_evaluation(period=-1)])

        prob = m.predict_proba(Xva)[:,1]
        pred = (prob >= THRESHOLD).astype(int)
        auc  = roc_auc_score(yva, prob)
        ap   = average_precision_score(yva, prob)
        f1   = f1_score(yva, pred)
        best_iters.append(m.best_iteration_)
        cv_metrics.append(dict(fold=fold, auc=auc, ap=ap, f1=f1,
                               best_iter=m.best_iteration_))
        print(f"    Fold {fold}: AUC={auc:.4f}  AP={ap:.4f}  "
              f"F1={f1:.4f}  trees={m.best_iteration_}")

    cv_df = pd.DataFrame(cv_metrics)
    print(f"\n  CV Summary:")
    print(f"    AUC : {cv_df.auc.mean():.4f} ± {cv_df.auc.std():.4f}")
    print(f"    AP  : {cv_df.ap.mean():.4f}  ± {cv_df.ap.std():.4f}")
    print(f"    F1  : {cv_df.f1.mean():.4f}  ± {cv_df.f1.std():.4f}")

    # Final model on full data with mean best iteration
    n_est = max(50, int(np.mean(best_iters)))
    print(f"\n  Training final model (n_estimators={n_est}) on full data …")
    final_params = {**LGB_PARAMS, "n_estimators": n_est}
    clf = lgb.LGBMClassifier(**final_params)
    clf.fit(X, y)
    print(f"  Done. Trees in final model: {clf.n_estimators_}")
    return clf, cv_df


# ══════════════════════════════════════════════════════════════════════════════
# SECTION G — EVALUATION & PLOTS
# ══════════════════════════════════════════════════════════════════════════════

def evaluate(clf, X, y, cv_df, feat_cols, out_png):
    prob  = clf.predict_proba(X)[:,1]
    pred  = (prob >= THRESHOLD).astype(int)
    auc   = roc_auc_score(y, prob)
    ap    = average_precision_score(y, prob)
    cm    = confusion_matrix(y, pred)
    fpr, tpr, _ = roc_curve(y, prob)
    prec, rec, _= precision_recall_curve(y, prob)

    top_n = 30
    imp = pd.Series(clf.feature_importances_, index=feat_cols).nlargest(top_n)

    fig, axes = plt.subplots(2, 3, figsize=(22, 13))
    fig.suptitle("RAN Cell Model — Training Report", fontsize=15, fontweight="bold")
    plt.subplots_adjust(hspace=0.38, wspace=0.32)

    # 1. CV metrics per fold
    ax = axes[0, 0]
    x  = np.arange(1, len(cv_df)+1)
    w  = 0.25
    ax.bar(x-w, cv_df.auc, w, label="AUC",  color="#3b82f6", alpha=0.85)
    ax.bar(x,   cv_df.ap,  w, label="AP",   color="#f59e0b", alpha=0.85)
    ax.bar(x+w, cv_df.f1,  w, label="F1",   color="#10b981", alpha=0.85)
    for val, label, col in [(cv_df.auc.mean(),"#3b82f6","#3b82f6"),
                             (cv_df.ap.mean(), "#f59e0b","#f59e0b"),
                             (cv_df.f1.mean(), "#10b981","#10b981")]:
        ax.axhline(val, color=col, ls="--", lw=1, alpha=0.7)
    ax.set_title("CV Scores per Fold"); ax.set_xlabel("Fold"); ax.set_ylim(0, 1.05)
    ax.legend(); ax.grid(axis="y", alpha=0.3)

    # 2. ROC
    ax = axes[0, 1]
    ax.plot(fpr, tpr, "#3b82f6", lw=2, label=f"AUC = {auc:.4f}")
    ax.fill_between(fpr, tpr, alpha=0.1, color="#3b82f6")
    ax.plot([0,1],[0,1],"k--",lw=1)
    ax.set_title("ROC Curve"); ax.set_xlabel("FPR"); ax.set_ylabel("TPR")
    ax.legend(); ax.grid(alpha=0.3)

    # 3. PR Curve
    ax = axes[0, 2]
    ax.plot(rec, prec, "#10b981", lw=2, label=f"AP = {ap:.4f}")
    ax.fill_between(rec, prec, alpha=0.1, color="#10b981")
    ax.set_title("Precision-Recall Curve")
    ax.set_xlabel("Recall"); ax.set_ylabel("Precision")
    ax.legend(); ax.grid(alpha=0.3)

    # 4. Confusion matrix
    ax = axes[1, 0]
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=ax,
                xticklabels=["Normal","Failure"],
                yticklabels=["Normal","Failure"])
    ax.set_title("Confusion Matrix (t=0.5)")
    ax.set_xlabel("Predicted"); ax.set_ylabel("True")

    # 5. Score distribution
    ax = axes[1, 1]
    ax.hist(prob[y==0], bins=50, alpha=0.6, color="#3b82f6",
            density=True, label="Normal (0)")
    ax.hist(prob[y==1], bins=50, alpha=0.6, color="#ef4444",
            density=True, label="Failure (1)")
    ax.axvline(THRESHOLD, color="k", ls="--", lw=1.3, label="Threshold")
    ax.set_title("Predicted Probability Distribution")
    ax.set_xlabel("P(failure)"); ax.set_ylabel("Density")
    ax.legend(); ax.grid(alpha=0.3)

    # 6. Top features
    ax = axes[1, 2]
    colors = ["#ef4444" if v == imp.max() else "#6366f1"
              for v in imp.values]
    imp[::-1].plot.barh(ax=ax, color=colors[::-1], alpha=0.85)
    ax.set_title(f"Top {top_n} Feature Importances")
    ax.set_xlabel("Split Count"); ax.tick_params(axis="y", labelsize=7)
    ax.grid(axis="x", alpha=0.3)

    plt.savefig(out_png, dpi=140, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"  Report saved → {out_png}")

    print("\n  Classification Report (threshold=0.5, full train set):")
    print(classification_report(y, pred, target_names=["Normal","Failure"],
                                 digits=4))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION H — SAVE PRODUCTION ARTEFACTS
# ══════════════════════════════════════════════════════════════════════════════

def save_artefacts(clf, feat_cols):
    # 1. LightGBM native text
    clf.booster_.save_model(MODEL_TXT)
    print(f"  LightGBM native → {MODEL_TXT}  "
          f"({Path(MODEL_TXT).stat().st_size/1024:.1f} KB)")

    # 2. joblib pickle
    joblib.dump(clf, MODEL_PKL, compress=3)
    print(f"  joblib pickle   → {MODEL_PKL}  "
          f"({Path(MODEL_PKL).stat().st_size/1024:.1f} KB)")

    # 3. Feature manifest  (used by the inference script)
    meta = {
        "feature_columns":      feat_cols,
        "n_features":           len(feat_cols),
        "cell_metric_suffixes": CELL_METRIC_SUFFIXES,
        "roll_kpis":            ROLL_KPIS,
        "lag_kpis":             LAG_KPIS,
        "delta_kpis":           DELTA_KPIS,
        "roll_windows":         ROLL_WINDOWS,
        "lag_steps":            LAG_STEPS,
        "delta_steps":          DELTA_STEPS,
        "n_cells":              N_CELLS,
        "label_col":            LABEL_COL,
        "normal_value":         NORMAL_VALUE,
        "threshold":            THRESHOLD,
        "status_map":           _STATUS_MAP,
        "tech_map":             _TECH_MAP,
    }
    with open(FEATURES_JSON, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  Feature manifest→ {FEATURES_JSON}")
    print("\n  ✅  All production artefacts saved.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    t0 = time.time()
    print("\n" + "="*65)
    print("  RAN CELL FAILURE MODEL — TRAINING PIPELINE")
    print("="*65)

    print("\n[1/6] Loading data …")
    raw = load_data(DATA_FILE)

    print("\n[2/6] Wide → Long …")
    long = wide_to_long(raw)

    print("\n[3/6] Building label …")
    y = build_label(long)

    print("\n[4/6] Engineering features …")
    feat_df = engineer_features(long)
    feat_cols = select_features(feat_df)
    X = feat_df[feat_cols].fillna(0).astype(np.float32)
    print(f"  Final feature matrix: {X.shape}")

    print("\n[5/6] Training …")
    clf, cv_df = train(X, y)

    print("\n[6/6] Evaluating & saving …")
    evaluate(clf, X, y, cv_df, feat_cols, REPORT_PNG)
    save_artefacts(clf, feat_cols)

    print(f"\n{'='*65}")
    print(f"  Done in {time.time()-t0:.1f}s")
    print(f"  Models   : {MODEL_TXT}  |  {MODEL_PKL}")
    print(f"  Manifest : {FEATURES_JSON}")
    print(f"  Report   : {REPORT_PNG}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
