"""
03_predict.py
==============
RAN Cell Failure Prediction — Production-Ready Inference

USAGE
-----
    python 03_predict.py \
        --data   new_site_data.parquet   \
        --model  ran_cell_model.txt      \
        --meta   ran_cell_model_features.json \
        --output predictions.csv

    # or import as a library:
    from 03_predict import RANCellPredictor
    predictor = RANCellPredictor("ran_cell_model.txt",
                                  "ran_cell_model_features.json")
    results = predictor.predict("new_site_data.parquet")

INPUT FORMAT
------------
A .parquet file with the same wide-format schema produced by the data
generator (one row per site per hour, columns: cell_1_*, cell_2_*, cell_3_*).
The file does NOT need to contain scenario_label.

OUTPUT FORMAT
-------------
A CSV with one row per (site, cell, timestamp):
    timestamp, site_id, cell_id, failure_probability,
    predicted_failure, risk_level

RISK LEVELS
-----------
    LOW       P < 0.30
    MEDIUM    0.30 <= P < 0.55
    HIGH      0.55 <= P < 0.75
    CRITICAL  P >= 0.75

Designed for Google Colab — all installs included.
"""

# ── 0. Install ─────────────────────────────────────────────────────────────────
import subprocess, sys
def _pip(*p): subprocess.check_call([sys.executable,"-m","pip","install","-q",*p])
_pip("lightgbm","pyarrow","joblib","pandas","numpy")

# ── 1. Imports ─────────────────────────────────────────────────────────────────
import argparse, json, warnings
from pathlib import Path
import numpy as np
import pandas as pd
import lightgbm as lgb
import joblib

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# DEFAULTS  (override via CLI args or RANCellPredictor constructor)
# ══════════════════════════════════════════════════════════════════════════════
DEFAULT_DATA    = "my_test_2.parquet"   # any site parquet
DEFAULT_MODEL   = "ran_cell_model.txt"
DEFAULT_META    = "ran_cell_model_features.json"
DEFAULT_OUTPUT  = "ran_predictions.csv"
DEFAULT_THRESH  = 0.5                           # overridden by meta JSON if present

# ══════════════════════════════════════════════════════════════════════════════
# STATIC ENCODINGS  (must match training script exactly)
# ══════════════════════════════════════════════════════════════════════════════
_STATUS_MAP = {
    "UP":1,"ACTIVE":1,"ON":1,"OK":1,"OPERATIONAL":1,"NORMAL":1,
    "DOWN":0,"INACTIVE":0,"OFF":0,"FAILED":0,"FAULT":0,
    "DEGRADED":2,"WARNING":2,"PARTIAL":2,
    "STANDBY":3,"IDLE":3,
}
_TECH_MAP = {"2G":0,"3G":1,"4G":2,"5G":3,"NR":3}


# ══════════════════════════════════════════════════════════════════════════════
# RISK LEVEL  — deterministic string derivation from probability
# Using np.select instead of pd.cut to avoid Categorical serialisation issues
# and floating-point edge cases at bin boundaries.
# ══════════════════════════════════════════════════════════════════════════════
def _prob_to_risk(prob: np.ndarray) -> np.ndarray:
    """
    Map failure probabilities to risk-level strings.
        LOW      P < 0.30
        MEDIUM   0.30 <= P < 0.55
        HIGH     0.55 <= P < 0.75
        CRITICAL P >= 0.75
    Returns a plain numpy object array of strings — no Categorical, no NaN.
    """
    conditions = [
        prob < 0.30,
        (prob >= 0.30) & (prob < 0.55),
        (prob >= 0.55) & (prob < 0.75),
        prob >= 0.75,
    ]
    choices = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    return np.select(conditions, choices, default="LOW")


# ══════════════════════════════════════════════════════════════════════════════
# NOTEBOOK / COLAB DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def _is_notebook() -> bool:
    """Return True when running inside a Jupyter / Colab kernel."""
    try:
        shell = get_ipython().__class__.__name__   # type: ignore[name-defined]
        return shell in ("ZMQInteractiveShell", "Shell", "google.colab._shell")
    except NameError:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# CLASS: RANCellPredictor
# ══════════════════════════════════════════════════════════════════════════════

class RANCellPredictor:
    """
    Production-ready inference class for RAN cell failure prediction.

    Parameters
    ----------
    model_path  : str   Path to ran_cell_model.txt  OR  .pkl
    meta_path   : str   Path to ran_cell_model_features.json
    threshold   : float Decision threshold (default from meta or 0.5)
    """

    def __init__(self,
                 model_path: str = DEFAULT_MODEL,
                 meta_path:  str = DEFAULT_META,
                 threshold:  float | None = None):

        self.model_path = Path(model_path)
        self.meta_path  = Path(meta_path)

        # ── Load feature manifest ──────────────────────────────────────────────
        if not self.meta_path.exists():
            raise FileNotFoundError(
                f"Feature manifest not found: {meta_path}\n"
                "Run 02_train_cell_model.py first to generate it."
            )
        with open(self.meta_path) as f:
            self.meta = json.load(f)

        self.feature_columns      = self.meta["feature_columns"]
        self.cell_metric_suffixes = self.meta["cell_metric_suffixes"]
        self.roll_kpis            = self.meta["roll_kpis"]
        self.lag_kpis             = self.meta["lag_kpis"]
        self.delta_kpis           = self.meta["delta_kpis"]
        self.roll_windows         = self.meta["roll_windows"]
        self.lag_steps            = self.meta["lag_steps"]
        self.delta_steps          = self.meta["delta_steps"]
        self.n_cells              = self.meta.get("n_cells", 3)
        self.threshold            = threshold or self.meta.get("threshold", DEFAULT_THRESH)

        # ── Load model ────────────────────────────────────────────────────────
        if not self.model_path.exists():
            raise FileNotFoundError(f"Model file not found: {model_path}")

        if self.model_path.suffix == ".pkl":
            obj = joblib.load(self.model_path)
            # unwrap LGBMClassifier → Booster for uniform .predict() interface
            self.booster = obj.booster_ if hasattr(obj, "booster_") else obj
            self._is_classifier = hasattr(obj, "predict_proba")
            self._clf = obj
        else:
            self.booster = lgb.Booster(model_file=str(self.model_path))
            self._is_classifier = False
            self._clf = None

        n_model_feats = (self.booster.num_feature()
                         if not self._is_classifier
                         else self._clf.n_features_in_)
        print(f"[MODEL] Loaded: {self.model_path.name}  "
              f"({n_model_feats} features  |  "
              f"threshold={self.threshold})")

    # ──────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ──────────────────────────────────────────────────────────────────────────

    def predict(self,
                data_source: "str | pd.DataFrame",
                output_path: str | None = None) -> pd.DataFrame:
        """
        Full pipeline: load → preprocess → predict → return DataFrame.

        Parameters
        ----------
        data_source : path to parquet file  OR  already-loaded wide DataFrame
        output_path : if given, saves predictions to CSV at this path

        Returns
        -------
        pd.DataFrame with columns:
            timestamp, site_id, cell_id, failure_probability,
            predicted_failure, risk_level
        """
        raw = self._load(data_source)
        long_df = self._wide_to_long(raw)
        feat_df = self._engineer_features(long_df)
        X       = self._align_features(feat_df)
        prob    = self._score(X)
        results = self._assemble_output(feat_df, prob)

        if output_path:
            results.to_csv(output_path, index=False)
            print(f"[OUTPUT] Saved {len(results):,} rows → {output_path}")

        self._print_summary(results)
        return results

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 1: LOAD
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _sniff_and_read(path: Path) -> pd.DataFrame:
        """
        Detect the real file format by magic bytes and extension, then read it.
        Handles files that are named .parquet but are actually CSV/JSON/Excel,
        or genuine Parquet files with any extension.

        Priority:
          1. Magic-byte detection  (Parquet PAR1, Gzip \x1f\x8b, ZIP PK)
          2. Extension fallback    (.csv, .tsv, .json, .xlsx, .xls)
          3. Plain-text heuristic  (try CSV as last resort)
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Data file not found: {path}")

        # Read first 8 bytes for magic detection
        with open(path, "rb") as fh:
            magic = fh.read(8)

        # ── Parquet: starts with PAR1 (or ends with PAR1 — check both) ────────
        is_parquet_magic = magic[:4] == b"PAR1"
        if not is_parquet_magic:
            # Parquet footer magic is at the end of the file
            with open(path, "rb") as fh:
                fh.seek(-4, 2)
                is_parquet_magic = fh.read(4) == b"PAR1"

        if is_parquet_magic:
            print(f"[LOAD]  Detected format: Parquet")
            return pd.read_parquet(path)

        # ── Gzip-compressed (could be .parquet.gz or .csv.gz) ─────────────────
        if magic[:2] == b"\x1f\x8b":
            ext = "".join(path.suffixes).lower()
            if ".csv" in ext or ".tsv" in ext:
                print(f"[LOAD]  Detected format: gzip CSV")
                return pd.read_csv(path, compression="gzip")
            # Assume gzip-parquet
            print(f"[LOAD]  Detected format: gzip Parquet")
            return pd.read_parquet(path)

        # ── ZIP / xlsx ─────────────────────────────────────────────────────────
        if magic[:2] == b"PK":
            print(f"[LOAD]  Detected format: Excel/ZIP")
            return pd.read_excel(path)

        # ── Extension-based fallback ───────────────────────────────────────────
        ext = path.suffix.lower()
        if ext in (".csv", ".tsv"):
            sep = "\t" if ext == ".tsv" else ","
            print(f"[LOAD]  Detected format: {'TSV' if ext == '.tsv' else 'CSV'}")
            return pd.read_csv(path, sep=sep)
        if ext in (".json", ".jsonl"):
            print(f"[LOAD]  Detected format: JSON")
            return pd.read_json(path, lines=(ext == ".jsonl"))
        if ext in (".xlsx", ".xls"):
            print(f"[LOAD]  Detected format: Excel")
            return pd.read_excel(path)

        # ── Last resort: try CSV (plain text) ─────────────────────────────────
        try:
            print(f"[LOAD]  Detected format: CSV (fallback)")
            return pd.read_csv(path)
        except Exception:
            pass

        raise ValueError(
            f"Cannot determine file format for: {path}\n"
            "Supported formats: Parquet, CSV, TSV, JSON, Excel (.xlsx/.xls), "
            "or gzip-compressed variants."
        )

    def _load(self, source) -> pd.DataFrame:
        if isinstance(source, pd.DataFrame):
            df = source.copy()
        else:
            df = self._sniff_and_read(Path(source))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values(["site_id", "timestamp"]).reset_index(drop=True)
        print(f"[LOAD]  {len(df):,} site-rows × {len(df.columns)} columns")
        return df

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 2: WIDE → LONG
    # ──────────────────────────────────────────────────────────────────────────

    def _wide_to_long(self, df: pd.DataFrame) -> pd.DataFrame:
        # Shared columns: id + anything that is NOT a cell_N_ column
        shared = ["timestamp", "site_id"]
        shared = [c for c in shared if c in df.columns]

        frames = []
        for idx in range(1, self.n_cells + 1):
            prefix = f"cell_{idx}_"
            rename = {f"{prefix}{s}": s
                      for s in self.cell_metric_suffixes
                      if f"{prefix}{s}" in df.columns}
            tmp = df[shared + list(rename)].copy().rename(columns=rename)
            tmp["cell_id"] = np.int8(idx)
            frames.append(tmp)

        long = (pd.concat(frames, ignore_index=True)
                  .sort_values(["site_id", "cell_id", "timestamp"])
                  .reset_index(drop=True))
        print(f"[TRANSFORM] Wide→Long: {len(long):,} cell-rows")
        return long

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 3: FEATURE ENGINEERING  (must be identical to training)
    # ──────────────────────────────────────────────────────────────────────────

    def _encode_cats(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.select_dtypes("object").columns:
            if col in ("site_id", "timestamp"):
                continue
            if col == "technology":
                df[col] = df[col].map(_TECH_MAP).fillna(2).astype(np.int8)
            else:
                df[col] = df[col].map(_STATUS_MAP).fillna(0).astype(np.int8)
        return df

    def _add_temporal(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        ts = df["timestamp"]
        df["hour_of_day"]  = ts.dt.hour.astype(np.int8)
        df["day_of_week"]  = ts.dt.dayofweek.astype(np.int8)
        df["day_of_month"] = ts.dt.day.astype(np.int8)
        df["month"]        = ts.dt.month.astype(np.int8)
        df["is_weekend"]   = (df["day_of_week"] >= 5).astype(np.int8)
        df["is_night"]     = ((df["hour_of_day"] >= 22) |
                               (df["hour_of_day"] < 6)).astype(np.int8)
        return df

    def _add_rolling(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        g = df.groupby(["site_id", "cell_id"])
        for w in self.roll_windows:
            for kpi in self.roll_kpis:
                if kpi not in df.columns:
                    continue
                r = g[kpi].transform
                df[f"{kpi}_roll{w}_mean"] = r(lambda x,w=w: x.rolling(w,min_periods=1).mean())
                df[f"{kpi}_roll{w}_std"]  = r(lambda x,w=w: x.rolling(w,min_periods=1).std().fillna(0))
                df[f"{kpi}_roll{w}_max"]  = r(lambda x,w=w: x.rolling(w,min_periods=1).max())
                df[f"{kpi}_roll{w}_min"]  = r(lambda x,w=w: x.rolling(w,min_periods=1).min())
        return df

    def _add_lags(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        g = df.groupby(["site_id", "cell_id"])
        for kpi in self.lag_kpis:
            if kpi not in df.columns:
                continue
            for lag in self.lag_steps:
                df[f"{kpi}_lag{lag}"] = g[kpi].transform(
                    lambda x, l=lag: x.shift(l).bfill().fillna(x.mean()))
        return df

    def _add_deltas(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        g = df.groupby(["site_id", "cell_id"])
        for kpi in self.delta_kpis:
            if kpi not in df.columns:
                continue
            for step in self.delta_steps:
                df[f"{kpi}_delta{step}"] = g[kpi].transform(
                    lambda x, s=step: x.diff(s).fillna(0))
        return df

    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        print("[FEATURES] Encoding → temporal → rolling → lag → delta …")
        df = self._encode_cats(df)
        df = self._add_temporal(df)
        df = self._add_rolling(df)
        df = self._add_lags(df)
        df = self._add_deltas(df)
        return df

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4: ALIGN TO EXACT TRAINING FEATURE ORDER
    # ──────────────────────────────────────────────────────────────────────────

    def _align_features(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [f for f in self.feature_columns if f not in df.columns]
        if missing:
            print(f"[ALIGN]  {len(missing)} features missing → filled with 0")
            for f in missing:
                df[f] = 0.0
        X = df[self.feature_columns].fillna(0).astype(np.float32)
        print(f"[ALIGN]  Feature matrix: {X.shape}")
        return X

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 5: SCORE
    # ──────────────────────────────────────────────────────────────────────────

    def _score(self, X: pd.DataFrame) -> np.ndarray:
        if self._is_classifier and self._clf is not None:
            prob = self._clf.predict_proba(X.values)[:, 1]
        else:
            prob = self.booster.predict(X.values)
        print(f"[SCORE]  Scored {len(prob):,} rows")
        return prob.astype(np.float32)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 6: ASSEMBLE OUTPUT
    # ──────────────────────────────────────────────────────────────────────────

    def _assemble_output(self, df: pd.DataFrame,
                          prob: np.ndarray) -> pd.DataFrame:
        out = pd.DataFrame({
            "TIMESTAMP":           df["timestamp"],
            "SITE_ID":             df["site_id"],
            "CELL_ID":             "CELL_" + df["cell_id"].astype(str),
            "FAILURE_PROBABILITY": np.round(prob, 4),
            "PREDICTED_FAILURE":   (prob >= self.threshold).astype(np.int8),
            # FIX: use np.select instead of pd.cut to guarantee plain string
            # output with no NaN and no Categorical serialisation surprises.
            "RISK_LEVEL":          _prob_to_risk(prob),
        })
        return out

    # ──────────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _print_summary(results: pd.DataFrame):
        total = len(results)
        n_fail = results["PREDICTED_FAILURE"].sum()
        print(f"\n{'─'*55}")
        print(f"  Total cell-hours scored : {total:,}")
        print(f"  Predicted failures      : {n_fail:,}  "
              f"({100*n_fail/total:.1f}%)")
        print(f"\n  By cell:")
        for cid, grp in results.groupby("CELL_ID"):
            f = grp["PREDICTED_FAILURE"].sum()
            print(f"    {cid}: {f:5,} / {len(grp):,}  "
                  f"({100*f/len(grp):.1f}%)")
        print(f"\n  Risk breakdown:")
        for lv in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
            cnt = (results["RISK_LEVEL"] == lv).sum()
            print(f"    {lv:<10s}: {cnt:7,}  ({100*cnt/total:.1f}%)")
        print(f"{'─'*55}\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def _parse_args():
    p = argparse.ArgumentParser(
        description="RAN Cell Failure — Production Inference")
    p.add_argument("--data",   default=DEFAULT_DATA,
                   help="Input .parquet file (wide format)")
    p.add_argument("--model",  default=DEFAULT_MODEL,
                   help=".txt or .pkl model file")
    p.add_argument("--meta",   default=DEFAULT_META,
                   help="Feature manifest JSON from training")
    p.add_argument("--output", default=DEFAULT_OUTPUT,
                   help="Output CSV path")
    p.add_argument("--threshold", type=float, default=None,
                   help="Decision threshold (overrides meta JSON)")
    # FIX: use parse_known_args so Jupyter's kernel flags (e.g. -f kernel.json)
    # are silently ignored instead of raising SystemExit(2).
    args, _ = p.parse_known_args()
    return args


def main():
    args  = _parse_args()
    pred  = RANCellPredictor(args.model, args.meta, args.threshold)
    results = pred.predict(args.data, args.output)
    print(results.head(20).to_string(index=False))


# ── Colab / notebook convenience: run directly without CLI ───────────────────
if __name__ == "__main__":
    if _is_notebook():
        # Running inside Jupyter / Colab — skip argparse entirely
        print("Running with default paths (Colab/notebook mode) …\n")
        predictor = RANCellPredictor(DEFAULT_MODEL, DEFAULT_META)
        results   = predictor.predict(DEFAULT_DATA, DEFAULT_OUTPUT)
        print(results.head(30).to_string(index=False))
    else:
        main()