# 03_predict.py — Airflow Integration Contract

> Inspection date: 2026-06-05. Source: `cell_level_prediction/03_predict.py` +
> `cell_level_prediction/models/ran_cell_model_features.json` + `ran_cell_ml_prep.py`.
>
> **`predict_s3.py` does not exist in this repo.** The authoritative inference
> script is `03_predict.py`.

---

## 1. CLI Flags

All flags are optional.

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--data` | `str` | `my_test_2.parquet` | Input file path — **not** `--input` |
| `--model` | `str` | `ran_cell_model.txt` | `.txt` (LGB Booster) or `.pkl` (joblib) |
| `--meta` | `str` | `ran_cell_model_features.json` | Feature manifest from training |
| `--output` | `str` | `ran_predictions.csv` | Output CSV path |
| `--threshold` | `float` | `None` → `meta["threshold"]` → **0.5** | Overrides meta JSON when set |

Parser uses `parse_known_args` — unknown flags (e.g., Jupyter kernel `-f`) are
silently dropped instead of raising `SystemExit(2)`.

---

## 2. Input Contract (`--data`)

Expects a **single file path** to a wide-format Parquet (or CSV/JSON/Excel —
format is sniffed from magic bytes). Does **not** read recursively or accept an
S3 prefix/directory.

Required wide-format columns:

```
timestamp       # parseable by pd.to_datetime
site_id
cell_1_<suffix>  cell_2_<suffix>  cell_3_<suffix>
```

where `<suffix>` is each of the 27 KPI names in `cell_metric_suffixes`
(e.g., `status`, `prb_utilization_percent`, `sinr_db`, …).

### S3 / Airflow note

`ran_cell_ml_prep.py` writes gold ML input as Hive-partitioned Parquet at:

```
s3a://tower-iti-project/gold/ran_ml_input/gold_date=YYYY-MM-DD/
```

Passing a single partition directory to `pd.read_parquet` works with pyarrow,
but the script was written for a flat local file. When calling from Airflow,
pass the full partition path or the prefix root and let pyarrow discover parts:

```python
# acceptable with pyarrow:
pd.read_parquet("s3://…/gold/ran_ml_input/gold_date=2026-06-05/")
```

---

## 3. Output Write

```python
results.to_csv(output_path, index=False)   # 03_predict.py:189
```

| Property | Value |
|---|---|
| Write method | `pandas.DataFrame.to_csv` |
| Files written | One file at `--output` |
| Overwrite/append | **Overwrites** (pandas opens in `'w'` mode) |
| Header row | **Included** (`header=True` is pandas default) |
| Row index | Excluded (`index=False`) |
| Separator | `,` |
| Quoting | `QUOTE_MINIMAL` — quotes only when field contains `,`, `"`, or newline |
| Null representation | `""` (pandas default `na_rep=""`) |
| S3 write | None — no boto3 anywhere in the script |

---

## 4. Output Schema

Columns in order, as assembled in `_assemble_output` (line 411):

| # | Column | dtype | Role |
|---|---|---|---|
| 1 | `timestamp` | datetime64 → ISO string in CSV | Data hour (from gold input) |
| 2 | `site_id` | object/str | Cell identifier (part 1) |
| 3 | `cell_id` | int8 (1, 2, 3) | Cell identifier (part 2) |
| 4 | `failure_probability` | float32, 4 d.p. | **Score** |
| 5 | `predicted_failure` | int8 (0 or 1) | **Binary prediction** |
| 6 | `risk_level` | Categorical str | LOW / MEDIUM / HIGH / CRITICAL |

Risk level bins:

```
LOW       P < 0.30
MEDIUM    0.30 ≤ P < 0.55
HIGH      0.55 ≤ P < 0.75
CRITICAL  P ≥ 0.75
```

**No `gold_date` or run-date column in the output rows.** The composite cell
identifier is `(site_id, cell_id)` — there is no single-column cell key.

---

## 5. Time Semantics

`timestamp` is present in **every output row**. It is the KPI observation hour
carried through from the input (`hour_ts` in the gold table) — it is not the
time scoring was run.

There is no scoring-timestamp or run-date column in the output rows. If you
need a run-date, encode it in the output filename (e.g., `{gold_date}_predictions.csv`)
or add a column in the Airflow wrapper before writing.

---

## 6. Model Loading

```python
# Feature manifest (03_predict.py:124-136)
with open(self.meta_path) as f:
    self.meta = json.load(f)
self.feature_columns = self.meta["feature_columns"]      # 284 feature names
self.threshold = threshold or self.meta.get("threshold", 0.5)

# Model (03_predict.py:142-151)
if self.model_path.suffix == ".pkl":
    obj = joblib.load(self.model_path)
    self.booster = obj.booster_ if hasattr(obj, "booster_") else obj
else:
    self.booster = lgb.Booster(model_file=str(self.model_path))
```

### Feature mismatch (03_predict.py:383-391)

```python
missing = [f for f in self.feature_columns if f not in df.columns]
if missing:
    print(f"[ALIGN]  {len(missing)} features missing → filled with 0")
    for f in missing:
        df[f] = 0.0
X = df[self.feature_columns].fillna(0).astype(np.float32)
```

**Missing features are silently filled with `0.0` — no exception, no non-zero
exit.** Extra input columns not in `feature_columns` are silently ignored.
There is no hard validation that the input actually carries the expected 284
features.

---

## 7. Failure Modes

| Scenario | Behavior | Exit code |
|---|---|---|
| Empty input file | Runs to completion; writes **header-only CSV** | 0 |
| One or more features missing from input | Backfilled with `0.0`; prediction continues | 0 |
| Zero rows after wide→long transform | Same as empty input — header-only CSV | 0 |
| `ran_cell_model.txt` or `.json` not found | `FileNotFoundError` raised in `__init__` | non-zero |
| Unrecognisable file format | `ValueError` raised in `_sniff_and_read` | non-zero |
| `timestamp` column absent from input | `KeyError` in `_load` (`df["timestamp"]`) | non-zero |

The script never partially writes then raises — it either completes (including
writing an empty/header-only file on zero rows) or raises before any write.

---

## 8. Feature Manifest Summary

File: `cell_level_prediction/models/ran_cell_model_features.json`

| Key | Value |
|---|---|
| `n_features` | 284 |
| `n_cells` | 3 |
| `threshold` | 0.5 |
| `roll_windows` | [3, 6, 12, 24] |
| `lag_steps` | [1, 3, 6, 12] |
| `delta_steps` | [1, 3] |
| `roll_kpis` | 14 KPIs |
| `lag_kpis` | 5 KPIs |
| `delta_kpis` | 3 KPIs (`prb_utilization_percent`, `sinr_db`, `bler_downlink_percent`) |
