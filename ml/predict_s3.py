"""
predict_s3.py
=============
S3-aware wrapper around RANCellPredictor from 03_predict.py.

USAGE
-----
    python predict_s3.py \
        --input  s3://tower-iti-project/gold/ran_ml_input/gold_date=2026-06-04/ \
        --output s3://tower-iti-project/gold/ran_ml_predictions/2026-06-04_predictions.csv \
        --model  /opt/ml/ran_cell_model.txt \
        --meta   /opt/ml/ran_cell_model_features.json
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import boto3


# ── Import RANCellPredictor from 03_predict.py (filename starts with digit) ──
def _load_predictor_class():
    from importlib.util import spec_from_file_location, module_from_spec
    script = Path(__file__).parent / "03_predict.py"
    if not script.exists():
        raise FileNotFoundError(f"03_predict.py not found at {script}")
    spec = spec_from_file_location("ran_predict", script)
    mod  = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.RANCellPredictor


# ── S3 helpers ────────────────────────────────────────────────────────────────

def _parse_s3(uri):
    p = urlparse(uri)
    return p.netloc, p.path.lstrip("/")


def _download_input(s3_uri, local_dir):
    """
    Download all parquet files from an S3 URI (file or folder prefix)
    into local_dir. Returns the local path to read from.
    """
    bucket, key = _parse_s3(s3_uri)
    s3 = boto3.client("s3")

    # Single file
    if key.endswith(".parquet"):
        local = os.path.join(local_dir, "input.parquet")
        s3.download_file(bucket, key, local)
        print(f"[S3] Downloaded {key}")
        return local

    # Folder / partition prefix — collect all parquet files
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key)
    files = [
        o["Key"] for o in resp.get("Contents", [])
        if o["Key"].endswith(".parquet") and os.path.basename(o["Key"])  # skip pseudo-folder entries
    ]
    if not files:
        raise FileNotFoundError(f"No parquet files found under s3://{bucket}/{key}")

    # Always concatenate into a single combined file so _sniff_and_read
    # always receives a file path, never a directory.
    import pandas as pd
    parts = []
    for f in files:
        with tempfile.NamedTemporaryFile(dir=local_dir, suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        s3.download_file(bucket, f, tmp_path)
        parts.append(pd.read_parquet(tmp_path))
        print(f"[S3] Downloaded {f}")

    combined = os.path.join(local_dir, "input.parquet")
    pd.concat(parts, ignore_index=True).to_parquet(combined)
    print(f"[S3] Combined {len(parts)} part(s) -> input.parquet")
    return combined


def _upload_output(local_path, s3_uri):
    bucket, key = _parse_s3(s3_uri)
    if key.endswith("/"):
        key += Path(local_path).name
    boto3.client("s3").upload_file(local_path, bucket, key)
    print(f"[S3] Uploaded predictions → s3://{bucket}/{key}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="RAN Cell Failure Prediction — S3 I/O")
    p.add_argument("--input",     required=True,  help="S3 URI or local path to ML-ready parquet")
    p.add_argument("--output",    required=True,  help="S3 URI or local path for predictions CSV")
    p.add_argument("--model",     required=True,  help="Local path to ran_cell_model.txt")
    p.add_argument("--meta",      required=True,  help="Local path to ran_cell_model_features.json")
    p.add_argument("--threshold", type=float, default=None, help="Decision threshold (overrides meta JSON)")
    args = p.parse_args()

    RANCellPredictor = _load_predictor_class()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Download input from S3 (or use local path directly)
        if args.input.startswith("s3"):
            input_path = _download_input(args.input, tmpdir)
        else:
            input_path = args.input

        local_output = os.path.join(tmpdir, "predictions.csv")

        # Run prediction — 03_predict.py does all feature engineering internally
        predictor = RANCellPredictor(args.model, args.meta, args.threshold)
        predictor.predict(input_path, local_output)

        # Upload output to S3 (or copy locally)
        if args.output.startswith("s3"):
            _upload_output(local_output, args.output)
        else:
            import shutil
            shutil.copy(local_output, args.output)
            print(f"[OUTPUT] Saved to {args.output}")


if __name__ == "__main__":
    main()