# Tower Health — Full Project Documentation

> **Graduation Project — Data Engineering**
> Owner: Ahmed | Institute: ITI | Last updated: 2026-06-08

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Tech Stack](#3-tech-stack)
4. [Infrastructure — EC2](#4-infrastructure--ec2)
5. [Data Generation](#5-data-generation)
6. [Bronze Layer](#6-bronze-layer)
7. [Silver Layer](#7-silver-layer)
8. [Gold Layer — DWH](#8-gold-layer--dwh)
9. [ML Layer](#9-ml-layer)
10. [Snowflake Setup](#10-snowflake-setup)
11. [Snowflake Objects](#11-snowflake-objects)
12. [Airflow Orchestration](#12-airflow-orchestration)
13. [Cortex Analyst — Semantic Model](#13-cortex-analyst--semantic-model)
14. [Streamlit App](#14-streamlit-app)
15. [Power BI Dashboard](#15-power-bi-dashboard)
16. [Key SQL Objects](#16-key-sql-objects)
17. [Diagnostic Queries](#17-diagnostic-queries)
18. [Defense Framing](#18-defense-framing)
19. [File Inventory](#19-file-inventory)
20. [Status Tracker](#20-status-tracker)
21. [Code Inventory](#21-code-inventory)

---

## 1. Project Overview

**Name:** Tower Health (previously NetPulse)
**Domain:** Telecom tower monitoring and analytics for Egyptian RAN infrastructure
**Type:** End-to-end Data Engineering graduation project

### Business Problem

Egyptian telecom operators run 4G/5G RAN infrastructure across multiple regions. NOC engineers need to:
- Detect equipment failures before they cause outages
- Monitor real-time alarm severity across sites
- Predict which towers are at highest risk of failure
- Query network KPIs in natural language without writing SQL

### Solution

An end-to-end data pipeline ingesting simulated RAN telemetry through a Medallion Architecture (Bronze → Silver → Gold), running ML failure predictions via LightGBM, and exposing results through Power BI dashboards and an AI-powered bilingual chat interface (Cortex Analyst + Streamlit).

### Target Users

| User | Tool | Purpose |
|---|---|---|
| NOC Manager | Power BI | Operational dashboards, alarm monitoring |
| Network Analyst | Streamlit + Cortex Analyst | Natural language KPI querying |
| Data Engineer | Airflow | Pipeline monitoring and orchestration |

---

## 2. Architecture

```
RAN Data Generator (Python + Faker)
        │
        ▼
   S3 Raw / Bronze Landing
   s3a://tower-iti-project/raw-data/ran_telemetry/
   Nested JSON snapshots
        │
        ▼ PySpark on EC2 (Airflow task: silver)
   S3 Silver Layer
   s3a://tower-iti-project/silver/ran_telemetry_normalized
   10 normalized Parquet tables
   Incremental processing with processed-file manifest
        │
        ▼ PySpark on EC2 (Airflow task: gold)
   S3 Gold BI Layer
   s3a://tower-iti-project/gold/ran_telemetry_bi/
   10 Spark-written BI tables (7 dims + 3 facts)
        │
        ├── ML prep (Airflow task: ml_prep)
        │   └── s3a://tower-iti-project/gold/ran_ml_input/
        │
        ├── LightGBM inference (Airflow task: predict)
        │   └── s3://tower-iti-project/gold/ran_ml_predictions/YYYY-MM-DD_predictions.csv
        │
        ▼
   Snowflake External Tables
   YOUR_SNOWFLAKE_DATABASE.PUBLIC
   11 external tables (7 dims + 3 facts + FACT_ML_PREDICTIONS)
        │
        ├── Power BI Desktop
        ├── Cortex Analyst semantic model on Snowflake stage
        ├── EC2 Streamlit chat app (JWT key-pair auth)
        └── Snowflake Streamlit artifact export (TOWER_HEALTH_NOC)
```

### Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Batch orchestration | Airflow DAG `ran_pipeline` | Real EC2 DAG chains Silver, Gold, ML prep, prediction, and Snowflake external-table refresh |
| Silver processing | File-manifest incremental | `ran_telemetry_silver.py` records processed raw files in `_state/processed_files` |
| Dim tracking | Deterministic hashes | Gold script uses `md5(site_id)` for `site_sk` / `RAN_key`; SCD Type 2 behavior not visible in final EC2 Spark code <!-- TODO: verify --> |
| Failure prediction | LightGBM (batch) | Fast inference, handles tabular RAN data well |
| ML tracking | MLflow | Mentioned in draft, but no readable MLflow code was available <!-- TODO: verify --> |
| Snowflake access | External tables over S3 | No data duplication, trial account compatible |
| Chat hosting | EC2 Streamlit for JWT key-pair auth; Snowflake Streamlit export also exists | EC2 file `tower_health_streamlit.py` uses JWT; Snowflake export defines app package `TOWER_HEALTH_NOC` |

## 3. Tech Stack

| Layer | Technology | Version / Verified Detail |
|---|---|---|
| Storage | AWS S3 (`tower-iti-project`) | Raw, Silver, Gold BI, ML input, ML predictions |
| Compute | AWS EC2 c7i-flex.large | Ubuntu 24.04 |
| Processing | PySpark | Spark submit via Airflow, Hadoop AWS package `org.apache.hadoop:hadoop-aws:3.3.4` |
| Table format | Parquet external tables | Final EC2 Spark jobs write Parquet; Delta Lake not visible <!-- TODO: verify --> |
| ML — batch | LightGBM binary classifier | 284 features in `ran_cell_model_features.json` |
| ML — streaming | Isolation Forest | Mentioned in draft, but no readable streaming-anomaly code was found <!-- TODO: verify --> |
| ML tracking | MLflow | Mentioned in draft; no readable MLflow code found <!-- TODO: verify --> |
| Orchestration | Apache Airflow | EC2 config uses `SequentialExecutor`, SQLite metadata DB, manual DAG schedule |
| Warehouse | Snowflake | Account locator `YOUR_SNOWFLAKE_ACCOUNT`, warehouse `YOUR_SNOWFLAKE_WAREHOUSE`, database `YOUR_SNOWFLAKE_DATABASE`, schema `PUBLIC` |
| AI layer | Cortex Analyst REST API | `/api/v2/cortex/analyst/message` |
| Chat UI | Streamlit | EC2 JWT app plus Snowflake Streamlit artifact package |
| BI | Power BI Desktop | File documented as `finalITI.pbix` <!-- TODO: verify path --> |
| Language | Python | EC2 venv path `/home/ubuntu/towerhealth-env312`; Snowflake app Python `~=3.11.0` |

## 4. Infrastructure — EC2

### Connection

```bash
# SSH
ssh -i C:\Windows\system32\towerhealth-key.pem ubuntu@<public-dns>

# Airflow tunnel
ssh -i ...pem -L 9090:localhost:8080 ubuntu@<dns>

# Streamlit tunnel
ssh -i ...pem -L 8501:localhost:8501 ubuntu@<dns>
```

> **Note:** Public DNS changes on every EC2 restart. Previously documented DNS: `ec2-3-90-2-246.compute-1.amazonaws.com` <!-- TODO: verify current DNS -->

### Environment

| Item | Verified / Documented Value |
|---|---|
| Instance type | c7i-flex.large |
| OS | Ubuntu 24.04 |
| Python venv | `/home/ubuntu/towerhealth-env312` |
| Airflow home | `/home/ubuntu/airflow` |
| Airflow DAG folder | `/home/ubuntu/airflow/dags` |
| Airflow executor | `SequentialExecutor` |
| Airflow metadata DB | `sqlite:////home/ubuntu/airflow/airflow.db` |
| Airflow logs | `/home/ubuntu/airflow/logs` |
| Streamlit app | `/home/ubuntu/tower_health_streamlit.py` |
| Streamlit JWT key | `/home/ubuntu/.streamlit/rsa_key.p8` |
| Key file | `C:\Windows\system32\towerhealth-key.pem` |

### Run Streamlit

The EC2 app is a JWT key-pair Streamlit deployment. It reads Snowflake credentials from `~/.streamlit/secrets.toml` and the RSA private key from `/home/ubuntu/.streamlit/rsa_key.p8`.

```bash
source /home/ubuntu/towerhealth-env312/bin/activate
pkill -f "streamlit run"
streamlit run /home/ubuntu/tower_health_streamlit.py --server.port 8501
```

## 5. Data Generation

> **Verification note:** No data generator Python file was present in the EC2 backup. The generator behavior below is preserved from the original project documentation and should be verified if the generator source is downloaded. <!-- TODO: verify -->

### Source

Python script using `Faker` to generate realistic RAN telemetry. Produces synthetic records simulating Egyptian telecom sites across 4 regions: Alexandria, Cairo, Giza, North Sinai.

### Schema — Kafka Message (flat, per cell per 30 seconds)

**43 source-only fields** including:

| Category | Fields |
|---|---|
| Identity | `site_id`, `cell_id`, `timestamp`, `technology` (4G/5G) |
| RF signal chain | `rsrp`, `rsrq`, `sinr`, `cqi_avg`, `bler_dl`, `bler_ul` |
| Traffic | `dl_throughput_mbps`, `ul_throughput_mbps`, `prb_utilization_dl`, `prb_utilization_ul` |
| Equipment state | `ru_op_state`, `bbu_op_state`, `antenna_op_state`, `battery_level`, `vswr` |
| Alarms | `alarm_type`, `severity`, `alarm_count` |
| Environmental | `temperature_c`, `humidity_pct` |

### Domain Constraints Enforced

- Status cascade logic: if `site_status = DOWN` then all cell statuses = `DOWN`
- RF signal chain correlations: low RSRP → low CQI → high BLER
- Technology-specific ranges: 5G has higher throughput ceilings than 4G
- VSWR > 2.0 always triggers antenna alarm

---

## 6. Bronze Layer

**Configured raw landing path in the readable Silver job:** `s3a://tower-iti-project/raw-data/ran_telemetry/`
**Previously documented path:** `s3://tower-iti-project/bronze/` <!-- TODO: verify on EC2 if a separate `bronze/` prefix also exists -->
**Format:** Raw JSON files written by Kafka Connect / backfill, read with an explicit PySpark `StructType`
**Processing:** No transformation in the landing zone; `ran_telemetry_silver.py` performs the first normalization step
**Incremental state:** File-level manifest at `s3a://tower-iti-project/silver/ran_telemetry_normalized/_state/processed_files`

### Verified from `ran_telemetry_silver.py`

| Item | Real Value |
|---|---|
| Spark app name | `TowerHealth-Silver` |
| Raw input prefix | `s3a://tower-iti-project/raw-data/ran_telemetry/` |
| Silver output base | `s3a://tower-iti-project/silver/ran_telemetry_normalized` |
| Incremental method | Lists `.json` objects in S3, subtracts files already recorded in `_state/processed_files` |
| Write mode | `append` |
| Partitioning | `partitionBy("region")` on every Silver table |

The source schema is nested JSON, not a flat 43-column message by the time Silver reads it. Major nested structures are `ran_metadata`, `environment`, `antennas`, `baseband_units`, `transport_links`, `cells`, `radio_units`, `power_system`, `alerts`, and `alert_summary`.

## 7. Silver Layer

**Location:** `s3a://tower-iti-project/silver/ran_telemetry_normalized`
**Format:** Parquet
**Processing:** PySpark on EC2 / local Spark script
**Pattern:** File-manifest incremental processing. The job records processed raw file names in `_state/processed_files`; this is file-level idempotence, not event-time watermarking.

### 10 Normalized Tables Verified in `ran_telemetry_silver.py`

| Table | Grain / Description |
|---|---|
| `site_snapshot` | One row per site snapshot with site metadata, environment status, generator status, and alert totals |
| `environment_sensors` | One row per temperature or humidity sensor per snapshot; derives sensor `status` from thresholds |
| `cells` | One row per cell per snapshot with RF, throughput, latency, success-rate, BLER, HARQ, and handover KPIs |
| `antennas` | One row per antenna per snapshot with MIMO, azimuth, tilt, RSSI, and SNR |
| `radio_units` | One row per RU per snapshot; aliases `rx_signal_strength_dbm` to `rx_signal_dbm` |
| `baseband_units` | One row per BBU per snapshot with active users, CPU, memory, disk, and latency metrics |
| `batteries` | One row per battery per snapshot; raw battery data has charge and temperature, not voltage |
| `rectifiers` | One row per rectifier per snapshot with current and output voltage |
| `transport_links` | One row per backhaul link per snapshot; `link_type` is uppercased |
| `alerts` | One row per alert event; splits raw alert `value` into numeric and string fields |

### Silver Notes

- All tables are written with `mode("append")` and partitioned by `region`.
- `alerts` uses `explode_outer`, so snapshots without alerts are handled safely.
- Temperature sensor status is `CRITICAL` above 40 C and `HIGH` above 37 C; humidity status is `HIGH` above 80%.

## 8. Gold Layer — DWH

**BI location:** `s3a://tower-iti-project/gold/ran_telemetry_bi/`
**ML input location:** `s3a://tower-iti-project/gold/ran_ml_input/`
**ML prediction location:** `s3://tower-iti-project/gold/ran_ml_predictions/`
**Format:** Parquet for Spark-written BI/ML input tables; CSV for ML predictions
**Model:** Dimensional star schema exposed to Snowflake external tables

### Gold Spark Job Verified From EC2

| Item | Real Value |
|---|---|
| File | `/home/ubuntu/ml/ran_telemetry_gold.py` |
| Spark app name | `Telecom_RAN_Gold_Layer_Pipeline` |
| Silver input | `s3a://tower-iti-project/silver/ran_telemetry_normalized/` |
| Gold BI output | `s3a://tower-iti-project/gold/ran_telemetry_bi/` |
| Write format | Parquet |
| Write mode | `overwrite` |

### 7 Dimension Tables Written by Gold Spark

| Table | Output folder | Key / Notes |
|---|---|---|
| `dim_site` | `ran_telemetry_bi/dim_site/` | `site_sk = md5(site_id)` |
| `dim_cell` | `ran_telemetry_bi/dim_cell/` | `cell_sk`; includes `sector_id`, technology, bandwidth, frequency |
| `dim_date` | `ran_telemetry_bi/dim_date/` | Date attributes, `date_sk` / `DATE_SK` usage exposed via views and semantic YAML |
| `dim_time` | `ran_telemetry_bi/dim_time/` | Time-of-day attributes including hour/minute/day part |
| `dim_RU` | `ran_telemetry_bi/dim_RU/` | Radio unit dimension |
| `dim_Antenna` | `ran_telemetry_bi/dim_Antenna/` | Antenna dimension |
| `dim_Link` | `ran_telemetry_bi/dim_Link/` | Transport link dimension |

### 4 Fact Tables Exposed in Snowflake

| Table | Source | Grain | Key Notes |
|---|---|---|---|
| `Fact_Cells` | Gold Spark | Per cell snapshot / period | Cell KPIs; `RAN_key = md5(site_id)`; `sector_id` renamed to `sector_bk` |
| `Fact_RAN` | Gold Spark | Per site equipment snapshot / period | Equipment and environmental health; includes real `site_id` |
| `Fact_Alarms` | Gold Spark | Per alarm event | `site_key = md5(site_id)` exposed as `site_key` |
| `Fact_ML_Predictions` | `predict_s3.py` CSV output | Per site/cell prediction row | `CELL_ID` is `STRING`; `PREDICTION_DATE` derives from CSV filename |

### Key Design Notes

- The final EC2 Gold Spark script writes 7 dimension outputs and 3 BI fact outputs. ML predictions are produced by the downstream Python inference task and then exposed as the fourth fact in Snowflake.
- Earlier notes about separate `dim_sector` and `dim_technology` are not supported by the final EC2 Gold Spark file. The Airflow refresh task still attempts to refresh `DIM_SECTOR` and `DIM_TECHNOLOGY`, but catches and logs skipped tables if they do not exist.
- No Delta Lake writes were visible in the EC2 backup; the verified storage format is Parquet. <!-- TODO: verify if any older Delta tables exist outside the downloaded backup -->

## 9. ML Layer

### Batch — LightGBM Failure Prediction

| Property | Value |
|---|---|
| Model type | LightGBM binary classifier / Booster |
| Model file | `/home/ubuntu/ml/ran_cell_model.txt` |
| Feature metadata | `/home/ubuntu/ml/ran_cell_model_features.json` |
| Feature count | 284 `feature_columns` |
| Feature engineering metadata | 3 cells, 27 cell metric suffixes, 14 rolling KPIs, 5 lag KPIs, 3 delta KPIs, windows `[3, 6, 12, 24]`, lags `[1, 3, 6, 12]`, deltas `[1, 3]` |
| Input builder | `/home/ubuntu/ml/ran_cell_ml_prep.py` |
| ML prep Spark app | `TowerHealth-ML-Prep` |
| ML input source | `s3a://tower-iti-project/silver/ran_telemetry_normalized/cells` |
| ML input output | `s3a://tower-iti-project/gold/ran_ml_input/`, overwritten and partitioned by `gold_date` |
| Inference wrapper | `/home/ubuntu/ml/predict_s3.py` |
| Core inference file | `/home/ubuntu/ml/03_predict.py` |
| Output path pattern | `s3://tower-iti-project/gold/ran_ml_predictions/YYYY-MM-DD_predictions.csv` |
| Output columns | `timestamp`, `site_id`, `cell_id`, `failure_probability`, `predicted_failure`, `risk_level` |
| Snowflake external table | `FACT_ML_PREDICTIONS`, with `PREDICTION_DATE` derived from the CSV filename |

**Python runtime requirements verified from `/home/ubuntu/ml/requirements.txt`:**

```text
lightgbm>=4.0.0
pandas>=2.0.0
numpy>=1.24.0
pyarrow>=12.0.0
joblib>=1.3.0
boto3>=1.26.0
```

**Risk thresholds verified in `03_predict.py`:**

- `LOW`: `failure_probability < 0.30`
- `MEDIUM`: `0.30 <= failure_probability < 0.55`
- `HIGH`: `0.55 <= failure_probability < 0.75`
- `CRITICAL`: `failure_probability >= 0.75`

**Binary failure threshold:** defaults to metadata value or `0.5`, with optional CLI override `--threshold`.

### Airflow ML Execution

The `ran_pipeline` DAG runs ML as three tasks:

| Task | Purpose |
|---|---|
| `ml_prep` | Spark-submit `ran_cell_ml_prep.py` to generate latest partition under `gold/ran_ml_input/` |
| `resolve_partition` | Select latest `gold_date=YYYY-MM-DD` partition and build S3 input/output paths |
| `predict` | Run `predict_s3.py` with the LightGBM model and metadata, then upload predictions CSV |

### Streaming — Isolation Forest Anomaly Detection

The markdown describes an Isolation Forest streaming layer, but no readable `/home/ubuntu` streaming-anomaly file was available in the EC2 backup. <!-- TODO: verify -->

## 10. Snowflake Setup

### Account Details

| Property | Value |
|---|---|
| Account locator | `YOUR_SNOWFLAKE_ACCOUNT` |
| EC2 JWT app host | `YOUR_SNOWFLAKE_ACCOUNT.snowflakecomputing.com` |
| Snowflake-exported app URL form seen earlier | `YOUR_SNOWFLAKE_ACCOUNT.snowflakecomputing.com` |
| User used in EC2 app JWT subject | `YOUR_SNOWFLAKE_USER` |
| Warehouse | `YOUR_SNOWFLAKE_WAREHOUSE` |
| Database | `YOUR_SNOWFLAKE_DATABASE` |
| Schema | `PUBLIC` |
| Cortex endpoint | `/api/v2/cortex/analyst/message` |

### EC2 Streamlit Authentication — Verified JWT Key-Pair Flow

The EC2 `/home/ubuntu/tower_health_streamlit.py` file is the active JWT version.

| Step | Verified Implementation |
|---|---|
| Private key path | `/home/ubuntu/.streamlit/rsa_key.p8` |
| Key loading | `serialization.load_pem_private_key(...)` |
| Public key fingerprint | DER public key SHA-256 digest, base64 encoded, prefixed with `SHA256:` |
| JWT issuer | `YOUR_SNOWFLAKE_ACCOUNT.YOUR_SNOWFLAKE_USER.<fingerprint>` |
| JWT subject | `YOUR_SNOWFLAKE_ACCOUNT.YOUR_SNOWFLAKE_USER` |
| JWT lifetime | 55 minutes |
| JWT algorithm | `RS256` |
| Cortex authorization header | `Authorization: Bearer <jwt>` |
| Token type header | `X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT` |

The same EC2 app uses `snowflake.connector.connect(...)` with values from `st.secrets["snowflake"]` for direct SQL queries such as the NOC summary and generated Cortex SQL.

### Cortex Analyst Request Structure

```python
payload = {
    "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
    "semantic_model_file": "@YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE/tower_health_semantic_model.yaml",
}
```

The EC2 app posts to:

```text
https://YOUR_SNOWFLAKE_ACCOUNT.snowflakecomputing.com/api/v2/cortex/analyst/message
```

and handles Cortex content blocks of type `sql`, `text`, `error`, and `suggestions`.

### Snowflake Streamlit App Packaging

Verified from Snowflake-exported files in `Downloads`:

| File | Verified Detail |
|---|---|
| `snowflake.yml` | `definition_version: 2`; entity type `streamlit`; database `YOUR_SNOWFLAKE_DATABASE`; schema `PUBLIC`; app identifier name `UNTITLED`; title `TOWER_HEALTH_NOC` |
| `snowflake.yml` | `query_warehouse: YOUR_SNOWFLAKE_WAREHOUSE`; `compute_pool: SYSTEM_COMPUTE_POOL_CPU`; `run_mode: SpcsOnly`; `execute_as: OWNER`; `main_file: TOWER_HEALTH_NOC.py` |
| `snowflake.yml` artifacts | `pyproject.toml`, `TOWER_HEALTH_NOC.py`, `.streamlit/config.toml` |
| `pyproject.toml` | Python `~=3.11.0`; dependency `streamlit[snowflake]`; default Streamlit-in-Snowflake packages are used unless external access is configured |
| `config.toml` | Placeholder theme file only; no custom theme keys configured |

### Trial Account Limitations

| Feature | Status |
|---|---|
| External Access Integrations | Documented as blocked in draft; not verifiable from downloaded files <!-- TODO: verify --> |
| Cortex Analyst | Called through REST endpoint `/api/v2/cortex/analyst/message` |
| Active EC2 semantic stage | `@YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE/tower_health_semantic_model.yaml` |
| Upload helper stage | `upload_yaml.py` uses `@SEMANTIC_MODELS`; EC2 runtime app uses `@SEMANTIC_STAGE` |

## 11. Snowflake Objects

### External Tables (11) — over S3 Gold

| Table | Notes |
|---|---|
| `DIM_SITE` | Site dimension, surfaced to Cortex through `V_SITE` |
| `DIM_CELL` | Cell dimension, surfaced to Cortex through `V_CELL` |
| `DIM_DATE` | Date dimension, surfaced to Cortex through `V_DATE` |
| `DIM_TIME` | Time-of-day dimension |
| `DIM_RU` | Radio unit dimension |
| `DIM_ANTENNA` | Antenna dimension |
| `DIM_LINK` | Transport link dimension |
| `FACT_CELLS` | Cell performance KPIs, surfaced to Cortex through `V_CELL_PERFORMANCE` |
| `FACT_RAN` | Equipment health fact |
| `FACT_ALARMS` | Alarm events, surfaced to Cortex through `V_ALARM` |
| `FACT_ML_PREDICTIONS` | LightGBM prediction CSV external table |

### Views for Cortex Analyst

Verified from the semantic model physical tables:

| View / Table Used by YAML | Physical Name |
|---|---|
| `site` | `V_SITE` |
| `cell` | `V_CELL` |
| `date` | `V_DATE` |
| `cell_performance` | `V_CELL_PERFORMANCE` |
| `alarm` | `V_ALARM` |
| `radio_unit` | `DIM_RU` |
| `antenna` | `DIM_ANTENNA` |
| `link` | `DIM_LINK` |
| `time_of_day` | `DIM_TIME` |
| `equipment_health` | `FACT_RAN` |
| `ml_predictions` | `FACT_ML_PREDICTIONS` |

The actual `CREATE VIEW` statements were not present as `.sql` files in the EC2 backup. <!-- TODO: verify view DDL from Snowflake history or worksheet export -->

### NOC Summary Objects

#### `V_NOC_DAILY_SUMMARY` View

The EC2 Streamlit app queries:

```sql
SELECT * FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.V_NOC_DAILY_SUMMARY LIMIT 1
```

Fields consumed by the NOC card:

| Field | Usage |
|---|---|
| `HEADLINE` | Status headline in hero/NOC card |
| `SUMMARY_TEXT` | Human-readable operational summary |
| `GENERATED_AT` | Timestamp displayed as “Last updated” |
| `CRITICAL_ALARMS` | Critical alarm KPI |
| `TOTAL_ALARMS` | Total alarm KPI |
| `AVG_FAILURE_RISK_PCT` | Average predicted failure risk KPI |
| `HIGH_RISK_SITES` | Count of high-risk sites |
| `WORST_SITE` | Worst tower/site label |

The downloaded files do not include the `CREATE VIEW V_NOC_DAILY_SUMMARY` SQL definition. <!-- TODO: verify -->

#### `NOC_SUMMARY_NATIVE` Table

Native Snowflake table mentioned in the existing documentation for fast Power BI loading:

```sql
CREATE OR REPLACE TABLE NOC_SUMMARY_NATIVE AS
SELECT * FROM V_NOC_DAILY_SUMMARY;
```

The exact refresh worksheet for this table was not present in the EC2 backup. <!-- TODO: verify -->

### Semantic Model

| Property | Verified Value |
|---|---|
| Active EC2 stage path | `@YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE/tower_health_semantic_model.yaml` |
| Upload helper stage path | `@SEMANTIC_MODELS` in `upload_yaml.py` |
| File | `/home/ubuntu/tower_health_semantic_model.yaml` |
| Size | 909 lines / 35,131 bytes |
| Model name | `tower_health` |
| Logical tables | 11 |
| Relationships | 13 |
| Verified queries | 8 |

## 12. Airflow Orchestration

**Verified DAG folder:** `/home/ubuntu/airflow/dags/`
**Verified DAG file:** `/home/ubuntu/airflow/dags/ran_pipeline_dag.py`

### Airflow Runtime Configuration

Verified from the EC2 backup `airflow.cfg`:

| Setting | Value |
|---|---|
| `dags_folder` | `/home/ubuntu/airflow/dags` |
| `default_timezone` | `utc` |
| `executor` | `SequentialExecutor` |
| `sql_alchemy_conn` | `sqlite:////home/ubuntu/airflow/airflow.db` |
| `base_log_folder` | `/home/ubuntu/airflow/logs` |
| `load_examples` | `False` |

### DAG Definition

| Property | Value |
|---|---|
| DAG ID | `ran_pipeline` |
| Start date | `2026-01-01` |
| Schedule | `None` / manual trigger |
| Catchup | `False` |
| Retries | `0` |
| Tags | `towerhealth`, `ran` |

### Constants in DAG

| Constant | Value |
|---|---|
| Venv activation | `source ~/towerhealth-env312/bin/activate` |
| Spark packages | `org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262` |
| S3 bucket | `tower-iti-project` |
| ML input prefix | `gold/ran_ml_input/` |
| Prediction prefix | `gold/ran_ml_predictions/` |

### Task Graph

```text
silver ─┬─> gold ───────────────┐
        │                       ├─> refresh_snowflake
        └─> ml_prep -> resolve_partition -> predict ┘
```

Equivalent Airflow dependency expression:

```python
silver >> [gold, ml_prep]
ml_prep >> resolve >> predict
[gold, predict] >> refresh
```

### Task Details

| Task ID | Operator | Command / Callable | Purpose |
|---|---|---|---|
| `silver` | `BashOperator` | `spark-submit --packages ... ran_telemetry_silver.py` from `/opt/ml` | Normalize raw RAN JSON into Silver Parquet tables |
| `gold` | `BashOperator` | `spark-submit --packages ... ran_telemetry_gold.py` from `/opt/ml` | Build Gold dimensional BI tables |
| `ml_prep` | `BashOperator` | `spark-submit --packages ... ran_cell_ml_prep.py` from `/opt/ml` | Build hourly wide ML input rows partitioned by `gold_date` |
| `resolve_partition` | `PythonOperator` | `resolve_partition()` | Lists S3 partitions under `gold/ran_ml_input/`, chooses the latest `gold_date=YYYY-MM-DD`, returns input/output paths through XCom |
| `predict` | `BashOperator` | `python predict_s3.py --input {{ ti.xcom_pull(...)['input_path'] }} --output {{ ti.xcom_pull(...)['output_path'] }} --model /opt/ml/ran_cell_model.txt --meta /opt/ml/ran_cell_model_features.json` | Runs LightGBM batch inference and uploads CSV predictions |
| `refresh_snowflake` | `PythonOperator` | `_refresh_snowflake()` | Connects to Snowflake and refreshes external tables |

### `resolve_partition` Output Contract

For the latest partition, the task returns:

| XCom key | Example / Pattern |
|---|---|
| `date` | Latest `YYYY-MM-DD` parsed from `gold_date=YYYY-MM-DD/` |
| `input_path` | `s3://tower-iti-project/gold/ran_ml_input/gold_date=<date>/` |
| `output_path` | `s3://tower-iti-project/gold/ran_ml_predictions/<date>_predictions.csv` |

### Snowflake Refresh Logic

The DAG refreshes these external tables after Gold and prediction tasks complete:

```text
FACT_RAN, FACT_CELLS, FACT_ALARMS, FACT_ML_PREDICTIONS,
DIM_SITE, DIM_CELL, DIM_SECTOR, DIM_TECHNOLOGY, DIM_DATE, DIM_TIME,
DIM_RU, DIM_ANTENNA, DIM_LINK
```

`DIM_SECTOR` and `DIM_TECHNOLOGY` are attempted by the DAG but are not produced by the final EC2 Gold Spark script or Snowflake setup SQL; the DAG catches exceptions and logs skipped refreshes for missing tables.

### Retry Logic

The DAG-level `default_args` sets `retries: 0`. No task-specific retry override was found.

### Scheduler Evidence

The downloaded scheduler log shows Airflow successfully parsing and syncing `ran_pipeline` from `/home/ubuntu/airflow/dags/ran_pipeline_dag.py`. Because `schedule=None`, `next_dagrun` is `None` until a manual trigger is issued.

## 13. Cortex Analyst — Semantic Model

**File verified:** `/home/ubuntu/tower_health_semantic_model.yaml`
**Active EC2 Streamlit reference:** `@YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE/tower_health_semantic_model.yaml`

### Model Summary

| Property | Real Value |
|---|---|
| `name` | `tower_health` |
| File size | 909 lines / 35,131 bytes |
| Logical tables | 11 |
| Relationships | 13 |
| Verified queries | 8 |

### Logical Tables and Physical Tables

| Logical table | Physical table / view | Primary key in YAML |
|---|---|---|
| `site` | `V_SITE` | `site_sk` |
| `cell` | `V_CELL` | `cell_sk` |
| `radio_unit` | `DIM_RU` | `RU_sk` |
| `antenna` | `DIM_ANTENNA` | `antenna_sk` |
| `link` | `DIM_LINK` | `link_sk` |
| `date` | `V_DATE` | `date_sk` |
| `time_of_day` | `DIM_TIME` | `time_pk` |
| `cell_performance` | `V_CELL_PERFORMANCE` | Not declared as PK; fact table |
| `equipment_health` | `FACT_RAN` | Not declared as PK; fact table |
| `alarm` | `V_ALARM` | Not declared as PK; fact table |
| `ml_predictions` | `FACT_ML_PREDICTIONS` | Not declared as PK; prediction fact |

### Relationship Join Paths

| Relationship | Left table | Right table | Join keys | Join type | Relationship type |
|---|---|---|---|---|---|
| `cellperf_to_site` | `cell_performance` | `site` | `ran_key = site_sk` | `left_outer` | `many_to_one` |
| `cellperf_to_cell` | `cell_performance` | `cell` | `cell_key = cell_sk` | `left_outer` | `many_to_one` |
| `cellperf_to_date` | `cell_performance` | `date` | `date_key = date_sk` | `left_outer` | `many_to_one` |
| `cellperf_to_time` | `cell_performance` | `time_of_day` | `time_key = time_pk` | `left_outer` | `many_to_one` |
| `cell_to_site` | `cell` | `site` | `site_id = site_id` | `left_outer` | `many_to_one` |
| `ran_to_site` | `equipment_health` | `site` | `site_id = site_id` | `left_outer` | `many_to_one` |
| `ran_to_date` | `equipment_health` | `date` | `date_key = date_sk` | `left_outer` | `many_to_one` |
| `ran_to_time` | `equipment_health` | `time_of_day` | `time_key = time_pk` | `left_outer` | `many_to_one` |
| `alarm_to_site` | `alarm` | `site` | `site_key = site_sk` | `left_outer` | `many_to_one` |
| `alarm_to_date` | `alarm` | `date` | `date_key = date_sk` | `left_outer` | `many_to_one` |
| `alarm_to_time` | `alarm` | `time_of_day` | `time_key = time_pk` | `left_outer` | `many_to_one` |
| `predictions_to_site` | `ml_predictions` | `site` | `site_id = site_id` | `left_outer` | `many_to_one` |
| `predictions_to_date` | `ml_predictions` | `date` | `prediction_date = full_date` | `left_outer` | `many_to_one` |

### Verified Queries

| Name | Question | `verified_at` |
|---|---|---:|
| `alarms_by_region` | Show total alarms by region | `1749254400` |
| `avg_sinr_by_site` | Show average SINR by site | `1749254400` |
| `top_cells_call_drop` | Top 3 cells by call drop rate | `1749254400` |
| `top_cells_call_drop_ar` | Arabic: أعلى 3 خلايا في معدل انقطاع المكالمات | `1749254400` |
| `failure_prob_by_region` | Average failure probability by region | `1749254400` |
| `throughput_by_technology` | Show average downlink throughput by technology | `1749254400` |
| `high_risk_towers` | Which towers are high-risk this week? | `1749254400` |
| `call_drop_by_technology` | Show call drop rate by technology | `1749254400` |

### Exact `expr:` Field Mappings

#### `site` -> `V_SITE`

| Field | `expr` |
|---|---|
| `site_sk` | `SITE_SK` |
| `site_id` | `SITE_ID` |
| `site_name` | `SITE_NAME` |
| `region` | `REGION` |
| `vendor` | `VENDOR` |
| `latitude` | `LATITUDE` |
| `longitude` | `LONGITUDE` |
| `site_count` | `SITE_SK` |

#### `cell` -> `V_CELL`

| Field | `expr` |
|---|---|
| `cell_sk` | `CELL_SK` |
| `cell_id` | `CELL_ID` |
| `site_id` | `SITE_ID` |
| `sector_id` | `sector_id` |
| `technology` | `TECHNOLOGY` |
| `bandwidth_mhz` | `BANDWIDTH_MHZ` |
| `carrier_frequency_mhz` | `CARRIER_FREQUENCY_MHZ` |
| `cell_count` | `cell_sk` |

#### `radio_unit` -> `DIM_RU`

| Field | `expr` |
|---|---|
| `ru_sk` | `RU_sk` |
| `ru_id` | `ru_id` |
| `sector_id` | `sector_id` |
| `ru_count` | `ru_sk` |

#### `antenna` -> `DIM_ANTENNA`

| Field | `expr` |
|---|---|
| `antenna_sk` | `antenna_sk` |
| `antenna_id` | `antenna_id` |
| `sector_id` | `sector_id` |
| `mimo_layers` | `mimo_layers` |
| `azimuth_degree` | `azimuth_degree` |
| `tilt_degree` | `tilt_degree` |
| `antenna_count` | `antenna_sk` |

#### `link` -> `DIM_LINK`

| Field | `expr` |
|---|---|
| `link_sk` | `link_sk` |
| `link_id` | `link_id` |
| `link_type` | `link_type` |
| `link_count` | `link_sk` |

#### `date` -> `V_DATE`

| Field | `expr` |
|---|---|
| `date_sk` | `DATE_SK` |
| `year` | `YEAR` |
| `month` | `MONTH` |
| `month_name` | `MonthName` |
| `quarter` | `quarter` |
| `quarter_name` | `quarterName` |
| `day_of_month` | `DAY_OF_MONTH` |
| `day_of_week` | `DayOfWeek` |
| `week_of_year` | `WeekOfYear` |
| `full_date` | `FULL_DATE` |

#### `time_of_day` -> `DIM_TIME`

| Field | `expr` |
|---|---|
| `time_pk` | `time_pk` |
| `hour` | `Hour` |
| `minute` | `Minute` |
| `time_label` | `time_label` |
| `day_part` | `DayPart` |
| `business_hours` | `BusinessHours` |

#### `cell_performance` -> `V_CELL_PERFORMANCE`

| Field | `expr` |
|---|---|
| `cell_key` | `CELL_KEY` |
| `ran_key` | `RAN_KEY` |
| `date_key` | `DATE_KEY` |
| `time_key` | `TIME_KEY` |
| `cell_status` | `cell_status` |
| `op_state` | `op_state` |
| `congestion_flag` | `congestion_flag` |
| `snapshot_ts` | `timestamp` |
| `site_count` | `ran_key` |
| `cell_count` | `cell_key` |
| `rsrp_dbm` | `RSRP_DBM` |
| `rsrq_db` | `RSRQ_DB` |
| `sinr_db` | `SINR_DB` |
| `cqi_avg` | `CQI_AVG` |
| `spectral_efficiency_bps_per_hz` | `SPECTRAL_EFFICIENCY_BPS_PER_HZ` |
| `prb_utilization_percent` | `PRB_UTILIZATION_PERCENT` |
| `throughput_downlink_mbps` | `THROUGHPUT_DOWNLINK_MBPS` |
| `throughput_uplink_mbps` | `THROUGHPUT_UPLINK_MBPS` |
| `traffic_volume_gb` | `TRAFFIC_VOLUME_GB` |
| `latency_downlink_ms` | `LATENCY_DOWNLINK_MS` |
| `latency_uplink_ms` | `LATENCY_UPLINK_MS` |
| `rrc_connection_attempts` | `RRC_CONNECTION_ATTEMPTS` |
| `rrc_success_rate_percent` | `RRC_SUCCESS_RATE_PERCENT` |
| `erab_setup_success_rate_percent` | `ERAB_SETUP_SUCCESS_RATE_PERCENT` |
| `call_drop_rate_percent` | `CALL_DROP_RATE_PERCENT` |
| `abnormal_release_rate_percent` | `abnormal_release_rate_percent` |
| `bler_downlink_percent` | `BLER_DOWNLINK_PERCENT` |
| `bler_uplink_percent` | `BLER_UPLINK_PERCENT` |
| `harq_retransmission_rate_percent` | `harq_retransmission_rate_percent` |
| `handover_attempts` | `handover_attempts` |
| `handover_failures` | `handover_failures` |
| `handover_success_rate_percent` | `HANDOVER_SUCCESS_RATE_PERCENT` |
| `connected_users` | `connected_users` |
| `active_users` | `active_users` |
| `peak_users` | `peak_users` |
| `snapshot_count` | `1` |

#### `equipment_health` -> `FACT_RAN`

| Field | `expr` |
|---|---|
| `site_id` | `SITE_ID` |
| `ran_key` | `RAN_KEY` |
| `date_key` | `DATE_KEY` |
| `time_key` | `TIME_KEY` |
| `power_status` | `power_status` |
| `env_status` | `env_status` |
| `smoke_detected` | `smoke_detected` |
| `door_status` | `door_status` |
| `generator_status` | `generator_status` |
| `snapshot_ts` | `timestamp` |
| `bbu_cpu_utilization_percent` | `BBU_cpu_utilization_percent` |
| `bbu_memory_utilization_percent` | `BBU_memory_utilization_percent` |
| `battery1_charge_percent` | `battery1_charge_percent` |
| `rect1_output_voltage_volt` | `rect1_output_voltage_volt` |
| `ru1_temperature_c` | `RU1_temperature_c` |
| `ru2_temperature_c` | `RU2_temperature_c` |
| `ru3_temperature_c` | `RU3_temperature_c` |
| `ru1_vswr` | `RU1_vswr` |
| `ru2_vswr` | `RU2_vswr` |
| `ru3_vswr` | `RU3_vswr` |
| `temp_sensor1_value_c` | `temp_sensor1_value_c` |
| `humidity_percent` | `Humd_sensor_value_percent` |
| `gen_fuel_level_percent` | `gen_fuel_level_percent` |
| `link1_latency_ms` | `link1_latency_ms` |
| `link1_packet_loss_percent` | `link1_packet_loss_percent` |
| `snapshot_count` | `1` |

#### `alarm` -> `V_ALARM`

| Field | `expr` |
|---|---|
| `site_key` | `SITE_KEY` |
| `component_key` | `COMPONENT_KEY` |
| `severity` | `SEVERITY` |
| `alarm_category` | `ALARM_CATEGORY` |
| `alarm_msg` | `ALARM_MSG` |
| `date_key` | `DATE_KEY` |
| `time_key` | `TIME_KEY` |
| `alarm_ts` | `SNAPSHOT_TIME` |
| `alarm_count` | `1` |

#### `ml_predictions` -> `FACT_ML_PREDICTIONS`

| Field | `expr` |
|---|---|
| `site_id` | `SITE_ID` |
| `cell_id` | `CELL_ID` |
| `predicted_failure` | `PREDICTED_FAILURE` |
| `risk_level` | `RISK_LEVEL` |
| `prediction_date` | `PREDICTION_DATE` |
| `observation_ts` | `TIMESTAMP_COL` |
| `failure_probability` | `FAILURE_PROBABILITY` |
| `prediction_count` | `1` |

### Important Semantic Notes

- The final EC2 semantic model uses Snowflake views for `site`, `cell`, `date`, `cell_performance`, and `alarm`, while using physical external tables directly for `DIM_RU`, `DIM_ANTENNA`, `DIM_LINK`, `DIM_TIME`, `FACT_RAN`, and `FACT_ML_PREDICTIONS`.
- Several mappings are intentionally mixed case, for example `MonthName`, `DayOfWeek`, `time_pk`, and `BBU_cpu_utilization_percent`. These should not be normalized without changing the underlying Snowflake objects.
- The Arabic verified query is explicitly present as `top_cells_call_drop_ar`; bilingual support is also handled in the Streamlit app with Arabic Unicode detection.

## 14. Streamlit App

### EC2 Streamlit App

| Property | Verified Value |
|---|---|
| File | `/home/ubuntu/tower_health_streamlit.py` |
| Header | `EC2 deployment | v7.0 — JWT key-pair auth` |
| Runtime requirements | `~/.streamlit/secrets.toml` and `/home/ubuntu/.streamlit/rsa_key.p8` |
| Snowflake host | `YOUR_SNOWFLAKE_ACCOUNT.snowflakecomputing.com` |
| Cortex endpoint | `/api/v2/cortex/analyst/message` |
| Semantic model file | `@YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE/tower_health_semantic_model.yaml` |

### Features Verified in EC2 Code

| Feature | Details |
|---|---|
| NOC summary card | Queries `SELECT * FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.V_NOC_DAILY_SUMMARY LIMIT 1` through `snowflake.connector.DictCursor` |
| NOC KPI fields | `HEADLINE`, `SUMMARY_TEXT`, `GENERATED_AT`, `CRITICAL_ALARMS`, `TOTAL_ALARMS`, `AVG_FAILURE_RISK_PCT`, `HIGH_RISK_SITES`, `WORST_SITE` |
| Direct SQL execution | `run_query(sql)` executes generated Cortex SQL through Snowflake connector and returns dictionaries |
| Cortex Analyst chat | Posts JWT-authenticated message payload to `/api/v2/cortex/analyst/message` and executes returned SQL blocks |
| Cortex block handling | Handles response block types `sql`, `text`, `error`, and `suggestions` |
| Auth | `make_jwt()` builds RS256 JWT from the RSA key pair; Cortex header uses `Bearer` token plus `KEYPAIR_JWT` token type |
| Bilingual detection | `is_arabic(text)` returns `True` when regex `r'[\u0600-\u06FF]'` matches Arabic characters |
| UI suggestions | Displays suggestion pills and a suggestions box |

### Cortex Request Payload Verified in EC2 App

```python
{
    "messages": [
        {"role": "user", "content": [{"type": "text", "text": question}]}
    ],
    "semantic_model_file": "@YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE/tower_health_semantic_model.yaml",
}
```

### Suggested Questions in EC2 Code

```text
How many total sites exist across the network?
Show average SINR by site
Top 3 cells by call drop rate
Which towers are high-risk this week?
Average failure probability by region
```

### Snowflake Streamlit Export

A separate Snowflake-exported app package was also provided:

| File | Role |
|---|---|
| `snowflake.yml` | Defines app title `TOWER_HEALTH_NOC`, `main_file: TOWER_HEALTH_NOC.py`, `query_warehouse: YOUR_SNOWFLAKE_WAREHOUSE`, `compute_pool: SYSTEM_COMPUTE_POOL_CPU`, `run_mode: SpcsOnly` |
| `TOWER_HEALTH_NOC.py` | Snowflake-native app file using active Snowflake/Snowpark session token rather than EC2 JWT |
| `pyproject.toml` | Python `~=3.11.0`, dependency `streamlit[snowflake]` |
| `config.toml` | Placeholder Streamlit theme config |

### Theme

The Snowflake-exported app uses the same dark NOC visual style documented earlier:

| Element | Color |
|---|---|
| Background | `#0D1821` |
| Cards | `#1B2A3A` |
| Gold accent | `#C9A86C` |
| Text primary | `#E8EDF2` |
| Text muted | `#8FA3B8` |

## 15. Power BI Dashboard

> **Verification note:** The PBIX file was not parsed in this pass. The dashboard details below are preserved from the original documentation and should be verified against `finalITI.pbix`. <!-- TODO: verify -->

**Pages:** 5
**DAX measures:** 36
**Snowflake chart views:** 17
**File:** `finalITI.pbix`

### Pages

| Page | Business Question |
|---|---|
| Overview | What is the network status right now? |
| Reliability | Is our network becoming more or less reliable? |
| Radio | Is radio performance improving or degrading? |
| User Experience | Is service quality improving or deteriorating? |
| Capacity | Where will we run out of capacity? |

### Overview Page Layout

```
┌─────────────────────────────────────────────────────────────────┐
│ 🔴 Network under stress — immediate NOC attention required.      │
│ Latest day: 364 critical alarms across 729 total...             │
└─────────────────────────────────────────────────────────────────┘
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────────┐
│ Critical     │ │ Total        │ │ Avg Failure  │ │ High-Risk  │
│ Alarms       │ │ Alarms       │ │ Risk         │ │ Towers     │
│ 364          │ │ 729          │ │ 47.6%        │ │ 4          │
└──────────────┘ └──────────────┘ └──────────────┘ └────────────┘
┌──────────────────────┐ ┌─────────────────────────────────────────┐
│ Ask the Network      │ │ Failure Risk by Tower (bar chart)        │
│ [AI Chat Button]     │ │ KS_TOWER_04 ████████████ 68%            │
│ localhost:8501       │ │ GZ_TOWER_03 ██████████   54%            │
└──────────────────────┘ └─────────────────────────────────────────┘
┌─────────────────────────────┐ ┌──────────────────────────────────┐
│ Alarms by Region (bar)      │ │ Network Health Trend (line)       │
│ FACT_ALARMS + DIM_SITE      │ │ FACT_RAN component_health_score  │
└─────────────────────────────┘ └──────────────────────────────────┘
```

### Color Palette

| Element | Color |
|---|---|
| Page background | `#0B1020` |
| Card background | `#FFFFFF` |
| KPI accent bar | `#29B6F6` |
| Critical red | `#C0392B` |
| Warning amber | `#C2832A` |
| Good green | `#2D7A4C` |
| Title text | `#1565C0` |
| Value text | `#0D1E2E` |
| AI button fill | `#C9A86C` |
| AI button text | `#0D1821` |
| Nav active border | `#29B6F6` |

### Existing Measures (from pbix analysis)

**UX page:**
- `Call Drop Rate Pct`
- `RRC Setup Success Pct`
- `Handover Success Pct`
- `Avg Latency DL`
- `Avg Latency UL`
- `Total Handover Attempts`
- `Volume-Weighted RRC Rate`
-
- `percentHandover Success Pct` <!-- TODO: verify measure name -->

**Radio page:**
- `Spectral Efficiency`
- `Avg SINR`
- `BLER DL Pct`
- `Peak PRB Utilization`
- `PRB Utilization Pct`

### Overview Page — Chart Data Sources

| Visual | Type | Table | Field |
|---|---|---|---|
| Banner | Text box / Card | `NOC_SUMMARY_NATIVE` | `SUMMARY_TEXT` |
| Critical Alarms card | Card | `NOC_SUMMARY_NATIVE` | `CRITICAL_ALARMS` |
| Total Alarms card | Card | `NOC_SUMMARY_NATIVE` | `TOTAL_ALARMS` |
| Avg Failure Risk card | Card | `NOC_SUMMARY_NATIVE` | `AVG_FAILURE_RISK_PCT` |
| High-Risk Towers card | Card | `NOC_SUMMARY_NATIVE` | `HIGH_RISK_SITES` |
| AI Chat button | Button | — | Action → `http://localhost:8501` |
| Failure Risk bars | Horizontal bar | `FACT_ML_PREDICTIONS` | Axis: `SITE_ID`, Value: `FAILURE_PROBABILITY` |
| Alarms by Region | Column chart | `FACT_ALARMS` + `DIM_SITE` | Axis: `region`, Value: Count of `alarm_id` |
| Network Health Trend | Line chart | `FACT_RAN` | Axis: `DIM_DATE[Date]`, Value: Avg `component_health_score` |

### Relationships Needed

```
FACT_ALARMS[site_key]   → DIM_SITE[site_sk]
FACT_CELLS[RAN_key]     → DIM_SITE[site_sk]
FACT_RAN[site_id]       → DIM_SITE[site_id]
FACT_ALARMS[date_key]   → DIM_DATE[date_key]
FACT_CELLS[date_key]    → DIM_DATE[date_key]
```

---

## 16. Key SQL Objects

### SQL Files Verified

| SQL file | Status |
|---|---|
| `tower_health_gold_setup.sql` / `tower_health_gold_setup (1).sql` | Verified from Snowflake/downloaded files. Contains Snowflake external table setup and refresh statements; newer exported copy defines `Fact_ML_Predictions.CELL_ID` as `STRING`. |
| `/home/ubuntu/**/*.sql` | No `.sql` files were found in the downloaded EC2 backup `tower_health_full_backup`. View definitions remain unverified from disk. <!-- TODO: verify --> |

### `tower_health_gold_setup.sql` — External Table DDL

**Context:**

```sql
USE DATABASE YOUR_SNOWFLAKE_DATABASE;
USE SCHEMA PUBLIC;
CREATE OR REPLACE FILE FORMAT TOWER_PARQUET_FMT TYPE = PARQUET;
```

**Parquet external tables created via `INFER_SCHEMA`:**

| External table | S3 stage location | File format | Pattern | Auto refresh |
|---|---|---|---|---|
| `dim_site` | `@GOLD_S3_STAGE/ran_telemetry_bi/dim_site/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `dim_cell` | `@GOLD_S3_STAGE/ran_telemetry_bi/dim_cell/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `dim_date` | `@GOLD_S3_STAGE/ran_telemetry_bi/dim_date/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `dim_time` | `@GOLD_S3_STAGE/ran_telemetry_bi/dim_time/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `dim_RU` | `@GOLD_S3_STAGE/ran_telemetry_bi/dim_RU/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `dim_Antenna` | `@GOLD_S3_STAGE/ran_telemetry_bi/dim_Antenna/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `dim_Link` | `@GOLD_S3_STAGE/ran_telemetry_bi/dim_Link/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `Fact_RAN` | `@GOLD_S3_STAGE/ran_telemetry_bi/Fact_RAN/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `Fact_Cells` | `@GOLD_S3_STAGE/ran_telemetry_bi/Fact_Cells/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |
| `Fact_Alarms` | `@GOLD_S3_STAGE/ran_telemetry_bi/Fact_Alarms/` | `TOWER_PARQUET_FMT` | `.*[.]parquet` | `FALSE` |

Each Parquet table follows this structure:

```sql
CREATE OR REPLACE EXTERNAL TABLE <table_name>
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION => '@GOLD_S3_STAGE/ran_telemetry_bi/<table_name>/',
      FILE_FORMAT => 'TOWER_PARQUET_FMT'))
  )
  LOCATION = @GOLD_S3_STAGE/ran_telemetry_bi/<table_name>/
  FILE_FORMAT = TOWER_PARQUET_FMT
  AUTO_REFRESH = FALSE
  PATTERN = '.*[.]parquet';
```

### ML Predictions External Table

```sql
CREATE OR REPLACE FILE FORMAT CSV_PREDICTIONS
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'null', 'NULL')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE EXTERNAL TABLE Fact_ML_Predictions (
    TIMESTAMP_COL         TIMESTAMP_NTZ  AS (TO_TIMESTAMP_NTZ(VALUE:c1::STRING)),
    SITE_ID               STRING         AS (VALUE:c2::STRING),
    CELL_ID               STRING         AS (VALUE:c3::STRING),
    FAILURE_PROBABILITY   FLOAT          AS (VALUE:c4::FLOAT),
    PREDICTED_FAILURE     SMALLINT       AS (VALUE:c5::SMALLINT),
    RISK_LEVEL            STRING         AS (VALUE:c6::STRING),
    PREDICTION_DATE       DATE           AS (TO_DATE(LEFT(SPLIT_PART(METADATA$FILENAME, '/', -1), 10)))
)
  WITH LOCATION = @GOLD_S3_STAGE/ran_ml_predictions/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_PREDICTIONS')
  PATTERN = '.*_predictions[.]csv'
  AUTO_REFRESH = FALSE;
```

### View Definitions

No `.sql` files in the EC2 backup contained `CREATE VIEW` statements. The following objects are referenced by app/semantic/docs but their DDL was not available on disk:

| View / Table | Status |
|---|---|
| `V_NOC_DAILY_SUMMARY` | Queried by EC2 Streamlit NOC card; DDL not present <!-- TODO: verify --> |
| `NOC_SUMMARY_NATIVE` | Mentioned for Power BI fast load; DDL not present in backup <!-- TODO: verify --> |
| `V_SITE` | Referenced by semantic YAML; DDL not present <!-- TODO: verify --> |
| `V_CELL` | Referenced by semantic YAML; DDL not present <!-- TODO: verify --> |
| `V_DATE` | Referenced by semantic YAML; DDL not present <!-- TODO: verify --> |
| `V_CELL_PERFORMANCE` | Referenced by semantic YAML; DDL not present <!-- TODO: verify --> |
| `V_ALARM` | Referenced by semantic YAML; DDL not present <!-- TODO: verify --> |

### Refresh Statements

The Snowflake SQL file refreshes the 11 external tables defined in the setup script:

```sql
ALTER EXTERNAL TABLE Fact_RAN            REFRESH;
ALTER EXTERNAL TABLE Fact_Cells          REFRESH;
ALTER EXTERNAL TABLE Fact_Alarms         REFRESH;
ALTER EXTERNAL TABLE Fact_ML_Predictions REFRESH;
ALTER EXTERNAL TABLE dim_site            REFRESH;
ALTER EXTERNAL TABLE dim_cell            REFRESH;
ALTER EXTERNAL TABLE dim_date            REFRESH;
ALTER EXTERNAL TABLE dim_time            REFRESH;
ALTER EXTERNAL TABLE dim_RU              REFRESH;
ALTER EXTERNAL TABLE dim_Antenna         REFRESH;
ALTER EXTERNAL TABLE dim_Link            REFRESH;
```

The Airflow DAG refresh task also attempts `DIM_SECTOR` and `DIM_TECHNOLOGY`; those objects were not present in the setup SQL and are handled as skipped failures in the DAG.

## 17. Diagnostic Queries

```sql
-- NOC banner
SELECT * FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.V_NOC_DAILY_SUMMARY LIMIT 1;

-- NOC native table (fast Power BI source)
SELECT * FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.NOC_SUMMARY_NATIVE;

-- Confirm all views exist
SHOW VIEWS IN SCHEMA YOUR_SNOWFLAKE_DATABASE.PUBLIC;

-- Confirm external tables exist
SHOW EXTERNAL TABLES IN SCHEMA YOUR_SNOWFLAKE_DATABASE.PUBLIC;

-- Cortex Analyst active semantic stage used by EC2 app
LIST @YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE;

-- Optional upload-helper stage used by upload_yaml.py
LIST @YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_MODELS;

-- ML predictions check
SELECT RISK_LEVEL, COUNT(*)
FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.FACT_ML_PREDICTIONS
GROUP BY 1
ORDER BY 2 DESC;

-- Prediction freshness by filename-derived date
SELECT PREDICTION_DATE, COUNT(*)
FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.FACT_ML_PREDICTIONS
GROUP BY 1
ORDER BY 1 DESC;

-- Alarm severity breakdown
SELECT SEVERITY, COUNT(*) AS cnt
FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.V_ALARM
GROUP BY 1
ORDER BY cnt DESC;
```

## 18. Defense Framing

### On EC2 Streamlit vs Snowflake Streamlit

> "The operational chat app has two deployment paths. The EC2 version is verified from `/home/ubuntu/tower_health_streamlit.py` and uses Snowflake key-pair JWT authentication for Cortex Analyst. The Snowflake export is also present as `TOWER_HEALTH_NOC`, packaged with `snowflake.yml`, `TOWER_HEALTH_NOC.py`, `pyproject.toml`, and `.streamlit/config.toml`."

### On NOC Summary card logic

> "The NOC card reads a single curated Snowflake object, `V_NOC_DAILY_SUMMARY`, and displays critical alarms, total alarms, average failure risk, high-risk tower count, and worst site. This keeps the dashboard fast while preserving the operational KPIs needed by a NOC user."

### On the semantic model

> "Cortex Analyst is grounded in a YAML semantic model with 11 logical tables, 13 relationships, and 8 verified queries. The model maps business-friendly names like `avg_sinr_by_site` and `high_risk_towers` to exact Snowflake views and external tables."

### On Airflow orchestration

> "The pipeline is orchestrated as one manual Airflow DAG named `ran_pipeline`. It runs Silver normalization, Gold dimensional modeling, ML feature preparation, latest-partition resolution, LightGBM inference, and finally Snowflake external table refresh."

### On hardcoded or missing SQL view definitions

> "The Spark, Airflow, Streamlit, semantic YAML, and Snowflake external table setup are verified from files. The only remaining unverified part is the DDL for helper views like `V_NOC_DAILY_SUMMARY` and `V_CELL_PERFORMANCE`, because no `.sql` view-definition files were included in the EC2 backup."

### On synthetic data

> "The data generator enforces telecom-domain constraints such as signal-chain correlations, status cascade behavior, and technology-specific KPI ranges. The data is synthetic, but the engineering path mirrors a production batch analytics pipeline."

## 19. File Inventory

**EC2 backup source used for verification:** `C:\Users\A-bsy\Downloads\tower_health_full_backup`
**Represents target path:** `/home/ubuntu/`

| File | Location | Description |
|---|---|---|
| `tower_health_streamlit.py` | `/home/ubuntu/` | EC2 Streamlit v7 app using Snowflake JWT key-pair auth, NOC summary card, Cortex Analyst chat, and Arabic detection |
| `tower_health_semantic_model.yaml` | `/home/ubuntu/` | Cortex Analyst semantic model with 11 logical tables, 13 relationships, 8 verified queries, and exact Snowflake mappings |
| `upload_yaml.py` | `/home/ubuntu/` | Helper script that uploads the semantic YAML to a Snowflake stage named `SEMANTIC_MODELS`; EC2 runtime app points to `SEMANTIC_STAGE` |
| `ran_pipeline_dag.py` | `/home/ubuntu/airflow/dags/` | Real Airflow DAG `ran_pipeline` chaining Silver, Gold, ML prep, prediction, and Snowflake refresh |
| `airflow.cfg` | `/home/ubuntu/airflow/` | Airflow runtime config: SequentialExecutor, SQLite DB, `/home/ubuntu/airflow/dags`, no examples |
| `webserver_config.py` | `/home/ubuntu/airflow/` | Airflow webserver configuration file |
| `03_predict.py` | `/home/ubuntu/ml/` | Core LightGBM inference script with feature alignment and LOW/MEDIUM/HIGH/CRITICAL risk labels |
| `predict_s3.py` | `/home/ubuntu/ml/` | S3-aware wrapper that downloads ML input Parquet and uploads predictions CSV |
| `ran_cell_ml_prep.py` | `/home/ubuntu/ml/` | Spark job `TowerHealth-ML-Prep` building hourly wide ML input rows from Silver `cells` |
| `ran_telemetry_gold.py` | `/home/ubuntu/ml/` | Spark Gold job `Telecom_RAN_Gold_Layer_Pipeline` writing dimensional BI Parquet outputs |
| `ran_telemetry_silver.py` | `/home/ubuntu/ml/` | Spark Silver job `TowerHealth-Silver` normalizing raw nested JSON into 10 Parquet tables |
| `ran_cell_model.txt` | `/home/ubuntu/ml/` | Trained LightGBM model file used by the Airflow prediction task |
| `ran_cell_model_features.json` | `/home/ubuntu/ml/` | Model metadata and 284-column feature contract |
| `requirements.txt` | `/home/ubuntu/ml/` | Python dependencies for LightGBM inference and S3/Parquet handling |
| `TOWER_HEALTH_NOC.py` | Snowflake app export in Downloads | Snowflake-native Streamlit app artifact using active Snowflake session token |
| `tower_health_gold_setup (1).sql` | Snowflake export in Downloads | Snowflake file formats, 11 external tables, ML predictions table, validation queries, and refresh statements |
| `snowflake.yml` | Snowflake app export in Downloads | Defines Streamlit app entity `TOWER_HEALTH_NOC`, main file, warehouse, compute pool, and artifacts |
| `pyproject.toml` | Snowflake app export in Downloads | Python/package metadata for Streamlit in Snowflake; depends on `streamlit[snowflake]` |
| `config.toml` | Snowflake app export in Downloads | Placeholder Streamlit theme config with no custom keys |
| `finalITI.pbix` | local / Power BI export <!-- TODO: verify exact path --> | Power BI dashboard file |
| `rsa_key.p8` | `/home/ubuntu/.streamlit/` | RSA private key referenced by EC2 Streamlit app; not included in backup listing <!-- TODO: verify existence on EC2 --> |
| `secrets.toml` | `/home/ubuntu/.streamlit/` | Snowflake connector secrets referenced by EC2 Streamlit app; not included in backup listing <!-- TODO: verify existence on EC2 --> |

## 20. Status Tracker

### Completed / Verified From Readable Files

- [x] EC2 backup downloaded and used as `/home/ubuntu` source of truth: `C:\Users\A-bsy\Downloads\tower_health_full_backup`
- [x] Silver PySpark job verified: raw nested JSON -> 10 normalized Parquet tables
- [x] Gold PySpark job verified: 7 dimensions + 3 BI facts written to `s3a://tower-iti-project/gold/ran_telemetry_bi/`
- [x] ML prep and inference verified: `ran_cell_ml_prep.py`, `predict_s3.py`, `03_predict.py`, model file, and feature metadata
- [x] Airflow DAG verified: `ran_pipeline`, manual schedule, no retries, real task graph and Snowflake refresh logic
- [x] EC2 Streamlit app verified: JWT key-pair auth, Cortex Analyst REST call, NOC card fields, Arabic Unicode detection
- [x] Semantic model YAML verified: 11 logical tables, 13 relationships, 8 verified queries, all `expr:` mappings documented
- [x] Snowflake Streamlit packaging verified from `snowflake.yml`, `pyproject.toml`, `config.toml`, and `TOWER_HEALTH_NOC.py`
- [x] Snowflake external table setup verified from `tower_health_gold_setup (1).sql`
- [x] Code inventory added for every Python/SQL file found in the EC2 backup

### Remaining TODO / Needs Verification

- [ ] SQL DDL for helper views such as `V_NOC_DAILY_SUMMARY`, `V_SITE`, `V_CELL`, `V_DATE`, `V_CELL_PERFORMANCE`, and `V_ALARM` was not present in the EC2 backup. <!-- TODO: verify -->
- [ ] `NOC_SUMMARY_NATIVE` refresh SQL was documented but not present as a downloaded file. <!-- TODO: verify -->
- [ ] Isolation Forest / streaming anomaly implementation was mentioned in the original markdown, but no readable code file was found in the EC2 backup. <!-- TODO: verify -->
- [ ] MLflow tracking was mentioned in the original markdown, but no readable MLflow code was found. <!-- TODO: verify -->
- [ ] Power BI file path, report pages, and DAX measures remain based on existing documentation rather than a parsed PBIX export. <!-- TODO: verify -->
- [ ] EC2 private secrets files (`rsa_key.p8`, `secrets.toml`) are referenced by code but were not included in the backup listing. <!-- TODO: verify -->

### Next Steps

1. Export Snowflake worksheets or `GET_DDL` output for the missing views and add them to section 16.
2. Verify the PBIX file directly if Power BI model metadata is needed for defense-level accuracy.
3. Confirm whether streaming anomaly and MLflow components exist outside the downloaded backup.

## 21. Code Inventory

**Scope:** Python and SQL files found in the downloaded `/home/ubuntu` backup at `C:\Users\A-bsy\Downloads\tower_health_full_backup`.

No `.sql` files were found inside the EC2 backup. SQL setup was verified from the separate Snowflake-exported `tower_health_gold_setup (1).sql` file.

| Filename | Size KB | One-line description |
|---|---:|---|
| `/home/ubuntu/airflow/dags/ran_pipeline_dag.py` | 3.6 | Airflow DAG `ran_pipeline` for Silver, Gold, ML prep, prediction, and Snowflake external table refresh |
| `/home/ubuntu/airflow/webserver_config.py` | 4.7 | Airflow webserver configuration file |
| `/home/ubuntu/ml/03_predict.py` | 25.7 | LightGBM inference engine with feature alignment, probability scoring, and risk-level classification |
| `/home/ubuntu/ml/predict_s3.py` | 4.8 | S3 wrapper that downloads Parquet ML input, runs `03_predict.py`, and uploads predictions CSV |
| `/home/ubuntu/ml/ran_cell_ml_prep.py` | 6.9 | PySpark job that builds hourly wide ML feature input from Silver cell telemetry |
| `/home/ubuntu/ml/ran_telemetry_gold.py` | 31.5 | PySpark Gold-layer dimensional model builder for BI facts and dimensions |
| `/home/ubuntu/ml/ran_telemetry_silver.py` | 22.3 | PySpark Silver-layer normalizer from raw nested RAN JSON to 10 Parquet tables |
| `/home/ubuntu/tower_health_streamlit.py` | 18.5 | EC2 Streamlit NOC/Cortex app using Snowflake JWT key-pair auth |
| `/home/ubuntu/upload_yaml.py` | 0.6 | Snowflake connector helper that uploads `tower_health_semantic_model.yaml` to a semantic model stage |
