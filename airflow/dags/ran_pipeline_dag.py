import os
from datetime import datetime

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

VENV = "source ~/towerhealth-env312/bin/activate"
PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
BUCKET = "tower-iti-project"
ML_INPUT_PREFIX = "gold/ran_ml_input/"
PRED_PREFIX = "gold/ran_ml_predictions/"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def resolve_partition(**context):
    import boto3
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=ML_INPUT_PREFIX, Delimiter="/")
    dates = []
    for p in resp.get("CommonPrefixes", []):
        part = p["Prefix"].rstrip("/").split("/")[-1]
        if part.startswith("gold_date="):
            dates.append(part.split("=", 1)[1])
    if not dates:
        raise ValueError(f"No gold_date= partitions under s3://{BUCKET}/{ML_INPUT_PREFIX}")
    latest = sorted(dates)[-1]
    return {
        "date": latest,
        "input_path": f"s3://{BUCKET}/{ML_INPUT_PREFIX}gold_date={latest}/",
        "output_path": f"s3://{BUCKET}/{PRED_PREFIX}{latest}_predictions.csv",
    }


def _refresh_snowflake(**context):
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "rmb62104"),
        user=os.getenv("SNOWFLAKE_USER", "towerproject"),
        password=require_env("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TOWER_HEALTH_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )
    cur = conn.cursor()
    tables = [
        'FACT_RAN', 'FACT_CELLS', 'FACT_ALARMS', 'FACT_ML_PREDICTIONS',
        'DIM_SITE', 'DIM_CELL', 'DIM_SECTOR', 'DIM_TECHNOLOGY',
        'DIM_DATE', 'DIM_TIME', 'DIM_RU', 'DIM_ANTENNA', 'DIM_LINK',
    ]
    for tbl in tables:
        try:
            cur.execute(f'ALTER EXTERNAL TABLE {tbl} REFRESH')
            print(f'{tbl} refreshed')
        except Exception as e:
            print(f'{tbl} skipped - {e}')
    cur.close()
    conn.close()


with DAG(
    dag_id="ran_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["towerhealth", "ran"],
) as dag:

    silver = BashOperator(
        task_id="silver",
        bash_command=f"{VENV} && cd /opt/ml && spark-submit --packages {PACKAGES} ran_telemetry_silver.py",
    )
    gold = BashOperator(
        task_id="gold",
        bash_command=f"{VENV} && cd /opt/ml && spark-submit --packages {PACKAGES} ran_telemetry_gold.py",
    )
    ml_prep = BashOperator(
        task_id="ml_prep",
        bash_command=f"{VENV} && cd /opt/ml && spark-submit --packages {PACKAGES} ran_cell_ml_prep.py",
    )
    resolve = PythonOperator(
        task_id="resolve_partition",
        python_callable=resolve_partition,
    )
    predict = BashOperator(
        task_id="predict",
        bash_command=(
            f"{VENV} && cd /opt/ml && python predict_s3.py "
            "--input  {{ ti.xcom_pull(task_ids='resolve_partition')['input_path'] }} "
            "--output {{ ti.xcom_pull(task_ids='resolve_partition')['output_path'] }} "
            "--model  /opt/ml/ran_cell_model.txt "
            "--meta   /opt/ml/ran_cell_model_features.json"
        ),
    )
    refresh_snowflake = PythonOperator(
        task_id="refresh_snowflake",
        python_callable=_refresh_snowflake,
    )

    silver >> [gold, ml_prep]
    ml_prep >> resolve >> predict
    [gold, predict] >> refresh_snowflake
